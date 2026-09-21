//! Pluggy (Open Finance Brasil) adapter.
//!
//! Read-only against Pluggy; writes to Wealthfolio only through the activity
//! service, only for accounts the user explicitly linked, and only with a
//! deterministic idempotency key. Link state lives in `<data_root>/pluggy_state.json`
//! so existing accounts/activities are never modified to make the integration work.

use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{info, warn};
use wealthfolio_core::{
    accounts::{Account, AccountServiceTrait, TrackingMode},
    activities::{ActivityBulkMutationRequest, NewActivity},
};

use crate::main_lib::AppState;

const PLUGGY_API: &str = "https://api.pluggy.ai";
pub const SOURCE_SYSTEM: &str = "PLUGGY";
const SYNC_INTERVAL_SECS: u64 = 6 * 60 * 60;

static SYNC_LOCK: Mutex<()> = Mutex::const_new(());

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct PluggyConfig {
    client_id: String,
    client_secret: String,
    item_ids: Vec<String>,
}

impl PluggyConfig {
    /// `None` when credentials are not configured (integration stays dormant).
    pub fn from_env() -> Option<Self> {
        let get = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
        let item_ids: Vec<String> = get("PLUGGY_ITEM_IDS")?
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Some(Self {
            client_id: get("PLUGGY_CLIENT_ID")?,
            client_secret: get("PLUGGY_CLIENT_SECRET")?,
            item_ids,
        })
    }
}

// ---------------------------------------------------------------------------
// Pluggy API models (only fields we use)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page<T> {
    results: Vec<T>,
    #[serde(default)]
    total_pages: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluggyAccount {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub subtype: Option<String>,
    pub name: Option<String>,
    pub marketing_name: Option<String>,
    pub number: Option<String>,
    pub balance: Option<f64>,
    pub currency_code: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluggyTransaction {
    pub id: String,
    pub description: Option<String>,
    pub amount: f64,
    pub date: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub status: Option<String>,
    pub category: Option<String>,
    pub currency_code: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PluggyInvestment {
    id: String,
    #[serde(rename = "type")]
    kind: Option<String>,
    subtype: Option<String>,
    name: Option<String>,
    code: Option<String>,
    balance: Option<f64>,
    quantity: Option<f64>,
    currency_code: Option<String>,
}

// ---------------------------------------------------------------------------
// Persisted state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LinkStatus {
    NeedsReview,
    Linked,
    Ignored,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountState {
    pub id: String,
    pub item_id: String,
    pub institution: Option<String>,
    pub name: String,
    pub kind: String,
    pub subtype: Option<String>,
    pub number: Option<String>,
    pub currency: String,
    pub balance: Option<f64>,
    pub status: LinkStatus,
    pub linked_account_id: Option<String>,
    /// Only transactions dated on/after this day are imported (no silent backfill).
    pub since: Option<String>,
    /// Existing Wealthfolio account ids that look like a match (never auto-applied).
    pub candidates: Vec<String>,
    pub last_synced_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvestmentState {
    pub id: String,
    pub item_id: String,
    pub kind: Option<String>,
    pub subtype: Option<String>,
    pub name: Option<String>,
    pub code: Option<String>,
    pub balance: Option<f64>,
    pub quantity: Option<f64>,
    pub currency: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSummary {
    pub at: String,
    pub ok: bool,
    pub accounts_seen: usize,
    pub needs_review: usize,
    pub transactions_seen: usize,
    pub activities_created: usize,
    pub activities_skipped_existing: usize,
    pub error: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluggyState {
    pub accounts: BTreeMap<String, AccountState>,
    pub investments: Vec<InvestmentState>,
    pub last_run: Option<RunSummary>,
}

fn state_path(data_root: &str) -> PathBuf {
    PathBuf::from(data_root).join("pluggy_state.json")
}

pub fn load_state(data_root: &str) -> PluggyState {
    std::fs::read(state_path(data_root))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

fn save_state(data_root: &str, state: &PluggyState) -> Result<()> {
    let path = state_path(data_root);
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(state)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Pure mappings (unit-tested)
// ---------------------------------------------------------------------------

pub fn idempotency_key(pluggy_account_id: &str, tx_id: &str) -> String {
    format!("pluggy:{pluggy_account_id}:{tx_id}")
}

fn norm(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

/// Existing accounts that plausibly correspond to a Pluggy account. Exact
/// normalized-name match only; the result is a suggestion for review.
pub fn match_candidates(
    p: &PluggyAccount,
    institution: Option<&str>,
    existing: &[Account],
) -> Vec<String> {
    let mut names: Vec<String> = [p.name.as_deref(), p.marketing_name.as_deref()]
        .into_iter()
        .flatten()
        .map(norm)
        .filter(|n| !n.is_empty())
        .collect();
    if let (Some(inst), Some(n)) = (institution, p.name.as_deref()) {
        names.push(norm(&format!("{inst}{n}")));
    }
    existing
        .iter()
        .filter(|a| !a.is_archived && names.contains(&norm(&a.name)))
        .map(|a| a.id.clone())
        .collect()
}

/// Maps a Pluggy bank transaction to a Wealthfolio cash activity.
/// Returns `None` for transactions that must not be imported.
pub fn map_transaction(
    pluggy_account_id: &str,
    wf_account_id: &str,
    account_currency: &str,
    since: Option<&str>,
    tx: &PluggyTransaction,
) -> Option<NewActivity> {
    if tx.status.as_deref().is_some_and(|s| s != "POSTED") {
        return None; // pending transactions can change/disappear
    }
    if since.is_some_and(|s| tx.date.get(..10).unwrap_or("") < s) {
        return None;
    }
    let activity_type = match tx.kind.as_str() {
        "CREDIT" => "DEPOSIT",
        "DEBIT" => "WITHDRAWAL",
        _ => return None,
    };
    let amount = Decimal::from_f64_retain(tx.amount.abs())?.round_dp(2);
    if amount.is_zero() {
        return None;
    }
    let meta = serde_json::json!({
        "pluggyAccountId": pluggy_account_id,
        "category": tx.category,
    });
    Some(NewActivity {
        id: None,
        account_id: wf_account_id.to_string(),
        asset: None,
        activity_type: activity_type.to_string(),
        subtype: None,
        activity_date: tx.date.clone(),
        quantity: None,
        unit_price: None,
        currency: tx
            .currency_code
            .clone()
            .unwrap_or_else(|| account_currency.to_string()),
        fee: None,
        tax: None,
        amount: Some(amount),
        status: None,
        notes: tx.description.clone(),
        fx_rate: None,
        metadata: Some(meta.to_string()),
        needs_review: Some(false),
        source_system: Some(SOURCE_SYSTEM.to_string()),
        source_record_id: Some(tx.id.clone()),
        source_group_id: Some(pluggy_account_id.to_string()),
        idempotency_key: Some(idempotency_key(pluggy_account_id, &tx.id)),
        import_run_id: None,
    })
}

/// Drops activities whose idempotency key already exists in Wealthfolio or
/// repeats within the batch.
pub fn drop_existing(
    activities: Vec<NewActivity>,
    existing_keys: &HashSet<String>,
) -> (Vec<NewActivity>, usize) {
    let mut seen = HashSet::new();
    let mut skipped = 0;
    let fresh = activities
        .into_iter()
        .filter(|a| {
            let k = a.idempotency_key.clone().unwrap_or_default();
            let dup = existing_keys.contains(&k) || !seen.insert(k);
            if dup {
                skipped += 1;
            }
            !dup
        })
        .collect();
    (fresh, skipped)
}

// ---------------------------------------------------------------------------
// Pluggy HTTP client
// ---------------------------------------------------------------------------

struct Client {
    http: reqwest::Client,
    api_key: String,
}

impl Client {
    async fn connect(cfg: &PluggyConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        let resp = http
            .post(format!("{PLUGGY_API}/auth"))
            .json(&serde_json::json!({
                "clientId": cfg.client_id,
                "clientSecret": cfg.client_secret,
            }))
            .send()
            .await
            .context("pluggy auth request failed")?;
        if !resp.status().is_success() {
            bail!("pluggy auth rejected (HTTP {})", resp.status());
        }
        let api_key = resp.json::<serde_json::Value>().await?["apiKey"]
            .as_str()
            .ok_or_else(|| anyhow!("pluggy auth response missing apiKey"))?
            .to_string();
        Ok(Self { http, api_key })
    }

    async fn get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        let resp = self
            .http
            .get(format!("{PLUGGY_API}{path}"))
            .header("X-API-KEY", &self.api_key)
            .query(query)
            .send()
            .await
            .with_context(|| format!("pluggy GET {path} failed"))?;
        if !resp.status().is_success() {
            bail!("pluggy GET {path} -> HTTP {}", resp.status());
        }
        Ok(resp.json().await?)
    }

    async fn paged<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        base: &[(&str, String)],
    ) -> Result<Vec<T>> {
        let mut out = Vec::new();
        let mut page = 1u32;
        loop {
            let mut q = base.to_vec();
            q.push(("page", page.to_string()));
            q.push(("pageSize", "500".into()));
            let p: Page<T> = self.get(path, &q).await?;
            out.extend(p.results);
            if page >= p.total_pages.max(1) {
                return Ok(out);
            }
            page += 1;
        }
    }

    async fn institution(&self, item_id: &str) -> Option<String> {
        let v: serde_json::Value = self.get(&format!("/items/{item_id}"), &[]).await.ok()?;
        v["connector"]["name"].as_str().map(str::to_string)
    }
}

// ---------------------------------------------------------------------------
// Sync
// ---------------------------------------------------------------------------

pub async fn run_sync(state: &Arc<AppState>) -> Result<RunSummary> {
    let _guard = SYNC_LOCK
        .try_lock()
        .map_err(|_| anyhow!("a Pluggy sync is already running"))?;
    let cfg = PluggyConfig::from_env().ok_or_else(|| {
        anyhow!(
            "Pluggy not configured: set PLUGGY_CLIENT_ID, PLUGGY_CLIENT_SECRET, PLUGGY_ITEM_IDS"
        )
    })?;

    let mut st = load_state(&state.data_root);
    let mut summary = RunSummary {
        at: Utc::now().to_rfc3339(),
        ..Default::default()
    };
    let result = sync_inner(state, &cfg, &mut st, &mut summary).await;
    summary.ok = result.is_ok();
    summary.error = result.as_ref().err().map(|e| e.to_string());
    st.last_run = Some(summary.clone());
    save_state(&state.data_root, &st)?;
    result.map(|_| summary)
}

async fn sync_inner(
    state: &Arc<AppState>,
    cfg: &PluggyConfig,
    st: &mut PluggyState,
    summary: &mut RunSummary,
) -> Result<()> {
    let client = Client::connect(cfg).await?;
    let existing = state.account_service.get_all_accounts()?;

    // 1. Discover accounts + investments (read-only).
    let mut investments = Vec::new();
    for item_id in &cfg.item_ids {
        let institution = client.institution(item_id).await;
        let accounts: Vec<PluggyAccount> = client
            .paged("/accounts", &[("itemId", item_id.clone())])
            .await?;
        for p in accounts {
            let candidates = match_candidates(&p, institution.as_deref(), &existing);
            let entry = st
                .accounts
                .entry(p.id.clone())
                .or_insert_with(|| AccountState {
                    id: p.id.clone(),
                    item_id: item_id.clone(),
                    institution: None,
                    name: String::new(),
                    kind: String::new(),
                    subtype: None,
                    number: None,
                    currency: "BRL".into(),
                    balance: None,
                    status: LinkStatus::NeedsReview,
                    linked_account_id: None,
                    since: None,
                    candidates: vec![],
                    last_synced_at: None,
                    last_error: None,
                });
            entry.institution = institution.clone();
            entry.name = p
                .marketing_name
                .clone()
                .or(p.name.clone())
                .unwrap_or_else(|| p.id.clone());
            entry.kind = p.kind.clone();
            entry.subtype = p.subtype.clone();
            entry.number = p.number.clone();
            entry.currency = p.currency_code.clone().unwrap_or_else(|| "BRL".into());
            entry.balance = p.balance;
            entry.candidates = candidates;
        }
        match client
            .paged::<PluggyInvestment>("/investments", &[("itemId", item_id.clone())])
            .await
        {
            Ok(list) => investments.extend(list.into_iter().map(|i| InvestmentState {
                id: i.id,
                item_id: item_id.clone(),
                kind: i.kind,
                subtype: i.subtype,
                name: i.name,
                code: i.code,
                balance: i.balance,
                quantity: i.quantity,
                currency: i.currency_code,
            })),
            Err(e) => warn!("Pluggy investments unavailable for an item: {e}"),
        }
    }
    st.investments = investments;
    summary.accounts_seen = st.accounts.len();
    summary.needs_review = st
        .accounts
        .values()
        .filter(|a| a.status == LinkStatus::NeedsReview)
        .count();

    // 2. Transactions for explicitly linked bank accounts only.
    for acc in st
        .accounts
        .values_mut()
        .filter(|a| a.status == LinkStatus::Linked)
    {
        acc.last_error = None;
        if acc.kind != "BANK" {
            acc.last_error =
                Some("only BANK accounts are synced (credit cards not implemented)".into());
            continue;
        }
        let Some(wf_id) = acc.linked_account_id.clone() else {
            continue;
        };
        let wf = match state.account_service.get_account(&wf_id) {
            Ok(a) => a,
            Err(e) => {
                acc.last_error = Some(format!("linked account unavailable: {e}"));
                continue;
            }
        };
        if wf.tracking_mode != TrackingMode::Transactions {
            acc.last_error =
                Some("linked account is not in TRANSACTIONS tracking mode; skipped".into());
            continue;
        }
        let mut q = vec![("accountId", acc.id.clone())];
        if let Some(since) = &acc.since {
            q.push(("from", since.clone()));
        }
        let txs: Vec<PluggyTransaction> = match client.paged("/transactions", &q).await {
            Ok(t) => t,
            Err(e) => {
                acc.last_error = Some(e.to_string());
                continue;
            }
        };
        summary.transactions_seen += txs.len();
        let mapped: Vec<NewActivity> = txs
            .iter()
            .filter_map(|t| map_transaction(&acc.id, &wf_id, &wf.currency, acc.since.as_deref(), t))
            .collect();
        let keys: Vec<String> = mapped
            .iter()
            .filter_map(|a| a.idempotency_key.clone())
            .collect();
        let existing_keys: HashSet<String> = state
            .activity_service
            .check_existing_duplicates(keys)?
            .into_keys()
            .collect();
        let (fresh, skipped) = drop_existing(mapped, &existing_keys);
        summary.activities_skipped_existing += skipped;
        if !fresh.is_empty() {
            let n = fresh.len();
            state
                .activity_service
                .bulk_mutate_activities(ActivityBulkMutationRequest {
                    creates: fresh,
                    ..Default::default()
                })
                .await?;
            summary.activities_created += n;
        }
        acc.last_synced_at = Some(Utc::now().to_rfc3339());
    }
    info!(
        "Pluggy sync: {} accounts, {} activities created, {} already present",
        summary.accounts_seen, summary.activities_created, summary.activities_skipped_existing
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Review workflow
// ---------------------------------------------------------------------------

/// Explicit user decision: link a Pluggy account to an existing Wealthfolio
/// account (or ignore it). Imports start at `since` (default: today).
pub fn apply_link(
    state: &Arc<AppState>,
    pluggy_account_id: &str,
    account_id: Option<&str>,
    since: Option<&str>,
    ignore: bool,
) -> Result<AccountState> {
    let mut st = load_state(&state.data_root);
    if let Some(wf_id) = account_id {
        state.account_service.get_account(wf_id)?; // must exist
        if st
            .accounts
            .values()
            .any(|a| a.id != pluggy_account_id && a.linked_account_id.as_deref() == Some(wf_id))
        {
            bail!("Wealthfolio account already linked to another Pluggy account");
        }
    }
    let acc = st
        .accounts
        .get_mut(pluggy_account_id)
        .ok_or_else(|| anyhow!("unknown Pluggy account (run a sync first)"))?;
    if ignore {
        acc.status = LinkStatus::Ignored;
        acc.linked_account_id = None;
    } else {
        let wf_id = account_id.ok_or_else(|| anyhow!("accountId required"))?;
        acc.status = LinkStatus::Linked;
        acc.linked_account_id = Some(wf_id.to_string());
        acc.since = Some(
            since
                .map(str::to_string)
                .unwrap_or_else(|| Utc::now().format("%Y-%m-%d").to_string()),
        );
    }
    let out = acc.clone();
    save_state(&state.data_root, &st)?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

pub fn start_scheduler(state: Arc<AppState>) {
    if PluggyConfig::from_env().is_none() {
        info!("Pluggy sync disabled: credentials not configured");
        return;
    }
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(90)).await;
        let mut tick = tokio::time::interval(Duration::from_secs(SYNC_INTERVAL_SECS));
        loop {
            tick.tick().await;
            if let Err(e) = run_sync(&state).await {
                warn!("Scheduled Pluggy sync failed: {e}");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(
        id: &str,
        kind: &str,
        amount: f64,
        date: &str,
        status: Option<&str>,
    ) -> PluggyTransaction {
        PluggyTransaction {
            id: id.into(),
            description: Some("PIX".into()),
            amount,
            date: date.into(),
            kind: kind.into(),
            status: status.map(Into::into),
            category: None,
            currency_code: Some("BRL".into()),
        }
    }

    #[test]
    fn maps_credit_and_debit_with_stable_key() {
        let a = map_transaction(
            "pa",
            "wf",
            "BRL",
            None,
            &tx(
                "t1",
                "CREDIT",
                10.5,
                "2026-09-01T00:00:00.000Z",
                Some("POSTED"),
            ),
        )
        .unwrap();
        assert_eq!(a.activity_type, "DEPOSIT");
        assert_eq!(a.amount, Some(Decimal::new(1050, 2)));
        assert_eq!(a.idempotency_key.as_deref(), Some("pluggy:pa:t1"));
        let b = map_transaction(
            "pa",
            "wf",
            "BRL",
            None,
            &tx("t2", "DEBIT", -3.0, "2026-09-01T00:00:00.000Z", None),
        )
        .unwrap();
        assert_eq!(b.activity_type, "WITHDRAWAL");
        assert_eq!(b.amount, Some(Decimal::new(300, 2)));
    }

    #[test]
    fn skips_pending_old_zero_and_unknown() {
        let d = "2026-09-01T00:00:00.000Z";
        assert!(map_transaction(
            "pa",
            "wf",
            "BRL",
            None,
            &tx("t", "CREDIT", 1.0, d, Some("PENDING"))
        )
        .is_none());
        assert!(map_transaction(
            "pa",
            "wf",
            "BRL",
            Some("2026-09-02"),
            &tx("t", "CREDIT", 1.0, d, None)
        )
        .is_none());
        assert!(
            map_transaction("pa", "wf", "BRL", None, &tx("t", "CREDIT", 0.0, d, None)).is_none()
        );
        assert!(
            map_transaction("pa", "wf", "BRL", None, &tx("t", "OTHER", 1.0, d, None)).is_none()
        );
    }

    #[test]
    fn second_run_creates_nothing() {
        let d = "2026-09-01T00:00:00.000Z";
        let batch = || {
            vec![
                map_transaction("pa", "wf", "BRL", None, &tx("t1", "CREDIT", 1.0, d, None))
                    .unwrap(),
                map_transaction("pa", "wf", "BRL", None, &tx("t1", "CREDIT", 1.0, d, None))
                    .unwrap(),
                map_transaction("pa", "wf", "BRL", None, &tx("t2", "DEBIT", 2.0, d, None)).unwrap(),
            ]
        };
        let (fresh, skipped) = drop_existing(batch(), &HashSet::new());
        assert_eq!((fresh.len(), skipped), (2, 1)); // in-batch duplicate dropped
        let existing: HashSet<String> = fresh
            .iter()
            .filter_map(|a| a.idempotency_key.clone())
            .collect();
        let (again, skipped) = drop_existing(batch(), &existing);
        assert!(again.is_empty());
        assert_eq!(skipped, 3);
    }

    #[test]
    fn normalizes_names_for_candidates() {
        assert_eq!(norm("Nu Pagamentos - Conta"), norm("nu pagamentos conta"));
    }
}
