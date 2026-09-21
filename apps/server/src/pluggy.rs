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
    portfolio::snapshot::{
        CashBalanceInput, ManualHoldingInput, ManualSnapshotRequest, ManualSnapshotService,
        SnapshotSource,
    },
    utils::time_utils::{parse_user_timezone_or_default, user_today},
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
    pub credit_data: Option<PluggyCreditData>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluggyCreditData {
    pub credit_limit: Option<f64>,
    pub available_credit_limit: Option<f64>,
    pub balance_due_date: Option<String>,
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
    pub credit_card_metadata: Option<PluggyCardMeta>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluggyCardMeta {
    pub bill_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PluggyBill {
    id: String,
    due_date: Option<String>,
    bill_closing_date: Option<String>,
    total_amount: Option<f64>,
    minimum_payment_amount: Option<f64>,
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
    status: Option<String>,
    amount_original: Option<f64>,
    due_date: Option<String>,
    issuer: Option<String>,
    /// Gross current value; `balance` is net of IR/IOF (`amount - taxes - taxes2`).
    amount: Option<f64>,
    taxes: Option<f64>,
    taxes2: Option<f64>,
    rate: Option<f64>,
    rate_type: Option<String>,
    fixed_annual_rate: Option<f64>,
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
    /// Read-only comparison of the Pluggy balance with Wealthfolio's cash; never auto-corrected.
    #[serde(default)]
    pub balance_check: Option<BalanceCheck>,
    /// Credit-card details, recorded for review only (cards are not imported yet).
    #[serde(default)]
    pub credit_limit: Option<f64>,
    #[serde(default)]
    pub available_credit: Option<f64>,
    #[serde(default)]
    pub due_date: Option<String>,
    /// Latest card invoices (most recent first), recorded for review.
    #[serde(default)]
    pub bills: Vec<BillState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BillState {
    pub id: String,
    pub due_date: Option<String>,
    pub closing_date: Option<String>,
    pub total_amount: Option<f64>,
    pub minimum_payment: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BalanceCheck {
    pub pluggy: Option<f64>,
    pub wealthfolio: Option<f64>,
    pub diff: Option<f64>,
    /// OK | DRIFT | UNKNOWN
    pub status: String,
    pub checked_at: String,
}

/// Investments belong to a Pluggy item (institution), not to a bank account, so
/// they are linked per item to a HOLDINGS-mode Wealthfolio account.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemLink {
    pub item_id: String,
    pub institution: Option<String>,
    pub status: LinkStatus,
    pub linked_account_id: Option<String>,
    pub positions_written: usize,
    /// Existing Wealthfolio account ids that look like this institution (never auto-applied).
    #[serde(default)]
    pub candidates: Vec<String>,
    pub total_value: Option<f64>,
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
    /// ACTIVE | TOTAL_WITHDRAWAL | PENDING; only ACTIVE positions are written.
    #[serde(default)]
    pub status: Option<String>,
    /// Original invested amount (cost); recorded for review, not yet used as cost basis.
    #[serde(default)]
    pub amount_original: Option<f64>,
    #[serde(default)]
    pub due_date: Option<String>,
    #[serde(default)]
    pub issuer: Option<String>,
    /// Gross current value (before IR/IOF). `balance` is the net value used as market value.
    #[serde(default)]
    pub gross_amount: Option<f64>,
    /// IR + IOF withheld, so `gross_amount - taxes == balance`.
    #[serde(default)]
    pub taxes: Option<f64>,
    /// Contracted rate (e.g. 120 = 120% of CDI when `rate_type` is CDI).
    #[serde(default)]
    pub rate: Option<f64>,
    #[serde(default)]
    pub rate_type: Option<String>,
    #[serde(default)]
    pub fixed_annual_rate: Option<f64>,
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
    #[serde(default)]
    pub investment_positions_written: usize,
    pub error: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluggyState {
    pub accounts: BTreeMap<String, AccountState>,
    pub investments: Vec<InvestmentState>,
    #[serde(default)]
    pub items: BTreeMap<String, ItemLink>,
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

fn tokens(s: &str) -> HashSet<String> {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.chars().flat_map(|c| c.to_lowercase()).collect())
        .collect()
}

/// Existing accounts that plausibly correspond to Pluggy names (institution and
/// account names). A match is either an equal normalized name, or every token of
/// the Wealthfolio account name appearing among the Pluggy tokens ("BTG" in
/// "BTG Investimentos"). Suggestions only; nothing is applied automatically.
pub fn match_names(pluggy_names: &[&str], existing: &[Account]) -> Vec<String> {
    let exact: Vec<String> = pluggy_names.iter().map(|n| norm(n)).collect();
    let pool: HashSet<String> = pluggy_names.iter().flat_map(|n| tokens(n)).collect();
    existing
        .iter()
        .filter(|a| !a.is_archived)
        .filter(|a| {
            let t = tokens(&a.name);
            exact.contains(&norm(&a.name)) || (!t.is_empty() && t.is_subset(&pool))
        })
        .map(|a| a.id.clone())
        .collect()
}

pub fn match_candidates(
    p: &PluggyAccount,
    institution: Option<&str>,
    existing: &[Account],
) -> Vec<String> {
    let mut names: Vec<&str> = [p.name.as_deref(), p.marketing_name.as_deref(), institution]
        .into_iter()
        .flatten()
        .collect();
    names.dedup();
    match_names(&names, existing)
}

/// MeuPluggy proxies every bank through one connector, so the connector name says
/// nothing about the institution; label the item after its first account instead.
pub fn derive_institution(connector: Option<&str>, accounts: &[PluggyAccount]) -> Option<String> {
    let name_of = |a: &PluggyAccount| a.marketing_name.clone().or_else(|| a.name.clone());
    match connector {
        Some(c) if c.eq_ignore_ascii_case("MeuPluggy") => accounts
            .iter()
            .find(|a| a.kind == "BANK")
            .or_else(|| accounts.first())
            .and_then(name_of)
            .map(|n| format!("MeuPluggy · {n}")),
        other => other.map(str::to_string),
    }
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
    let activity_type = match tx.kind.as_str() {
        "CREDIT" => "DEPOSIT",
        "DEBIT" => "WITHDRAWAL",
        _ => return None,
    };
    cash_activity(
        pluggy_account_id,
        wf_account_id,
        account_currency,
        since,
        tx,
        activity_type,
    )
}

/// Maps a credit-card transaction using the semantics observed on real MeuPluggy
/// data: a charge is `DEBIT` with a positive amount, a payment/refund is `CREDIT`
/// with a negative amount. Anything inconsistent is skipped rather than guessed.
/// On a liability account a charge lowers cash (more debt) and a payment raises it.
pub fn map_card_transaction(
    pluggy_account_id: &str,
    wf_account_id: &str,
    account_currency: &str,
    since: Option<&str>,
    tx: &PluggyTransaction,
) -> Option<NewActivity> {
    let activity_type = match (tx.kind.as_str(), tx.amount) {
        ("DEBIT", a) if a > 0.0 => "WITHDRAWAL",
        ("CREDIT", a) if a < 0.0 => "DEPOSIT",
        _ => return None,
    };
    cash_activity(
        pluggy_account_id,
        wf_account_id,
        account_currency,
        since,
        tx,
        activity_type,
    )
}

fn cash_activity(
    pluggy_account_id: &str,
    wf_account_id: &str,
    account_currency: &str,
    since: Option<&str>,
    tx: &PluggyTransaction,
    activity_type: &str,
) -> Option<NewActivity> {
    if tx.status.as_deref().is_some_and(|s| s != "POSTED") {
        return None; // pending transactions can change/disappear
    }
    if since.is_some_and(|s| tx.date.get(..10).unwrap_or("") < s) {
        return None;
    }
    let amount = Decimal::from_f64_retain(tx.amount.abs())?.round_dp(2);
    if amount.is_zero() {
        return None;
    }
    let meta = serde_json::json!({
        "pluggyAccountId": pluggy_account_id,
        "category": tx.category,
        "billId": tx.credit_card_metadata.as_ref().and_then(|m| m.bill_id.clone()),
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

/// Stable UUID per Pluggy investment so repeated syncs reuse the same asset.
pub fn asset_id_for_investment(investment_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(format!("pluggy:investment:{investment_id}"));
    let mut b = [0u8; 16];
    b.copy_from_slice(&hash[..16]);
    b[6] = (b[6] & 0x0f) | 0x50; // version 5-style
    b[8] = (b[8] & 0x3f) | 0x80; // RFC 4122 variant
    uuid::Uuid::from_bytes(b).to_string()
}

/// Which Pluggy value becomes a position's market value. Pluggy's `amount` is
/// gross; `balance` is net of IR/IOF. Existing manual balances track gross
/// (BTG/BV within 0.2% of gross vs up to 1.3% of net), so gross is the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueBasis {
    Gross,
    Net,
}

impl ValueBasis {
    pub fn from_env() -> Self {
        match std::env::var("PLUGGY_VALUE_BASIS")
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("net") => Self::Net,
            _ => Self::Gross,
        }
    }
}

/// Maps a Pluggy investment to a manual-priced custom position. The symbol is
/// namespaced (`PLUGGY-<id>`) so it can never collide with a real ticker. Market value
/// (per `basis`) and cost basis (Pluggy's original amount) are kept separate.
pub fn map_investment(i: &InvestmentState, basis: ValueBasis) -> Option<ManualHoldingInput> {
    if i.status.as_deref().is_some_and(|s| s != "ACTIVE") {
        return None; // redeemed (TOTAL_WITHDRAWAL) or pending positions are not holdings
    }
    // Net = Pluggy's `balance`; gross = `amount` (falls back to `balance` when absent).
    let value = match basis {
        ValueBasis::Gross => i.gross_amount.filter(|g| *g > 0.0).or(i.balance),
        ValueBasis::Net => i.balance,
    };
    let balance = Decimal::from_f64_retain(value?)?.round_dp(2);
    if balance <= Decimal::ZERO {
        return None;
    }
    let quantity = i
        .quantity
        .and_then(Decimal::from_f64_retain)
        .filter(|q| *q > Decimal::ZERO)
        .unwrap_or(Decimal::ONE);
    let unit_price = (balance / quantity).round_dp(10);
    // Cost basis is the original invested amount when Pluggy provides it.
    let average_cost = i
        .amount_original
        .and_then(Decimal::from_f64_retain)
        .filter(|c| *c > Decimal::ZERO)
        .map(|c| (c / quantity).round_dp(10))
        .unwrap_or(unit_price);
    let currency = i.currency.clone().unwrap_or_else(|| "BRL".into());
    let short: String =
        i.id.chars()
            .filter(|c| c.is_alphanumeric())
            .take(8)
            .collect();
    let name = match (&i.name, &i.code) {
        (Some(n), Some(c)) => format!("{n} ({c})"),
        (Some(n), None) => n.clone(),
        (None, Some(c)) => c.clone(),
        _ => format!("Pluggy investment {short}"),
    };
    Some(ManualHoldingInput {
        asset_id: Some(asset_id_for_investment(&i.id)),
        symbol: format!("PLUGGY-{}", short.to_uppercase()),
        exchange_mic: None,
        quantity,
        currency: currency.clone(),
        average_cost,
        unit_price: Some(unit_price),
        name: Some(name),
        data_source: Some("MANUAL".into()),
        asset_kind: Some("INVESTMENT".into()),
        quote_ccy: Some(currency),
        instrument_type: None,
        provider_id: None,
        provider_symbol: None,
    })
}

pub fn reconcile_balance(pluggy: Option<f64>, wealthfolio: Option<f64>) -> BalanceCheck {
    let diff = pluggy
        .zip(wealthfolio)
        .map(|(p, w)| ((p - w) * 100.0).round() / 100.0);
    let status = match diff {
        None => "UNKNOWN",
        Some(d) if d.abs() <= 0.01 => "OK",
        Some(_) => "DRIFT",
    };
    BalanceCheck {
        pluggy,
        wealthfolio,
        diff,
        status: status.into(),
        checked_at: Utc::now().to_rfc3339(),
    }
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
        let connector = client.institution(item_id).await;
        let accounts: Vec<PluggyAccount> = client
            .paged("/accounts", &[("itemId", item_id.clone())])
            .await?;
        let institution = derive_institution(connector.as_deref(), &accounts);
        let link = st.items.entry(item_id.clone()).or_insert_with(|| ItemLink {
            item_id: item_id.clone(),
            institution: None,
            status: LinkStatus::NeedsReview,
            linked_account_id: None,
            positions_written: 0,
            candidates: vec![],
            total_value: None,
            last_synced_at: None,
            last_error: None,
        });
        link.institution = institution.clone();
        let mut names: Vec<&str> = institution.iter().map(String::as_str).collect();
        names.extend(
            accounts
                .iter()
                .filter_map(|a| a.marketing_name.as_deref().or(a.name.as_deref())),
        );
        link.candidates = match_names(&names, &existing);
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
                    balance_check: None,
                    credit_limit: None,
                    available_credit: None,
                    due_date: None,
                    bills: vec![],
                });
            entry.institution = institution.clone();
            entry.credit_limit = p.credit_data.as_ref().and_then(|c| c.credit_limit);
            entry.available_credit = p
                .credit_data
                .as_ref()
                .and_then(|c| c.available_credit_limit);
            entry.due_date = p
                .credit_data
                .as_ref()
                .and_then(|c| c.balance_due_date.clone())
                .map(|d| d.chars().take(10).collect());
            if p.kind == "CREDIT" {
                if let Ok(mut bills) = client
                    .paged::<PluggyBill>("/bills", &[("accountId", p.id.clone())])
                    .await
                {
                    bills.sort_by(|x, y| y.due_date.cmp(&x.due_date));
                    entry.bills = bills
                        .into_iter()
                        .take(12)
                        .map(|b| BillState {
                            id: b.id,
                            due_date: b.due_date.map(|d| d.chars().take(10).collect()),
                            closing_date: b.bill_closing_date.map(|d| d.chars().take(10).collect()),
                            total_amount: b.total_amount,
                            minimum_payment: b.minimum_payment_amount,
                        })
                        .collect();
                }
            }
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
                gross_amount: i.amount,
                taxes: match (i.taxes, i.taxes2) {
                    (None, None) => None,
                    (a, b) => Some(a.unwrap_or(0.0) + b.unwrap_or(0.0)),
                },
                rate: i.rate,
                rate_type: i.rate_type,
                fixed_annual_rate: i.fixed_annual_rate,
                status: i.status,
                amount_original: i.amount_original,
                due_date: i.due_date.map(|d| d.chars().take(10).collect()),
                issuer: i.issuer,
            })),
            Err(e) => {
                warn!("Pluggy investments unavailable for an item: {e}");
                if let Some(l) = st.items.get_mut(item_id) {
                    l.last_error = Some(format!("investments fetch failed: {e}"));
                }
            }
        }
    }
    st.investments = investments;
    summary.accounts_seen = st.accounts.len();
    summary.needs_review = st
        .accounts
        .values()
        .filter(|a| a.status == LinkStatus::NeedsReview)
        .count();

    let timezone = state.timezone.read().unwrap().clone();
    let today = user_today(parse_user_timezone_or_default(&timezone));
    let base_currency = state.base_currency.read().unwrap().clone();
    let basis = ValueBasis::from_env();

    // 2. Linked accounts: bank transactions, or card balance / transactions.
    for acc in st
        .accounts
        .values_mut()
        .filter(|a| a.status == LinkStatus::Linked)
    {
        acc.last_error = None;
        let is_card = acc.kind == "CREDIT";
        if acc.kind != "BANK" && !is_card {
            acc.last_error = Some(format!("unsupported account kind {}", acc.kind));
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
        if is_card != (wf.account_type == "CREDIT_CARD") {
            acc.last_error = Some(
                "credit cards link to CREDIT_CARD accounts and bank accounts to non-card accounts"
                    .into(),
            );
            continue;
        }
        match wf.tracking_mode {
            TrackingMode::Holdings if is_card => {
                // Balance-only: a card's debt is negative cash on a liability account.
                let Some(owed) = acc.balance else {
                    acc.last_error = Some("Pluggy returned no card balance; skipped".into());
                    continue;
                };
                let cash = vec![CashBalanceInput {
                    currency: acc.currency.clone(),
                    amount: Decimal::from_f64_retain(-owed)
                        .unwrap_or_default()
                        .round_dp(2),
                }];
                match write_snapshot(state, &wf, vec![], cash, today, &timezone, &base_currency)
                    .await
                {
                    Ok(()) => acc.last_synced_at = Some(Utc::now().to_rfc3339()),
                    Err(e) => acc.last_error = Some(format!("snapshot write failed: {e}")),
                }
                continue;
            }
            TrackingMode::Transactions => {}
            _ => {
                acc.last_error = Some(
                    if is_card {
                        "linked card account must be HOLDINGS (balance) or TRANSACTIONS mode"
                    } else {
                        "bank accounts import transactions only in TRANSACTIONS mode; use the item link for a balance snapshot"
                    }
                    .into(),
                );
                continue;
            }
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
        let mapper = if is_card {
            map_card_transaction
        } else {
            map_transaction
        };
        let mapped: Vec<NewActivity> = txs
            .iter()
            .filter_map(|t| mapper(&acc.id, &wf_id, &wf.currency, acc.since.as_deref(), t))
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
        let wf_cash = state
            .valuation_service
            .get_latest_valuations(std::slice::from_ref(&wf_id))
            .ok()
            .and_then(|v| v.into_iter().next())
            .and_then(|v| v.cash_balance.to_string().parse::<f64>().ok());
        let expected = if is_card {
            acc.balance.map(|b| -b)
        } else {
            acc.balance
        };
        acc.balance_check = Some(reconcile_balance(expected, wf_cash));
    }

    // 3. Linked items -> one snapshot (active positions + the item's bank cash) on a
    //    HOLDINGS-mode account. Snapshots carry no net contribution, so this is
    //    flow-neutral and adds no transaction history.
    for link in st
        .items
        .values_mut()
        .filter(|l| l.status == LinkStatus::Linked)
    {
        let Some(wf_id) = link.linked_account_id.clone() else {
            continue;
        };
        let wf = match state.account_service.get_account(&wf_id) {
            Ok(a) => a,
            Err(e) => {
                link.last_error = Some(format!("linked account unavailable: {e}"));
                continue;
            }
        };
        if wf.tracking_mode != TrackingMode::Holdings {
            link.last_error =
                Some("linked account is not in HOLDINGS tracking mode; skipped".into());
            continue;
        }
        let positions: Vec<ManualHoldingInput> = st
            .investments
            .iter()
            .filter(|i| i.item_id == link.item_id)
            .filter_map(|i| map_investment(i, basis))
            .collect();
        let mut cash: BTreeMap<String, Decimal> = BTreeMap::new();
        for a in st
            .accounts
            .values()
            .filter(|a| a.item_id == link.item_id && a.kind == "BANK")
        {
            if let Some(b) = a.balance {
                *cash.entry(a.currency.clone()).or_default() +=
                    Decimal::from_f64_retain(b).unwrap_or_default().round_dp(2);
            }
        }
        // Never write an empty snapshot: it would zero out the account.
        if positions.is_empty() && cash.values().all(|c| c.is_zero()) {
            link.last_error =
                Some("no Pluggy positions or cash returned; snapshot not written".into());
            continue;
        }
        let total: Decimal = positions
            .iter()
            .map(|p| p.quantity * p.unit_price.unwrap_or(p.average_cost))
            .sum::<Decimal>()
            + cash.values().copied().sum::<Decimal>();
        let n = positions.len();
        let cash_inputs = cash
            .into_iter()
            .map(|(currency, amount)| CashBalanceInput { currency, amount })
            .collect();
        match write_snapshot(
            state,
            &wf,
            positions,
            cash_inputs,
            today,
            &timezone,
            &base_currency,
        )
        .await
        {
            Ok(()) => {
                link.last_error = None;
                link.positions_written = n;
                link.total_value = total.round_dp(2).to_string().parse::<f64>().ok();
                link.last_synced_at = Some(Utc::now().to_rfc3339());
                summary.investment_positions_written += n;
            }
            Err(e) => link.last_error = Some(format!("snapshot write failed: {e}")),
        }
    }
    info!(
        "Pluggy sync: {} accounts, {} activities created, {} already present",
        summary.accounts_seen, summary.activities_created, summary.activities_skipped_existing
    );
    Ok(())
}

async fn write_snapshot(
    state: &Arc<AppState>,
    wf: &Account,
    positions: Vec<ManualHoldingInput>,
    cash_balances: Vec<CashBalanceInput>,
    today: chrono::NaiveDate,
    timezone: &str,
    base_currency: &str,
) -> Result<()> {
    ManualSnapshotService::new(
        state.asset_service.clone(),
        state.fx_service.clone(),
        state.snapshot_service.clone(),
        state.quote_service.clone(),
    )
    .with_timezone(timezone.to_string())
    .save_manual_snapshot(ManualSnapshotRequest {
        account_id: wf.id.clone(),
        account_currency: wf.currency.clone(),
        snapshot_date: today,
        positions,
        cash_balances,
        base_currency: Some(base_currency.to_string()),
        source: SnapshotSource::ManualEntry,
    })
    .await?;
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
        let wf = state.account_service.get_account(wf_id)?; // must exist
        if let Some(p) = st.accounts.get(pluggy_account_id) {
            check_link_compat(&p.kind, &wf.account_type)?;
        }
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

/// Cards link to CREDIT_CARD accounts and bank accounts to any other account type.
pub fn check_link_compat(pluggy_kind: &str, wf_account_type: &str) -> Result<()> {
    match (pluggy_kind == "CREDIT", wf_account_type == "CREDIT_CARD") {
        (true, false) => bail!("a Pluggy credit card must link to a CREDIT_CARD account"),
        (false, true) => bail!("a Pluggy bank account cannot link to a CREDIT_CARD account"),
        _ => Ok(()),
    }
}

/// Explicit user decision for a Pluggy item's investments: link to an existing
/// HOLDINGS-mode Wealthfolio account, or ignore.
pub fn apply_item_link(
    state: &Arc<AppState>,
    item_id: &str,
    account_id: Option<&str>,
    ignore: bool,
) -> Result<ItemLink> {
    let mut st = load_state(&state.data_root);
    if let Some(wf_id) = account_id {
        let wf = state.account_service.get_account(wf_id)?;
        if wf.tracking_mode != TrackingMode::Holdings {
            bail!("account must be in HOLDINGS tracking mode to receive Pluggy investments");
        }
        if st
            .items
            .values()
            .any(|l| l.item_id != item_id && l.linked_account_id.as_deref() == Some(wf_id))
        {
            bail!("Wealthfolio account already linked to another Pluggy item");
        }
    }
    let link = st
        .items
        .get_mut(item_id)
        .ok_or_else(|| anyhow!("unknown Pluggy item (run a sync first)"))?;
    if ignore {
        link.status = LinkStatus::Ignored;
        link.linked_account_id = None;
    } else {
        let wf_id = account_id.ok_or_else(|| anyhow!("accountId required"))?;
        link.status = LinkStatus::Linked;
        link.linked_account_id = Some(wf_id.to_string());
    }
    let out = link.clone();
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
            credit_card_metadata: None,
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

    fn map_net(i: &InvestmentState) -> Option<ManualHoldingInput> {
        map_investment(i, ValueBasis::Net)
    }

    fn inv(id: &str, balance: Option<f64>, qty: Option<f64>) -> InvestmentState {
        InvestmentState {
            id: id.into(),
            item_id: "item".into(),
            kind: Some("ETF".into()),
            subtype: None,
            name: Some("BOVA11".into()),
            code: Some("BOVA11".into()),
            balance,
            quantity: qty,
            currency: Some("BRL".into()),
            status: Some("ACTIVE".into()),
            amount_original: None,
            due_date: None,
            issuer: None,
            gross_amount: None,
            taxes: None,
            rate: None,
            rate_type: None,
            fixed_annual_rate: None,
        }
    }

    #[test]
    fn redeemed_investments_are_not_holdings() {
        for status in ["TOTAL_WITHDRAWAL", "PENDING"] {
            let mut i = inv("x", Some(100.0), Some(1.0));
            i.status = Some(status.into());
            assert!(map_net(&i).is_none());
        }
        let mut legacy = inv("x", Some(100.0), Some(1.0));
        legacy.status = None; // state written before this field existed
        assert!(map_net(&legacy).is_some());
    }

    fn acct(kind: &str, name: &str) -> PluggyAccount {
        PluggyAccount {
            id: "a".into(),
            kind: kind.into(),
            subtype: None,
            name: Some(name.into()),
            marketing_name: None,
            number: None,
            balance: None,
            currency_code: None,
            credit_data: None,
        }
    }

    #[test]
    fn meupluggy_items_are_labelled_by_their_bank_account() {
        let accounts = vec![
            acct("CREDIT", "MASTERCARD LIVRE"),
            acct("BANK", "BANCO BV S.A."),
        ];
        assert_eq!(
            derive_institution(Some("MeuPluggy"), &accounts).as_deref(),
            Some("MeuPluggy · BANCO BV S.A.")
        );
        assert_eq!(
            derive_institution(Some("Pluggy Bank"), &accounts).as_deref(),
            Some("Pluggy Bank")
        );
        assert_eq!(derive_institution(None, &accounts), None);
    }

    #[test]
    fn investment_asset_id_is_deterministic_and_valid_uuid() {
        let a = asset_id_for_investment("abc");
        assert_eq!(a, asset_id_for_investment("abc"));
        assert_ne!(a, asset_id_for_investment("abd"));
        assert!(uuid::Uuid::parse_str(&a).is_ok());
    }

    #[test]
    fn maps_investments_with_namespaced_symbol_and_unit_price() {
        let h = map_net(&inv("9df10577-9b13", Some(1359.39), Some(3.0))).unwrap();
        assert_eq!(h.symbol, "PLUGGY-9DF10577");
        assert_eq!(h.quantity, Decimal::from(3));
        assert_eq!(h.average_cost, Decimal::new(45313, 2)); // 1359.39 / 3
        assert_eq!(h.data_source.as_deref(), Some("MANUAL"));
        assert_eq!(h.asset_id, Some(asset_id_for_investment("9df10577-9b13")));
    }

    #[test]
    fn zero_or_missing_quantity_falls_back_to_one_unit_of_full_value() {
        for q in [None, Some(0.0)] {
            let h = map_net(&inv("x1", Some(118.4), q)).unwrap();
            assert_eq!(h.quantity, Decimal::ONE);
            assert_eq!(h.average_cost, Decimal::new(11840, 2));
        }
    }

    #[test]
    fn skips_investments_without_positive_balance() {
        assert!(map_net(&inv("x", None, Some(1.0))).is_none());
        assert!(map_net(&inv("x", Some(0.0), Some(1.0))).is_none());
        assert!(map_net(&inv("x", Some(-5.0), Some(1.0))).is_none());
    }

    #[test]
    fn balance_check_flags_drift_and_unknown() {
        assert_eq!(reconcile_balance(Some(100.0), Some(100.0)).status, "OK");
        let d = reconcile_balance(Some(28939.6), Some(28839.6));
        assert_eq!((d.status.as_str(), d.diff), ("DRIFT", Some(100.0)));
        assert_eq!(reconcile_balance(Some(1.0), None).status, "UNKNOWN");
    }

    #[test]
    fn normalizes_names_for_candidates() {
        assert_eq!(norm("Nu Pagamentos - Conta"), norm("nu pagamentos conta"));
    }
    #[test]
    fn real_cdb_separates_net_value_from_cost_basis() {
        // Shape observed on a real 120% CDI CDB (rounded): gross 10637.69, IR 143.48,
        // net balance 10494.21, principal 10000, quantity 1,000,000 units.
        let mut i = inv("cdb", Some(10494.21), Some(1_000_000.0));
        i.amount_original = Some(10000.0);
        i.gross_amount = Some(10637.69);
        i.taxes = Some(143.48);
        let h = map_net(&i).unwrap();
        let q = h.quantity;
        assert_eq!(q * h.unit_price.unwrap(), Decimal::new(1049421, 2)); // market value = net
        assert_eq!(q * h.average_cost, Decimal::from(10000)); // cost = principal
                                                              // Gross basis values the same position at the gross amount; cost is unchanged.
        let g = map_investment(&i, ValueBasis::Gross).unwrap();
        assert_eq!(q * g.unit_price.unwrap(), Decimal::new(1063769, 2));
        assert_eq!(q * g.average_cost, Decimal::from(10000));
        // Pluggy's balance is net of taxes: gross - taxes == balance.
        assert!((i.gross_amount.unwrap() - i.taxes.unwrap() - i.balance.unwrap()).abs() < 0.01);
    }

    #[test]
    fn missing_original_amount_falls_back_to_current_price_as_cost() {
        let h = map_net(&inv("x", Some(200.0), Some(4.0))).unwrap();
        assert_eq!(Some(h.average_cost), h.unit_price);
    }

    fn card_tx(id: &str, kind: &str, amount: f64, status: &str) -> PluggyTransaction {
        let mut t = tx(id, kind, amount, "2026-09-01T00:00:00.000Z", Some(status));
        t.credit_card_metadata = Some(PluggyCardMeta {
            bill_id: Some("bill-1".into()),
        });
        t
    }

    #[test]
    fn card_charges_and_payments_follow_real_sign_semantics() {
        // Real data: purchases are DEBIT with a positive amount, payments CREDIT with a negative one.
        let charge = map_card_transaction(
            "c",
            "wf",
            "BRL",
            None,
            &card_tx("1", "DEBIT", 49.9, "POSTED"),
        )
        .unwrap();
        assert_eq!(charge.activity_type, "WITHDRAWAL");
        assert_eq!(charge.amount, Some(Decimal::new(4990, 2)));
        assert!(charge.metadata.unwrap().contains("bill-1"));
        let pay = map_card_transaction(
            "c",
            "wf",
            "BRL",
            None,
            &card_tx("2", "CREDIT", -500.0, "POSTED"),
        )
        .unwrap();
        assert_eq!(pay.activity_type, "DEPOSIT");
        assert_eq!(pay.amount, Some(Decimal::from(500)));
    }

    #[test]
    fn card_pending_and_inconsistent_transactions_are_skipped() {
        assert!(map_card_transaction(
            "c",
            "wf",
            "BRL",
            None,
            &card_tx("1", "DEBIT", 10.0, "PENDING")
        )
        .is_none());
        assert!(map_card_transaction(
            "c",
            "wf",
            "BRL",
            None,
            &card_tx("2", "DEBIT", -10.0, "POSTED")
        )
        .is_none());
        assert!(map_card_transaction(
            "c",
            "wf",
            "BRL",
            None,
            &card_tx("3", "CREDIT", 10.0, "POSTED")
        )
        .is_none());
    }

    #[test]
    fn card_import_is_idempotent() {
        let mk = || {
            vec![map_card_transaction(
                "c",
                "wf",
                "BRL",
                None,
                &card_tx("1", "DEBIT", 9.0, "POSTED"),
            )
            .unwrap()]
        };
        let (fresh, _) = drop_existing(mk(), &HashSet::new());
        let existing: HashSet<String> = fresh
            .iter()
            .filter_map(|a| a.idempotency_key.clone())
            .collect();
        assert!(drop_existing(mk(), &existing).0.is_empty());
    }

    #[test]
    fn links_enforce_card_vs_bank_account_types() {
        assert!(check_link_compat("CREDIT", "CREDIT_CARD").is_ok());
        assert!(check_link_compat("BANK", "CASH").is_ok());
        assert!(check_link_compat("CREDIT", "CASH").is_err());
        assert!(check_link_compat("BANK", "CREDIT_CARD").is_err());
    }

    fn wf(id: &str, name: &str) -> Account {
        Account {
            id: id.into(),
            name: name.into(),
            ..Default::default()
        }
    }

    #[test]
    fn candidates_match_on_institution_tokens_not_substrings() {
        let existing = vec![
            wf("btg", "BTG"),
            wf("bv", "BV"),
            wf("bvm", "BV Mãe"),
            wf("mp", "Mercado Pago"),
            wf("mp2", "Mercado Pago 2"),
            wf("nu", "Nubank"),
        ];
        assert_eq!(
            match_names(&["MeuPluggy · BTG Investimentos", "BTG Banking"], &existing),
            vec!["btg"]
        );
        // "BV" matches the BV account, not "BV Mãe" (its extra token is absent).
        assert_eq!(
            match_names(&["MeuPluggy · BANCO BV S.A."], &existing),
            vec!["bv"]
        );
        // Only the account whose every token appears is a candidate.
        assert_eq!(
            match_names(&["MeuPluggy · Mercado Pago (Conta Pré-paga)"], &existing),
            vec!["mp"]
        );
        assert!(match_names(&["Unknown Bank"], &existing).is_empty());
    }
}
