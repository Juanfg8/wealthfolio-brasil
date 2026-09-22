//! Consolidated read-only diagnostics for production support: one endpoint
//! that answers "is everything OK?" without SSH/dashboard digging.
//!
//! Everything here is derived from state already tracked elsewhere
//! (`pluggy::PluggyState`, the live `DbAccess`, and the backup snapshot
//! catalogue) — nothing new is persisted. It sits behind the same
//! `auth::require_jwt` layer as the rest of `/api/v1` (see `api.rs`), so it
//! carries no more exposure than the existing `/pluggy/status` route, which
//! already returns the full `PluggyState` this reuses.

use std::{collections::BTreeMap, sync::Arc};

use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;

use crate::{
    main_lib::AppState,
    pluggy::{self, AccountState, BalanceCheck, LinkStatus, PluggyConfig, PluggyState, RunSummary},
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildInfo {
    pub version: String,
    /// Git SHA baked in at image build time via `WF_GIT_SHA` (see Dockerfile).
    /// `None` on builds that didn't set it (e.g. local `cargo run`).
    pub git_sha: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluggyItemSummary {
    pub item_id: String,
    pub institution: Option<String>,
    pub status: LinkStatus,
    /// True when the item is linked to a Wealthfolio account (not pending review or ignored).
    pub active: bool,
    pub linked_account_id: Option<String>,
    pub last_synced_at: Option<String>,
    pub last_error: Option<String>,
    pub pluggy_updated_at: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluggyDiagnostics {
    /// Whether PLUGGY_CLIENT_ID/SECRET/ITEM_IDS are set. `start_scheduler`
    /// (see pluggy.rs) only spawns its loop when this is true, and that loop
    /// has no exit path, so `configured` doubles as "scheduler is running".
    pub configured: bool,
    pub scheduler_running: bool,
    pub item_count: usize,
    pub active_item_count: usize,
    pub items: Vec<PluggyItemSummary>,
    pub last_run: Option<RunSummary>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseHealth {
    pub reachable: bool,
    pub wal_mode: bool,
    pub journal_mode: Option<String>,
    pub size_bytes: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupInfo {
    pub filename: String,
    pub modified_at: String,
    pub size_bytes: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationFlag {
    pub account_id: String,
    pub name: String,
    /// DRIFT | UNKNOWN (OK accounts are counted but not listed here).
    pub status: String,
    pub diff: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationSummary {
    pub ok: usize,
    pub drift: usize,
    pub unknown: usize,
    pub flags: Vec<ReconciliationFlag>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsSummary {
    pub generated_at: String,
    pub build: BuildInfo,
    pub pluggy: PluggyDiagnostics,
    pub database: DatabaseHealth,
    pub last_backup: Option<BackupInfo>,
    pub balance_reconciliation: ReconciliationSummary,
}

// ---------------------------------------------------------------------------
// Pure data-gathering (unit-tested)
// ---------------------------------------------------------------------------

pub(crate) fn summarize_items(state: &PluggyState) -> Vec<PluggyItemSummary> {
    state
        .items
        .values()
        .map(|link| PluggyItemSummary {
            item_id: link.item_id.clone(),
            institution: link.institution.clone(),
            status: link.status,
            active: link.status == LinkStatus::Linked,
            linked_account_id: link.linked_account_id.clone(),
            last_synced_at: link.last_synced_at.clone(),
            last_error: link.last_error.clone(),
            pluggy_updated_at: link.pluggy_updated_at.clone(),
        })
        .collect()
}

pub(crate) fn summarize_reconciliation(
    accounts: &BTreeMap<String, AccountState>,
) -> ReconciliationSummary {
    let mut summary = ReconciliationSummary::default();
    for account in accounts.values() {
        let Some(check) = account.balance_check.as_ref() else {
            continue;
        };
        match check.status.as_str() {
            "OK" => summary.ok += 1,
            "DRIFT" => {
                summary.drift += 1;
                summary.flags.push(flag(account, check));
            }
            _ => {
                summary.unknown += 1;
                summary.flags.push(flag(account, check));
            }
        }
    }
    summary
}

fn flag(account: &AccountState, check: &BalanceCheck) -> ReconciliationFlag {
    ReconciliationFlag {
        account_id: account.id.clone(),
        name: account.name.clone(),
        status: check.status.clone(),
        diff: check.diff,
    }
}

pub(crate) fn journal_mode_is_wal(journal_mode: &str) -> bool {
    journal_mode.eq_ignore_ascii_case("wal")
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

async fn diagnostics_summary(State(state): State<Arc<AppState>>) -> Json<DiagnosticsSummary> {
    let pluggy_state = pluggy::load_state(&state.data_root);
    let items = summarize_items(&pluggy_state);
    let active_item_count = items.iter().filter(|i| i.active).count();
    let configured = PluggyConfig::from_env().is_some();

    let size_bytes = std::fs::metadata(&state.db_path).ok().map(|m| m.len());
    let database = match state.db_access.connect_rusqlite() {
        Ok(conn) => {
            let journal_mode: Option<String> = conn
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .ok();
            let wal_mode = journal_mode.as_deref().is_some_and(journal_mode_is_wal);
            DatabaseHealth {
                reachable: true,
                wal_mode,
                journal_mode,
                size_bytes,
                error: None,
            }
        }
        Err(e) => DatabaseHealth {
            reachable: false,
            wal_mode: false,
            journal_mode: None,
            size_bytes,
            error: Some(e.to_string()),
        },
    };

    let last_backup = {
        let root = state.data_root.clone();
        let key = state.database_key.clone();
        tokio::task::spawn_blocking(move || {
            wealthfolio_storage_sqlite::db::snapshots::list(&root, Some(key))
        })
        .await
        .ok()
        .and_then(|r| r.ok())
        .and_then(|snapshots| snapshots.into_iter().next())
        .map(|s| BackupInfo {
            filename: s.filename,
            modified_at: s.modified_at,
            size_bytes: s.size_bytes,
        })
    };

    let balance_reconciliation = summarize_reconciliation(&pluggy_state.accounts);

    Json(DiagnosticsSummary {
        generated_at: chrono::Utc::now().to_rfc3339(),
        build: BuildInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            git_sha: std::env::var("WF_GIT_SHA").ok(),
        },
        pluggy: PluggyDiagnostics {
            configured,
            scheduler_running: configured,
            item_count: items.len(),
            active_item_count,
            items,
            last_run: pluggy_state.last_run,
        },
        database,
        last_backup,
        balance_reconciliation,
    })
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/diagnostics/summary", get(diagnostics_summary))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pluggy::ItemLink;

    fn item(status: LinkStatus, institution: &str) -> ItemLink {
        ItemLink {
            item_id: format!("item-{institution}"),
            institution: Some(institution.to_string()),
            status,
            linked_account_id: Some("wf-acc-1".to_string()),
            positions_written: 3,
            candidates: vec![],
            pluggy_updated_at: Some("2026-09-22T00:00:00Z".to_string()),
            total_value: Some(1000.0),
            last_synced_at: Some("2026-09-22T01:00:00Z".to_string()),
            last_error: None,
        }
    }

    #[test]
    fn summarize_items_counts_only_linked_as_active() {
        let mut state = PluggyState::default();
        state
            .items
            .insert("1".into(), item(LinkStatus::Linked, "BV"));
        state
            .items
            .insert("2".into(), item(LinkStatus::Linked, "BTG"));
        state
            .items
            .insert("3".into(), item(LinkStatus::NeedsReview, "Mercado Pago"));
        state
            .items
            .insert("4".into(), item(LinkStatus::Ignored, "Mercado Pago 2"));

        let items = summarize_items(&state);
        assert_eq!(items.len(), 4);
        assert_eq!(items.iter().filter(|i| i.active).count(), 2);
    }

    #[test]
    fn summarize_items_surfaces_last_error() {
        let mut state = PluggyState::default();
        let mut broken = item(LinkStatus::Linked, "BTG");
        broken.last_error = Some("item unavailable".to_string());
        state.items.insert("1".into(), broken);

        let items = summarize_items(&state);
        assert_eq!(items[0].last_error, Some("item unavailable".to_string()));
    }

    fn account(status: &str, diff: Option<f64>) -> AccountState {
        AccountState {
            id: format!("acc-{status}"),
            item_id: "item-1".to_string(),
            institution: None,
            name: format!("{status} account"),
            kind: "BANK".to_string(),
            subtype: None,
            number: None,
            currency: "BRL".to_string(),
            balance: Some(100.0),
            status: LinkStatus::Linked,
            linked_account_id: Some("wf-1".to_string()),
            since: None,
            candidates: vec![],
            last_synced_at: None,
            last_error: None,
            balance_check: Some(BalanceCheck {
                pluggy: Some(100.0),
                wealthfolio: Some(100.0 - diff.unwrap_or(0.0)),
                diff,
                status: status.to_string(),
                checked_at: "2026-09-22T00:00:00Z".to_string(),
            }),
            credit_limit: None,
            available_credit: None,
            due_date: None,
            bills: vec![],
            reserves: vec![],
        }
    }

    #[test]
    fn summarize_reconciliation_counts_and_flags_non_ok() {
        let mut accounts = BTreeMap::new();
        accounts.insert("a".into(), account("OK", Some(0.0)));
        accounts.insert("b".into(), account("DRIFT", Some(42.5)));
        accounts.insert("c".into(), account("UNKNOWN", None));

        let summary = summarize_reconciliation(&accounts);
        assert_eq!(summary.ok, 1);
        assert_eq!(summary.drift, 1);
        assert_eq!(summary.unknown, 1);
        assert_eq!(summary.flags.len(), 2);
        assert!(summary
            .flags
            .iter()
            .any(|f| f.status == "DRIFT" && f.diff == Some(42.5)));
        assert!(summary.flags.iter().any(|f| f.status == "UNKNOWN"));
    }

    #[test]
    fn summarize_reconciliation_ignores_accounts_without_a_check() {
        let mut accounts = BTreeMap::new();
        let mut no_check = account("OK", Some(0.0));
        no_check.balance_check = None;
        accounts.insert("a".into(), no_check);

        let summary = summarize_reconciliation(&accounts);
        assert_eq!(summary.ok, 0);
        assert_eq!(summary.flags.len(), 0);
    }

    #[test]
    fn journal_mode_is_wal_is_case_insensitive() {
        assert!(journal_mode_is_wal("wal"));
        assert!(journal_mode_is_wal("WAL"));
        assert!(!journal_mode_is_wal("delete"));
    }
}
