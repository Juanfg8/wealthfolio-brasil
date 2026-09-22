//! Database model for account state snapshots.

use chrono::{NaiveDate, NaiveDateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::{Integer, Nullable, Text};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::str::FromStr;

use wealthfolio_core::constants::DECIMAL_PRECISION;
use wealthfolio_core::portfolio::snapshot::{
    AccountStateSnapshot, Position, SnapshotMetadata, SnapshotSource,
};

use crate::errors::StorageError;

/// Database model for account state snapshots
#[derive(Debug, Clone, Queryable, QueryableByName, Insertable, Serialize, Deserialize)]
#[diesel(table_name = crate::schema::holdings_snapshots)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
#[serde(rename_all = "camelCase")]
pub struct AccountStateSnapshotDB {
    #[diesel(sql_type = Text)]
    pub id: String,
    #[diesel(sql_type = Text)]
    pub account_id: String,
    #[diesel(sql_type = Text)]
    pub snapshot_date: String,
    #[diesel(sql_type = Text)]
    pub currency: String,
    #[diesel(sql_type = Text)]
    pub positions: String,
    #[diesel(sql_type = Text)]
    pub cash_balances: String,
    #[diesel(sql_type = Text)]
    pub cost_basis: String,
    #[diesel(sql_type = Text)]
    pub net_contribution: String,
    #[diesel(sql_type = Text)]
    pub calculated_at: String,
    #[diesel(sql_type = Text)]
    pub net_contribution_base: String,
    #[diesel(sql_type = Text)]
    pub cash_total_account_currency: String,
    #[diesel(sql_type = Text)]
    pub cash_total_base_currency: String,
    #[diesel(sql_type = Text)]
    pub source: String,
}

impl From<&AccountStateSnapshotDB> for SnapshotMetadata {
    fn from(db: &AccountStateSnapshotDB) -> Self {
        let object_len = |json: &str| {
            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(json)
                .map(|values| values.len())
                .unwrap_or_default()
        };

        Self {
            id: db.id.clone(),
            account_id: db.account_id.clone(),
            snapshot_date: db.snapshot_date.clone(),
            source: db.source.clone(),
            position_count: object_len(&db.positions),
            cash_currency_count: object_len(&db.cash_balances),
            cash_total_account_currency: db.cash_total_account_currency.clone(),
        }
    }
}

/// Parses `calculated_at` as written by the app (`...Z`) or by external tools
/// that emit RFC 3339 offsets (`...+00:00`). Offsets are normalised to UTC.
fn parse_calculated_at(value: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    match chrono::DateTime::parse_from_rfc3339(value) {
        Ok(dt) => Ok(dt.naive_utc()),
        Err(_) => NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.fZ"),
    }
}

impl TryFrom<AccountStateSnapshotDB> for AccountStateSnapshot {
    type Error = StorageError;

    fn try_from(db: AccountStateSnapshotDB) -> Result<Self, Self::Error> {
        let snapshot_date =
            NaiveDate::parse_from_str(&db.snapshot_date, "%Y-%m-%d").map_err(|error| {
                StorageError::SerializationError(format!(
                    "Invalid snapshot date '{}' for account '{}': {}",
                    db.snapshot_date, db.account_id, error
                ))
            })?;

        Ok(AccountStateSnapshot {
            id: db.id,
            account_id: db.account_id,
            snapshot_date,
            currency: db.currency,
            positions: serde_json::from_str(&db.positions).unwrap_or_default(),
            cash_balances: serde_json::from_str(&db.cash_balances).unwrap_or_default(),
            cost_basis: Decimal::from_str(&db.cost_basis).unwrap_or_default(),
            net_contribution: Decimal::from_str(&db.net_contribution).unwrap_or_default(),
            net_contribution_base: Decimal::from_str(&db.net_contribution_base).unwrap_or_default(),
            cash_total_account_currency: Decimal::from_str(&db.cash_total_account_currency)
                .unwrap_or_default(),
            cash_total_base_currency: Decimal::from_str(&db.cash_total_base_currency)
                .unwrap_or_default(),
            calculated_at: parse_calculated_at(&db.calculated_at).unwrap_or_else(|e| {
                log::error!(
                    "Failed to parse DB calculated_at '{}': {}",
                    db.calculated_at,
                    e
                );
                Utc::now().naive_utc()
            }),
            source: match db.source.as_str() {
                // `MANUAL` is the legacy persisted spelling for a manually
                // maintained holdings keyframe. Treating it as CALCULATED
                // prevents later cash or position changes from being
                // classified as external flows.
                "MANUAL" => SnapshotSource::ManualEntry,
                _ => serde_json::from_str(&format!("\"{}\"", db.source))
                    .unwrap_or(SnapshotSource::Calculated),
            },
        })
    }
}

// Conversion from Domain model to DB model
impl From<AccountStateSnapshot> for AccountStateSnapshotDB {
    fn from(domain: AccountStateSnapshot) -> Self {
        Self {
            id: domain.id.clone(),
            account_id: domain.account_id,
            snapshot_date: domain.snapshot_date.format("%Y-%m-%d").to_string(),
            currency: domain.currency,
            positions: serde_json::to_string(&domain.positions)
                .unwrap_or_else(|_| "{}".to_string()),
            cash_balances: serde_json::to_string(&domain.cash_balances)
                .unwrap_or_else(|_| "{}".to_string()),
            cost_basis: domain.cost_basis.round_dp(DECIMAL_PRECISION).to_string(),
            net_contribution: domain
                .net_contribution
                .round_dp(DECIMAL_PRECISION)
                .to_string(),
            net_contribution_base: domain
                .net_contribution_base
                .round_dp(DECIMAL_PRECISION)
                .to_string(),
            cash_total_account_currency: domain
                .cash_total_account_currency
                .round_dp(DECIMAL_PRECISION)
                .to_string(),
            cash_total_base_currency: domain
                .cash_total_base_currency
                .round_dp(DECIMAL_PRECISION)
                .to_string(),
            calculated_at: domain
                .calculated_at
                .format("%Y-%m-%dT%H:%M:%S%.fZ")
                .to_string(),
            source: serde_json::to_string(&domain.source)
                .unwrap_or_else(|_| "\"CALCULATED\"".to_string())
                .trim_matches('"')
                .to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot_db(source: &str) -> AccountStateSnapshotDB {
        AccountStateSnapshotDB {
            id: "snapshot".to_string(),
            account_id: "account".to_string(),
            snapshot_date: "2026-09-21".to_string(),
            currency: "BRL".to_string(),
            positions: "{}".to_string(),
            cash_balances: "{}".to_string(),
            cost_basis: "0".to_string(),
            net_contribution: "0".to_string(),
            calculated_at: "2026-09-21T00:00:00Z".to_string(),
            net_contribution_base: "0".to_string(),
            cash_total_account_currency: "0".to_string(),
            cash_total_base_currency: "0".to_string(),
            source: source.to_string(),
        }
    }

    #[test]
    fn legacy_manual_snapshot_is_a_manual_keyframe() {
        let snapshot = AccountStateSnapshot::try_from(snapshot_db("MANUAL"))
            .expect("legacy snapshot should deserialize");
        assert_eq!(snapshot.source, SnapshotSource::ManualEntry);
    }

    #[test]
    fn malformed_snapshot_date_is_not_coerced_to_epoch() {
        let snapshot = AccountStateSnapshot {
            account_id: "account-1".to_string(),
            ..Default::default()
        };
        let mut row = AccountStateSnapshotDB::from(snapshot);
        row.snapshot_date = "not-a-date".to_string();

        let error = AccountStateSnapshot::try_from(row)
            .expect_err("malformed stored dates must remain an explicit read failure");

        assert!(error
            .to_string()
            .contains("Invalid snapshot date 'not-a-date'"));
        assert!(error.to_string().contains("account-1"));
    }

    #[test]
    fn calculated_at_accepts_app_and_rfc3339_offset_formats() {
        let expected = NaiveDate::from_ymd_opt(2026, 9, 18)
            .unwrap()
            .and_hms_opt(20, 0, 0)
            .unwrap();
        // Format found in 7,567 existing rows (written by external tooling).
        assert_eq!(
            parse_calculated_at("2026-09-18T20:00:00.000000+00:00").unwrap(),
            expected
        );
        // Format the app itself writes.
        assert_eq!(
            parse_calculated_at("2026-09-18T20:00:00.000000Z").unwrap(),
            expected
        );
        assert_eq!(
            parse_calculated_at("2026-09-18T20:00:00Z").unwrap(),
            expected
        );
        // Non-UTC offsets are normalised to UTC.
        assert_eq!(
            parse_calculated_at("2026-09-18T17:00:00-03:00").unwrap(),
            expected
        );
        assert!(parse_calculated_at("not a date").is_err());
    }
}

// --- snapshot_positions table ---
//
// Relational sibling of the legacy `holdings_snapshots.positions` JSON column.
// Written alongside the JSON column (dual-write); read with a JSON fallback so
// snapshots from older app versions keep working.

#[derive(Debug, Clone, Queryable, QueryableByName, Insertable)]
#[diesel(table_name = crate::schema::snapshot_positions)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct SnapshotPositionRecord {
    #[diesel(sql_type = Integer)]
    pub id: i32,
    #[diesel(sql_type = Text)]
    pub snapshot_id: String,
    #[diesel(sql_type = Text)]
    pub asset_id: String,
    #[diesel(sql_type = Text)]
    pub quantity: String,
    #[diesel(sql_type = Text)]
    pub average_cost: String,
    #[diesel(sql_type = Text)]
    pub total_cost_basis: String,
    #[diesel(sql_type = Text)]
    pub currency: String,
    #[diesel(sql_type = Text)]
    pub inception_date: String,
    #[diesel(sql_type = Integer)]
    pub is_alternative: i32,
    #[diesel(sql_type = Text)]
    pub contract_multiplier: String,
    #[diesel(sql_type = Text)]
    pub created_at: String,
    #[diesel(sql_type = Text)]
    pub last_updated: String,
    #[diesel(sql_type = Nullable<Text>)]
    pub cost_basis_base: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub cost_basis_account: Option<String>,
}

/// Insertable version without the autoincrement `id`.
#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = crate::schema::snapshot_positions)]
pub struct NewSnapshotPositionRecord {
    pub snapshot_id: String,
    pub asset_id: String,
    pub quantity: String,
    pub average_cost: String,
    pub total_cost_basis: String,
    pub currency: String,
    pub inception_date: String,
    pub is_alternative: i32,
    pub contract_multiplier: String,
    pub created_at: String,
    pub last_updated: String,
    pub cost_basis_base: Option<String>,
    pub cost_basis_account: Option<String>,
}

impl SnapshotPositionRecord {
    /// Convert a DB row into the in-memory Position struct. Lots are not
    /// reconstructed here — callers that need lot detail should query the
    /// `lots` table directly.
    pub fn to_position(&self, account_id: &str) -> Position {
        let parse_dt = |s: &str| -> chrono::DateTime<Utc> {
            chrono::DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.with_timezone(&Utc))
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ")
                        .map(|ndt| ndt.and_utc())
                })
                .unwrap_or_else(|_| Utc::now())
        };

        Position {
            id: format!("POS-{}-{}", self.asset_id, account_id),
            account_id: account_id.to_string(),
            asset_id: self.asset_id.clone(),
            quantity: Decimal::from_str(&self.quantity).unwrap_or_default(),
            average_cost: Decimal::from_str(&self.average_cost).unwrap_or_default(),
            total_cost_basis: Decimal::from_str(&self.total_cost_basis).unwrap_or_default(),
            currency: self.currency.clone(),
            inception_date: parse_dt(&self.inception_date),
            lots: VecDeque::new(),
            created_at: parse_dt(&self.created_at),
            last_updated: parse_dt(&self.last_updated),
            is_alternative: self.is_alternative != 0,
            contract_multiplier: Decimal::from_str(&self.contract_multiplier)
                .unwrap_or(Decimal::ONE),
            cost_basis_account: self
                .cost_basis_account
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok()),
            cost_basis_base: self
                .cost_basis_base
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok()),
        }
    }
}

impl NewSnapshotPositionRecord {
    /// Build from an in-memory Position for a given snapshot_id.
    pub fn from_position(snapshot_id: &str, pos: &Position) -> Self {
        Self {
            snapshot_id: snapshot_id.to_string(),
            asset_id: pos.asset_id.clone(),
            quantity: pos.quantity.round_dp(DECIMAL_PRECISION).to_string(),
            average_cost: pos.average_cost.round_dp(DECIMAL_PRECISION).to_string(),
            total_cost_basis: pos.total_cost_basis.round_dp(DECIMAL_PRECISION).to_string(),
            currency: pos.currency.clone(),
            inception_date: pos.inception_date.to_rfc3339(),
            is_alternative: if pos.is_alternative { 1 } else { 0 },
            contract_multiplier: pos.contract_multiplier.to_string(),
            created_at: pos.created_at.to_rfc3339(),
            last_updated: pos.last_updated.to_rfc3339(),
            // Stored at full precision (no round_dp) so the precomputed scalar
            // stays byte-identical to valuation's lot-walked cost basis when a
            // future step reads it back in place of the embedded lots.
            cost_basis_base: pos.cost_basis_base.map(|value| value.to_string()),
            cost_basis_account: pos.cost_basis_account.map(|value| value.to_string()),
        }
    }
}
