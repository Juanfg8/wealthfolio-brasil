use std::collections::HashMap;
use std::sync::Arc;

use chrono::{NaiveDate, TimeZone, Utc};
use log::{debug, warn};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::assets::{AssetKind, AssetMetadata, AssetServiceTrait, InstrumentType, QuoteMode};
use crate::constants::DECIMAL_PRECISION;
use crate::errors::Result;
use crate::fx::FxServiceTrait;
use crate::portfolio::snapshot::{
    validate_snapshot_write_date, AccountStateSnapshot, Position, SnapshotServiceTrait,
    SnapshotSource,
};
use crate::quotes::constants::DATA_SOURCE_MANUAL;
use crate::quotes::{Quote, QuoteServiceTrait};
use crate::utils::time_utils::{parse_user_timezone_or_default, user_today};

#[derive(Debug, Clone)]
pub struct ManualHoldingInput {
    pub asset_id: Option<String>,
    pub symbol: String,
    pub exchange_mic: Option<String>,
    pub quantity: Decimal,
    pub currency: String,
    pub average_cost: Decimal,
    /// Current unit price for MANUAL-priced assets. When set, the day's manual quote
    /// uses it instead of `average_cost`, so cost basis and market value stay separate.
    pub unit_price: Option<Decimal>,
    /// Asset name for custom assets
    pub name: Option<String>,
    /// Data source (e.g., "MANUAL") — when "MANUAL", quote mode is set to manual
    pub data_source: Option<String>,
    /// Asset kind string (e.g., "INVESTMENT", "OTHER")
    pub asset_kind: Option<String>,
    /// Quote currency resolved during search/review (e.g., GBp)
    pub quote_ccy: Option<String>,
    /// Instrument type resolved during search/review (e.g., EQUITY, CRYPTO)
    pub instrument_type: Option<String>,
    /// Market data provider that resolved this holding, if selected.
    pub provider_id: Option<String>,
    /// Provider-native symbol/code selected by search/import.
    pub provider_symbol: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CashBalanceInput {
    pub currency: String,
    pub amount: Decimal,
}

#[derive(Debug, Clone)]
pub struct ManualSnapshotRequest {
    pub account_id: String,
    pub account_currency: String,
    pub snapshot_date: NaiveDate,
    pub positions: Vec<ManualHoldingInput>,
    pub cash_balances: Vec<CashBalanceInput>,
    pub base_currency: Option<String>,
    pub source: SnapshotSource,
}

pub struct ManualSnapshotService {
    asset_service: Arc<dyn AssetServiceTrait>,
    fx_service: Arc<dyn FxServiceTrait>,
    snapshot_service: Arc<dyn SnapshotServiceTrait>,
    quote_service: Arc<dyn QuoteServiceTrait>,
    timezone: String,
}

/// A manual snapshot write (Pluggy sync or hand entry alike) reports the account's
/// *current* state; it carries no information about what, if anything, actually
/// flowed in or out since the last snapshot. Trusting it blindly breaks two ways:
///
/// 1. `net_contribution` would reset to zero even when this account has real,
///    activity-tracked contribution history - the account this write is for may
///    have been tracked through ordinary DEPOSIT/WITHDRAWAL/TRANSFER activities
///    for months before it started receiving snapshot writes at all (e.g. a
///    manual account later linked to Pluggy).
/// 2. The first time a position appears where the account previously held only
///    cash, whatever cost basis this write reports (Pluggy's own `amountOriginal`,
///    or a fresh manual entry) gets read as the account's entire investment gain
///    in that single instant, regardless of how long the money had actually been
///    there before this write started tracking it as "invested".
///
/// Bridges both against the account's own last known state (`prior`) instead of
/// assuming either "no history" (case 1) or "trust this write completely" (case
/// 2). Mutates `positions` in place, scaling every position's cost proportionally
/// so their sum matches the bridged total exactly. Returns
/// `(net_contribution, net_contribution_base, cost_basis)` for the new snapshot.
fn bridge_snapshot_continuity(
    prior: Option<&AccountStateSnapshot>,
    positions: &mut HashMap<String, Position>,
    raw_total_cost_basis: Decimal,
    cash_total_account_currency: Decimal,
) -> (Decimal, Decimal, Decimal) {
    let Some(prior) = prior else {
        // No prior snapshot at all: a genuinely new account. Zero contribution and
        // whatever this first write reports as cost are both correct as-is.
        return (Decimal::ZERO, Decimal::ZERO, raw_total_cost_basis);
    };

    let mut total_cost_basis = raw_total_cost_basis;

    // A position appearing where the account previously tracked zero cost basis
    // (pure cash, or genuinely no snapshot history yet) is the migration moment:
    // bridge cost basis so book_basis (cost_basis + cash) is preserved across it,
    // rather than reading the write's own cost figures as this instant's gain.
    // Once bridged, prior.cost_basis is nonzero on every later write, so this
    // never re-fires - ordinary position-level cost tracking takes over from here.
    if prior.cost_basis.is_zero() && !raw_total_cost_basis.is_zero() {
        let prior_book_basis = prior.cost_basis + prior.cash_total_account_currency;
        // Only bridge when there is a non-negative leftover of the prior book
        // basis to allocate to cost basis after honoring this write's own cash
        // total. When cash alone already accounts for (or exceeds) the entire
        // prior book basis, there is nothing left to bridge from - flooring the
        // shortfall at zero would scale every position's cost basis to zero,
        // making its whole market value read as fabricated gain (worse than not
        // bridging at all). Trust the write's own raw cost basis, unscaled.
        if cash_total_account_currency < prior_book_basis {
            let bridged_cost_basis = prior_book_basis - cash_total_account_currency;
            let scale = bridged_cost_basis / raw_total_cost_basis;
            for position in positions.values_mut() {
                position.average_cost = (position.average_cost * scale).round_dp(DECIMAL_PRECISION);
                position.total_cost_basis =
                    (position.total_cost_basis * scale).round_dp(DECIMAL_PRECISION);
            }
            // Sum the now-rounded positions rather than using `bridged_cost_basis`
            // directly, so the account-level total always matches what the
            // positions themselves actually add up to.
            total_cost_basis = positions
                .values()
                .map(|p| p.total_cost_basis)
                .sum::<Decimal>()
                .round_dp(DECIMAL_PRECISION);
        }
    }

    (
        prior.net_contribution,
        prior.net_contribution_base,
        total_cost_basis,
    )
}

impl ManualSnapshotService {
    pub fn new(
        asset_service: Arc<dyn AssetServiceTrait>,
        fx_service: Arc<dyn FxServiceTrait>,
        snapshot_service: Arc<dyn SnapshotServiceTrait>,
        quote_service: Arc<dyn QuoteServiceTrait>,
    ) -> Self {
        Self {
            asset_service,
            fx_service,
            snapshot_service,
            quote_service,
            timezone: String::new(),
        }
    }

    pub fn with_timezone(mut self, timezone: String) -> Self {
        self.timezone = timezone;
        self
    }

    pub async fn save_manual_snapshot(
        &self,
        request: ManualSnapshotRequest,
    ) -> Result<Vec<String>> {
        validate_snapshot_write_date(
            &request.account_id,
            request.snapshot_date,
            request.source.as_str(),
            user_today(parse_user_timezone_or_default(&self.timezone)),
        )?;

        let mut positions: HashMap<String, Position> = HashMap::new();
        let mut asset_ids: Vec<String> = Vec::new();

        for holding in request.positions {
            if holding.quantity.is_zero() {
                continue;
            }

            let asset_id = match holding.asset_id.as_deref() {
                Some(id) if !id.is_empty() => id.to_string(),
                _ => Uuid::new_v4().to_string(),
            };

            let kind = match holding.asset_kind.as_deref() {
                Some("OTHER") => Some(AssetKind::Other),
                Some("INVESTMENT") => Some(AssetKind::Investment),
                _ => None,
            };

            let metadata = AssetMetadata {
                instrument_symbol: Some(holding.symbol.clone()),
                instrument_exchange_mic: holding.exchange_mic.clone(),
                display_code: Some(holding.symbol.clone()),
                name: holding.name.clone(),
                kind,
                instrument_type: holding
                    .instrument_type
                    .as_deref()
                    .and_then(InstrumentType::from_external_str),
                requested_quote_ccy: holding.quote_ccy.clone(),
                provider_config: None,
                provider_id: holding.provider_id.clone(),
                provider_symbol: holding.provider_symbol.clone(),
                ..Default::default()
            };

            let quote_mode = match holding.data_source.as_deref() {
                Some(DATA_SOURCE_MANUAL) => Some(DATA_SOURCE_MANUAL.to_string()),
                _ => None,
            };

            let asset = self
                .asset_service
                .get_or_create_minimal_asset(
                    &asset_id,
                    Some(holding.currency.clone()),
                    Some(metadata),
                    quote_mode.clone(),
                )
                .await?;

            // Update quote mode on existing assets if MANUAL data source specified
            if let Some(ref mode) = quote_mode {
                let requested_mode = mode.to_uppercase();
                let current_mode = asset.quote_mode.as_db_str();
                if requested_mode != current_mode {
                    self.asset_service
                        .update_quote_mode_silent(&asset.id, &requested_mode)
                        .await?;
                }
            }

            // Create a quote from the snapshot price as a fallback.
            // Only for MANUAL-mode assets: average cost is a cost basis, not a market
            // price, and writing it for MARKET-mode assets would overwrite provider
            // quotes for the snapshot date.
            let is_manual_mode = asset.quote_mode == QuoteMode::Manual
                || matches!(quote_mode.as_deref(), Some(DATA_SOURCE_MANUAL));
            let quote_price = holding.unit_price.unwrap_or(holding.average_cost);
            if is_manual_mode && !quote_price.is_zero() {
                let source = DATA_SOURCE_MANUAL.to_string();
                self.create_quote_from_snapshot(
                    &asset.id,
                    quote_price,
                    &holding.currency,
                    request.snapshot_date,
                    source,
                )
                .await;
            }

            asset_ids.push(asset.id.clone());

            if holding.currency != request.account_currency {
                self.fx_service
                    .register_currency_pair(&holding.currency, &request.account_currency)
                    .await?;
            }

            if asset.quote_ccy != request.account_currency && asset.quote_ccy != holding.currency {
                self.fx_service
                    .register_currency_pair(&asset.quote_ccy, &request.account_currency)
                    .await?;
            }

            let total_cost_basis = holding.quantity * holding.average_cost;

            let position = Position {
                id: format!("POS-{}-{}", asset.id, request.account_id),
                account_id: request.account_id.clone(),
                asset_id: asset.id.clone(),
                quantity: holding.quantity,
                average_cost: holding.average_cost,
                total_cost_basis,
                currency: holding.currency,
                inception_date: Utc::now(),
                lots: std::collections::VecDeque::new(),
                created_at: Utc::now(),
                last_updated: Utc::now(),
                is_alternative: false,
                contract_multiplier: Decimal::ONE,
                cost_basis_account: None,
                cost_basis_base: None,
            };
            positions.insert(asset.id, position);
        }

        let mut cash_balances: HashMap<String, Decimal> = HashMap::new();
        for cash in request.cash_balances {
            if cash.amount.is_zero() {
                continue;
            }

            if cash.currency != request.account_currency {
                self.fx_service
                    .register_currency_pair(&cash.currency, &request.account_currency)
                    .await?;
            }

            cash_balances.insert(cash.currency, cash.amount);
        }

        if let Some(base_currency) = request.base_currency.as_deref() {
            if base_currency != request.account_currency {
                self.fx_service
                    .register_currency_pair(&request.account_currency, base_currency)
                    .await?;
            }
        }

        let raw_total_cost_basis: Decimal = positions.values().map(|p| p.total_cost_basis).sum();

        // Cache the cash totals on the keyframe: the daily holdings calculator
        // only fills them on CALCULATED snapshots, and the snapshot history UI
        // reads them straight off the keyframe.
        let cash_total_account_currency = self.sum_cash_in_currency(
            &cash_balances,
            &request.account_currency,
            request.snapshot_date,
        );
        let cash_total_base_currency = match request.base_currency.as_deref() {
            Some(base_currency) => {
                self.sum_cash_in_currency(&cash_balances, base_currency, request.snapshot_date)
            }
            None => Decimal::ZERO,
        };

        let prior = self
            .snapshot_service
            .get_latest_holdings_snapshot(&request.account_id)
            .unwrap_or(None);
        let (net_contribution, net_contribution_base, total_cost_basis) =
            bridge_snapshot_continuity(
                prior.as_ref(),
                &mut positions,
                raw_total_cost_basis,
                cash_total_account_currency,
            );

        let snapshot = AccountStateSnapshot {
            id: format!(
                "{}_{}",
                request.account_id,
                request.snapshot_date.format("%Y-%m-%d")
            ),
            account_id: request.account_id.clone(),
            snapshot_date: request.snapshot_date,
            currency: request.account_currency.clone(),
            positions,
            cash_balances,
            cost_basis: total_cost_basis,
            net_contribution,
            net_contribution_base,
            cash_total_account_currency,
            cash_total_base_currency,
            calculated_at: Utc::now().naive_utc(),
            source: request.source,
        };

        self.snapshot_service
            .save_manual_snapshot(&request.account_id, snapshot)
            .await?;

        asset_ids.sort();
        asset_ids.dedup();

        Ok(asset_ids)
    }

    /// Sums cash balances converted into `target_currency` at `date`.
    /// Falls back to the unconverted amount when no FX rate is available,
    /// matching the holdings calculator's cash-total semantics.
    fn sum_cash_in_currency(
        &self,
        cash_balances: &HashMap<String, Decimal>,
        target_currency: &str,
        date: NaiveDate,
    ) -> Decimal {
        let mut total = Decimal::ZERO;
        for (currency, &amount) in cash_balances {
            if currency == target_currency {
                total += amount;
            } else {
                match self.fx_service.convert_currency_for_date(
                    amount,
                    currency,
                    target_currency,
                    date,
                ) {
                    Ok(converted) => total += converted,
                    Err(e) => {
                        warn!(
                            "Failed to convert cash balance {} to {}: {}. Using unconverted amount.",
                            currency, target_currency, e
                        );
                        total += amount;
                    }
                }
            }
        }
        total
    }

    /// Creates a quote from snapshot data to serve as a price fallback.
    /// Uses `DataSource::Manual` for MANUAL-mode assets, `DataSource::Broker` for others.
    async fn create_quote_from_snapshot(
        &self,
        asset_id: &str,
        price: Decimal,
        currency: &str,
        date: NaiveDate,
        data_source: String,
    ) {
        let timestamp = Utc.from_utc_datetime(&date.and_hms_opt(12, 0, 0).unwrap());

        let quote_id = if data_source == DATA_SOURCE_MANUAL {
            let date_part = timestamp.format("%Y%m%d").to_string();
            format!("{}_{}", date_part, asset_id.to_uppercase())
        } else {
            let date_str = timestamp.format("%Y-%m-%d").to_string();
            format!("{}_{}_{}", asset_id, date_str, data_source)
        };

        let quote = Quote {
            id: quote_id,
            asset_id: asset_id.to_string(),
            timestamp,
            open: price,
            high: price,
            low: price,
            close: price,
            adjclose: price,
            volume: Decimal::ZERO,
            currency: currency.to_string(),
            data_source,
            created_at: Utc::now(),
            notes: None,
        };

        match self.quote_service.update_quote(quote).await {
            Ok(_) => {
                debug!(
                    "Created quote for asset {} on {} at price {}",
                    asset_id, date, price
                );
            }
            Err(e) => {
                debug!("Failed to create quote for asset {}: {}", asset_id, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn position(asset_id: &str, quantity: Decimal, average_cost: Decimal) -> Position {
        Position {
            id: format!("POS-{asset_id}"),
            account_id: "acct".to_string(),
            asset_id: asset_id.to_string(),
            quantity,
            average_cost,
            total_cost_basis: quantity * average_cost,
            currency: "BRL".to_string(),
            ..Position::default()
        }
    }

    fn positions(entries: &[(&str, Decimal, Decimal)]) -> HashMap<String, Position> {
        entries
            .iter()
            .map(|(id, qty, cost)| (id.to_string(), position(id, *qty, *cost)))
            .collect()
    }

    fn prior_snapshot(
        cost_basis: Decimal,
        cash: Decimal,
        net_contribution: Decimal,
    ) -> AccountStateSnapshot {
        AccountStateSnapshot {
            cost_basis,
            net_contribution,
            net_contribution_base: net_contribution,
            cash_total_account_currency: cash,
            ..AccountStateSnapshot::default()
        }
    }

    #[test]
    fn brand_new_account_has_no_prior_and_is_untouched() {
        let mut pos = positions(&[("cdb", dec!(1), dec!(500))]);
        let (nc, nc_base, cb) =
            bridge_snapshot_continuity(None, &mut pos, dec!(500), Decimal::ZERO);
        assert_eq!(nc, Decimal::ZERO);
        assert_eq!(nc_base, Decimal::ZERO);
        assert_eq!(cb, dec!(500));
        assert_eq!(pos["cdb"].average_cost, dec!(500)); // unscaled
    }

    /// BTG's real shape (anonymized production numbers, 2026-09-21): a pure-cash
    /// account with real activity history (net_contribution=136544.37) got linked
    /// to Pluggy, which reported 7 investment positions summing to cost_basis
    /// 131138.2252141 and cash dropping to 0. Before this fix, that 131138.23
    /// vs the account's true 136544.37 book_basis - a ~5406 gap - read as
    /// instantaneous fabricated gain, on top of losing the 136544.37 itself
    /// (net_contribution reset to 0 discarded ALL of it, not just the gap).
    #[test]
    fn btg_pluggy_link_preserves_net_contribution_and_bridges_cost_basis() {
        let prior = prior_snapshot(Decimal::ZERO, dec!(140673.00), dec!(136544.36526210));
        let mut pos = positions(&[
            ("t1", dec!(5_000_000), dec!(0.01)), // 50,000
            ("t2", dec!(1_000_000), dec!(0.01)), // 10,000
            ("t3", dec!(1_000_000), dec!(0.01)), // 10,000
            ("t4", dec!(1), dec!(1138.2252141)), // 1,138.2252141
            ("t5", dec!(4_000_000), dec!(0.01)), // 40,000
            ("t6", dec!(1_000_000), dec!(0.01)), // 10,000
            ("t7", dec!(1_000_000), dec!(0.01)), // 10,000
        ]);
        let raw_total_cost_basis: Decimal = pos.values().map(|p| p.total_cost_basis).sum();
        assert_eq!(raw_total_cost_basis.round_dp(2), dec!(131138.23));

        let (nc, _nc_base, cb) =
            bridge_snapshot_continuity(Some(&prior), &mut pos, raw_total_cost_basis, Decimal::ZERO);

        // net_contribution must survive the link untouched - not reset to zero.
        assert_eq!(nc, dec!(136544.36526210));
        // cost_basis is bridged to the prior book_basis (cost 0 + cash 140673.00),
        // not left at Pluggy's raw 131138.23.
        // Summing 7 independently-rounded (8dp) positions can accumulate a few
        // billionths of a real; immaterial at currency precision.
        assert_eq!(cb.round_dp(2), dec!(140673.00));
        // Every position was scaled by (approximately) the same ratio - each is
        // independently rounded to 8dp, so their sum matches at currency precision.
        let scaled_sum: Decimal = pos.values().map(|p| p.total_cost_basis).sum();
        assert_eq!(scaled_sum.round_dp(2), cb.round_dp(2));
        // Relative weighting between positions is preserved (at currency precision;
        // independent per-position rounding can differ by a billionth of a real).
        assert_eq!(
            pos["t1"].total_cost_basis.round_dp(2),
            (pos["t2"].total_cost_basis * dec!(5)).round_dp(2)
        );
    }

    /// BV's real shape: 4 positions summing to exactly 70000.00, cash dropping
    /// to 0, prior net_contribution 69606.97860456 on a prior book_basis of
    /// 71446.00 (cost 0 + cash 71446.00).
    #[test]
    fn bv_pluggy_link_bridges_cost_basis_to_prior_book_basis() {
        let prior = prior_snapshot(Decimal::ZERO, dec!(71446.00), dec!(69606.97860456));
        let mut pos = positions(&[
            ("a", dec!(1_000_000), dec!(0.01)), // 10,000
            ("b", dec!(1_000_000), dec!(0.01)), // 10,000
            ("c", dec!(1_965_097), dec!(0.01)), // 19,650.97
            ("d", dec!(3_034_903), dec!(0.01)), // 30,349.03
        ]);
        let raw_total_cost_basis: Decimal = pos.values().map(|p| p.total_cost_basis).sum();
        assert_eq!(raw_total_cost_basis, dec!(70000.00));

        let (nc, _nc_base, cb) =
            bridge_snapshot_continuity(Some(&prior), &mut pos, raw_total_cost_basis, Decimal::ZERO);

        assert_eq!(nc, dec!(69606.97860456));
        assert_eq!(cb, dec!(71446.00));
    }

    /// Mercado Pago's real shape: a Caixinha position (cost 4457.06) plus a tiny
    /// residual position (cost 0.01), cash staying nonzero at 7254.30 (unlike
    /// BTG/BV, MP kept some cash outside the Caixinha) - and unlike BTG/BV,
    /// that cash alone already exceeds the prior book_basis. Prior
    /// net_contribution 4129.02420329 on a prior book_basis of 6120.00 (cost
    /// 0 + cash 6120.00).
    #[test]
    fn mercado_pago_falls_back_to_raw_cost_basis_when_cash_exceeds_prior_book_basis() {
        let prior = prior_snapshot(Decimal::ZERO, dec!(6120.00), dec!(4129.02420329));
        let mut pos = positions(&[
            ("caixinha", dec!(1), dec!(4457.06)),
            ("residual", dec!(1), dec!(0.01)),
        ]);
        let raw_total_cost_basis: Decimal = pos.values().map(|p| p.total_cost_basis).sum();
        assert_eq!(raw_total_cost_basis, dec!(4457.07));

        let new_cash = dec!(7254.30); // MP's real post-link cash (not all converted)
        let (nc, _nc_base, cb) =
            bridge_snapshot_continuity(Some(&prior), &mut pos, raw_total_cost_basis, new_cash);

        assert_eq!(nc, dec!(4129.02420329));
        // Cash alone (7254.30) already exceeds the prior book_basis (6120.00):
        // there is no non-negative leftover to bridge. Flooring cost_basis at
        // zero here would scale every position's cost to zero, making the
        // Caixinha's entire market value read as fabricated gain - worse than
        // not bridging at all. Fall back to the write's own raw cost basis,
        // unscaled.
        assert_eq!(cb, dec!(4457.07));
        assert_eq!(pos["caixinha"].total_cost_basis, dec!(4457.06)); // unscaled
        assert_eq!(pos["residual"].total_cost_basis, dec!(0.01)); // unscaled
    }

    /// Mercado Pago 2's real shape: a single Caixinha position, all cash
    /// converted (cash=0 after link, unlike Mercado Pago).
    #[test]
    fn mercado_pago_2_single_position_bridges_cleanly() {
        let prior = prior_snapshot(Decimal::ZERO, dec!(4693.11417426), dec!(4693.11417426));
        let mut pos = positions(&[("caixinha2", dec!(1), dec!(4329.98))]);
        let raw_total_cost_basis: Decimal = pos.values().map(|p| p.total_cost_basis).sum();

        let (nc, _nc_base, cb) =
            bridge_snapshot_continuity(Some(&prior), &mut pos, raw_total_cost_basis, Decimal::ZERO);

        assert_eq!(nc, dec!(4693.11417426));
        assert_eq!(cb, dec!(4693.11417426));
        assert_eq!(pos["caixinha2"].total_cost_basis, cb); // single position takes it all
    }

    /// Once bridged, prior.cost_basis is nonzero, so a later resync must not
    /// re-bridge - ordinary position-level cost tracking (already handled
    /// elsewhere) takes over, and this function becomes a pure net_contribution
    /// carry-forward with cost_basis passed through unchanged.
    #[test]
    fn already_bridged_account_does_not_re_bridge_on_next_sync() {
        let prior = prior_snapshot(dec!(140673.00), Decimal::ZERO, dec!(136544.36526210));
        let mut pos = positions(&[("t1", dec!(1), dec!(140975.19))]); // a later day's real value
        let raw_total_cost_basis: Decimal = pos.values().map(|p| p.total_cost_basis).sum();

        let (nc, _nc_base, cb) =
            bridge_snapshot_continuity(Some(&prior), &mut pos, raw_total_cost_basis, Decimal::ZERO);

        assert_eq!(nc, dec!(136544.36526210));
        assert_eq!(cb, dec!(140975.19)); // passed through, not re-bridged
        assert_eq!(pos["t1"].average_cost, dec!(140975.19)); // unscaled
    }

    /// A manual (non-Pluggy) account with no prior investment position and no
    /// prior activity history at all behaves exactly as before this fix: zero
    /// contribution, cost basis taken as-is. Existing correct manual accounts
    /// must not regress.
    #[test]
    fn manual_account_with_no_prior_snapshot_and_no_positions_is_unaffected() {
        let mut pos: HashMap<String, Position> = HashMap::new();
        let (nc, nc_base, cb) =
            bridge_snapshot_continuity(None, &mut pos, Decimal::ZERO, dec!(1000.00));
        assert_eq!(nc, Decimal::ZERO);
        assert_eq!(nc_base, Decimal::ZERO);
        assert_eq!(cb, Decimal::ZERO);
    }
}
