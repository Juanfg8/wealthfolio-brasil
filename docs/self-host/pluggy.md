# Pluggy / Open Finance sync

Optional, off unless credentials are set. Read-only against Pluggy.

## Setup (Railway service variables — never commit)

| Variable | Where to get it |
|---|---|
| `PLUGGY_CLIENT_ID` | Pluggy dashboard (dashboard.pluggy.ai) > Applications > your app, or MeuPluggy (meu.pluggy.ai) > Aplicações |
| `PLUGGY_CLIENT_SECRET` | Same page (shown once) |
| `PLUGGY_ITEM_IDS` | Comma-separated Item IDs of your connected institutions (Pluggy dashboard > Items, or MeuPluggy). One item = one bank login |

Redeploy after setting. A sync runs 90 s after boot, then every 6 h.

## Workflow (all endpoints are behind Access + Wealthfolio auth)

1. `POST /api/v1/pluggy/sync` — discovers accounts + investments into `<data>/pluggy_state.json`.
2. `GET /api/v1/pluggy/status` — every account starts `NEEDS_REVIEW`, with `candidates`
   (existing Wealthfolio accounts with the same normalized name). Nothing is merged automatically.
3. `POST /api/v1/pluggy/links` `{"pluggyAccountId","accountId","since":"YYYY-MM-DD"}` links one,
   or `{"pluggyAccountId","ignore":true}`. `since` defaults to today (no backfill of history
   that is already in Wealthfolio).
4. Next sync imports transactions for linked BANK accounts as `DEPOSIT`/`WITHDRAWAL`
   with `source_system=PLUGGY` and `idempotency_key=pluggy:<accountId>:<txId>`.
   Re-running never duplicates. Pending transactions are ignored.

## Investments (per Pluggy item)

Investments belong to an institution (item), not to a bank account.
`POST /api/v1/pluggy/investment-links` `{"itemId","accountId"}` (or `{"itemId","ignore":true}`) links an
item to an existing **HOLDINGS-mode** Wealthfolio account. Each sync then writes one holdings snapshot
(dated today, replaced on re-sync) through the same path as the Holdings UI:

- one manual-priced custom asset per Pluggy investment, symbol `PLUGGY-<id8>` (never a real ticker),
  deterministic asset id, so re-syncs reuse the asset;
- only `ACTIVE` positions; `TOTAL_WITHDRAWAL` (redeemed) and pending ones are skipped;
- quantity from Pluggy (or 1 unit if 0/missing); **market value and cost are separate**: the day's manual
  quote is the unit price (`ManualHoldingInput.unit_price`), the cost basis is Pluggy's `amountOriginal`;
- market value basis: `PLUGGY_VALUE_BASIS=gross` (default, Pluggy `amount`) or `net` (Pluggy `balance`).
  Pluggy's `balance` is net of IR/IOF (`amount - taxes - taxes2`); existing manual balances track gross;
- the item's BANK account balances are written as cash in the same snapshot; snapshots carry no net
  contribution and create no activities, so this is flow-neutral and adds no transaction history;
- **reserved pockets** (`bankData.reservedBalances`, e.g. Mercado Pago Caixinhas) are written as their own
  positions, never merged into cash: Pluggy's account `balance` excludes them (verified: the sum of all
  transactions since inception equals `balance` while reservations appear as debits), and they earn a
  different rate than the current account. Each keeps its own rate (e.g. 115% CDI) in the asset name;
- **pocket cost basis is tracked, never derived from value**: Pluggy exposes no principal for
  `reservedBalances`, and the "Dinheiro reservado/retirado" trail proved incomplete on both real Caixinhas
  (running principal goes negative; simulating it at 100% CDI overshoots Pluggy's value), so cost is
  bootstrapped at the first observation and then moves only with new flows:
  reserved +cost, retired -cost (floor 0). Yield = value - cost, so a sync that reports a higher value raises
  the return instead of resetting it, and cash<->pocket moves are flow-neutral. Flow ids are remembered so a
  movement is never counted twice; a pocket that cannot be bootstrapped blocks the item's snapshot;
- **modeled history seed (user-confirmed operating model)**: each Caixinha is kept at ~R$5,000 with its
  yield (115% CDI) periodically withdrawn. At the bootstrap, for 115%-of-CDI pockets, the lifetime yield is
  modeled as `5000 x 1.15 x sum(daily CDI)` from 2026-01-01 (official BCB SGS 12 series, simple accrual, no
  compounding) and cost is set to `observed value - modeled yield` (`costOrigin=MODELED_HISTORY`). The first
  observation therefore shows the modeled lifetime yield instead of zero; the observed value stays exactly
  Pluggy's (it may be below 5,000); the gap between observed and modeled lands in cost, so it is neither
  return nor a flow; yield withdrawn back to cash is realized return, never a loss. If the CDI series is
  unreachable, the bootstrap (and that item's snapshot) is deferred and retried;
- an empty result (no positions, pockets or cash) never writes a snapshot.

## Sync cadence

MeuPluggy proxy items refresh once every 24 h (`nextAutoSyncAt = lastUpdatedAt + 24h`). The scheduler
therefore runs every 12 h by default (`PLUGGY_SYNC_INTERVAL_HOURS`, min 1); more polling only spends API calls.
Never call `PATCH /items/{id}` on a schedule.

## Credit cards

Link a Pluggy `CREDIT` account to a `CREDIT_CARD` Wealthfolio account (enforced at link time).
- HOLDINGS mode (default recommendation): each sync writes the card debt as negative cash (liability).
- TRANSACTIONS mode: posted charges (`DEBIT` with positive amount) -> `WITHDRAWAL`, payments/refunds
  (`CREDIT` with negative amount) -> `DEPOSIT`; pending and sign-inconsistent rows are skipped.
- Limit, available credit, due date and the latest 12 bills are recorded in `/status` for review.

Bank accounts are not imported as transactions unless linked to a TRANSACTIONS-mode account; the
default for balance-focused accounts is the item snapshot above (no ledger noise, no backfill).

## Balance reconciliation

For each linked bank account `balanceCheck` in `/status` compares the Pluggy balance with Wealthfolio's
cash: `OK`, `DRIFT` (with `diff`) or `UNKNOWN`. It never changes data; valuations recalculate
asynchronously, so a fresh import may read `DRIFT`/`UNKNOWN` until the next sync.

## Limits

- Linked account must be in TRANSACTIONS tracking mode, else it is skipped with `lastError`.
- Investment transactions (aporte/resgate history) are available from Pluggy but not consumed.
- Existing accounts/activities are never modified by the sync.
