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
- an empty result (no positions and no cash) never writes a snapshot.

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
