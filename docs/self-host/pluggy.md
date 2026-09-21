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
- quantity from Pluggy (or 1 unit of full value if quantity is 0/missing), price = balance / quantity;
- cost basis = current value (Pluggy has no reliable cost), so no unrealized gain is shown;
- an empty result never writes a snapshot.

## Balance reconciliation

For each linked bank account `balanceCheck` in `/status` compares the Pluggy balance with Wealthfolio's
cash: `OK`, `DRIFT` (with `diff`) or `UNKNOWN`. It never changes data; valuations recalculate
asynchronously, so a fresh import may read `DRIFT`/`UNKNOWN` until the next sync.

## Limits

- Linked account must be in TRANSACTIONS tracking mode, else it is skipped with `lastError`.
- Credit cards/invoices are not imported.
- Existing accounts/activities are never modified by the sync.
