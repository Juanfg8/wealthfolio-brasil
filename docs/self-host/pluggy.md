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

## Limits

- Linked account must be in TRANSACTIONS tracking mode, else it is skipped with `lastError`.
- Investments and balances are read and exposed in `/status` only (not written to holdings).
- Credit cards/invoices are not imported.
- Existing accounts/activities are never modified by the sync.
