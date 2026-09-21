use std::sync::Arc;

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::{
    error::{ApiError, ApiResult},
    main_lib::AppState,
    pluggy::{self, AccountState, PluggyConfig, PluggyState, RunSummary},
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusResponse {
    configured: bool,
    #[serde(flatten)]
    state: PluggyState,
}

async fn status(State(state): State<Arc<AppState>>) -> Json<StatusResponse> {
    Json(StatusResponse {
        configured: PluggyConfig::from_env().is_some(),
        state: pluggy::load_state(&state.data_root),
    })
}

async fn sync(State(state): State<Arc<AppState>>) -> ApiResult<Json<RunSummary>> {
    pluggy::run_sync(&state)
        .await
        .map(Json)
        .map_err(|e| ApiError::BadRequest(e.to_string()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LinkRequest {
    pluggy_account_id: String,
    account_id: Option<String>,
    since: Option<String>,
    #[serde(default)]
    ignore: bool,
}

async fn link(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LinkRequest>,
) -> ApiResult<Json<AccountState>> {
    pluggy::apply_link(
        &state,
        &req.pluggy_account_id,
        req.account_id.as_deref(),
        req.since.as_deref(),
        req.ignore,
    )
    .map(Json)
    .map_err(|e| ApiError::BadRequest(e.to_string()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ItemLinkRequest {
    item_id: String,
    account_id: Option<String>,
    #[serde(default)]
    ignore: bool,
}

async fn link_item(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ItemLinkRequest>,
) -> ApiResult<Json<pluggy::ItemLink>> {
    pluggy::apply_item_link(&state, &req.item_id, req.account_id.as_deref(), req.ignore)
        .map(Json)
        .map_err(|e| ApiError::BadRequest(e.to_string()))
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/pluggy/status", get(status))
        .route("/pluggy/sync", post(sync))
        .route("/pluggy/links", post(link))
        .route("/pluggy/investment-links", post(link_item))
}
