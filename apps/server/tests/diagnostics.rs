use std::net::SocketAddr;

use argon2::{password_hash::SaltString, Argon2, PasswordHasher};
use axum::{
    body::{to_bytes, Body},
    extract::ConnectInfo,
    http::{header, Method, Request},
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use rand::{rngs::OsRng, RngCore};
use tempfile::{tempdir, TempDir};
use tower::ServiceExt;
use wealthfolio_server::{api::app_router, build_state, config::Config};

// Keeps the TempDir alive for the caller's whole test: the diagnostics route
// opens a fresh sqlite connection per request (rather than reusing the pool),
// which fails once the backing file is unlinked by an early-dropped TempDir.
async fn build_test_router(password: &str) -> (axum::Router, TempDir) {
    let tmp = tempdir().unwrap();
    std::env::set_var("WF_DB_PATH", tmp.path().join("test.db"));

    let salt = SaltString::generate(&mut OsRng);
    let password_hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string();
    std::env::set_var("WF_AUTH_PASSWORD_HASH", password_hash);

    let mut secret_bytes = [0u8; 32];
    OsRng.fill_bytes(&mut secret_bytes);
    let secret_b64 = BASE64.encode(secret_bytes);
    std::env::set_var("WF_SECRET_KEY", secret_b64);
    std::env::set_var("WF_CORS_ALLOW_ORIGINS", "http://localhost:3000");

    let config = Config::from_env().unwrap();
    let state = build_state(&config).await.unwrap();
    (app_router(state, &config).unwrap(), tmp)
}

fn cleanup_env() {
    for key in [
        "WF_DB_PATH",
        "WF_AUTH_PASSWORD_HASH",
        "WF_SECRET_KEY",
        "WF_CORS_ALLOW_ORIGINS",
    ] {
        std::env::remove_var(key);
    }
}

/// The diagnostics summary sits behind the same session auth as every other
/// `/api/v1` route (see `api.rs`'s `require_jwt` layer) — this proves it,
/// then checks the payload shape answers the questions it's meant to answer.
#[tokio::test]
async fn diagnostics_summary_requires_auth_and_reports_expected_shape() {
    let password = "super-secret";
    let (app, _tmp) = build_test_router(password).await;

    // Unauthenticated request is rejected, same as any other protected route.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/diagnostics/summary")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 401);

    // Log in.
    let login_body = serde_json::json!({ "password": password });
    let mut login_req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/login")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(login_body.to_string()))
        .unwrap();
    login_req
        .extensions_mut()
        .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))));
    let login_response = app.clone().oneshot(login_req).await.unwrap();
    assert_eq!(login_response.status(), 200);
    let set_cookie = login_response
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let cookie_token = set_cookie
        .split(';')
        .next()
        .unwrap()
        .trim_start_matches("wf_session=");

    // Authenticated request succeeds and answers the on-call questions.
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/diagnostics/summary")
                .header(header::COOKIE, format!("wf_session={cookie_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    // Pluggy: not configured in this test env, so no items and the
    // scheduler-running proxy is false.
    assert_eq!(body["pluggy"]["configured"], false);
    assert_eq!(body["pluggy"]["schedulerRunning"], false);
    assert_eq!(body["pluggy"]["itemCount"], 0);
    assert!(body["pluggy"]["items"].as_array().unwrap().is_empty());

    // Database: reachable, freshly created file is WAL by default.
    assert_eq!(body["database"]["reachable"], true);
    assert_eq!(body["database"]["walMode"], true);
    assert!(body["database"]["sizeBytes"].as_u64().unwrap() > 0);

    // No backups taken yet in a fresh test data dir.
    assert!(body["lastBackup"].is_null());

    // No Pluggy accounts synced yet, so no reconciliation flags.
    assert_eq!(body["balanceReconciliation"]["ok"], 0);
    assert!(body["balanceReconciliation"]["flags"]
        .as_array()
        .unwrap()
        .is_empty());

    // Build info always present.
    assert!(!body["build"]["version"].as_str().unwrap().is_empty());

    cleanup_env();
}
