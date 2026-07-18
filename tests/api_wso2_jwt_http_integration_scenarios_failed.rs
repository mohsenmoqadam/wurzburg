mod support;

use std::{env, sync::Arc};

use jsonwebtoken::Algorithm;
use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{api::router::build_app_router, config::Settings, state::AppState};

/// Scenario goal:
/// Prove that the production WSO2 trust boundary rejects every important JWT
/// confusion/validity failure through a real running Wurzburg HTTP server.
///
/// Database facts: none are inserted; rejection must happen before repositories.
/// External effects: no Kafka, Dragonfly, or TigerBeetle operation is permitted.
/// Final proof: every request returns the centralized INVALID_ACTOR_CLAIM result.
#[tokio::test]
async fn rejects_untrusted_jwt_variants_through_running_wurzburg() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }

    let settings = Settings::new().expect("full integration settings should load");
    let state = Arc::new(
        AppState::new(settings)
            .await
            .expect("Wurzburg application state should start"),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener address should exist");
    let app = build_app_router(state);

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("Wurzburg HTTP instance should serve");
    });
    tokio::task::yield_now().await;

    let cases = vec![
        ("unsigned", support::unsigned_platform_admin_jwt()),
        ("hs-rsa-confusion", support::signed_hs256_confusion_jwt()),
        (
            "unexpected-rs384",
            support::signed_test_jwt(support::TestJwtOptions::default(), Algorithm::RS384),
        ),
        (
            "invalid-issuer",
            support::signed_test_jwt(
                support::TestJwtOptions {
                    issuer: "https://untrusted.example.test".to_string(),
                    ..support::TestJwtOptions::default()
                },
                Algorithm::RS256,
            ),
        ),
        (
            "invalid-audience",
            support::signed_test_jwt(
                support::TestJwtOptions {
                    audience: "different-api".to_string(),
                    ..support::TestJwtOptions::default()
                },
                Algorithm::RS256,
            ),
        ),
        (
            "expired",
            support::signed_test_jwt(
                support::TestJwtOptions {
                    exp: 1_600_000_000,
                    ..support::TestJwtOptions::default()
                },
                Algorithm::RS256,
            ),
        ),
        (
            "not-yet-valid",
            support::signed_test_jwt(
                support::TestJwtOptions {
                    nbf: 2_000_000_000,
                    ..support::TestJwtOptions::default()
                },
                Algorithm::RS256,
            ),
        ),
        (
            "conflicting-client",
            support::signed_conflicting_client_jwt(),
        ),
    ];

    let client = reqwest::Client::new();
    for (case_name, token) in cases {
        let response = client
            .get(format!("http://{address}/api/v1/card-ranges"))
            .bearer_auth(token)
            .header("X-Correlation-Id", format!("jwt-rejection-{case_name}"))
            .header("X-Request-Id", Uuid::new_v4().to_string())
            .header("X-WSO2-Client-IP", "198.51.100.10")
            .header("X-WSO2-Gateway-Id", "wso2-integration-test")
            .send()
            .await
            .expect("JWT rejection request should complete");
        let status = response.status();
        let body = response.text().await.expect("response body should read");

        assert_eq!(
            status,
            reqwest::StatusCode::UNAUTHORIZED,
            "case={case_name}, body={body}"
        );
        assert!(
            body.contains("INVALID_ACTOR_CLAIM"),
            "case={case_name}, body={body}"
        );
    }
}
