mod support;

use std::{env, sync::Arc, time::Duration};

use rdkafka::{
    ClientConfig,
    consumer::{BaseConsumer, Consumer},
};
use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router,
    config::Settings,
    db::oracle::prepare_oracle_schema,
    kafka::ProviderKafkaAccessSpec,
    services::provider_kafka::{ProviderKafkaService, start_provider_kafka_provisioning_worker},
    state::AppState,
};

/// Scenario goal: prove asynchronous Provider Kafka provisioning and secret
/// retrieval against the real Oracle, Kafka, HTTP, and signed-WSO2 boundaries.
///
/// Oracle facts: Provider creation atomically stores connection metadata, an
/// encrypted candidate credential, a provisioning job, and redacted audit.
/// Kafka facts: the worker creates the provider topic, SCRAM-SHA-512 identity,
/// exact topic/group ACLs, then verifies all broker state before activation.
/// Final proof: the API reveals only the active credential, rotation preserves
/// the old secret until broker success, suspension removes access, resume
/// restores it, and provider credentials can authenticate to the exact topic.
#[tokio::test]
async fn provisions_rotates_suspends_and_resumes_real_provider_kafka_access() {
    if env::var("RUN_PROVIDER_KAFKA_INTEGRATION_TESTS")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    support::init_test_tracing();
    let mut settings = Settings::new().expect("integration settings should load");
    settings.provider_kafka_access.enabled = true;
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be prepared before scenarios run");
    settings.provider_core_provisioning.enabled = false;
    let state = Arc::new(AppState::new(settings.clone()).await.unwrap());
    let worker = start_provider_kafka_provisioning_worker(
        state.db.clone(),
        ProviderKafkaService::new(
            state.db.clone(),
            state.kafka_admin.clone(),
            state
                .provider_kafka_credentials
                .clone()
                .expect("Provider Kafka credentials should initialize"),
            settings.provider_kafka_access.scram_iterations,
        ),
        settings.provider_kafka_access.clone(),
    )
    .expect("Provider Kafka worker should run");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app_state = state.clone();
    tokio::spawn(async move {
        axum::serve(listener, build_app_router(app_state))
            .await
            .unwrap()
    });
    tokio::task::yield_now().await;
    let client = reqwest::Client::new();

    let created = post(
        &client,
        address,
        "/api/v1/providers",
        &Uuid::new_v4().to_string(),
        &provider_body(),
    )
    .await;
    assert_eq!(created.0, reqwest::StatusCode::CREATED, "{}", created.1);
    let created_json: serde_json::Value = serde_json::from_str(&created.1).unwrap();
    assert_eq!(created_json["kafka_provisioning_status"], "PENDING");
    let provider_id = Uuid::parse_str(created_json["provider_id"].as_str().unwrap()).unwrap();

    let first = wait_for_credentials(&client, address, provider_id, Some(1)).await;
    assert_eq!(first["credential_status"], "ACTIVE");
    assert_eq!(first["security_protocol"], "SASL_SSL");
    assert_eq!(first["sasl_mechanism"], "SCRAM-SHA-512");
    assert!(
        first["security_cert"]
            .as_str()
            .unwrap()
            .contains("BEGIN CERTIFICATE")
    );
    verify_provider_consumer_contract(&first);

    let old_password = first["password"].as_str().unwrap().to_string();
    let rotation = post(
        &client,
        address,
        &format!("/api/v1/providers/{provider_id}/kafka/rotate-credentials"),
        &Uuid::new_v4().to_string(),
        &serde_json::json!({"reason":"scheduled integration credential rotation"}).to_string(),
    )
    .await;
    assert_eq!(rotation.0, reqwest::StatusCode::ACCEPTED, "{}", rotation.1);
    let during_rotation = get_credentials(&client, address, provider_id).await;
    assert_eq!(
        during_rotation.0,
        reqwest::StatusCode::OK,
        "{}",
        during_rotation.1
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&during_rotation.1).unwrap()["password"],
        old_password
    );
    let second = wait_for_credentials(&client, address, provider_id, Some(2)).await;
    assert_ne!(second["password"], old_password);
    verify_provider_consumer_contract(&second);

    let suspension = post(
        &client,
        address,
        &format!("/api/v1/providers/{provider_id}/kafka/suspend"),
        &Uuid::new_v4().to_string(),
        &serde_json::json!({"reason":"exercise emergency provider channel control"}).to_string(),
    )
    .await;
    assert_eq!(
        suspension.0,
        reqwest::StatusCode::ACCEPTED,
        "{}",
        suspension.1
    );
    wait_for_access_status(&client, address, provider_id, "SUSPENDED").await;
    let unavailable = get_credentials(&client, address, provider_id).await;
    assert_eq!(unavailable.0, reqwest::StatusCode::CONFLICT);

    let resume = post(
        &client,
        address,
        &format!("/api/v1/providers/{provider_id}/kafka/resume"),
        &Uuid::new_v4().to_string(),
        &serde_json::json!({"reason":"provider channel approved for resumed delivery"}).to_string(),
    )
    .await;
    assert_eq!(resume.0, reqwest::StatusCode::ACCEPTED, "{}", resume.1);
    let resumed = wait_for_credentials(&client, address, provider_id, Some(2)).await;
    assert_eq!(resumed["password"], second["password"]);

    let spec = ProviderKafkaAccessSpec {
        topic_name: resumed["topic"].as_str().unwrap().to_string(),
        username: resumed["username"].as_str().unwrap().to_string(),
        consumer_group: resumed["consumer_group"].as_str().unwrap().to_string(),
    };
    state
        .kafka_admin
        .revoke_provider_access(spec.clone())
        .await
        .expect("test cleanup should revoke Provider Kafka access");
    state
        .kafka_admin
        .delete_topic(&spec.topic_name)
        .await
        .expect("test cleanup should delete the isolated provider topic");
    worker.shutdown().await;
}

fn verify_provider_consumer_contract(value: &serde_json::Value) {
    let mut config = ClientConfig::new();
    config
        .set(
            "bootstrap.servers",
            value["brokers"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<Vec<_>>()
                .join(","),
        )
        .set("group.id", value["consumer_group"].as_str().unwrap())
        .set(
            "security.protocol",
            value["security_protocol"].as_str().unwrap(),
        )
        .set("sasl.mechanism", value["sasl_mechanism"].as_str().unwrap())
        .set("sasl.username", value["username"].as_str().unwrap())
        .set("sasl.password", value["password"].as_str().unwrap())
        .set("ssl.ca.pem", value["security_cert"].as_str().unwrap());
    let consumer: BaseConsumer = config
        .create()
        .expect("provider consumer should initialize");
    let metadata = consumer
        .fetch_metadata(
            Some(value["topic"].as_str().unwrap()),
            Duration::from_secs(10),
        )
        .expect("provider credential should authenticate and describe its topic");
    assert_eq!(metadata.topics().len(), 1);
    assert_eq!(
        metadata.topics()[0].name(),
        value["topic"].as_str().unwrap()
    );
}

async fn wait_for_credentials(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    provider_id: Uuid,
    version: Option<u64>,
) -> serde_json::Value {
    for _ in 0..80 {
        let response = get_credentials(client, address, provider_id).await;
        if response.0 == reqwest::StatusCode::OK {
            let value: serde_json::Value = serde_json::from_str(&response.1).unwrap();
            if version.is_none_or(|expected| value["credential_version"] == expected)
                && value["credential_status"] == "ACTIVE"
            {
                return value;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("Provider Kafka credential did not become active before timeout");
}

async fn wait_for_access_status(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    provider_id: Uuid,
    expected: &str,
) {
    for _ in 0..80 {
        let response = client
            .get(format!(
                "http://{address}/api/v1/providers/{provider_id}/kafka/status"
            ))
            .bearer_auth(support::signed_platform_admin_jwt())
            .header("X-Correlation-Id", "provider-kafka-status")
            .header("X-Request-Id", Uuid::new_v4().to_string())
            .header("X-WSO2-Client-IP", "198.51.100.32")
            .header("X-WSO2-Gateway-Id", "wso2-integration-test")
            .send()
            .await
            .unwrap();
        let status = response.status();
        if status == reqwest::StatusCode::OK {
            let value: serde_json::Value =
                serde_json::from_str(&response.text().await.unwrap()).unwrap();
            if value["access_status"] == expected {
                assert_eq!(value["latest_operation"]["status"], "SUCCEEDED");
                return;
            }
        } else {
            let _ = response.text().await;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("Provider Kafka access did not reach {expected} before timeout");
}

async fn get_credentials(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    provider_id: Uuid,
) -> (reqwest::StatusCode, String) {
    let response = client
        .get(format!(
            "http://{address}/api/v1/providers/{provider_id}/kafka/credentials"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("X-Correlation-Id", "provider-kafka-credential-read")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.32")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.text().await.unwrap())
}

async fn post(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    path: &str,
    key: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .post(format!("http://{address}{path}"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", key)
        .header("X-Correlation-Id", "provider-kafka-command")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.32")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.text().await.unwrap())
}

fn provider_body() -> String {
    serde_json::json!({"legal_name":"Provider Kafka Real Broker Scenario","trade_name":"Provider Kafka Scenario","metadata":{},"contacts":[],"operational_profile":{"effective_at":"2026-01-01T00:00:00Z","profile":{"timezone":"Asia/Tehran","user_onboarding":{"enabled":true,"active_windows":[],"max_total_users":null},"credit_grant":{"enabled":true,"mode":"FixedLimit","limit_amount_rials":1000000},"credit_return":{"enabled":true},"card_operations":{"new_assignment_enabled":true,"same_pan_reprint_enabled":true,"new_pan_replacement_enabled":true,"attach_existing_multi_provider_card_enabled":true},"event_delivery":{"enabled":true,"disabled_reason":null}}}}).to_string()
}
