use super::*;
use std::{env, sync::Mutex};

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn test_load_default_config() {
    let _guard = ENV_LOCK.lock().unwrap();
    unsafe { env::set_var("APP_ENVIRONMENT", "test") };

    let settings = Settings::new().expect("configuration should load");
    assert_eq!(settings.server.port, 65001);
    assert_eq!(settings.swagger.port, 65002);
    assert_eq!(settings.database.username, "wurzburg_user");
    assert_eq!(
        settings.database.connect_string,
        "//87.247.175.207:1521/wurzburg"
    );
}

#[test]
fn test_environment_helpers() {
    let _guard = ENV_LOCK.lock().unwrap();
    unsafe { env::set_var("APP_ENVIRONMENT", "production") };
    assert!(Settings::is_production());
    assert!(!Settings::is_development());

    unsafe { env::set_var("APP_ENVIRONMENT", "development") };
    assert!(Settings::is_development());
    assert!(!Settings::is_production());
}

#[test]
fn test_environment_overrides_migration_force_recreate() {
    let _guard = ENV_LOCK.lock().unwrap();
    unsafe {
        env::set_var("APP_ENVIRONMENT", "test");
        env::set_var("APP_MIGRATIONS__FORCE_RECREATE", "true");
    }
    let settings = Settings::new().expect("settings should load");
    assert!(settings.migrations.force_recreate);
    unsafe { env::remove_var("APP_MIGRATIONS__FORCE_RECREATE") };
}

#[test]
fn kafka_validation_rejects_compression_missing_from_this_build() {
    let _guard = ENV_LOCK.lock().unwrap();
    unsafe { env::set_var("APP_ENVIRONMENT", "test") };
    let mut settings = Settings::new().expect("settings should load");
    settings.kafka.producer.compression_type = "zstd".to_string();
    let error = settings
        .kafka
        .validate()
        .expect_err("zstd must require the matching librdkafka build feature");
    assert!(error.to_string().contains("supports none and lz4"));
}

#[test]
fn runtime_identity_is_unique_without_changing_consumer_group() {
    let _guard = ENV_LOCK.lock().unwrap();
    unsafe { env::set_var("APP_ENVIRONMENT", "test") };
    let mut settings = Settings::new().expect("settings should load");
    let group_id = settings.kafka.materialization_receipts.group_id.clone();
    settings.apply_runtime_instance_identity();

    assert!(settings.kafka.outbox_relay.worker_id.contains(':'));
    assert!(settings.provider_core_provisioning.worker_id.contains(':'));
    assert!(settings.provider_kafka_access.worker_id.contains(':'));
    assert!(settings.kafka.producer.client_id.contains(':'));
    assert!(
        settings
            .kafka
            .materialization_receipts
            .client_id
            .contains(':')
    );
    assert_eq!(settings.kafka.materialization_receipts.group_id, group_id);
}
