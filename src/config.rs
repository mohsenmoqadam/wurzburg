// src/config.rs
use anyhow::{Context, Result};
use config::{Config, Environment, File};
use serde::Deserialize;
use std::env;
use std::time::Duration;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: ServerConfig,
    pub swagger: SwaggerConfig,
    pub database: DatabaseConfig,
    pub migrations: MigrationConfig,
    pub wso2: Wso2Config,
    pub redis: RedisConfig,
    pub telemetry: TelemetryConfig,
    pub kafka: KafkaConfig,
    pub validation: ValidationConfig,
    pub tigerbeetle: TigerBeetleConfig,
    pub provider_core_provisioning: ProviderCoreProvisioningConfig,
    pub provider_kafka_access: ProviderKafkaAccessConfig,
    pub provider_operational_profile_scheduler: ProviderOperationalProfileSchedulerConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub graceful_shutdown_timeout_ms: u64,
    pub telemetry_shutdown_timeout_ms: u64,
}

impl ServerConfig {
    pub fn graceful_shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.graceful_shutdown_timeout_ms)
    }

    pub fn telemetry_shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.telemetry_shutdown_timeout_ms)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct SwaggerConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub username: String,
    pub password: String,
    pub connect_string: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub max_lifetime_ms: u64,
    pub statement_cache_capacity: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MigrationConfig {
    pub enabled: bool,
    pub force_recreate: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Wso2Config {
    pub backend_token_transport: BackendTokenTransport,
    pub accepted_correlation_pattern: String,
    pub issuer: String,
    pub audience: String,
    pub allowed_algorithms: Vec<String>,
    pub clock_skew_seconds: u64,
    pub public_key_pem: String,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackendTokenTransport {
    AuthorizationBearer,
    XJwtAssertion,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RedisConfig {
    pub url: String,
    pub pool_min_idle: u32,
    pub pool_max_open: u32,
    pub connection_timeout_ms: u64,
    pub response_timeout_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub service_name: String,
    pub service_version: String,
    pub environment: String,
    pub otlp_endpoint: String,
    pub log_format: String,
    pub log_level: String,
    pub batch_max_queue: usize,
    pub batch_size: usize,
    pub batch_delay_ms: u64,
    pub sampling_ratio: f64,
}

// --- Kafka Configurations ---

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub security_protocol: String,
    pub security_cert: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub producer: KafkaProducerConfig,
    pub outbox_relay: KafkaOutboxRelayConfig,
    pub materialization_receipts: KafkaReceiptConsumerConfig,
    pub admin: KafkaAdminConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaProducerConfig {
    pub client_id: String,
    pub delivery_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub max_request_size: u64,
    pub retries: u32,
    pub linger_ms: u64,
    pub compression_type: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaOutboxRelayConfig {
    pub enabled: bool,
    pub topic: String,
    pub worker_id: String,
    pub batch_size: u16,
    pub poll_interval_ms: u64,
    pub lease_duration_ms: u64,
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaReceiptConsumerConfig {
    pub enabled: bool,
    pub topic: String,
    pub group_id: String,
    pub client_id: String,
    pub session_timeout_ms: u64,
    pub max_poll_interval_ms: u64,
    pub auto_offset_reset: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaAdminConfig {
    pub request_timeout_ms: u64,
    pub partitions: u32,
    pub replication_factor: u32,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ValidationConfig {
    pub provider: ProviderValidationConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderValidationConfig {
    pub legal_name_min: usize,
    pub legal_name_msg: String,

    pub trade_name_min: usize,
    pub trade_name_msg: String,

    pub tax_id_min: usize,
    pub tax_id_msg: String,

    pub email_msg: String,
    pub email_regex: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TigerBeetleConfig {
    pub cluster_id: u32,
    pub replica_addresses: Vec<String>,
    pub concurrency_max: u32,
    pub batch_max_size: usize,
    pub batch_timeout_ms: u64,
    pub channel_capacity: usize,
    pub operation_timeout_ms: u64,
    pub ledger_id: u32,
    pub provider_owned_account_code: u16,
    pub provider_fee_account_code: u16,
    pub cms_settlement_account_code: u16,
    pub platform_fee_account_code: u16,
    pub user_account_code: u16,
    pub system_account_code: u16,
    pub transfer_code: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderCoreProvisioningConfig {
    pub enabled: bool,
    pub worker_id: String,
    pub batch_size: u16,
    pub poll_interval_ms: u64,
    pub lease_duration_ms: u64,
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderKafkaAccessConfig {
    pub enabled: bool,
    pub worker_id: String,
    pub batch_size: u16,
    pub poll_interval_ms: u64,
    pub lease_duration_ms: u64,
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub master_key_source: String,
    pub master_key_environment_variable: String,
    pub master_key_file: Option<String>,
    pub encryption_key_version: String,
    pub scram_iterations: i32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderOperationalProfileSchedulerConfig {
    pub enabled: bool,
    pub batch_size: u16,
    pub poll_interval_ms: u64,
}

impl ProviderKafkaAccessConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }
}

impl ProviderCoreProvisioningConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }
}

// --- Implementations ---
impl TigerBeetleConfig {
    pub fn batch_timeout(&self) -> Duration {
        Duration::from_millis(self.batch_timeout_ms)
    }

    pub fn operation_timeout(&self) -> Duration {
        Duration::from_millis(self.operation_timeout_ms)
    }

    pub fn provider_account_code(
        &self,
        category: crate::domain::provider::ProviderAccountCategory,
    ) -> u16 {
        use crate::domain::provider::ProviderAccountCategory;
        match category {
            ProviderAccountCategory::ProviderOwned => self.provider_owned_account_code,
            ProviderAccountCategory::ProviderFee => self.provider_fee_account_code,
            ProviderAccountCategory::CmsSettlement => self.cms_settlement_account_code,
            ProviderAccountCategory::PlatformFee => self.platform_fee_account_code,
        }
    }
}

impl Settings {
    /// Load configuration using environment priorities
    pub fn new() -> Result<Self> {
        let run_mode = env::var("APP_ENVIRONMENT").unwrap_or_else(|_| "development".into());

        let config = Config::builder()
            .add_source(File::with_name("config/default"))
            .add_source(File::with_name(&format!("config/{}", run_mode)).required(false))
            .add_source(File::with_name("config/local").required(false))
            .add_source(
                Environment::with_prefix("APP")
                    .prefix_separator("_")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .context("Failed to build configuration")?;

        let settings: Self = config
            .try_deserialize()
            .context("Failed to deserialize configuration")?;
        settings.kafka.validate()?;
        if settings.provider_kafka_access.enabled {
            let master_key_source = settings.provider_kafka_access.master_key_source.trim();
            anyhow::ensure!(
                matches!(master_key_source, "env" | "file"),
                "Provider Kafka master key source must be either 'env' or 'file'"
            );
            if master_key_source == "env" {
                anyhow::ensure!(
                    !settings
                        .provider_kafka_access
                        .master_key_environment_variable
                        .trim()
                        .is_empty(),
                    "Provider Kafka master-key environment variable is required"
                );
            }
            if master_key_source == "file" {
                anyhow::ensure!(
                    settings
                        .provider_kafka_access
                        .master_key_file
                        .as_deref()
                        .is_some_and(|value| !value.trim().is_empty()),
                    "Provider Kafka master-key file is required"
                );
            }
            anyhow::ensure!(
                !settings
                    .provider_kafka_access
                    .encryption_key_version
                    .trim()
                    .is_empty(),
                "Provider Kafka encryption key version is required"
            );
            anyhow::ensure!(
                settings.provider_kafka_access.scram_iterations >= 4096,
                "Provider Kafka SCRAM iterations must be at least 4096"
            );
            anyhow::ensure!(
                settings
                    .kafka
                    .admin
                    .sasl_username
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty()),
                "Provider Kafka administration username is required"
            );
            anyhow::ensure!(
                settings
                    .kafka
                    .admin
                    .sasl_password
                    .as_deref()
                    .is_some_and(|value| !value.is_empty()),
                "Provider Kafka administration password is required"
            );
            anyhow::ensure!(
                settings.provider_kafka_access.max_attempts > 0,
                "Provider Kafka max attempts must be positive"
            );
        }
        anyhow::ensure!(
            settings.provider_operational_profile_scheduler.batch_size > 0,
            "Provider operational profile scheduler batch size must be positive"
        );
        anyhow::ensure!(
            settings
                .provider_operational_profile_scheduler
                .poll_interval_ms
                > 0,
            "Provider operational profile scheduler poll interval must be positive"
        );
        Ok(settings)
    }

    pub fn environment() -> String {
        env::var("APP_ENVIRONMENT").unwrap_or_else(|_| "development".into())
    }

    pub fn is_development() -> bool {
        Self::environment() == "development"
    }

    pub fn is_production() -> bool {
        Self::environment() == "production"
    }

    pub fn is_staging() -> bool {
        Self::environment() == "staging"
    }

    /// Adds a process-unique suffix to diagnostic and lease-owner identities.
    /// Kafka group IDs remain stable so replicas cooperate as one consumer.
    pub fn apply_runtime_instance_identity(&mut self) {
        let host = env::var("POD_NAME")
            .or_else(|_| env::var("HOSTNAME"))
            .unwrap_or_else(|_| "local".to_string());
        let host = host
            .chars()
            .map(|value| {
                if value.is_ascii_alphanumeric() || matches!(value, '.' | '_' | '-') {
                    value
                } else {
                    '-'
                }
            })
            .collect::<String>();
        let instance = format!("{host}-{}", std::process::id());
        self.kafka.producer.client_id = append_instance(&self.kafka.producer.client_id, &instance);
        self.kafka.materialization_receipts.client_id =
            append_instance(&self.kafka.materialization_receipts.client_id, &instance);
        self.kafka.outbox_relay.worker_id =
            append_instance(&self.kafka.outbox_relay.worker_id, &instance);
        self.provider_core_provisioning.worker_id =
            append_instance(&self.provider_core_provisioning.worker_id, &instance);
        self.provider_kafka_access.worker_id =
            append_instance(&self.provider_kafka_access.worker_id, &instance);
    }
}

fn append_instance(base: &str, instance: &str) -> String {
    const MAX_IDENTITY_LENGTH: usize = 240;
    let available = MAX_IDENTITY_LENGTH.saturating_sub(instance.len() + 1);
    let base = base.chars().take(available).collect::<String>();
    format!("{base}:{instance}")
}

impl DatabaseConfig {
    pub fn acquire_timeout(&self) -> Duration {
        Duration::from_millis(self.acquire_timeout_ms)
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }

    pub fn idle_timeout(&self) -> Duration {
        Duration::from_millis(self.idle_timeout_ms)
    }

    pub fn max_lifetime(&self) -> Duration {
        Duration::from_millis(self.max_lifetime_ms)
    }
}

impl RedisConfig {
    pub fn connection_timeout(&self) -> Duration {
        Duration::from_millis(self.connection_timeout_ms)
    }

    pub fn response_timeout(&self) -> Duration {
        Duration::from_millis(self.response_timeout_ms)
    }
}

// Kafka Duration Helpers
impl KafkaProducerConfig {
    pub fn delivery_timeout(&self) -> Duration {
        Duration::from_millis(self.delivery_timeout_ms)
    }
}

impl KafkaConfig {
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.bootstrap_servers.trim().is_empty(),
            "Kafka bootstrap_servers must not be empty"
        );
        anyhow::ensure!(
            matches!(
                self.security_protocol.as_str(),
                "PLAINTEXT" | "SSL" | "SASL_PLAINTEXT" | "SASL_SSL"
            ),
            "unsupported Kafka security_protocol"
        );
        if self.security_protocol.contains("SASL") {
            anyhow::ensure!(
                self.sasl_mechanism
                    .as_deref()
                    .is_some_and(|v| !v.is_empty()),
                "Kafka SASL mechanism is required"
            );
            anyhow::ensure!(
                self.sasl_username.as_deref().is_some_and(|v| !v.is_empty()),
                "Kafka SASL username is required"
            );
            anyhow::ensure!(
                self.sasl_password.as_deref().is_some_and(|v| !v.is_empty()),
                "Kafka SASL password is required"
            );
        }
        anyhow::ensure!(
            self.producer.delivery_timeout_ms > self.producer.request_timeout_ms,
            "Kafka delivery_timeout_ms must exceed request_timeout_ms"
        );
        anyhow::ensure!(
            matches!(self.producer.compression_type.as_str(), "none" | "lz4"),
            "unsupported Kafka compression_type; this build supports none and lz4"
        );
        if self.outbox_relay.enabled {
            anyhow::ensure!(
                !self.outbox_relay.topic.trim().is_empty(),
                "Kafka outbox topic is required"
            );
            anyhow::ensure!(
                !self.outbox_relay.worker_id.trim().is_empty(),
                "Kafka outbox worker_id is required"
            );
            anyhow::ensure!(
                self.outbox_relay.batch_size > 0,
                "Kafka outbox batch_size must be positive"
            );
            anyhow::ensure!(
                self.outbox_relay.lease_duration_ms > self.producer.delivery_timeout_ms,
                "Kafka outbox lease must exceed producer delivery timeout"
            );
            anyhow::ensure!(
                self.outbox_relay.max_attempts > 0,
                "Kafka outbox max_attempts must be positive"
            );
            anyhow::ensure!(
                self.outbox_relay.initial_backoff_ms <= self.outbox_relay.max_backoff_ms,
                "Kafka outbox backoff bounds are invalid"
            );
        }
        if self.materialization_receipts.enabled {
            anyhow::ensure!(
                !self.materialization_receipts.topic.trim().is_empty(),
                "Kafka receipt topic is required"
            );
            anyhow::ensure!(
                !self.materialization_receipts.group_id.trim().is_empty(),
                "Kafka receipt group_id is required"
            );
            anyhow::ensure!(
                matches!(
                    self.materialization_receipts.auto_offset_reset.as_str(),
                    "earliest" | "latest" | "error"
                ),
                "invalid Kafka auto_offset_reset"
            );
        }
        Ok(())
    }
}

impl KafkaAdminConfig {
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
}

impl KafkaReceiptConsumerConfig {
    pub fn session_timeout(&self) -> Duration {
        Duration::from_millis(self.session_timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_load_default_config() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { env::set_var("APP_ENVIRONMENT", "test") };

        let settings = Settings::new();
        assert!(settings.is_ok(), "Failed to load configuration");

        let settings = settings.unwrap();
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

        unsafe {
            env::remove_var("APP_MIGRATIONS__FORCE_RECREATE");
        }
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
            .expect_err("zstd must not pass without the matching librdkafka build feature");

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
}
