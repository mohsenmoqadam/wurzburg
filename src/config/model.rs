use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: ServerConfig,
    pub swagger: SwaggerConfig,
    pub database: DatabaseConfig,
    pub migrations: MigrationConfig,
    pub wso2: Wso2Config,
    pub redis: RedisConfig,
    pub card_profile_lock: CardProfileLockConfig,
    pub telemetry: TelemetryConfig,
    pub kafka: KafkaConfig,
    pub validation: ValidationConfig,
    pub tigerbeetle: TigerBeetleConfig,
    pub provider_core_provisioning: ProviderCoreProvisioningConfig,
    pub provider_kafka_access: ProviderKafkaAccessConfig,
    pub provider_operational_profile_scheduler: ProviderOperationalProfileSchedulerConfig,
    pub wal_recovery: WalRecoveryConfig,
    pub object_storage: ObjectStorageConfig,
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

impl RedisConfig {
    pub fn connection_timeout(&self) -> Duration {
        Duration::from_millis(self.connection_timeout_ms)
    }

    pub fn response_timeout(&self) -> Duration {
        Duration::from_millis(self.response_timeout_ms)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct CardProfileLockConfig {
    pub coordinator_enabled: bool,
    pub coordinator_batch_size: u16,
    pub lease_duration_ms: u64,
    pub renew_interval_ms: u64,
}

impl CardProfileLockConfig {
    pub fn lease_duration(&self) -> Duration {
        Duration::from_millis(self.lease_duration_ms)
    }

    pub fn renew_interval(&self) -> Duration {
        Duration::from_millis(self.renew_interval_ms)
    }
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

impl KafkaProducerConfig {
    pub fn delivery_timeout(&self) -> Duration {
        Duration::from_millis(self.delivery_timeout_ms)
    }
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

impl KafkaReceiptConsumerConfig {
    pub fn session_timeout(&self) -> Duration {
        Duration::from_millis(self.session_timeout_ms)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaAdminConfig {
    pub request_timeout_ms: u64,
    pub partitions: u32,
    pub replication_factor: u32,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
}

impl KafkaAdminConfig {
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
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

impl ProviderCoreProvisioningConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct WalRecoveryConfig {
    pub enabled: bool,
    pub worker_id: String,
    pub batch_size: u16,
    pub poll_interval_ms: u64,
    pub lease_duration_ms: u64,
    pub stale_after_ms: u64,
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl WalRecoveryConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }
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

impl ProviderKafkaAccessConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderOperationalProfileSchedulerConfig {
    pub enabled: bool,
    pub batch_size: u16,
    pub poll_interval_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ObjectStorageConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub card_issuance_bucket: String,
    pub max_upload_bytes: usize,
    pub batch_retention_days: i64,
}
