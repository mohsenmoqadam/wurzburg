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
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
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
    pub producer: KafkaProducerConfig,
    pub admin: KafkaAdminConfig,
    pub consumer_defaults: KafkaConsumerDefaultsConfig,
    pub nuremberg: NurembergConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaProducerConfig {
    pub bootstrap_servers: String,
    pub client_id: String,
    pub message_timeout_ms: u64,
    pub max_request_size: u64,
    pub retries: u32,
    pub security_protocol: String,
    pub security_cert: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaAdminConfig {
    pub bootstrap_servers: String,
    pub request_timeout_ms: u64,
    pub kafka_bin_dir: String,
    pub partitions: u32,
    pub replication_factor: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConsumerDefaultsConfig {
    pub bootstrap_servers: String,
    pub session_timeout_ms: u64,
    pub auto_offset_reset: String,
    pub security_protocol: String,
    pub security_cert: String,
    pub sasl_mechanism: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NurembergProducerConfig {
    pub bootstrap_servers: String,
    pub client_id: String,
    pub message_timeout_ms: u32,
    pub max_request_size: u32,
    pub retries: u32,
    pub security_protocol: String,
    pub security_cert: String,
    pub sasl_mechanism: String,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub topic_name: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NurembergConfig {
    pub producer: NurembergProducerConfig,
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
    pub ledger_id: u32,
    pub provider_account_code: u16,
    pub user_account_code: u16,
    pub system_account_code: u16,
    pub transfer_code: u16,
    pub platform_fee_account_id: String,
    pub cms_settlement_account_id: String,
}

// --- Implementations ---
impl TigerBeetleConfig {
    pub fn batch_timeout(&self) -> Duration {
        Duration::from_millis(self.batch_timeout_ms)
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

        config
            .try_deserialize()
            .context("Failed to deserialize configuration")
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
    pub fn message_timeout(&self) -> Duration {
        Duration::from_millis(self.message_timeout_ms)
    }
}

impl KafkaAdminConfig {
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
}

impl KafkaConsumerDefaultsConfig {
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
}
