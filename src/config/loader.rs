use anyhow::{Context, Result};
use config::{Config, Environment, File};
use std::env;

use super::model::Settings;

impl Settings {
    /// Loads layered file configuration and applies environment overrides.
    pub fn new() -> Result<Self> {
        let run_mode = env::var("APP_ENVIRONMENT").unwrap_or_else(|_| "development".into());
        let config = Config::builder()
            .add_source(File::with_name("config/default"))
            .add_source(File::with_name(&format!("config/{run_mode}")).required(false))
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
        settings.validate()?;
        Ok(settings)
    }

    fn validate(&self) -> Result<()> {
        self.kafka.validate()?;
        anyhow::ensure!(
            self.card_profile_lock.lease_duration_ms >= 5_000,
            "Card-profile lock lease must be at least five seconds"
        );
        anyhow::ensure!(
            self.card_profile_lock.renew_interval_ms > 0
                && self.card_profile_lock.renew_interval_ms
                    < self.card_profile_lock.lease_duration_ms,
            "Card-profile lock renewal interval must be positive and shorter than its lease"
        );
        anyhow::ensure!(
            self.card_profile_lock.coordinator_batch_size > 0,
            "Card-profile coordinator batch size must be positive"
        );
        if self.provider_kafka_access.enabled {
            let master_key_source = self.provider_kafka_access.master_key_source.trim();
            anyhow::ensure!(
                matches!(master_key_source, "env" | "file"),
                "Provider Kafka master key source must be either 'env' or 'file'"
            );
            if master_key_source == "env" {
                anyhow::ensure!(
                    !self
                        .provider_kafka_access
                        .master_key_environment_variable
                        .trim()
                        .is_empty(),
                    "Provider Kafka master-key environment variable is required"
                );
            }
            if master_key_source == "file" {
                anyhow::ensure!(
                    self.provider_kafka_access
                        .master_key_file
                        .as_deref()
                        .is_some_and(|value| !value.trim().is_empty()),
                    "Provider Kafka master-key file is required"
                );
            }
            anyhow::ensure!(
                !self
                    .provider_kafka_access
                    .encryption_key_version
                    .trim()
                    .is_empty(),
                "Provider Kafka encryption key version is required"
            );
            anyhow::ensure!(
                self.provider_kafka_access.scram_iterations >= 4096,
                "Provider Kafka SCRAM iterations must be at least 4096"
            );
            anyhow::ensure!(
                self.kafka
                    .admin
                    .sasl_username
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty()),
                "Provider Kafka administration username is required"
            );
            anyhow::ensure!(
                self.kafka
                    .admin
                    .sasl_password
                    .as_deref()
                    .is_some_and(|value| !value.is_empty()),
                "Provider Kafka administration password is required"
            );
            anyhow::ensure!(
                self.provider_kafka_access.max_attempts > 0,
                "Provider Kafka max attempts must be positive"
            );
        }
        anyhow::ensure!(
            self.provider_operational_profile_scheduler.batch_size > 0,
            "Provider operational profile scheduler batch size must be positive"
        );
        anyhow::ensure!(
            self.provider_operational_profile_scheduler.poll_interval_ms > 0,
            "Provider operational profile scheduler poll interval must be positive"
        );
        anyhow::ensure!(
            self.wal_recovery.batch_size > 0
                && self.wal_recovery.poll_interval_ms > 0
                && self.wal_recovery.lease_duration_ms > 0
                && self.wal_recovery.stale_after_ms > self.wal_recovery.lease_duration_ms
                && self.wal_recovery.max_attempts > 0,
            "WAL recovery timing, batch size, and attempt limits are invalid"
        );
        anyhow::ensure!(
            !self.object_storage.endpoint.trim().is_empty(),
            "Object-storage endpoint is required"
        );
        anyhow::ensure!(
            !self.object_storage.card_issuance_bucket.trim().is_empty(),
            "Card-issuance object-storage bucket is required"
        );
        anyhow::ensure!(
            self.object_storage.max_upload_bytes > 0,
            "Object-storage maximum upload size must be positive"
        );
        anyhow::ensure!(
            self.object_storage.batch_retention_days > 0,
            "Card-issuance batch retention must be positive"
        );
        Ok(())
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
