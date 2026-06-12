use anyhow::{Context, Result};
use std::path::PathBuf;
use std::process::Command;

use crate::config::Settings;

/// A synchronous Kafka administrator module.
/// 
/// This struct relies on executing local Kafka shell scripts 
/// (e.g., `kafka-topics.sh`, `kafka-configs.sh`, `kafka-acls.sh`) via `std::process::Command`.
/// 
/// IMPORTANT: Because these operations are synchronous and block the current thread,
/// they must be executed inside `tokio::task::spawn_blocking` when called from an
/// asynchronous context (like an Axum handler) to prevent starving the Tokio executor.
#[derive(Clone)]
pub struct AppKafkaAdmin {
    /// The Kafka connection string (e.g., "localhost:9092").
    bootstrap_servers: String,
    /// The absolute path to the directory containing Kafka binaries/shell scripts.
    kafka_bin_dir: PathBuf,
    /// The default number of partitions to use when creating new topics.
    partitions: String,
    /// The default replication factor to use when creating new topics.
    replication_factor: String,
}

impl AppKafkaAdmin {
    /// Initializes a new instance of `AppKafkaAdmin` using the provided application settings.
    /// 
    /// Extracts the binary directory path, bootstrap servers, partition count, and replication
    /// factor from the configuration.
    pub fn new(config: &Settings) -> Result<Self> {
        let bin_dir = PathBuf::from(&config.kafka.admin.kafka_bin_dir);

        Ok(Self {
            bootstrap_servers: config.kafka.admin.bootstrap_servers.clone(),
            kafka_bin_dir: bin_dir,
            partitions: config.kafka.admin.partitions.to_string(),
            replication_factor: config.kafka.admin.replication_factor.to_string(),
        })
    }

    /// Creates a new Kafka topic synchronously.
    /// 
    /// This uses the `kafka-topics.sh` script. The configured default partitions 
    /// and replication factor are automatically applied.
    /// 
    /// This operation is idempotent: if the topic already exists, the script will output 
    /// a `TopicExistsException`, which is caught and ignored, returning a success `Ok(())`.
    pub fn create_provider_topic(&self, topic_name: &str) -> Result<()> {
        let script_path = self.kafka_bin_dir.join("kafka-topics.sh");
        
        let output = Command::new(&script_path)
            .args([
                "--bootstrap-server", &self.bootstrap_servers,
                "--create",
                "--topic", topic_name,
                "--partitions", &self.partitions,
                "--replication-factor", &self.replication_factor,
            ])
            .output()
            .with_context(|| format!("Failed to execute {:?}", script_path))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Ensure idempotency by ignoring errors when the topic is already present.
            if !stderr.contains("TopicExistsException") {
                return Err(anyhow::anyhow!("Failed to create topic: {}", stderr));
            }
        }

        Ok(())
    }

    /// Deletes an existing Kafka topic synchronously.
    /// 
    /// This uses the `kafka-topics.sh` script with the `--delete` flag.
    /// 
    /// This operation is idempotent: if the topic does not exist (or was already deleted), 
    /// the script will output an `UnknownTopicOrPartitionException`. This specific exception 
    /// is ignored, and the function successfully returns `Ok(())`.
    pub fn delete_provider_topic(&self, topic_name: &str) -> Result<()> {
        let script_path = self.kafka_bin_dir.join("kafka-topics.sh");
        
        let output = Command::new(&script_path)
            .args([
                "--bootstrap-server", &self.bootstrap_servers,
                "--delete",
                "--topic", topic_name,
            ])
            .output()
            .with_context(|| format!("Failed to execute {:?}", script_path))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Ensure idempotency by ignoring errors when the topic is already absent.
            if !stderr.contains("UnknownTopicOrPartitionException") {
                return Err(anyhow::anyhow!("Failed to delete topic: {}", stderr));
            }
        }

        Ok(())
    }

    /// Creates or updates a Kafka user with SCRAM-SHA-512 credentials.
    /// 
    /// Uses the `kafka-configs.sh` script to alter the 'users' entity type and add
    /// the SCRAM configuration string. If the user already exists, their password is updated.
    pub fn create_scram_user(&self, username: &str, password: &str) -> Result<()> {
        let config_string = format!("SCRAM-SHA-512=[password={}]", password);
        let script_path = self.kafka_bin_dir.join("kafka-configs.sh");
        
        let output = Command::new(&script_path)
            .args([
                "--bootstrap-server", &self.bootstrap_servers,
                "--alter",
                "--add-config", &config_string,
                "--entity-type", "users",
                "--entity-name", username,
            ])
            .output()
            .with_context(|| format!("Failed to execute {:?}", script_path))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("Failed to create user: {}", stderr));
        }

        Ok(())
    }

    /// Deletes the SCRAM-SHA-512 credentials for a specific Kafka user.
    /// 
    /// This effectively revokes the user's ability to authenticate. Uses the 
    /// `kafka-configs.sh` script to delete the SCRAM configuration from the 'users' entity.
    /// 
    /// This operation is idempotent: if the user or configuration does not exist,
    /// exceptions like `InvalidConfigurationException` are ignored.
    pub fn delete_scram_user(&self, username: &str) -> Result<()> {
        let script_path = self.kafka_bin_dir.join("kafka-configs.sh");
        
        let output = Command::new(&script_path)
            .args([
                "--bootstrap-server", &self.bootstrap_servers,
                "--alter",
                "--delete-config", "SCRAM-SHA-512",
                "--entity-type", "users",
                "--entity-name", username,
            ])
            .output()
            .with_context(|| format!("Failed to execute {:?}", script_path))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Ensure idempotency by ignoring non-existent configuration or user errors.
            if !stderr.contains("InvalidConfigurationException") && !stderr.contains("does not exist") {
                return Err(anyhow::anyhow!("Failed to delete user: {}", stderr));
            }
        }

        Ok(())
    }
    
    /// Grants necessary consumption ACLs to a designated user for a specific topic.
    /// 
    /// This process involves two synchronous script executions using `kafka-acls.sh`:
    /// 1. Grants 'Read' and 'Describe' operations to the user strictly on the specified topic.
    /// 2. Grants 'Read' operations to the user across all consumer groups (wildcard '*'), 
    ///    which is required for consumer group coordination.
    pub fn grant_consumer_acls(&self, topic_name: &str, username: &str) -> Result<()> {
        let script_path = self.kafka_bin_dir.join("kafka-acls.sh");
        let principal = format!("User:{}", username);

        // Step 1: Grant 'Read' and 'Describe' permissions on the specific Topic.
        let topic_output = Command::new(&script_path)
            .args([
                "--bootstrap-server", &self.bootstrap_servers,
                "--add",
                "--allow-principal", &principal,
                "--operation", "Read",
                "--operation", "Describe",
                "--topic", topic_name,
            ])
            .output()
            .with_context(|| format!("Failed to execute {:?}", script_path))?;

        if !topic_output.status.success() {
            let stderr = String::from_utf8_lossy(&topic_output.stderr);
            return Err(anyhow::anyhow!("Failed to grant topic ACLs: {}", stderr));
        }

        // Step 2: Grant 'Read' permission on all Consumer Groups ('*').
        // This is mandatory for consumers to join groups and commit offsets.
        let group_output = Command::new(&script_path)
            .args([
                "--bootstrap-server", &self.bootstrap_servers,
                "--add",
                "--allow-principal", &principal,
                "--operation", "Read",
                "--group", "*",
            ])
            .output()
            .with_context(|| format!("Failed to execute {:?}", script_path))?;

        if !group_output.status.success() {
            let stderr = String::from_utf8_lossy(&group_output.stderr);
            return Err(anyhow::anyhow!("Failed to grant group ACLs: {}", stderr));
        }

        Ok(())
    }

    /// Creates a generic Kafka topic synchronously.
    pub fn create_topic(&self, topic_name: &str) -> Result<()> {
        let script_path = self.kafka_bin_dir.join("kafka-topics.sh");

        let output = Command::new(&script_path)
            .args([
                "--bootstrap-server", &self.bootstrap_servers,
                "--create",
                "--topic", topic_name,
                "--partitions", &self.partitions,
                "--replication-factor", &self.replication_factor,
            ])
            .output()
            .with_context(|| format!("Failed to execute {:?}", script_path))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.contains("TopicExistsException") {
                return Err(anyhow::anyhow!("Failed to create topic: {}", stderr));
            }
        }

        Ok(())
    }

    /// Grants necessary production ACLs to a designated user for a specific topic.
    pub fn grant_producer_acls(&self, topic_name: &str, username: &str) -> Result<()> {
        let script_path = self.kafka_bin_dir.join("kafka-acls.sh");
        let principal = format!("User:{}", username);

        let output = Command::new(&script_path)
            .args([
                "--bootstrap-server", &self.bootstrap_servers,
                "--add",
                "--allow-principal", &principal,
                "--operation", "Write",
                "--operation", "Describe",
                "--topic", topic_name,
            ])
            .output()
            .with_context(|| format!("Failed to execute {:?}", script_path))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("Failed to grant producer ACLs: {}", stderr));
        }

        Ok(())
    }
}
