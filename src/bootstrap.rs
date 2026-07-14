use anyhow::{Context, Result};

use crate::config::Settings;
use crate::kafka::AppKafkaAdmin;

/// Bootstraps nuremberg kafka infrastructure: topics, users, and ACLs.
pub async fn seed_nuremberg_kafka_infrastructure(config: &Settings) -> Result<()> {
    tracing::info!("Starting Kafka infrastructure bootstrapping...");
    let admin = AppKafkaAdmin::new(config)?;

    // 1. Extract OWNED copies of the data you need
    let producer_user = config
        .kafka
        .nuremberg
        .producer
        .sasl_username
        .clone()
        .unwrap();
    let producer_password = config
        .kafka
        .nuremberg
        .producer
        .sasl_password
        .clone()
        .unwrap();

    // CLONE the topic name here so it is an owned String, not a reference
    let topic_name = config.kafka.nuremberg.producer.topic_name.clone();

    // Using spawn_blocking because admin shell scripts block the current thread
    tokio::task::spawn_blocking(move || -> Result<()> {
        // 1. Create the Topic
        admin.create_topic(&topic_name)?;
        tracing::debug!("Ensured Kafka topic '{}' exists.", &topic_name);

        // 2. Create the SCRAM user for the producer
        admin.create_scram_user(&producer_user, &producer_password)?;
        tracing::debug!("Ensured SCRAM user '{}' exists.", &producer_user);

        // 3. Grant Producer ACLs exclusively to the producer client
        admin.grant_producer_acls(&topic_name, &producer_user)?;
        tracing::debug!(
            "Granted producer ACLs on '{}' to '{}'.",
            &topic_name,
            &producer_user
        );

        Ok(())
    })
    .await
    .context("Failed to join blocking task for Kafka bootstrap")?
    .context("Kafka infrastructure seeding failed")?;

    tracing::info!("Kafka infrastructure bootstrapping completed successfully.");
    Ok(())
}
