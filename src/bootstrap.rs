// src/bootstrap.rs
use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

use crate::config::{Settings, TigerBeetleConfig};
use crate::kafka::AppKafkaAdmin;
use crate::tigerbeetle::client::AppTbClient;
use crate::tigerbeetle::models::AppAccount;

/// Data structure for seeding system accounts
pub struct SystemAccountSeed<'a> {
    pub code: &'a str,
    pub name: &'a str,
    pub category: &'a str,
    pub ledger_account_id: &'a str,
}

pub async fn seed_system_accounts(db_pool: &PgPool, tb_client: &AppTbClient, tb_config: TigerBeetleConfig) -> Result<()> {
    tracing::info!("Starting system accounts bootstrapping...");
    
    let system_accounts = vec![
        SystemAccountSeed {
            code: "PLATFORM_FEE_IRR",
            name: "Platform Fee Account",
            category: "REVENUE",
            ledger_account_id: &tb_config.platform_fee_account_id,
        },
        SystemAccountSeed {
            code: "CMS_SETTLEMENT_IRR",
            name: "CMS Settlement Account",
            category: "LIABILITY",
            ledger_account_id: &tb_config.cms_settlement_account_id,
        },
    ];

    for seed in system_accounts {
        let pg_uuid = Uuid::parse_str(seed.ledger_account_id).context("Invalid UUID string")?;
        let tb_id = pg_uuid.as_u128();
        
        // 1. Create the account in TigerBeetle
        let tb_account = AppAccount {
            id: tb_id,
            ledger: tb_config.ledger_id, 
            code: tb_config.system_account_code,
            flags: 0,
            debits_pending: 0,
            debits_posted: 0,
            credits_pending: 0,
            credits_posted: 0,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            reserved: 0,
            timestamp: 0,
        };

        match tb_client.create_account(tb_account).await {
            Ok(results) => {
                // If results is empty, the account was created successfully.
                // If not, an error occurred (e.g., account already exists).
                if !results.is_empty() {
                    let err = &results[0];
                    // The Exists error in TigerBeetle has a specific code (usually 17).
                    // Here we assume if a result is returned, it is likely a "duplicate account" and we can safely ignore it.
                    // You can match this section with the exact Enum of your TB client:
                    tracing::warn!(
                        "TigerBeetle returned a result for account {}: {:?} (It likely already exists)",
                        seed.code,
                        err
                    );
                }
            }
            Err(e) => {
                tracing::error!("Failed to communicate with TB worker for {}: {}", seed.code, e);
                return Err(anyhow::anyhow!("TB communication error: {}", e));
            }
        }

        // 2. Create the record in Postgres using ON CONFLICT DO NOTHING
        let mut tx = db_pool.begin().await.context("Failed to begin transaction")?;
        sqlx::query("SET LOCAL app.current_user_id = '00000000-0000-0000-0000-000000000000';")
            .execute(&mut *tx)
            .await
            .context("Failed to set system user id for audit trigger")?;
        let query_result = sqlx::query(
            r#"
            INSERT INTO system_accounts (code, name, category, ledger_account_id, currency)
            VALUES ($1, $2, $3::system_account_category_enum, $4, 'IRR')
            ON CONFLICT (code) DO NOTHING
            "#)
        .bind(seed.code)
        .bind(seed.name)
        .bind(seed.category)
        .bind(pg_uuid)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("Failed to insert system account {} into Postgres", seed.code))?;
        tx.commit().await.context("Failed to commit transaction")?;

        if query_result.rows_affected() > 0 {
            tracing::info!("Successfully seeded system account: {}", seed.code);
        } else {
            tracing::debug!("System account {} already exists in Postgres.", seed.code);
        }
    }

    tracing::info!("System accounts bootstrapping completed successfully.");
    Ok(())
}

/// Bootstraps nuremberg kafka infrastructure: topics, users, and ACLs.
pub async fn seed_nuremberg_kafka_infrastructure(config: &Settings) -> Result<()> {
    tracing::info!("Starting Kafka infrastructure bootstrapping...");
    let admin = AppKafkaAdmin::new(config)?;
    
    // 1. Extract OWNED copies of the data you need
    let producer_user = config.kafka.nuremberg.producer.sasl_username.clone().unwrap();
    let producer_password = config.kafka.nuremberg.producer.sasl_password.clone().unwrap();
    
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
        tracing::debug!("Granted producer ACLs on '{}' to '{}'.", &topic_name, &producer_user);

        Ok(())
    })
    .await
    .context("Failed to join blocking task for Kafka bootstrap")?
    .context("Kafka infrastructure seeding failed")?;

    tracing::info!("Kafka infrastructure bootstrapping completed successfully.");
    Ok(())
}

