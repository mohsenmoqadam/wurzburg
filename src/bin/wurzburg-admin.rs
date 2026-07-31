use anyhow::{Context, Result, bail};
use deadpool_redis::{Config as RedisConfig, Runtime};
use wurzburg::{
    config::{MigrationConfig, Settings},
    db::oracle::{prepare_oracle_schema, verify_oracle_schema},
    messaging::MessageBrokerAdmin,
    object_storage::{ObjectStorage, initialize_bucket},
    telemetry,
};

const RESET_CONFIRMATION: &str = "--confirm-non-production-reset";

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::new().context("failed to load Wurzburg configuration")?;
    telemetry::tracing::init(&settings.telemetry)?;
    let result = run(&settings, std::env::args().skip(1).collect()).await;
    telemetry::tracing::shutdown();
    result
}

async fn run(settings: &Settings, arguments: Vec<String>) -> Result<()> {
    match arguments
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["db", "migrate"] => migrate_database(settings, false).await,
        ["db", "reset", confirmation] if *confirmation == RESET_CONFIRMATION => {
            migrate_database(settings, true).await
        }
        ["db", "reset", ..] => {
            bail!("database reset requires the explicit {RESET_CONFIRMATION} argument")
        }
        ["db", "verify"] => verify_database(settings).await,
        ["kafka", "verify"] => verify_kafka(settings),
        ["dragonfly", "verify"] => verify_dragonfly(settings).await,
        ["object-storage", "init"] => initialize_object_storage(settings).await,
        ["object-storage", "verify"] => verify_object_storage(settings).await,
        ["dependencies", "verify"] => {
            verify_kafka(settings)?;
            verify_dragonfly(settings).await?;
            verify_object_storage(settings).await
        }
        ["doctor"] => {
            verify_database(settings).await?;
            verify_kafka(settings)?;
            verify_dragonfly(settings).await?;
            verify_object_storage(settings).await
        }
        _ => bail!(usage()),
    }
}

async fn initialize_object_storage(settings: &Settings) -> Result<()> {
    initialize_bucket(&settings.object_storage).await?;
    tracing::info!("MinIO application bucket is ready");
    Ok(())
}

async fn verify_object_storage(settings: &Settings) -> Result<()> {
    ObjectStorage::new(&settings.object_storage)?
        .verify_bucket()
        .await?;
    tracing::info!("MinIO bucket verification succeeded");
    Ok(())
}

async fn migrate_database(settings: &Settings, reset: bool) -> Result<()> {
    if reset && Settings::is_production() {
        bail!("database reset is forbidden in production");
    }
    let migrations = MigrationConfig {
        enabled: true,
        force_recreate: reset,
    };
    prepare_oracle_schema(&settings.database, &migrations)
        .await
        .map_err(anyhow::Error::msg)?;
    tracing::info!(reset, "Oracle schema is ready");
    Ok(())
}

async fn verify_database(settings: &Settings) -> Result<()> {
    verify_oracle_schema(&settings.database, &settings.migrations)
        .await
        .map_err(anyhow::Error::msg)?;
    tracing::info!("Oracle schema verification succeeded");
    Ok(())
}

fn verify_kafka(settings: &Settings) -> Result<()> {
    let admin = MessageBrokerAdmin::new(&settings.kafka)?;
    admin.verify_topics([
        settings.kafka.outbox_relay.topic.as_str(),
        settings.kafka.materialization_receipts.topic.as_str(),
    ])?;
    tracing::info!("Kafka topic verification succeeded");
    Ok(())
}

async fn verify_dragonfly(settings: &Settings) -> Result<()> {
    let pool = RedisConfig::from_url(settings.redis.url.clone())
        .create_pool(Some(Runtime::Tokio1))
        .context("failed to create Dragonfly verification pool")?;
    let mut connection = pool.get().await.context("failed to connect to Dragonfly")?;
    let response: String = redis::cmd("PING")
        .query_async(&mut connection)
        .await
        .context("Dragonfly PING failed")?;
    anyhow::ensure!(
        response == "PONG",
        "Dragonfly returned an invalid PING response"
    );
    tracing::info!("Dragonfly verification succeeded");
    Ok(())
}

fn usage() -> &'static str {
    "usage: wurzburg-admin <db migrate|db verify|db reset --confirm-non-production-reset|kafka verify|dragonfly verify|object-storage init|object-storage verify|dependencies verify|doctor>"
}
