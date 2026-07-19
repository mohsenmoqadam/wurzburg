use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task;

use wurzburg::{
    api::{router::build_app_router, swagger::swagger_router},
    config::Settings,
    db::oracle::verify_oracle_schema,
    kafka::{outbox_relay::start_outbox_relay, receipt_consumer::start_receipt_consumer},
    state::AppState,
    telemetry,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load config
    let mut settings = Settings::new().expect("Failed to load configuration");
    settings.apply_runtime_instance_identity();

    // 2. Init telemetry
    telemetry::tracing::init(&settings.telemetry)?;
    tracing::info!("Starting server in {} environment", Settings::environment());

    // 3. Runtime replicas only verify schema. One deployment job or explicit
    // developer command owns migration and destructive reset operations.
    verify_oracle_schema(&settings.database, &settings.migrations).await?;

    // 4. Init state (DB, Redis)
    let app_state = Arc::new(AppState::new(settings.clone()).await?);
    let outbox_relay = start_outbox_relay(
        app_state.db.clone(),
        app_state.kafka_producer.clone(),
        settings.kafka.outbox_relay.clone(),
    );
    let receipt_consumer = start_receipt_consumer(app_state.db.clone(), settings.kafka.clone())?;

    // 5. Build main API router
    let app = build_app_router(app_state.clone());

    // 6. Setup main listener
    let main_addr = format!("{}:{}", settings.server.host, settings.server.port);
    let main_listener = TcpListener::bind(&main_addr).await?;
    tracing::info!("Main server listening on {}", main_addr);

    // 7. Setup Swagger server (if enabled)
    if settings.swagger.enabled {
        let swagger_addr = format!("{}:{}", settings.swagger.host, settings.swagger.port);
        let swagger_path = settings.swagger.path.clone();

        task::spawn(async move {
            let swagger_app =
                swagger_router(&swagger_path, &settings.server.host, settings.server.port);
            let swagger_listener = TcpListener::bind(&swagger_addr).await.unwrap();
            tracing::info!(
                "Swagger UI listening on http://{}{}",
                swagger_addr,
                swagger_path
            );
            axum::serve(swagger_listener, swagger_app).await.unwrap();
        });
    }

    // 8. Run main server with graceful shutdown
    let server_result = axum::serve(main_listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await;

    if let Some(outbox_relay) = outbox_relay {
        outbox_relay.shutdown().await;
    }
    if let Some(receipt_consumer) = receipt_consumer {
        receipt_consumer.shutdown().await;
    }
    server_result?;

    // 9. Shutdown telemetry after server stops
    telemetry::tracing::shutdown();
    tracing::info!("Server shutdown complete");

    Ok(())
}

/// Await the CTRL+C signal for graceful shutdown
async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install CTRL+C signal handler");
    tracing::info!("Shutdown signal received, starting graceful shutdown...");
}
