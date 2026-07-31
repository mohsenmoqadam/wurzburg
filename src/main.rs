use std::{future::IntoFuture, sync::Arc};
use tokio::net::TcpListener;
use tokio::sync::watch;

use wurzburg::{
    api::{router::build_app_router, swagger::swagger_router},
    config::Settings,
    db::oracle::verify_oracle_schema,
    kafka::{outbox_relay::start_outbox_relay, receipt_consumer::start_receipt_consumer},
    services::{
        provider::ProviderService,
        provider_kafka::{ProviderKafkaService, start_provider_kafka_provisioning_worker},
        provider_operational_profile::start_provider_operational_profile_scheduler,
        provider_provisioning::start_provider_provisioning_worker,
    },
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

    // 4. Initialize dependency clients (Oracle, Dragonfly, Kafka, TigerBeetle, MinIO).
    let app_state = Arc::new(AppState::new(settings.clone()).await?);
    let tb_worker = app_state.tb_worker.clone();
    let outbox_relay = start_outbox_relay(
        app_state.db.clone(),
        app_state.kafka_producer.clone(),
        settings.kafka.outbox_relay.clone(),
    );
    let receipt_consumer = start_receipt_consumer(app_state.db.clone(), settings.kafka.clone())?;
    let provider_core_provisioning = start_provider_provisioning_worker(
        app_state.db.clone(),
        ProviderService::new(
            app_state.db.clone(),
            app_state.tb_client.clone(),
            settings.tigerbeetle.clone(),
            app_state.provider_kafka_credentials.clone(),
        ),
        settings.provider_core_provisioning.clone(),
    );
    let provider_kafka_provisioning = match app_state.provider_kafka_credentials.clone() {
        Some(credentials) => start_provider_kafka_provisioning_worker(
            app_state.db.clone(),
            ProviderKafkaService::new(
                app_state.db.clone(),
                app_state.kafka_admin.clone(),
                credentials,
                settings.provider_kafka_access.scram_iterations,
            ),
            settings.provider_kafka_access.clone(),
        ),
        None => None,
    };
    let provider_operational_profile_scheduler = start_provider_operational_profile_scheduler(
        app_state.db.clone(),
        settings.provider_operational_profile_scheduler.clone(),
    );

    // 5. Build main API router
    let app = build_app_router(app_state.clone());

    // 6. Setup main listener
    let main_addr = format!("{}:{}", settings.server.host, settings.server.port);
    let main_listener = TcpListener::bind(&main_addr).await?;
    tracing::info!("Main server listening on {}", main_addr);

    // A single signal stops every HTTP listener. Keeping Swagger on the same
    // lifecycle prevents an orphan listener from surviving API shutdown.
    let (http_shutdown, http_shutdown_receiver) = watch::channel(false);

    // 7. Setup Swagger server (if enabled)
    let swagger_server = if settings.swagger.enabled {
        let swagger_addr = format!("{}:{}", settings.swagger.host, settings.swagger.port);
        let swagger_path = settings.swagger.path.clone();
        let swagger_listener = TcpListener::bind(&swagger_addr).await?;
        let swagger_shutdown = http_shutdown_receiver.clone();
        let swagger_app =
            swagger_router(&swagger_path, &settings.server.host, settings.server.port);
        tracing::info!(
            "Swagger UI listening on http://{}{}",
            swagger_addr,
            swagger_path
        );

        Some(tokio::spawn(async move {
            axum::serve(swagger_listener, swagger_app)
                .with_graceful_shutdown(wait_for_shutdown(swagger_shutdown))
                .await
        }))
    } else {
        None
    };

    // 8. Run until CTRL+C/SIGTERM or an unexpected main-listener failure.
    let mut main_server = Box::pin(
        axum::serve(main_listener, app)
            .with_graceful_shutdown(wait_for_shutdown(http_shutdown_receiver))
            .into_future(),
    );
    let mut server_result = None;
    tokio::select! {
        result = &mut main_server => {
            server_result = Some(result);
            tracing::error!("Main HTTP server stopped unexpectedly; shutting down the process");
        }
        _ = shutdown_signal() => {}
    }
    let _ = http_shutdown.send(true);

    let graceful_shutdown = async {
        if server_result.is_none() {
            server_result = Some((&mut main_server).await);
        }
        let swagger_result = async {
            if let Some(swagger_server) = swagger_server {
                match swagger_server.await {
                    Ok(result) => result,
                    Err(error) => Err(std::io::Error::other(error)),
                }
            } else {
                Ok(())
            }
        };
        let outbox_shutdown = async {
            if let Some(handle) = outbox_relay {
                handle.shutdown().await;
            }
        };
        let receipt_shutdown = async {
            if let Some(handle) = receipt_consumer {
                handle.shutdown().await;
            }
        };
        let core_shutdown = async {
            if let Some(handle) = provider_core_provisioning {
                handle.shutdown().await;
            }
        };
        let kafka_shutdown = async {
            if let Some(handle) = provider_kafka_provisioning {
                handle.shutdown().await;
            }
        };
        let operational_profile_shutdown = async {
            if let Some(handle) = provider_operational_profile_scheduler {
                handle.shutdown().await;
            }
        };
        let tigerbeetle_shutdown = tb_worker.shutdown();
        let (swagger_result, (), (), (), (), (), ()) = tokio::join!(
            swagger_result,
            outbox_shutdown,
            receipt_shutdown,
            core_shutdown,
            kafka_shutdown,
            operational_profile_shutdown,
            tigerbeetle_shutdown
        );
        swagger_result
    };

    if tokio::time::timeout(
        settings.server.graceful_shutdown_timeout(),
        graceful_shutdown,
    )
    .await
    .is_err()
    {
        tracing::error!(
            timeout_ms = settings.server.graceful_shutdown_timeout_ms,
            "graceful shutdown deadline exceeded; remaining tasks will be cancelled"
        );
    }

    // 9. Telemetry flush has its own shorter best-effort deadline.
    telemetry::tracing::shutdown_with_timeout(settings.server.telemetry_shutdown_timeout()).await;
    tracing::info!("Server shutdown complete");

    if let Some(server_result) = server_result {
        server_result?;
    }

    Ok(())
}

/// Await the CTRL+C signal for graceful shutdown
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Failed to install SIGTERM signal handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.expect("Failed to install CTRL+C signal handler"),
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install CTRL+C signal handler");
    tracing::info!("Shutdown signal received, starting graceful shutdown...");
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::wait_for_shutdown;

    #[tokio::test]
    async fn shared_http_shutdown_signal_releases_waiters() {
        let (sender, receiver) = tokio::sync::watch::channel(false);
        let waiter = tokio::spawn(wait_for_shutdown(receiver));

        sender.send(true).unwrap();

        tokio::time::timeout(std::time::Duration::from_millis(100), waiter)
            .await
            .expect("shutdown waiter should be released")
            .expect("shutdown waiter task should complete");
    }
}
