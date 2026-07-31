use anyhow::{Result, anyhow};
use s3::{Bucket, BucketConfiguration};

use crate::config::ObjectStorageConfig;

use super::client::{ObjectStorage, credentials, region};

/// Idempotently creates the application bucket for an explicit infrastructure
/// bootstrap command. Normal server processes only verify and use the bucket.
#[tracing::instrument(skip(config), fields(storage.system="minio", storage.operation="initialize_bucket"))]
pub async fn initialize_bucket(config: &ObjectStorageConfig) -> Result<()> {
    let storage = ObjectStorage::new(config)?;
    if storage.verify_bucket().await.is_ok() {
        return Ok(());
    }

    let response = Bucket::create_with_path_style(
        &config.card_issuance_bucket,
        region(config),
        credentials(config)?,
        BucketConfiguration::default(),
    )
    .await;

    match response {
        Ok(value) if (200..300).contains(&value.response_code) || value.response_code == 409 => {}
        Ok(value) => {
            return Err(anyhow!(
                "object-storage bucket creation returned HTTP {}",
                value.response_code
            ));
        }
        Err(error) => {
            // Another bootstrap instance may win the create race. A fresh
            // authenticated bucket read is the authoritative outcome.
            tracing::warn!(
                error.kind = "bucket_create",
                "object-storage bucket create requires verification"
            );
            let _ = error;
        }
    }
    storage.verify_bucket().await
}
