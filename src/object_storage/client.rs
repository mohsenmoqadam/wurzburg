use anyhow::{Context, Result, anyhow};
use s3::{Bucket, Region, creds::Credentials};

use crate::config::ObjectStorageConfig;

use super::model::StoredObject;
use super::validation::{sha256, validate_object_key_and_size};

#[derive(Clone)]
pub struct ObjectStorage {
    bucket: Box<Bucket>,
    max_upload_bytes: usize,
}

impl ObjectStorage {
    pub fn new(config: &ObjectStorageConfig) -> Result<Self> {
        let credentials = credentials(config)?;
        let region = region(config);
        let bucket = Bucket::new(&config.card_issuance_bucket, region, credentials)
            .context("invalid card-issuance bucket configuration")?
            .with_path_style();
        Ok(Self {
            bucket,
            max_upload_bytes: config.max_upload_bytes,
        })
    }

    #[tracing::instrument(skip(self), fields(storage.system="minio", storage.operation="verify_bucket"))]
    pub async fn verify_bucket(&self) -> Result<()> {
        self.bucket
            .list(String::new(), Some("/".to_string()))
            .await
            .map(|_| ())
            .map_err(|error| anyhow!("object-storage bucket verification failed: {error}"))
    }

    #[tracing::instrument(skip(self, bytes), fields(storage.system="minio", storage.operation="put", storage.size_bytes=bytes.len()))]
    pub async fn put_csv(&self, object_key: &str, bytes: &[u8]) -> Result<StoredObject> {
        validate_object_key_and_size(object_key, bytes.len(), self.max_upload_bytes)?;
        let response = self
            .bucket
            .put_object_with_content_type(object_key, bytes, "text/csv; charset=utf-8")
            .await
            .map_err(|error| anyhow!("object-storage put failed: {error}"))?;
        if !(200..300).contains(&response.status_code()) {
            return Err(anyhow!(
                "object-storage put returned HTTP {}",
                response.status_code()
            ));
        }
        Ok(StoredObject {
            checksum_sha256: sha256(bytes),
            size_bytes: bytes.len(),
        })
    }

    #[tracing::instrument(skip(self), fields(storage.system="minio", storage.operation="get"))]
    pub async fn get_csv(&self, object_key: &str) -> Result<Vec<u8>> {
        validate_object_key_and_size(object_key, 0, self.max_upload_bytes)?;
        let response = self
            .bucket
            .get_object(object_key)
            .await
            .map_err(|error| anyhow!("object-storage get failed: {error}"))?;
        if response.status_code() != 200 {
            return Err(anyhow!(
                "object-storage get returned HTTP {}",
                response.status_code()
            ));
        }
        let bytes = response.to_vec();
        validate_object_key_and_size(object_key, bytes.len(), self.max_upload_bytes)?;
        Ok(bytes)
    }
}

pub(super) fn credentials(config: &ObjectStorageConfig) -> Result<Credentials> {
    Credentials::new(
        Some(&config.access_key),
        Some(&config.secret_key),
        None,
        None,
        None,
    )
    .context("invalid object-storage credentials")
}

pub(super) fn region(config: &ObjectStorageConfig) -> Region {
    Region::Custom {
        region: config.region.clone(),
        endpoint: config.endpoint.clone(),
    }
}
