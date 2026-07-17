use axum::http::{HeaderMap, Method};
use sha2::{Digest, Sha256};

use crate::api::{error::ApiError, result_codes::WurzburgResultCode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn from_validated(value: impl Into<String>) -> Result<Self, ApiError> {
        let value = value.into();
        validate_idempotency_key_value(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub fn require_idempotency_key(headers: &HeaderMap) -> Result<IdempotencyKey, ApiError> {
    let value = headers
        .get("Idempotency-Key")
        .ok_or_else(|| ApiError::new(WurzburgResultCode::MissingIdempotencyKey))?
        .to_str()
        .map_err(|_| ApiError::new(WurzburgResultCode::InvalidIdempotencyKey))?;

    IdempotencyKey::from_validated(value)
}

pub fn mutating_method_requires_idempotency(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

pub fn canonical_request_hash(method: &Method, path: &str, body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(method.as_str().as_bytes());
    hasher.update(b"\n");
    hasher.update(path.as_bytes());
    hasher.update(b"\n");
    hasher.update(body);
    format!("{:x}", hasher.finalize())
}

fn validate_idempotency_key_value(value: &str) -> Result<(), ApiError> {
    // WSO2 preserves this value byte-for-byte, so Wurzburg keeps the accepted
    // character set intentionally small and deterministic before hashing or
    // storing it in Oracle.
    if value.is_empty()
        || value.len() > 255
        || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(ApiError::new(WurzburgResultCode::InvalidIdempotencyKey));
    }

    Ok(())
}
