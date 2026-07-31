use anyhow::{Result, anyhow};
use sha2::{Digest, Sha256};

pub(super) fn validate_object_key_and_size(
    object_key: &str,
    size: usize,
    max_upload_bytes: usize,
) -> Result<()> {
    if object_key.is_empty()
        || object_key.len() > 1000
        || object_key.starts_with('/')
        || object_key.contains("..")
        || !object_key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.'))
    {
        return Err(anyhow!("invalid object key"));
    }
    if size > max_upload_bytes {
        return Err(anyhow!("object exceeds configured size limit"));
    }
    Ok(())
}

pub(crate) fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::{sha256, validate_object_key_and_size};

    #[test]
    fn checksum_is_stable() {
        assert_eq!(
            sha256(b"wurzburg"),
            "5a2b3d539f2a7e9a03c2b4377808891335c77d967dfafd1f47bd551ef417b590"
        );
    }

    #[test]
    fn object_key_validation_rejects_traversal_and_oversized_objects() {
        assert!(validate_object_key_and_size("../secret", 1, 10).is_err());
        assert!(validate_object_key_and_size("issuance/result.csv", 11, 10).is_err());
        assert!(validate_object_key_and_size("issuance/result.csv", 10, 10).is_ok());
    }
}
