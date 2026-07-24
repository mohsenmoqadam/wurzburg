use ring::{
    aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey},
    rand::{SecureRandom, SystemRandom},
};
use uuid::Uuid;

use crate::{
    config::{KafkaConfig, ProviderKafkaAccessConfig},
    domain::provider::{NewProviderKafkaAccess, NewProviderKafkaCredential},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialCipherError {
    Configuration,
    Encryption,
    Decryption,
    InvalidCiphertext,
}

impl CredentialCipherError {
    pub fn diagnostic_kind(self) -> &'static str {
        match self {
            Self::Configuration => "configuration",
            Self::Encryption => "encryption",
            Self::Decryption => "decryption",
            Self::InvalidCiphertext => "invalid_ciphertext",
        }
    }
}

pub struct SecretBytes(Vec<u8>);

impl SecretBytes {
    pub fn new(value: Vec<u8>) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &[u8] {
        &self.0
    }

    pub fn expose_utf8(&self) -> Result<&str, CredentialCipherError> {
        std::str::from_utf8(&self.0).map_err(|_| CredentialCipherError::Decryption)
    }
}

impl serde::Serialize for SecretBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.expose_utf8().map_err(serde::ser::Error::custom)?)
    }
}

impl std::fmt::Display for CredentialCipherError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.diagnostic_kind())
    }
}

impl Drop for SecretBytes {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

#[derive(Clone)]
pub struct ProviderKafkaCredentialCipher {
    key: std::sync::Arc<LessSafeKey>,
    key_version: String,
}

#[derive(Clone)]
pub struct ProviderKafkaCredentialFactory {
    cipher: ProviderKafkaCredentialCipher,
    bootstrap_servers: Vec<String>,
    security_protocol: String,
    sasl_mechanism: String,
    security_cert: Option<String>,
}

impl ProviderKafkaCredentialFactory {
    pub fn from_config(
        kafka: &KafkaConfig,
        provider_kafka_access: &ProviderKafkaAccessConfig,
    ) -> Result<Self, CredentialCipherError> {
        let cipher = ProviderKafkaCredentialCipher::from_config(provider_kafka_access)?;
        let bootstrap_servers = kafka
            .bootstrap_servers
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if bootstrap_servers.is_empty() {
            return Err(CredentialCipherError::Configuration);
        }
        let security_cert = kafka
            .security_cert
            .as_ref()
            .map(std::fs::read_to_string)
            .transpose()
            .map_err(|_| CredentialCipherError::Configuration)?;
        Ok(Self {
            cipher,
            bootstrap_servers,
            security_protocol: kafka.security_protocol.clone(),
            sasl_mechanism: kafka
                .sasl_mechanism
                .clone()
                .ok_or(CredentialCipherError::Configuration)?,
            security_cert,
        })
    }

    pub fn prepare(
        &self,
        provider_id: Uuid,
    ) -> Result<NewProviderKafkaAccess, CredentialCipherError> {
        let simple = provider_id.simple();
        let secret = SecretBytes::new(Uuid::new_v4().simple().to_string().into_bytes());
        let credential_version = 1;
        let password_ciphertext = self
            .cipher
            .encrypt(provider_id, credential_version, &secret)?;
        Ok(NewProviderKafkaAccess {
            provider_kafka_access_id: Uuid::new_v4(),
            provider_kafka_credential_id: Uuid::new_v4(),
            topic_name: format!("provider.events.{simple}"),
            username: format!("provider_user_{simple}"),
            consumer_group: format!("provider_group_{simple}"),
            password_ciphertext,
            encryption_key_version: self.cipher.key_version().to_string(),
            credential_version,
            security_protocol: self.security_protocol.clone(),
            sasl_mechanism: self.sasl_mechanism.clone(),
            bootstrap_servers: self.bootstrap_servers.clone(),
        })
    }

    pub fn prepare_credential(
        &self,
        provider_id: Uuid,
        credential_version: u64,
    ) -> Result<NewProviderKafkaCredential, CredentialCipherError> {
        if credential_version == 0 {
            return Err(CredentialCipherError::Configuration);
        }
        let secret = SecretBytes::new(Uuid::new_v4().simple().to_string().into_bytes());
        Ok(NewProviderKafkaCredential {
            provider_kafka_credential_id: Uuid::new_v4(),
            password_ciphertext: self
                .cipher
                .encrypt(provider_id, credential_version, &secret)?,
            encryption_key_version: self.cipher.key_version().to_string(),
            credential_version,
        })
    }

    pub fn cipher(&self) -> &ProviderKafkaCredentialCipher {
        &self.cipher
    }

    pub fn security_cert(&self) -> Option<&str> {
        self.security_cert.as_deref()
    }
}

impl ProviderKafkaCredentialCipher {
    pub fn from_config(
        provider_kafka_access: &ProviderKafkaAccessConfig,
    ) -> Result<Self, CredentialCipherError> {
        let encoded = match provider_kafka_access.master_key_source.trim() {
            "env" => std::env::var(provider_kafka_access.master_key_environment_variable.trim())
                .map_err(|_| CredentialCipherError::Configuration)?,
            "file" => {
                let path = provider_kafka_access
                    .master_key_file
                    .as_deref()
                    .ok_or(CredentialCipherError::Configuration)?;
                std::fs::read_to_string(path).map_err(|_| CredentialCipherError::Configuration)?
            }
            _ => return Err(CredentialCipherError::Configuration),
        };
        Self::from_hex_key(
            encoded.trim(),
            provider_kafka_access.encryption_key_version.clone(),
        )
    }

    pub fn from_environment(
        variable_name: &str,
        key_version: String,
    ) -> Result<Self, CredentialCipherError> {
        let encoded =
            std::env::var(variable_name).map_err(|_| CredentialCipherError::Configuration)?;
        Self::from_hex_key(&encoded, key_version)
    }

    pub fn from_hex_key(encoded: &str, key_version: String) -> Result<Self, CredentialCipherError> {
        let key_bytes = decode_hex(encoded)?;
        if key_bytes.len() != 32 || key_version.trim().is_empty() {
            return Err(CredentialCipherError::Configuration);
        }
        let key = UnboundKey::new(&AES_256_GCM, &key_bytes)
            .map_err(|_| CredentialCipherError::Configuration)?;
        Ok(Self {
            key: std::sync::Arc::new(LessSafeKey::new(key)),
            key_version,
        })
    }

    pub fn key_version(&self) -> &str {
        &self.key_version
    }

    pub fn encrypt(
        &self,
        provider_id: Uuid,
        credential_version: u64,
        secret: &SecretBytes,
    ) -> Result<String, CredentialCipherError> {
        let mut nonce_bytes = [0_u8; 12];
        SystemRandom::new()
            .fill(&mut nonce_bytes)
            .map_err(|_| CredentialCipherError::Encryption)?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let mut ciphertext = secret.expose().to_vec();
        self.key
            .seal_in_place_append_tag(
                nonce,
                Aad::from(aad(provider_id, credential_version, &self.key_version)),
                &mut ciphertext,
            )
            .map_err(|_| CredentialCipherError::Encryption)?;
        let mut envelope = nonce_bytes.to_vec();
        envelope.extend_from_slice(&ciphertext);
        Ok(encode_hex(&envelope))
    }

    pub fn decrypt(
        &self,
        provider_id: Uuid,
        credential_version: u64,
        key_version: &str,
        ciphertext: &str,
    ) -> Result<SecretBytes, CredentialCipherError> {
        if key_version != self.key_version {
            return Err(CredentialCipherError::Configuration);
        }
        let envelope = decode_hex(ciphertext)?;
        if envelope.len() <= 12 + AES_256_GCM.tag_len() {
            return Err(CredentialCipherError::InvalidCiphertext);
        }
        let (nonce_bytes, encrypted) = envelope.split_at(12);
        let nonce_array: [u8; 12] = nonce_bytes
            .try_into()
            .map_err(|_| CredentialCipherError::InvalidCiphertext)?;
        let mut encrypted = encrypted.to_vec();
        let plaintext = self
            .key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce_array),
                Aad::from(aad(provider_id, credential_version, key_version)),
                &mut encrypted,
            )
            .map_err(|_| CredentialCipherError::Decryption)?;
        let length = plaintext.len();
        encrypted.truncate(length);
        Ok(SecretBytes::new(encrypted))
    }
}

fn aad(provider_id: Uuid, credential_version: u64, key_version: &str) -> Vec<u8> {
    format!(
        "wurzburg.provider.kafka.v1:{}:{credential_version}:{key_version}",
        provider_id.simple()
    )
    .into_bytes()
}

fn decode_hex(value: &str) -> Result<Vec<u8>, CredentialCipherError> {
    if !value.len().is_multiple_of(2) {
        return Err(CredentialCipherError::Configuration);
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(value: u8) -> Result<u8, CredentialCipherError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(CredentialCipherError::Configuration),
    }
}

fn encode_hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use crate::config::ProviderKafkaAccessConfig;

    use super::{ProviderKafkaCredentialCipher, SecretBytes};
    use uuid::Uuid;

    #[test]
    fn credential_cipher_round_trips_only_with_matching_aad() {
        let cipher =
            ProviderKafkaCredentialCipher::from_hex_key(&"11".repeat(32), "test-v1".to_string())
                .unwrap();
        let provider_id = Uuid::new_v4();
        let encrypted = cipher
            .encrypt(provider_id, 1, &SecretBytes::new(b"sensitive".to_vec()))
            .unwrap();
        assert!(!encrypted.contains("sensitive"));
        assert_eq!(
            cipher
                .decrypt(provider_id, 1, "test-v1", &encrypted)
                .unwrap()
                .expose(),
            b"sensitive"
        );
        assert!(
            cipher
                .decrypt(Uuid::new_v4(), 1, "test-v1", &encrypted)
                .is_err()
        );
    }

    #[test]
    fn credential_cipher_loads_master_key_from_file_source() {
        let path = std::env::temp_dir().join(format!(
            "wurzburg-provider-kafka-key-{}.hex",
            Uuid::new_v4()
        ));
        std::fs::write(&path, format!("{}\n", "22".repeat(32))).unwrap();
        let config = ProviderKafkaAccessConfig {
            enabled: true,
            worker_id: "test-worker".to_string(),
            batch_size: 1,
            poll_interval_ms: 1,
            lease_duration_ms: 1,
            max_attempts: 1,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
            master_key_source: "file".to_string(),
            master_key_environment_variable: "UNUSED_PROVIDER_KAFKA_TEST_KEY".to_string(),
            master_key_file: Some(path.to_string_lossy().into_owned()),
            encryption_key_version: "file-v1".to_string(),
            scram_iterations: 8192,
        };

        let cipher = ProviderKafkaCredentialCipher::from_config(&config).unwrap();
        let provider_id = Uuid::new_v4();
        let encrypted = cipher
            .encrypt(provider_id, 1, &SecretBytes::new(b"file-secret".to_vec()))
            .unwrap();

        assert_eq!(
            cipher
                .decrypt(provider_id, 1, "file-v1", &encrypted)
                .unwrap()
                .expose(),
            b"file-secret"
        );

        let _ = std::fs::remove_file(path);
    }
}
