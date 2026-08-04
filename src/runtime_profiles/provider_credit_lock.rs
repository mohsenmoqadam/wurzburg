use deadpool_redis::{Pool, redis};
use uuid::Uuid;

use crate::config::CardProfileLockConfig;

const RELEASE_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"#;

const ENSURE_SCRIPT: &str = r#"
local owner = redis.call('GET', KEYS[1])
if not owner then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
if owner == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCreditLock {
    key: String,
    token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderCreditLockOutcome {
    Acquired(ProviderCreditLock),
    Busy,
}

#[derive(Debug)]
pub enum ProviderCreditLockError {
    PoolUnavailable,
    CommandFailed,
}

impl ProviderCreditLockError {
    pub fn diagnostic_kind(&self) -> &'static str {
        match self {
            Self::PoolUnavailable => "pool_unavailable",
            Self::CommandFailed => "command_failed",
        }
    }
}

#[derive(Clone)]
pub struct ProviderCreditLockManager {
    pool: Pool,
    config: CardProfileLockConfig,
}

impl ProviderCreditLockManager {
    pub fn new(pool: Pool, config: CardProfileLockConfig) -> Self {
        Self { pool, config }
    }

    #[tracing::instrument(skip(self), fields(db.system="dragonfly", db.operation.name="provider_credit_lock.acquire", provider.id=%provider_id, operation.id=%operation_id))]
    pub async fn acquire(
        &self,
        provider_id: Uuid,
        operation_id: Uuid,
    ) -> Result<ProviderCreditLockOutcome, ProviderCreditLockError> {
        let lock = ProviderCreditLock {
            key: format!("Lock-Provider-Credit:{provider_id}"),
            token: operation_id.to_string(),
        };
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|_| ProviderCreditLockError::PoolUnavailable)?;
        let response: Option<String> = redis::cmd("SET")
            .arg(&lock.key)
            .arg(&lock.token)
            .arg("NX")
            .arg("PX")
            .arg(self.config.lease_duration_ms)
            .query_async(&mut connection)
            .await
            .map_err(|_| ProviderCreditLockError::CommandFailed)?;
        Ok(match response.as_deref() {
            Some("OK") => ProviderCreditLockOutcome::Acquired(lock),
            _ => ProviderCreditLockOutcome::Busy,
        })
    }

    #[tracing::instrument(skip(self), fields(db.system="dragonfly", db.operation.name="provider_credit_lock.ensure", provider.id=%provider_id, operation.id=%operation_id))]
    pub async fn ensure(
        &self,
        provider_id: Uuid,
        operation_id: Uuid,
    ) -> Result<Option<ProviderCreditLock>, ProviderCreditLockError> {
        let lock = ProviderCreditLock {
            key: format!("Lock-Provider-Credit:{provider_id}"),
            token: operation_id.to_string(),
        };
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|_| ProviderCreditLockError::PoolUnavailable)?;
        let owned: i64 = redis::Script::new(ENSURE_SCRIPT)
            .key(&lock.key)
            .arg(&lock.token)
            .arg(self.config.lease_duration_ms)
            .invoke_async(&mut connection)
            .await
            .map_err(|_| ProviderCreditLockError::CommandFailed)?;
        Ok((owned == 1).then_some(lock))
    }

    #[tracing::instrument(skip(self, lock), fields(db.system="dragonfly", db.operation.name="provider_credit_lock.release"))]
    pub async fn release(
        &self,
        lock: &ProviderCreditLock,
    ) -> Result<bool, ProviderCreditLockError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|_| ProviderCreditLockError::PoolUnavailable)?;
        let released: i64 = redis::Script::new(RELEASE_SCRIPT)
            .key(&lock.key)
            .arg(&lock.token)
            .invoke_async(&mut connection)
            .await
            .map_err(|_| ProviderCreditLockError::CommandFailed)?;
        Ok(released == 1)
    }
}
