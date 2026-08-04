use std::{error::Error, fmt};

use deadpool_redis::Pool;
use uuid::Uuid;

use crate::config::CardProfileLockConfig;

const RENEW_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"#;

const RELEASE_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"#;

const ENSURE_PENDING_SCRIPT: &str = r#"
local owner = redis.call('GET', KEYS[1])
if not owner then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
elseif owner == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
    return 0
end
redis.call('DEL', KEYS[2])
return 1
"#;

#[derive(Debug, Clone)]
pub struct CardProfileLockManager {
    pool: Pool,
    config: CardProfileLockConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardMutationLock {
    lock_key: String,
    profile_key: String,
    token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CardProfileLockOutcome {
    Acquired(CardMutationLock),
    Busy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CardProfileLockError {
    PoolUnavailable,
    CommandFailed,
    OwnershipLost,
}

impl CardProfileLockError {
    pub fn diagnostic_kind(&self) -> &'static str {
        match self {
            Self::PoolUnavailable => "pool_unavailable",
            Self::CommandFailed => "command_failed",
            Self::OwnershipLost => "ownership_lost",
        }
    }
}

impl fmt::Display for CardProfileLockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.diagnostic_kind())
    }
}

impl Error for CardProfileLockError {}

impl CardProfileLockManager {
    pub fn new(pool: Pool, config: CardProfileLockConfig) -> Self {
        Self { pool, config }
    }

    #[tracing::instrument(
        skip(self, card_number),
        fields(db.system = "dragonfly", db.operation.name = "card_profile_lock.acquire")
    )]
    pub async fn acquire(
        &self,
        card_number: &str,
        operation_id: Uuid,
    ) -> Result<CardProfileLockOutcome, CardProfileLockError> {
        let token = operation_id.to_string();
        let lock_key = lock_key(card_number);
        let profile_key = profile_key(card_number);
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|_| CardProfileLockError::PoolUnavailable)?;
        let response: Option<String> = redis::cmd("SET")
            .arg(&lock_key)
            .arg(&token)
            .arg("NX")
            .arg("PX")
            .arg(self.config.lease_duration_ms)
            .query_async(&mut connection)
            .await
            .map_err(|_| CardProfileLockError::CommandFailed)?;
        Ok(match response.as_deref() {
            Some("OK") => CardProfileLockOutcome::Acquired(CardMutationLock {
                lock_key,
                profile_key,
                token,
            }),
            _ => CardProfileLockOutcome::Busy,
        })
    }

    #[tracing::instrument(
        skip(self, lock),
        fields(db.system = "dragonfly", db.operation.name = "card_profile_lock.renew")
    )]
    pub async fn renew(&self, lock: &CardMutationLock) -> Result<(), CardProfileLockError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|_| CardProfileLockError::PoolUnavailable)?;
        let renewed: i64 = redis::Script::new(RENEW_SCRIPT)
            .key(&lock.lock_key)
            .arg(&lock.token)
            .arg(self.config.lease_duration_ms)
            .invoke_async(&mut connection)
            .await
            .map_err(|_| CardProfileLockError::CommandFailed)?;
        if renewed != 1 {
            return Err(CardProfileLockError::OwnershipLost);
        }
        Ok(())
    }

    /// Restores or renews the operation-owned lock and atomically removes a
    /// stale CP. A different operation's lock is never overwritten.
    #[tracing::instrument(
        skip(self, card_number),
        fields(db.system = "dragonfly", db.operation.name = "card_profile.ensure_pending", operation.id = %operation_id)
    )]
    pub async fn ensure_pending(
        &self,
        card_number: &str,
        operation_id: Uuid,
    ) -> Result<bool, CardProfileLockError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|_| CardProfileLockError::PoolUnavailable)?;
        let ensured: i64 = redis::Script::new(ENSURE_PENDING_SCRIPT)
            .key(lock_key(card_number))
            .key(profile_key(card_number))
            .arg(operation_id.to_string())
            .arg(self.config.lease_duration_ms)
            .invoke_async(&mut connection)
            .await
            .map_err(|_| CardProfileLockError::CommandFailed)?;
        Ok(ensured == 1)
    }

    #[tracing::instrument(
        skip(self, lock),
        fields(db.system = "dragonfly", db.operation.name = "card_profile.invalidate")
    )]
    pub async fn invalidate_profile(
        &self,
        lock: &CardMutationLock,
    ) -> Result<(), CardProfileLockError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|_| CardProfileLockError::PoolUnavailable)?;
        let owner: Option<String> = redis::cmd("GET")
            .arg(&lock.lock_key)
            .query_async(&mut connection)
            .await
            .map_err(|_| CardProfileLockError::CommandFailed)?;
        if owner.as_deref() != Some(lock.token.as_str()) {
            return Err(CardProfileLockError::OwnershipLost);
        }
        redis::cmd("DEL")
            .arg(&lock.profile_key)
            .query_async::<i64>(&mut connection)
            .await
            .map_err(|_| CardProfileLockError::CommandFailed)?;
        Ok(())
    }

    #[tracing::instrument(
        skip(self, lock),
        fields(db.system = "dragonfly", db.operation.name = "card_profile_lock.release")
    )]
    pub async fn release(&self, lock: &CardMutationLock) -> Result<bool, CardProfileLockError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|_| CardProfileLockError::PoolUnavailable)?;
        let released: i64 = redis::Script::new(RELEASE_SCRIPT)
            .key(&lock.lock_key)
            .arg(&lock.token)
            .invoke_async(&mut connection)
            .await
            .map_err(|_| CardProfileLockError::CommandFailed)?;
        Ok(released == 1)
    }

    /// Releases a materialized card lock using the operation identity carried
    /// by the Kafka command. Wolfsburg must call this only after the complete
    /// replacement CP value is durable.
    #[tracing::instrument(
        skip(self, card_number),
        fields(db.system = "dragonfly", db.operation.name = "card_profile_lock.release_materialized", operation.id = %operation_id)
    )]
    pub async fn release_materialized(
        &self,
        card_number: &str,
        operation_id: Uuid,
    ) -> Result<bool, CardProfileLockError> {
        self.release(&CardMutationLock {
            lock_key: lock_key(card_number),
            profile_key: profile_key(card_number),
            token: operation_id.to_string(),
        })
        .await
    }
}

fn lock_key(card_number: &str) -> String {
    format!("Lock-CP:{card_number}")
}

fn profile_key(card_number: &str) -> String {
    format!("CP:{card_number}")
}

#[cfg(test)]
mod tests {
    use super::{lock_key, profile_key};

    #[test]
    fn runtime_keys_match_the_nuremberg_contract() {
        assert_eq!(lock_key("1111222233334444"), "Lock-CP:1111222233334444");
        assert_eq!(profile_key("1111222233334444"), "CP:1111222233334444");
    }
}
