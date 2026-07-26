use crate::{
    config::{DatabaseConfig, MigrationConfig, Settings},
    db::{
        error::{DbError, DbResult},
        oracle::{OracleConnectConfig, OraclePool},
    },
};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct OracleMigration {
    pub version: &'static str,
    pub description: &'static str,
    pub checksum: String,
    pub legacy_checksum: Option<&'static str>,
    pub sql: &'static str,
}

#[derive(Clone)]
pub struct OracleMigrator {
    pool: OraclePool,
}

impl OracleMigrator {
    pub fn new(pool: OraclePool) -> Self {
        Self { pool }
    }

    pub async fn reset_schema(&self) -> DbResult<()> {
        self.pool
            .with_connection(|connection| {
                // Non-production reset can race a recently completed API/test
                // transaction. Oracle otherwise uses a zero-second DDL wait,
                // making harmless transient TM locks fail the entire reset.
                connection
                    .execute("ALTER SESSION SET DDL_LOCK_TIMEOUT = 30", &[])
                    .map_err(|error| {
                        DbError::Query(format!(
                            "failed to configure Oracle DDL lock timeout: {error}"
                        ))
                    })?;
                for table_name in [
                    "runtime_materialization_receipts",
                    "card_policy_profiles",
                    "card_range_providers",
                    "card_ranges",
                    "provider_fee_profiles",
                    "provider_event_subscriptions",
                    "provider_provisioning_jobs",
                    "provider_kafka_credentials",
                    "provider_kafka_access",
                    "provider_ledger_accounts",
                    "provider_operational_profiles",
                    "provider_contacts",
                    "providers",
                    "kafka_poison_messages",
                    "integration_inbox",
                    "integration_outbox",
                    "operation_wal",
                    "business_config_audit",
                    "business_config",
                    "audit_logs",
                    "idempotency_records",
                    "card_range_allocation_locks",
                    "oracle_migration_locks",
                    "schema_migrations",
                ] {
                    let statement = format!("DROP TABLE {table_name} CASCADE CONSTRAINTS PURGE");
                    match connection.execute(&statement, &[]) {
                        Ok(_) => {}
                        Err(error) if error.to_string().contains("ORA-00942") => {}
                        Err(error) => {
                            return Err(DbError::Query(format!(
                                "failed to drop Oracle table {table_name}: {error}"
                            )));
                        }
                    }
                }

                connection.commit().map_err(|error| {
                    DbError::Query(format!("failed to commit Oracle schema reset: {error}"))
                })?;

                Ok(())
            })
            .await
    }

    pub async fn apply(&self, migration: OracleMigration) -> DbResult<()> {
        self.pool
            .with_connection(move |connection| {
                match migration_checksum(connection, migration.version)? {
                    Some(applied_checksum) if applied_checksum == migration.checksum => {
                        return Ok(());
                    }
                    Some(applied_checksum)
                        if migration
                            .legacy_checksum
                            .is_some_and(|legacy| legacy == applied_checksum) =>
                    {
                        tracing::warn!(
                            version = migration.version,
                            "upgrading legacy Oracle migration checksum"
                        );
                        connection
                            .execute(
                                "UPDATE schema_migrations SET checksum = :1 WHERE version = :2",
                                &[&migration.checksum, &migration.version],
                            )
                            .map_err(|error| {
                                DbError::Query(format!(
                                    "failed to upgrade Oracle migration {} checksum: {error}",
                                    migration.version
                                ))
                            })?;
                        connection.commit().map_err(|error| {
                            DbError::Query(format!(
                                "failed to commit Oracle migration {} checksum upgrade: {error}",
                                migration.version
                            ))
                        })?;
                        return Ok(());
                    }
                    Some(applied_checksum) => {
                        return Err(DbError::Configuration(format!(
                            "Oracle migration {} checksum mismatch: applied {}, current {}",
                            migration.version, applied_checksum, migration.checksum
                        )));
                    }
                    None => {}
                }

                for statement in split_oracle_statements(migration.sql) {
                    connection.execute(&statement, &[]).map_err(|error| {
                        DbError::Query(format!(
                            "failed to execute Oracle migration {} statement `{}`: {error}",
                            migration.version, statement
                        ))
                    })?;
                }

                connection
                    .execute(
                        "INSERT INTO schema_migrations (version, description, checksum) VALUES (:1, :2, :3)",
                        &[&migration.version, &migration.description, &migration.checksum],
                    )
                    .map_err(|error| {
                        DbError::Query(format!(
                            "failed to record Oracle migration {}: {error}",
                            migration.version
                        ))
                    })?;

                connection.commit().map_err(|error| {
                    DbError::Query(format!(
                        "failed to commit Oracle migration {}: {error}",
                        migration.version
                    ))
                })?;

                Ok(())
            })
            .await
    }
}

pub async fn prepare_oracle_schema(
    database: &DatabaseConfig,
    migrations: &MigrationConfig,
) -> DbResult<()> {
    if !migrations.enabled {
        return Ok(());
    }

    if migrations.force_recreate && Settings::is_production() {
        return Err(DbError::Configuration(
            "Oracle force_recreate migration mode is not allowed in production".to_string(),
        ));
    }

    let oracle_config = OracleConnectConfig::from_driver_config(database)?;
    let oracle_pool = OraclePool::connect(oracle_config).await?;
    let migrator = OracleMigrator::new(oracle_pool);

    if migrations.force_recreate {
        tracing::warn!("Force recreating Wurzburg Oracle schema before migration");
        migrator.reset_schema().await?;
    }

    for migration in wurzburg_migrations() {
        tracing::info!("Applying Oracle migration {}", migration.version);
        migrator.apply(migration).await?;
    }

    Ok(())
}

/// Verifies the exact migration set without changing Oracle. Runtime service
/// instances use this check so schema ownership remains with one deployment
/// job or an explicit developer command.
pub async fn verify_oracle_schema(
    database: &DatabaseConfig,
    migrations: &MigrationConfig,
) -> DbResult<()> {
    if !migrations.enabled {
        return Ok(());
    }

    let oracle_config = OracleConnectConfig::from_driver_config(database)?;
    let oracle_pool = OraclePool::connect(oracle_config).await?;
    oracle_pool
        .with_connection(|connection| {
            let expected_migrations = wurzburg_migrations();
            for migration in &expected_migrations {
                match migration_checksum(connection, migration.version)? {
                    Some(checksum) if checksum == migration.checksum => {}
                    Some(checksum) => {
                        return Err(DbError::Configuration(format!(
                            "Oracle migration {} checksum mismatch: applied {}, expected {}",
                            migration.version, checksum, migration.checksum
                        )));
                    }
                    None => {
                        return Err(DbError::Configuration(format!(
                            "required Oracle migration {} is not applied",
                            migration.version
                        )));
                    }
                }
            }

            let applied_count = connection
                .query_row_as::<i64>("SELECT COUNT(*) FROM schema_migrations", &[])
                .map_err(|error| {
                    DbError::Query(format!(
                        "failed to count applied Oracle migrations: {error}"
                    ))
                })?;
            if applied_count != expected_migrations.len() as i64 {
                return Err(DbError::Configuration(format!(
                    "Oracle migration set mismatch: found {applied_count}, expected {}",
                    expected_migrations.len()
                )));
            }
            Ok(())
        })
        .await
}

pub fn wurzburg_migrations() -> Vec<OracleMigration> {
    let v001_sql = include_str!("../../../migrations/oracle/V001__foundation.sql");
    let v002_sql = include_str!("../../../migrations/oracle/V002__provider_foundation.sql");
    let v003_sql =
        include_str!("../../../migrations/oracle/V003__card_range_policy_foundation.sql");

    vec![
        OracleMigration {
            version: "V001",
            description: "production_foundation",
            checksum: sql_checksum(v001_sql),
            legacy_checksum: Some("V001__foundation.sql"),
            sql: v001_sql,
        },
        OracleMigration {
            version: "V002",
            description: "provider_foundation",
            checksum: sql_checksum(v002_sql),
            legacy_checksum: None,
            sql: v002_sql,
        },
        OracleMigration {
            version: "V003",
            description: "card_range_policy_foundation",
            checksum: sql_checksum(v003_sql),
            legacy_checksum: None,
            sql: v003_sql,
        },
    ]
}

fn migration_checksum(connection: &oracle::Connection, version: &str) -> DbResult<Option<String>> {
    match connection.query_row_as::<String>(
        "SELECT checksum FROM schema_migrations WHERE version = :1",
        &[&version],
    ) {
        Ok(checksum) => Ok(Some(checksum)),
        Err(error) => {
            let message = error.to_string();
            if message.contains("ORA-00942") || error.kind() == oracle::ErrorKind::NoDataFound {
                Ok(None)
            } else {
                Err(DbError::Query(format!(
                    "failed to check Oracle migration {version}: {error}"
                )))
            }
        }
    }
}

fn sql_checksum(sql: &str) -> String {
    let digest = Sha256::digest(sql.as_bytes());
    format!("{digest:x}")
}

fn split_oracle_statements(sql: &str) -> Vec<String> {
    sql.lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty() && !trimmed.starts_with("--")
        })
        .collect::<Vec<_>>()
        .join("\n")
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{split_oracle_statements, sql_checksum, wurzburg_migrations};

    #[test]
    fn split_oracle_statements_ignores_comments_and_blank_lines() {
        let statements = split_oracle_statements(
            r#"
            -- comment

            CREATE TABLE one (id NUMBER);
            CREATE INDEX one_idx ON one(id);
            "#,
        );

        assert_eq!(statements.len(), 2);
        assert!(statements[0].starts_with("CREATE TABLE one"));
        assert!(statements[1].starts_with("CREATE INDEX one_idx"));
    }

    #[test]
    fn sql_checksum_changes_with_content() {
        assert_ne!(sql_checksum("SELECT 1"), sql_checksum("SELECT 2"));
    }

    #[test]
    fn wurzburg_migrations_follow_final_dependency_order() {
        let migrations = wurzburg_migrations();

        assert_eq!(migrations[0].version, "V001");
        assert_eq!(migrations[1].version, "V002");
        assert_eq!(migrations[1].description, "provider_foundation");
        assert_eq!(migrations[2].version, "V003");
        assert_eq!(migrations[2].description, "card_range_policy_foundation");
    }

    #[test]
    fn provider_foundation_contains_final_core_resources() {
        let migration = wurzburg_migrations()
            .into_iter()
            .find(|migration| migration.version == "V002")
            .expect("provider foundation migration should exist");

        assert!(migration.sql.contains("CREATE TABLE providers"));
        assert!(
            !migration
                .sql
                .contains("status IN ('DRAFT', 'ACTIVE', 'SUSPENDED', 'INACTIVE', 'FAILED')")
        );
        assert!(migration.sql.contains("CREATE TABLE provider_contacts"));
        assert!(
            migration
                .sql
                .contains("CREATE TABLE provider_operational_profiles")
        );
        assert!(
            migration
                .sql
                .contains("CREATE TABLE provider_ledger_accounts")
        );
        assert!(migration.sql.contains("CREATE TABLE provider_fee_profiles"));
        assert!(migration.sql.contains("PROVIDER_OWNED"));
        assert!(migration.sql.contains("PROVIDER_FEE"));
        assert!(migration.sql.contains("CMS_SETTLEMENT"));
        assert!(migration.sql.contains("PLATFORM_FEE"));
        assert!(
            migration
                .sql
                .contains("CREATE TABLE provider_provisioning_jobs")
        );
    }

    #[test]
    fn card_foundation_migration_contains_required_constraints() {
        let migration = wurzburg_migrations()
            .into_iter()
            .find(|migration| migration.version == "V003")
            .expect("card foundation migration should exist");

        assert!(migration.sql.contains("CREATE TABLE card_ranges"));
        assert!(migration.sql.contains("withdrawal_limit_authority"));
        assert!(migration.sql.contains("ck_card_ranges_authority_calendar"));
        assert!(migration.sql.contains("CREATE TABLE card_range_providers"));
        assert!(
            migration
                .sql
                .contains("uq_crp_one_active_range_per_provider")
        );
        assert!(migration.sql.contains("CREATE TABLE card_policy_profiles"));
        assert!(migration.sql.contains("uq_cpp_one_candidate"));
        assert!(migration.sql.contains("ck_cpp_lifecycle_shape"));
    }

    #[test]
    fn foundation_migration_contains_request_context_columns() {
        let migration = wurzburg_migrations()
            .into_iter()
            .find(|migration| migration.version == "V001")
            .expect("foundation migration should exist");

        assert!(migration.sql.contains("CREATE TABLE idempotency_records"));
        assert!(migration.sql.contains("CREATE TABLE audit_logs"));
        assert!(migration.sql.contains("CREATE TABLE kafka_poison_messages"));
        assert!(migration.sql.contains("request_id VARCHAR2(128) NOT NULL"));
        assert!(
            migration
                .sql
                .contains("CREATE TABLE card_range_allocation_locks")
        );
        assert!(migration.sql.contains("CARD_RANGE_STRUCTURE"));
    }
}
