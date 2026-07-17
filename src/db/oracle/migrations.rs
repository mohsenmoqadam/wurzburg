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
                for table_name in [
                    "runtime_materialization_receipts",
                    "card_policy_profiles",
                    "card_range_providers",
                    "card_ranges",
                    "providers",
                    "integration_inbox",
                    "integration_outbox",
                    "operation_wal",
                    "business_config_audit",
                    "business_config",
                    "audit_logs",
                    "idempotency_records",
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

pub fn wurzburg_migrations() -> Vec<OracleMigration> {
    let v001_sql = include_str!("../../../migrations/oracle/V001__foundation.sql");
    let v002_sql = include_str!("../../../migrations/oracle/V002__card_foundation.sql");

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
            description: "card_foundation",
            checksum: sql_checksum(v002_sql),
            legacy_checksum: None,
            sql: v002_sql,
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
    fn wurzburg_migrations_include_card_foundation_after_base_foundation() {
        let migrations = wurzburg_migrations();

        assert_eq!(migrations[0].version, "V001");
        assert_eq!(migrations[1].version, "V002");
        assert_eq!(migrations[1].description, "card_foundation");
    }

    #[test]
    fn card_foundation_migration_contains_required_constraints() {
        let migration = wurzburg_migrations()
            .into_iter()
            .find(|migration| migration.version == "V002")
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
    }

    #[test]
    fn foundation_migration_contains_request_context_columns() {
        let migration = wurzburg_migrations()
            .into_iter()
            .find(|migration| migration.version == "V001")
            .expect("foundation migration should exist");

        assert!(migration.sql.contains("CREATE TABLE idempotency_records"));
        assert!(migration.sql.contains("CREATE TABLE audit_logs"));
        assert!(migration.sql.contains("request_id VARCHAR2(128) NOT NULL"));
    }
}
