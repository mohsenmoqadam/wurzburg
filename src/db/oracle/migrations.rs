use crate::db::{
    error::{DbError, DbResult},
    oracle::OraclePool,
};

#[derive(Debug, Clone)]
pub struct OracleMigration {
    pub version: &'static str,
    pub description: &'static str,
    pub checksum: &'static str,
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

    pub async fn apply(&self, migration: OracleMigration) -> DbResult<()> {
        self.pool
            .with_connection(move |connection| {
                let already_applied = migration_exists(connection, migration.version)?;
                if already_applied {
                    return Ok(());
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

fn migration_exists(connection: &oracle::Connection, version: &str) -> DbResult<bool> {
    match connection.query_row_as::<i64>(
        "SELECT COUNT(*) FROM schema_migrations WHERE version = :1",
        &[&version],
    ) {
        Ok(count) => Ok(count > 0),
        Err(error) => {
            let message = error.to_string();
            if message.contains("ORA-00942") {
                Ok(false)
            } else {
                Err(DbError::Query(format!(
                    "failed to check Oracle migration {version}: {error}"
                )))
            }
        }
    }
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
    use super::split_oracle_statements;

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
}
