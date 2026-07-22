use std::{error::Error, fmt};

#[derive(Debug)]
pub enum DbError {
    Configuration(String),
    Connection(String),
    Conflict(String),
    Query(String),
    BlockingTask(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message) => {
                write!(formatter, "database configuration error: {message}")
            }
            Self::Connection(message) => write!(formatter, "database connection error: {message}"),
            Self::Conflict(message) => write!(formatter, "database conflict: {message}"),
            Self::Query(message) => write!(formatter, "database query error: {message}"),
            Self::BlockingTask(message) => {
                write!(formatter, "database blocking task error: {message}")
            }
        }
    }
}

impl Error for DbError {}

impl DbError {
    pub fn diagnostic_kind(&self) -> &'static str {
        match self {
            Self::Configuration(_) => "configuration",
            Self::Connection(_) => "connection",
            Self::Conflict(_) => "conflict",
            Self::Query(_) => "query",
            Self::BlockingTask(_) => "blocking_task",
        }
    }

    pub fn is_connection_failure(&self) -> bool {
        matches!(self, Self::Connection(_) | Self::BlockingTask(_))
    }

    pub fn oracle_code(&self) -> Option<&str> {
        let message = match self {
            Self::Configuration(message)
            | Self::Connection(message)
            | Self::Conflict(message)
            | Self::Query(message)
            | Self::BlockingTask(message) => message,
        };
        let start = message.find("ORA-")?;
        let code = message.get(start..start + 9)?;
        code.bytes()
            .skip(4)
            .all(|byte| byte.is_ascii_digit())
            .then_some(code)
    }

    pub fn diagnostic_context(&self) -> &'static str {
        let message = match self {
            Self::Configuration(message)
            | Self::Connection(message)
            | Self::Conflict(message)
            | Self::Query(message)
            | Self::BlockingTask(message) => message,
        };
        if message.starts_with("failed to insert provider Kafka access") {
            "provider_kafka_access.insert"
        } else if message.starts_with("failed to insert Provider Kafka credential") {
            "provider_kafka_credential.insert"
        } else if message.starts_with("failed to seed Provider event subscriptions") {
            "provider_event_subscriptions.seed"
        } else if message.starts_with("failed to fetch provider") {
            "provider.fetch"
        } else if message.starts_with("failed to mark provider ready") {
            "provider.mark_ready"
        } else if message.starts_with("failed to refresh provider idempotency snapshot") {
            "provider.idempotency_snapshot.refresh"
        } else {
            "unclassified"
        }
    }
}

pub type DbResult<T> = Result<T, DbError>;
