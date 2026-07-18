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
}

pub type DbResult<T> = Result<T, DbError>;
