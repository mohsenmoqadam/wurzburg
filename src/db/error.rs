use std::{error::Error, fmt};

#[derive(Debug)]
pub enum DbError {
    Configuration(String),
    Connection(String),
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
            Self::Query(message) => write!(formatter, "database query error: {message}"),
            Self::BlockingTask(message) => {
                write!(formatter, "database blocking task error: {message}")
            }
        }
    }
}

impl Error for DbError {}

pub type DbResult<T> = Result<T, DbError>;
