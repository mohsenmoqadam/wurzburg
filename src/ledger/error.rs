use std::{error::Error, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TigerBeetleError {
    QueueUnavailable,
    WorkerUnavailable,
    DeadlineExceeded,
    ClientFailure { operation: &'static str },
}

impl TigerBeetleError {
    pub fn diagnostic_kind(&self) -> &'static str {
        match self {
            Self::QueueUnavailable => "queue_unavailable",
            Self::WorkerUnavailable => "worker_unavailable",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::ClientFailure { .. } => "client_failure",
        }
    }

    pub fn outcome_is_uncertain(&self) -> bool {
        matches!(self, Self::DeadlineExceeded | Self::ClientFailure { .. })
    }
}

impl fmt::Display for TigerBeetleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueUnavailable => {
                formatter.write_str("TigerBeetle command queue is unavailable")
            }
            Self::WorkerUnavailable => formatter.write_str("TigerBeetle worker is unavailable"),
            Self::DeadlineExceeded => {
                formatter.write_str("TigerBeetle operation deadline exceeded")
            }
            Self::ClientFailure { operation } => {
                write!(formatter, "TigerBeetle {operation} operation failed")
            }
        }
    }
}

impl Error for TigerBeetleError {}

pub type TigerBeetleResult<T> = Result<T, TigerBeetleError>;
