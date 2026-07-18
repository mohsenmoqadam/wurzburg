use axum::http::StatusCode;
use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
pub enum WurzburgResultCode {
    Success,
    SystemError,
    SerializationError,
    MissingIdempotencyKey,
    InvalidIdempotencyKey,
    IdempotencyKeyConflict,
    IdempotencyInProgress,
    IdempotencyError,
    TrustedActorContextRequired,
    MissingTrustedRequestHeader,
    InvalidTrustedRequestHeader,
    MissingBackendToken,
    AmbiguousBackendToken,
    InvalidBackendTokenTransport,
    MissingRequiredScope,
    InvalidActorClaim,
    InvalidWithdrawalLimitAuthority,
    InvalidCardRangeBoundary,
    CardRangeOverlap,
    CardRangeNotFound,
    InvalidCardRangeFilter,
    CardPolicyContractInvalid,
    CardPolicyNotFound,
    PolicyDraftFrozen,
    PolicyMaterializationMismatch,
    DatabaseUnavailable,
    DatabaseHealthUnavailable,
}

impl WurzburgResultCode {
    pub fn parts(self) -> (u16, &'static str, &'static str, StatusCode) {
        match self {
            Self::Success => (
                0,
                "SUCCESS",
                "Request completed successfully",
                StatusCode::OK,
            ),
            Self::SystemError => (
                5000,
                "SYSTEM_ERROR",
                "An internal system error occurred",
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            Self::SerializationError => (
                5001,
                "SERIALIZATION_ERROR",
                "Response serialization failed",
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            Self::MissingIdempotencyKey => (
                6100,
                "MISSING_IDEMPOTENCY_KEY",
                "Idempotency-Key header is required for mutating requests",
                StatusCode::BAD_REQUEST,
            ),
            Self::IdempotencyKeyConflict => (
                6101,
                "IDEMPOTENCY_KEY_CONFLICT",
                "Idempotency-Key was already used with a different request",
                StatusCode::CONFLICT,
            ),
            Self::IdempotencyInProgress => (
                6102,
                "IDEMPOTENCY_IN_PROGRESS",
                "An operation with this Idempotency-Key is already in progress",
                StatusCode::CONFLICT,
            ),
            Self::IdempotencyError => (
                6103,
                "IDEMPOTENCY_ERROR",
                "Idempotency processing failed",
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            Self::InvalidIdempotencyKey => (
                6104,
                "INVALID_IDEMPOTENCY_KEY",
                "Idempotency-Key header is invalid",
                StatusCode::BAD_REQUEST,
            ),
            Self::TrustedActorContextRequired => (
                6200,
                "TRUSTED_ACTOR_CONTEXT_REQUIRED",
                "Trusted WSO2 actor context is required",
                StatusCode::UNAUTHORIZED,
            ),
            Self::MissingTrustedRequestHeader => (
                6201,
                "MISSING_TRUSTED_REQUEST_HEADER",
                "A required trusted WSO2 request header is missing",
                StatusCode::BAD_REQUEST,
            ),
            Self::InvalidTrustedRequestHeader => (
                6202,
                "INVALID_TRUSTED_REQUEST_HEADER",
                "A trusted WSO2 request header is invalid",
                StatusCode::BAD_REQUEST,
            ),
            Self::MissingBackendToken => (
                6203,
                "MISSING_BACKEND_TOKEN",
                "Configured WSO2 backend token is missing",
                StatusCode::UNAUTHORIZED,
            ),
            Self::AmbiguousBackendToken => (
                6204,
                "AMBIGUOUS_BACKEND_TOKEN",
                "Only one configured WSO2 backend token transport is allowed",
                StatusCode::BAD_REQUEST,
            ),
            Self::InvalidBackendTokenTransport => (
                6205,
                "INVALID_BACKEND_TOKEN_TRANSPORT",
                "WSO2 backend token was sent through the wrong transport",
                StatusCode::UNAUTHORIZED,
            ),
            Self::MissingRequiredScope => (
                6206,
                "MISSING_REQUIRED_SCOPE",
                "Caller does not have the required scope",
                StatusCode::FORBIDDEN,
            ),
            Self::InvalidActorClaim => (
                6207,
                "INVALID_ACTOR_CLAIM",
                "Trusted actor claim is invalid",
                StatusCode::UNAUTHORIZED,
            ),
            Self::InvalidWithdrawalLimitAuthority => (
                6399,
                "INVALID_WITHDRAWAL_LIMIT_AUTHORITY",
                "Withdrawal limit authority is invalid",
                StatusCode::BAD_REQUEST,
            ),
            Self::InvalidCardRangeBoundary => (
                6400,
                "INVALID_CARD_RANGE_BOUNDARY",
                "Card range boundary or structural rule is invalid",
                StatusCode::BAD_REQUEST,
            ),
            Self::CardRangeOverlap => (
                6401,
                "CARD_RANGE_OVERLAP",
                "Card range overlaps an existing range",
                StatusCode::CONFLICT,
            ),
            Self::CardRangeNotFound => (
                6402,
                "CARD_RANGE_NOT_FOUND",
                "Card range was not found",
                StatusCode::NOT_FOUND,
            ),
            Self::InvalidCardRangeFilter => (
                6403,
                "INVALID_CARD_RANGE_FILTER",
                "Card range list filter or cursor is invalid",
                StatusCode::BAD_REQUEST,
            ),
            Self::CardPolicyContractInvalid => (
                6410,
                "CARD_POLICY_CONTRACT_INVALID",
                "Card policy does not match its range authority or limit contract",
                StatusCode::BAD_REQUEST,
            ),
            Self::CardPolicyNotFound => (
                6411,
                "CARD_POLICY_NOT_FOUND",
                "Card policy was not found",
                StatusCode::NOT_FOUND,
            ),
            Self::PolicyDraftFrozen => (
                6412,
                "POLICY_DRAFT_FROZEN",
                "Card policy draft is awaiting runtime materialization and cannot be edited",
                StatusCode::CONFLICT,
            ),
            Self::PolicyMaterializationMismatch => (
                6413,
                "POLICY_MATERIALIZATION_MISMATCH",
                "Runtime materialization receipt does not match the pending policy operation",
                StatusCode::CONFLICT,
            ),
            Self::DatabaseUnavailable => (
                6300,
                "DATABASE_UNAVAILABLE",
                "Oracle database is unavailable",
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            Self::DatabaseHealthUnavailable => (
                6301,
                "DATABASE_HEALTH_UNAVAILABLE",
                "Oracle health repository is not attached to application state",
                StatusCode::NOT_IMPLEMENTED,
            ),
        }
    }
}
