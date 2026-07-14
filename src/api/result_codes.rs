use axum::http::StatusCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WurzburgResultCode {
    SystemError,
    BadRequest,
    InvalidInput,
    MissingMandatoryParameter,
    NotFound,
    Conflict,
    SerializationError,
    MissingIdempotencyKey,
    IdempotencyKeyConflict,
    IdempotencyInProgress,
    IdempotencyError,
    CardRangeNotFound,
    CardRangeOverlap,
    InvalidCardRange,
    InvalidMetadata,
    CardRangeActivationRuleFailed,
    CardRangeProviderNotFound,
    CardRangeProviderRuleFailed,
    CardRangePolicyNotFound,
    InvalidCardPolicy,
}

impl WurzburgResultCode {
    pub fn details(self) -> (i32, &'static str, &'static str, StatusCode) {
        match self {
            Self::SystemError => (
                0,
                "SYSTEM_ERROR",
                "The operation could not be completed because of an internal error",
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            Self::BadRequest => (4000, "BAD_REQUEST", "Bad request", StatusCode::BAD_REQUEST),
            Self::InvalidInput => (
                4001,
                "INVALID_INPUT",
                "Input is invalid",
                StatusCode::BAD_REQUEST,
            ),
            Self::MissingMandatoryParameter => (
                4002,
                "MISSING_MANDATORY_PARAMETER",
                "A mandatory parameter is missing",
                StatusCode::BAD_REQUEST,
            ),
            Self::NotFound => (
                4040,
                "NOT_FOUND",
                "Resource not found",
                StatusCode::NOT_FOUND,
            ),
            Self::Conflict => (
                4090,
                "CONFLICT",
                "Request conflicts with current state",
                StatusCode::CONFLICT,
            ),
            Self::SerializationError => (
                5001,
                "SERIALIZATION_ERROR",
                "Failed to serialize API payload",
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
                "Failed to process idempotency state",
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            Self::CardRangeNotFound => (
                6200,
                "CARD_RANGE_NOT_FOUND",
                "Card range not found",
                StatusCode::NOT_FOUND,
            ),
            Self::CardRangeOverlap => (
                6201,
                "CARD_RANGE_OVERLAP",
                "Card range overlaps an existing range",
                StatusCode::CONFLICT,
            ),
            Self::InvalidCardRange => (
                6202,
                "INVALID_CARD_RANGE",
                "Card range is invalid",
                StatusCode::BAD_REQUEST,
            ),
            Self::InvalidMetadata => (
                6203,
                "INVALID_METADATA",
                "Metadata must be a JSON object",
                StatusCode::BAD_REQUEST,
            ),
            Self::CardRangeActivationRuleFailed => (
                6204,
                "CARD_RANGE_ACTIVATION_RULE_FAILED",
                "Card range activation rule failed",
                StatusCode::BAD_REQUEST,
            ),
            Self::CardRangeProviderNotFound => (
                6210,
                "CARD_RANGE_PROVIDER_NOT_FOUND",
                "Card range provider was not found",
                StatusCode::NOT_FOUND,
            ),
            Self::CardRangeProviderRuleFailed => (
                6211,
                "CARD_RANGE_PROVIDER_RULE_FAILED",
                "Card range provider rule failed",
                StatusCode::BAD_REQUEST,
            ),
            Self::CardRangePolicyNotFound => (
                6220,
                "CARD_RANGE_POLICY_NOT_FOUND",
                "Active card range policy not found",
                StatusCode::NOT_FOUND,
            ),
            Self::InvalidCardPolicy => (
                6221,
                "INVALID_CARD_POLICY",
                "Card policy is invalid",
                StatusCode::BAD_REQUEST,
            ),
        }
    }

    pub fn rs_code(self) -> i32 {
        self.details().0
    }

    pub fn code(self) -> &'static str {
        self.details().1
    }

    pub fn default_message(self) -> &'static str {
        self.details().2
    }

    pub fn http_status(self) -> StatusCode {
        self.details().3
    }
}
