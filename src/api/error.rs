use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use utoipa::ToSchema;

use crate::api::result_codes::WurzburgResultCode;

#[derive(Debug, Serialize, ToSchema)]
pub struct ApiErrorResponse {
    pub error: ApiErrorBody,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ApiErrorBody {
    pub rs_code: i32,
    pub code: String,
    pub message: String,
    pub details: serde_json::Value,
}

#[derive(Debug)]
pub struct ApiError {
    result_code: WurzburgResultCode,
    message: Option<String>,
    details: serde_json::Value,
}

pub type ApiResult<T> = Result<T, ApiError>;

impl ApiError {
    pub fn new(result_code: WurzburgResultCode) -> Self {
        Self {
            result_code,
            message: None,
            details: serde_json::json!({}),
        }
    }

    pub fn with_message(result_code: WurzburgResultCode, message: impl Into<String>) -> Self {
        Self {
            result_code,
            message: Some(message.into()),
            details: serde_json::json!({}),
        }
    }

    pub fn with_details(result_code: WurzburgResultCode, details: serde_json::Value) -> Self {
        Self {
            result_code,
            message: None,
            details,
        }
    }

    pub fn system(error: impl std::fmt::Display) -> Self {
        Self::with_message(WurzburgResultCode::SystemError, error.to_string())
    }

    pub fn serialization(error: impl std::fmt::Display) -> Self {
        Self::with_message(WurzburgResultCode::SerializationError, error.to_string())
    }

    pub fn status(&self) -> StatusCode {
        self.result_code.http_status()
    }

    pub fn response_body(&self) -> ApiErrorResponse {
        ApiErrorResponse {
            error: ApiErrorBody {
                rs_code: self.result_code.rs_code(),
                code: self.result_code.code().to_string(),
                message: self
                    .message
                    .clone()
                    .unwrap_or_else(|| self.result_code.default_message().to_string()),
                details: self.details.clone(),
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status(), Json(self.response_body())).into_response()
    }
}
