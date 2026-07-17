use axum::{
    Json,
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use utoipa::ToSchema;

use crate::{
    api::result_codes::WurzburgResultCode,
    telemetry::http::{RESULT_CODE_HEADER, RESULT_SYMBOL_HEADER},
};

#[derive(Debug, Serialize, ToSchema)]
pub struct ApiErrorResponse {
    pub error: ApiErrorBody,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ApiErrorBody {
    pub rs_code: u16,
    pub code: &'static str,
    pub message: String,
    pub details: serde_json::Value,
}

#[derive(Debug)]
pub struct ApiError {
    result_code: WurzburgResultCode,
    message: Option<String>,
    details: serde_json::Value,
}

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

    pub fn status(&self) -> StatusCode {
        self.result_code.parts().3
    }

    pub fn body(&self) -> ApiErrorResponse {
        let (rs_code, code, default_message, _) = self.result_code.parts();

        ApiErrorResponse {
            error: ApiErrorBody {
                rs_code,
                code,
                message: self
                    .message
                    .clone()
                    .unwrap_or_else(|| default_message.to_string()),
                details: self.details.clone(),
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (rs_code, code, _, _) = self.result_code.parts();
        let mut response = (self.status(), Json(self.body())).into_response();

        response.headers_mut().insert(
            RESULT_CODE_HEADER.clone(),
            HeaderValue::from_str(&rs_code.to_string()).expect("static result code is valid"),
        );
        response
            .headers_mut()
            .insert(RESULT_SYMBOL_HEADER.clone(), HeaderValue::from_static(code));

        response
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(error: serde_json::Error) -> Self {
        Self::with_message(WurzburgResultCode::SerializationError, error.to_string())
    }
}
