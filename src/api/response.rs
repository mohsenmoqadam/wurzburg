use axum::{
    body::Body,
    http::{StatusCode, header::CONTENT_TYPE},
    response::Response,
};
use serde::Serialize;

use crate::{
    api::{error::ApiError, result_codes::WurzburgResultCode},
    telemetry::http::{RESULT_CODE_HEADER, RESULT_SYMBOL_HEADER},
};

pub fn success_response<T>(status: StatusCode, body: T) -> Result<Response, ApiError>
where
    T: Serialize,
{
    let body = serde_json::to_vec(&body).map_err(ApiError::from)?;
    let (rs_code, code, _, _) = WurzburgResultCode::Success.parts();

    Ok(Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .header(RESULT_CODE_HEADER.as_str(), rs_code.to_string())
        .header(RESULT_SYMBOL_HEADER.as_str(), code)
        .body(Body::from(body))
        .expect("static success response headers are valid"))
}
