//! Well-formed JSON error responses for every failure the daemon answers itself.
//!
//! The API is consumed by a website, so a rejection (rate limited, timed out, unknown key, bad
//! key) is always a JSON object with a stable machine-readable `error` code and, where useful, a
//! human-readable `message`; never a bare status with an empty body.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

/// A failure the daemon reports to an HTTP client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    /// The client exceeded the per-IP or global rate limit.
    RateLimited,
    /// The request did not complete within the request deadline.
    RequestTimeout,
    /// The path did not match any route.
    NoRoute,
    /// The requested BLS key is not in the cache.
    RecordNotFound,
    /// The `{key}` path segment did not parse as a BLS public key; carries the parse error.
    InvalidKey(String),
    /// The daemon is not ready to serve; carries the readiness reason.
    NotReady(String),
}

impl ApiError {
    /// The HTTP status paired with this error.
    fn status(&self) -> StatusCode {
        match self {
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::RequestTimeout => StatusCode::REQUEST_TIMEOUT,
            Self::NoRoute | Self::RecordNotFound => StatusCode::NOT_FOUND,
            Self::InvalidKey(_) => StatusCode::BAD_REQUEST,
            Self::NotReady(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// The stable machine-readable code in the `error` field.
    pub fn code(&self) -> &'static str {
        match self {
            Self::RateLimited => "rate_limited",
            Self::RequestTimeout => "request_timeout",
            Self::NoRoute => "no_route",
            Self::RecordNotFound => "not_found",
            Self::InvalidKey(_) => "invalid_key",
            Self::NotReady(_) => "not_ready",
        }
    }

    /// The optional human-readable `message`.
    fn message(&self) -> Option<&str> {
        match self {
            Self::InvalidKey(message) | Self::NotReady(message) => Some(message),
            Self::RateLimited | Self::RequestTimeout | Self::NoRoute | Self::RecordNotFound => None,
        }
    }
}

/// The JSON body every [`ApiError`] renders to.
#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    /// The stable machine-readable code.
    error: &'static str,
    /// The optional human-readable detail.
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'a str>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody { error: self.code(), message: self.message() };
        (self.status(), Json(body)).into_response()
    }
}

/// Render `err` as its JSON response (a free function for the middleware call sites that do not
/// go through a handler's return type).
pub fn error_response(err: ApiError) -> Response {
    err.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn errors_render_as_json_with_a_stable_code() {
        let response = error_response(ApiError::InvalidKey("bad hex".into()));
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = to_bytes(response.into_body(), 1024).await.expect("body");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(body["error"], "invalid_key");
        assert_eq!(body["message"], "bad hex");

        let response = error_response(ApiError::RecordNotFound);
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let bytes = to_bytes(response.into_body(), 1024).await.expect("body");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(body, serde_json::json!({ "error": "not_found" }));
    }
}
