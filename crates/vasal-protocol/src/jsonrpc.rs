//! JSON-RPC 2.0 wire format types.

use serde::{Deserialize, Serialize};
use std::fmt;

/// The JSON-RPC protocol version string, always `"2.0"`.
pub const JSONRPC_VERSION: &str = "2.0";

/// A JSON-RPC 2.0 request message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Request {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    pub id: RequestId,
}

impl Request {
    pub fn new(
        method: impl Into<String>,
        params: Option<serde_json::Value>,
        id: impl Into<RequestId>,
    ) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            method: method.into(),
            params,
            id: id.into(),
        }
    }
}

/// A JSON-RPC 2.0 response message.
///
/// Per the spec, exactly one of `result` or `error` is present.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Response {
    pub jsonrpc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorObject>,
    pub id: RequestId,
}

impl Response {
    pub fn success(id: RequestId, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            result: Some(result),
            error: None,
            id,
        }
    }

    pub fn error(id: RequestId, error: ErrorObject) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            result: None,
            error: Some(error),
            id,
        }
    }

    pub fn is_success(&self) -> bool {
        self.error.is_none()
    }
}

/// JSON-RPC 2.0 error object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorObject {
    pub code: i32,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Request identifier — integer or string per the JSON-RPC 2.0 spec.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Integer(i64),
    String(String),
}

impl From<i64> for RequestId {
    fn from(v: i64) -> Self {
        Self::Integer(v)
    }
}

impl From<String> for RequestId {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for RequestId {
    fn from(v: &str) -> Self {
        Self::String(v.to_owned())
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(n) => write!(f, "{n}"),
            Self::String(s) => write!(f, "{s}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_roundtrip() {
        let req = Request::new("submit", Some(json!({"key": "value"})), 42i64);
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);
    }

    #[test]
    fn request_without_params() {
        let req = Request::new("health", None, 1i64);
        let json = serde_json::to_string(&req).unwrap();
        // params field should be absent, not null
        assert!(!json.contains("params"));
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.params, None);
    }

    #[test]
    fn success_response_roundtrip() {
        let resp = Response::success(RequestId::Integer(1), json!({"status": "ok"}));
        assert!(resp.is_success());
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: Response = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, parsed);
        assert!(parsed.is_success());
    }

    #[test]
    fn error_response_roundtrip() {
        let resp = Response::error(
            RequestId::String("abc".into()),
            ErrorObject {
                code: -32601,
                message: "method not found".into(),
                data: None,
            },
        );
        assert!(!resp.is_success());
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: Response = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, parsed);
    }

    #[test]
    fn request_id_integer() {
        let id: RequestId = 42i64.into();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "42");
        let parsed: RequestId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn request_id_string() {
        let id: RequestId = "req-001".into();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, r#""req-001""#);
        let parsed: RequestId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn request_id_display() {
        assert_eq!(RequestId::Integer(7).to_string(), "7");
        assert_eq!(RequestId::String("x".into()).to_string(), "x");
    }
}
