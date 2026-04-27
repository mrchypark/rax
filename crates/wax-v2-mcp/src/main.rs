use std::io::{self, BufRead, Write};
use std::path::Path;

use serde::Serialize;
use serde_json::{json, Value};
use wax_v2_mcp::{McpError, McpRequest, WaxMcpSurface, MAX_MCP_SEARCH_TOP_K};

#[derive(Debug)]
struct JsonRpcRequest {
    jsonrpc: Option<String>,
    id: JsonRpcRequestId,
    method: String,
    params: Value,
}

#[derive(Debug, Default)]
enum JsonRpcRequestId {
    #[default]
    Missing,
    Present(Value),
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut surface = match std::env::var("WAX_MCP_ALLOWED_ROOT") {
        Ok(root) => match WaxMcpSurface::with_allowed_root(Path::new(&root)) {
            Ok(surface) => surface,
            Err(error) => {
                let _ = write_response(
                    &mut stdout,
                    JsonRpcResponse::from_mcp_error(Value::Null, error),
                );
                return;
            }
        },
        Err(_) => WaxMcpSurface::default(),
    };

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) if line.trim().is_empty() => continue,
            Ok(line) => line,
            Err(error) => {
                let _ = write_response(
                    &mut stdout,
                    JsonRpcResponse::error(Value::Null, -32000, error.to_string(), None),
                );
                continue;
            }
        };

        let response = match serde_json::from_str::<Value>(&line) {
            Ok(value) => match parse_json_rpc_request(value) {
                Ok(request) => handle_json_rpc(&mut surface, request),
                Err(error) => Some(JsonRpcResponse::error(
                    error.id,
                    error.code,
                    error.message,
                    None,
                )),
            },
            Err(error) => Some(JsonRpcResponse::error(
                Value::Null,
                -32700,
                error.to_string(),
                None,
            )),
        };
        if let Some(response) = response {
            let _ = write_response(&mut stdout, response);
        }
    }
}

#[derive(Debug)]
struct JsonRpcInvalidRequest {
    id: Value,
    code: i64,
    message: String,
}

fn parse_json_rpc_request(value: Value) -> Result<JsonRpcRequest, JsonRpcInvalidRequest> {
    let id = value
        .get("id")
        .cloned()
        .map(JsonRpcRequestId::Present)
        .unwrap_or(JsonRpcRequestId::Missing);
    let error_id = match &id {
        JsonRpcRequestId::Missing => Value::Null,
        JsonRpcRequestId::Present(id) => id.clone(),
    };
    let Some(object) = value.as_object() else {
        return Err(JsonRpcInvalidRequest {
            id: Value::Null,
            code: -32600,
            message: "JSON-RPC request must be an object".to_owned(),
        });
    };
    let method = object
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| JsonRpcInvalidRequest {
            id: error_id.clone(),
            code: -32600,
            message: "JSON-RPC request method must be a string".to_owned(),
        })?
        .to_owned();
    let jsonrpc = object
        .get("jsonrpc")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let params = object.get("params").cloned().unwrap_or(Value::Null);
    if !params.is_null() && !params.is_object() {
        return Err(JsonRpcInvalidRequest {
            id: error_id,
            code: -32602,
            message: "JSON-RPC params must be an object".to_owned(),
        });
    }
    Ok(JsonRpcRequest {
        jsonrpc,
        id,
        method,
        params,
    })
}

fn handle_json_rpc(
    surface: &mut WaxMcpSurface,
    request: JsonRpcRequest,
) -> Option<JsonRpcResponse> {
    let id = request.id;
    if request.jsonrpc.as_deref() != Some("2.0") {
        return match id {
            JsonRpcRequestId::Missing => None,
            JsonRpcRequestId::Present(id) => Some(JsonRpcResponse::error(
                id,
                -32600,
                "jsonrpc must be 2.0".to_owned(),
                None,
            )),
        };
    }

    match request.method.as_str() {
        "notifications/initialized" => None,
        _ => {
            let JsonRpcRequestId::Present(id) = id else {
                return None;
            };
            match request.method.as_str() {
                "initialize" => Some(JsonRpcResponse::ok(
                    id,
                    json!({
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {}
                        },
                        "serverInfo": {
                            "name": "wax-mcp",
                            "version": env!("CARGO_PKG_VERSION")
                        }
                    }),
                )),
                "tools/list" => Some(JsonRpcResponse::ok(
                    id,
                    json!({
                        "tools": [
                            tool_schema("remember", "Store a memory in a local .wax file", json!({
                                "type": "object",
                                "properties": {
                                    "store": {"type": "string"},
                                    "content": {"type": "string"},
                                    "metadata": {"type": "object"}
                                },
                                "required": ["store", "content"]
                            })),
                            tool_schema("recall", "Recall memory from a local .wax file with hybrid search", json!({
                                "type": "object",
                                "properties": {
                                    "store": {"type": "string"},
                                    "query": {"type": "string"},
                                    "top_k": {"type": "integer", "default": 5, "minimum": 0, "maximum": MAX_MCP_SEARCH_TOP_K},
                                    "include_preview": {"type": "boolean", "default": true}
                                },
                                "required": ["store", "query"]
                            })),
                            tool_schema("search", "Search memory from a local .wax file", json!({
                                "type": "object",
                                "properties": {
                                    "store": {"type": "string"},
                                    "query": {"type": "string"},
                                    "mode": {"type": "string", "enum": ["text", "vector", "hybrid"], "default": "hybrid"},
                                    "top_k": {"type": "integer", "default": 5, "minimum": 0, "maximum": MAX_MCP_SEARCH_TOP_K},
                                    "include_preview": {"type": "boolean", "default": true}
                                },
                                "required": ["store", "query"]
                            }))
                        ]
                    }),
                )),
                "tools/call" => Some(handle_tool_call(surface, id, request.params)),
                _ => Some(JsonRpcResponse::error(
                    id,
                    -32601,
                    format!("unsupported method: {}", request.method),
                    None,
                )),
            }
        }
    }
}

fn handle_tool_call(surface: &mut WaxMcpSurface, id: Value, params: Value) -> JsonRpcResponse {
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let request = match tool_request(name, arguments) {
        Ok(request) => request,
        Err(error) => return JsonRpcResponse::from_mcp_error(id, error),
    };

    match surface.handle(request) {
        Ok(response) => JsonRpcResponse::ok(
            id,
            json!({
                "content": [
                    {
                        "type": "text",
                        "text": serde_json::to_string_pretty(&response).unwrap_or_else(|_| "{}".to_owned())
                    }
                ],
                "structuredContent": response
            }),
        ),
        Err(error) => JsonRpcResponse::from_mcp_error(id, error),
    }
}

fn tool_request(name: &str, arguments: Value) -> Result<McpRequest, McpError> {
    match name {
        "remember" => Ok(McpRequest::Remember {
            store: required_string(&arguments, "store")?,
            content: required_string(&arguments, "content")?,
            metadata: arguments
                .get("metadata")
                .cloned()
                .unwrap_or_else(|| json!({})),
        }),
        "recall" => Ok(McpRequest::Recall {
            store: required_string(&arguments, "store")?,
            query: required_string(&arguments, "query")?,
            top_k: optional_usize(&arguments, "top_k", 5)?,
            include_preview: optional_bool(&arguments, "include_preview", true)?,
        }),
        "search" => Ok(McpRequest::Search {
            store: required_string(&arguments, "store")?,
            query: required_string(&arguments, "query")?,
            mode: optional_string(&arguments, "mode", "hybrid")?,
            top_k: optional_usize(&arguments, "top_k", 5)?,
            include_preview: optional_bool(&arguments, "include_preview", true)?,
        }),
        other => Err(McpError::invalid_request(format!(
            "unsupported tool: {other}"
        ))),
    }
}

fn tool_schema(name: &str, description: &str, input_schema: Value) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema
    })
}

fn required_string(arguments: &Value, key: &str) -> Result<String, McpError> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| McpError::invalid_request(format!("missing string argument: {key}")))
}

fn optional_string(arguments: &Value, key: &str, default: &str) -> Result<String, McpError> {
    arguments
        .get(key)
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| McpError::invalid_request(format!("{key} must be a string")))
        })
        .unwrap_or_else(|| Ok(default.to_owned()))
}

fn optional_usize(arguments: &Value, key: &str, default: usize) -> Result<usize, McpError> {
    let value = arguments
        .get(key)
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| {
                    McpError::invalid_request(format!("{key} must be a positive integer"))
                })
        })
        .unwrap_or(Ok(default))?;
    if value > MAX_MCP_SEARCH_TOP_K {
        return Err(McpError::invalid_request(format!(
            "{key} must be <= {MAX_MCP_SEARCH_TOP_K}"
        )));
    }
    Ok(value)
}

fn optional_bool(arguments: &Value, key: &str, default: bool) -> Result<bool, McpError> {
    arguments
        .get(key)
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| McpError::invalid_request(format!("{key} must be a boolean")))
        })
        .unwrap_or(Ok(default))
}

fn write_response(stdout: &mut io::Stdout, response: JsonRpcResponse) -> io::Result<()> {
    serde_json::to_writer(&mut *stdout, &response)?;
    stdout.write_all(b"\n")?;
    stdout.flush()
}

impl JsonRpcResponse {
    fn ok(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Value, code: i64, message: String, data: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data,
            }),
        }
    }

    fn from_mcp_error(id: Value, error: McpError) -> Self {
        Self::error(
            id,
            -32000,
            error.message().to_owned(),
            Some(json!({ "code": error.code() })),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        handle_json_rpc, parse_json_rpc_request, JsonRpcRequest, JsonRpcRequestId, JsonRpcResponse,
    };
    use serde_json::{json, Value};
    use wax_v2_mcp::WaxMcpSurface;

    #[test]
    fn json_rpc_notifications_do_not_get_error_responses() {
        let mut surface = WaxMcpSurface::default();
        let response = handle_json_rpc(
            &mut surface,
            JsonRpcRequest {
                jsonrpc: Some("2.0".to_owned()),
                id: JsonRpcRequestId::Missing,
                method: "unknown/notification".to_owned(),
                params: json!({}),
            },
        );

        assert!(response.is_none());
    }

    #[test]
    fn json_rpc_error_responses_include_null_id_when_request_id_is_unknown() {
        let encoded = serde_json::to_value(JsonRpcResponse::error(
            Value::Null,
            -32700,
            "parse error".to_owned(),
            None,
        ))
        .unwrap();

        assert_eq!(encoded.get("id"), Some(&Value::Null));
    }

    #[test]
    fn json_rpc_explicit_null_id_is_a_request_id() {
        let mut surface = WaxMcpSurface::default();
        let request =
            parse_json_rpc_request(json!({"jsonrpc":"2.0","id":null,"method":"tools/list"}))
                .unwrap();

        let response = handle_json_rpc(&mut surface, request).expect("null id request responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded.get("id"), Some(&Value::Null));
        assert!(encoded.get("result").is_some());
    }

    #[test]
    fn json_rpc_invalid_request_uses_supplied_id() {
        let error = parse_json_rpc_request(json!({"jsonrpc":"2.0","id":"req-1"})).unwrap_err();

        assert_eq!(error.id, json!("req-1"));
        assert_eq!(error.code, -32600);
    }

    #[test]
    fn tool_request_rejects_unbounded_top_k() {
        let error = super::tool_request(
            "recall",
            json!({
                "store": "memory.wax",
                "query": "anything",
                "top_k": 1_000_000
            }),
        )
        .unwrap_err();

        assert!(error.message().contains("top_k must be <="));
    }
}
