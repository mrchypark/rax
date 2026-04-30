use std::io::{self, BufRead, Write};
#[cfg(all(unix, target_os = "linux"))]
use std::path::Path;

use serde::Serialize;
use serde_json::{json, Value};
#[cfg(all(unix, target_os = "linux"))]
use wax_v2_mcp::MAX_MCP_PREVIEW_BYTES;
use wax_v2_mcp::{McpError, McpErrorCode, McpRequest, WaxMcpSurface, MAX_MCP_SEARCH_TOP_K};

const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
const MAX_JSON_RPC_STDIO_FRAME_BYTES: usize = 1024 * 1024;
const MAX_MCP_TOOL_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_JSON_RPC_ID_STRING_BYTES: usize = 1024;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum McpLifecycleState {
    Uninitialized,
    Initializing,
    Ready,
}

#[derive(Debug, Clone, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum JsonRpcMessageResponse {
    Single(JsonRpcResponse),
    Batch(Vec<JsonRpcResponse>),
}

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut surface = initialize_surface();

    run_json_rpc_stdio(stdin.lock(), &mut stdout, &mut surface);
}

#[cfg(all(unix, target_os = "linux"))]
fn initialize_surface() -> WaxMcpSurface {
    match std::env::var("WAX_MCP_ALLOWED_ROOT") {
        Ok(root) => match WaxMcpSurface::with_allowed_root(Path::new(&root)) {
            Ok(surface) => surface,
            Err(error) => {
                eprintln!("invalid WAX_MCP_ALLOWED_ROOT: {}", error.message());
                std::process::exit(1);
            }
        },
        Err(_) => {
            eprintln!("WAX_MCP_ALLOWED_ROOT is required");
            std::process::exit(1);
        }
    }
}

#[cfg(not(all(unix, target_os = "linux")))]
fn initialize_surface() -> WaxMcpSurface {
    WaxMcpSurface::default()
}

fn run_json_rpc_stdio<R: BufRead, W: Write>(
    mut reader: R,
    writer: &mut W,
    surface: &mut WaxMcpSurface,
) {
    let mut lifecycle = McpLifecycleState::Uninitialized;
    let mut line = Vec::new();
    loop {
        match read_json_rpc_stdio_frame(&mut reader, &mut line) {
            Ok(StdioFrame::Eof) => break,
            Ok(StdioFrame::FrameTooLarge) => {
                write_parse_error(
                    writer,
                    format!("JSON-RPC stdio frame exceeds {MAX_JSON_RPC_STDIO_FRAME_BYTES} bytes"),
                );
                break;
            }
            Ok(StdioFrame::InvalidUtf8(message)) => write_parse_error(writer, message),
            Ok(StdioFrame::Line(line)) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str::<Value>(trimmed) {
                    Ok(value) => {
                        if let Some(response) =
                            handle_json_rpc_message(surface, &mut lifecycle, value)
                        {
                            let _ = write_response(writer, response);
                        }
                    }
                    Err(error) => write_parse_error(writer, error.to_string()),
                }
            }
            Err(error) => {
                let _ = write_response(
                    writer,
                    JsonRpcResponse::error(Value::Null, -32000, error.to_string(), None).into(),
                );
                break;
            }
        }
    }
}

enum StdioFrame {
    Eof,
    Line(String),
    FrameTooLarge,
    InvalidUtf8(String),
}

fn read_json_rpc_stdio_frame<R: BufRead>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> std::io::Result<StdioFrame> {
    line.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if line.is_empty() {
                return Ok(StdioFrame::Eof);
            }
            break;
        }

        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if line.len().saturating_add(take) > MAX_JSON_RPC_STDIO_FRAME_BYTES {
            let remaining = MAX_JSON_RPC_STDIO_FRAME_BYTES.saturating_sub(line.len());
            reader.consume(remaining.min(take));
            return Ok(StdioFrame::FrameTooLarge);
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if line.last() == Some(&b'\n') {
            break;
        }
    }

    match String::from_utf8(line.clone()) {
        Ok(line) => Ok(StdioFrame::Line(line)),
        Err(error) => Ok(StdioFrame::InvalidUtf8(error.to_string())),
    }
}

fn write_parse_error<W: Write>(writer: &mut W, message: String) {
    let _ = write_response(
        writer,
        JsonRpcResponse::error(Value::Null, -32700, message, None).into(),
    );
}
#[derive(Debug)]
struct JsonRpcInvalidRequest {
    id: Option<Value>,
    code: i64,
    message: String,
}

fn parse_json_rpc_request(value: Value) -> Result<JsonRpcRequest, JsonRpcInvalidRequest> {
    let Some(object) = value.as_object() else {
        return Err(JsonRpcInvalidRequest {
            id: Some(Value::Null),
            code: -32600,
            message: "JSON-RPC request must be an object".to_owned(),
        });
    };
    let id = parse_json_rpc_id(object.get("id"))?;
    let error_id = match &id {
        JsonRpcRequestId::Missing => None,
        JsonRpcRequestId::Present(id) => Some(id.clone()),
    };
    let method = object
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| JsonRpcInvalidRequest {
            id: error_id.clone().or(Some(Value::Null)),
            code: -32600,
            message: "JSON-RPC request method must be a string".to_owned(),
        })?
        .to_owned();
    let jsonrpc = object
        .get("jsonrpc")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let params = match object.get("params") {
        Some(params) if params.is_object() || params.is_array() => params.clone(),
        Some(_) => {
            return Err(JsonRpcInvalidRequest {
                id: error_id,
                code: -32602,
                message: "JSON-RPC params must be an object or array when present".to_owned(),
            });
        }
        None => Value::Null,
    };
    Ok(JsonRpcRequest {
        jsonrpc,
        id,
        method,
        params,
    })
}

fn parse_json_rpc_id(id: Option<&Value>) -> Result<JsonRpcRequestId, JsonRpcInvalidRequest> {
    let Some(id) = id else {
        return Ok(JsonRpcRequestId::Missing);
    };
    if id.is_null() || id.is_string() || id.is_number() {
        if id
            .as_str()
            .is_some_and(|id| id.len() > MAX_JSON_RPC_ID_STRING_BYTES)
        {
            return Err(JsonRpcInvalidRequest {
                id: Some(Value::Null),
                code: -32600,
                message: format!(
                    "JSON-RPC string id must be <= {MAX_JSON_RPC_ID_STRING_BYTES} bytes"
                ),
            });
        }
        return Ok(JsonRpcRequestId::Present(id.clone()));
    }
    Err(JsonRpcInvalidRequest {
        id: Some(Value::Null),
        code: -32600,
        message: "JSON-RPC id must be null, a string, or a number".to_owned(),
    })
}

fn handle_json_rpc_message(
    surface: &mut WaxMcpSurface,
    lifecycle: &mut McpLifecycleState,
    value: Value,
) -> Option<JsonRpcMessageResponse> {
    if let Some(items) = value.as_array() {
        if items.is_empty() {
            return Some(
                JsonRpcResponse::error(
                    Value::Null,
                    -32600,
                    "JSON-RPC batch must not be empty".to_owned(),
                    None,
                )
                .into(),
            );
        }
        let mut next_lifecycle = *lifecycle;
        let responses = items
            .iter()
            .filter_map(|item| {
                if let Some(response) = batch_preflight_error(item) {
                    return Some(response);
                }
                let mut item_lifecycle = *lifecycle;
                let response = handle_json_rpc_value(surface, &mut item_lifecycle, item.clone());
                next_lifecycle = next_lifecycle.max(item_lifecycle);
                response
            })
            .collect::<Vec<_>>();
        *lifecycle = next_lifecycle;
        if responses.is_empty() {
            None
        } else {
            let response = JsonRpcMessageResponse::Batch(responses);
            if let Err(error) = validate_message_response_size(&response) {
                Some(oversized_response_fallback(&response, error))
            } else {
                Some(response)
            }
        }
    } else {
        handle_json_rpc_value(surface, lifecycle, value).map(Into::into)
    }
}

fn batch_preflight_error(item: &Value) -> Option<JsonRpcResponse> {
    let object = item.as_object()?;
    match object.get("method").and_then(Value::as_str) {
        Some("initialize") => {
            batch_rejected_method_error(object, "initialize must not be sent in a JSON-RPC batch")
        }
        Some("tools/call") => {
            batch_rejected_method_error(object, "tools/call must not be sent in a JSON-RPC batch")
        }
        _ => None,
    }
}

fn batch_rejected_method_error(
    object: &serde_json::Map<String, Value>,
    message: &str,
) -> Option<JsonRpcResponse> {
    match parse_json_rpc_id(object.get("id")) {
        Ok(JsonRpcRequestId::Present(id)) => {
            Some(JsonRpcResponse::error(id, -32600, message.to_owned(), None))
        }
        Ok(JsonRpcRequestId::Missing) => None,
        Err(error) => error
            .id
            .map(|id| JsonRpcResponse::error(id, error.code, error.message, None)),
    }
}

fn handle_json_rpc_value(
    surface: &mut WaxMcpSurface,
    lifecycle: &mut McpLifecycleState,
    value: Value,
) -> Option<JsonRpcResponse> {
    match parse_json_rpc_request(value) {
        Ok(request) => handle_json_rpc(surface, lifecycle, request),
        Err(error) => error
            .id
            .map(|id| JsonRpcResponse::error(id, error.code, error.message, None)),
    }
}

fn handle_json_rpc(
    surface: &mut WaxMcpSurface,
    lifecycle: &mut McpLifecycleState,
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
        "notifications/initialized" => match id {
            JsonRpcRequestId::Missing => {
                if *lifecycle == McpLifecycleState::Initializing {
                    *lifecycle = McpLifecycleState::Ready;
                }
                None
            }
            JsonRpcRequestId::Present(id) => Some(JsonRpcResponse::error(
                id,
                -32600,
                "notifications/initialized must not include an id".to_owned(),
                None,
            )),
        },
        _ => {
            let JsonRpcRequestId::Present(id) = id else {
                return None;
            };
            match request.method.as_str() {
                "ping" => Some(JsonRpcResponse::ok(id, json!({}))),
                "initialize" => {
                    if *lifecycle != McpLifecycleState::Uninitialized {
                        return Some(JsonRpcResponse::error(
                            id,
                            -32600,
                            "initialize is only valid before initialization starts".to_owned(),
                            None,
                        ));
                    }
                    let response = handle_initialize(id, request.params);
                    if response.error.is_none() {
                        *lifecycle = McpLifecycleState::Initializing;
                    }
                    Some(response)
                }
                _ if *lifecycle != McpLifecycleState::Ready => Some(JsonRpcResponse::error(
                    id,
                    -32002,
                    "MCP server is not initialized".to_owned(),
                    None,
                )),
                "tools/list" => Some(JsonRpcResponse::ok(
                    id,
                    json!({
                        "tools": product_memory_tools()
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

fn handle_initialize(id: Value, params: Value) -> JsonRpcResponse {
    if params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .is_none()
    {
        return JsonRpcResponse::error(
            id,
            -32602,
            "initialize params.protocolVersion must be a string".to_owned(),
            None,
        );
    }
    if !params
        .get("capabilities")
        .is_some_and(|capabilities| capabilities.is_object())
    {
        return JsonRpcResponse::error(
            id,
            -32602,
            "initialize params.capabilities must be an object".to_owned(),
            None,
        );
    }
    let client_info = params.get("clientInfo");
    if !client_info.is_some_and(|client_info| {
        client_info.is_object()
            && client_info.get("name").and_then(Value::as_str).is_some()
            && client_info.get("version").and_then(Value::as_str).is_some()
    }) {
        return JsonRpcResponse::error(
            id,
            -32602,
            "initialize params.clientInfo must include string name and version".to_owned(),
            None,
        );
    }
    JsonRpcResponse::ok(
        id,
        json!({
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": server_capabilities(),
            "serverInfo": {
                "name": "wax-mcp",
                "version": env!("CARGO_PKG_VERSION")
            }
        }),
    )
}

#[cfg(all(unix, target_os = "linux"))]
fn server_capabilities() -> Value {
    json!({
        "tools": {}
    })
}

#[cfg(not(all(unix, target_os = "linux")))]
fn server_capabilities() -> Value {
    json!({})
}

fn handle_tool_call(surface: &mut WaxMcpSurface, id: Value, params: Value) -> JsonRpcResponse {
    #[cfg(not(all(unix, target_os = "linux")))]
    {
        let _ = surface;
        let _ = params;
        JsonRpcResponse::from_mcp_error(
            id,
            McpError::invalid_request(
                "MCP store tools are supported only on Linux with WAX_MCP_ALLOWED_ROOT",
            ),
        )
    }
    #[cfg(all(unix, target_os = "linux"))]
    {
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
            Ok(response) => match tool_success_result(&response) {
                Ok(result) => JsonRpcResponse::ok(id, result),
                Err(error) => JsonRpcResponse::from_mcp_error(id, error),
            },
            Err(error) if error.code() == &McpErrorCode::InvalidRequest => {
                JsonRpcResponse::from_mcp_error(id, error)
            }
            Err(error) => JsonRpcResponse::ok(
                id,
                json!({
                    "content": [
                        {
                            "type": "text",
                            "text": error.message()
                        }
                    ],
                    "structuredContent": {
                        "error": {
                            "code": error.code(),
                            "message": error.message()
                        }
                    },
                    "isError": true
                }),
            ),
        }
    }
}

#[cfg_attr(not(all(unix, target_os = "linux")), allow(dead_code))]
fn tool_success_result(response: &wax_v2_mcp::McpResponse) -> Result<Value, McpError> {
    let text_payload = serde_json::to_string(response)
        .map_err(|error| McpError::invalid_request(error.to_string()))?;
    let result = json!({
        "content": [
            {
                "type": "text",
                "text": text_payload
            }
        ],
        "structuredContent": response
    });
    let size = serde_json::to_vec(&result)
        .map_err(|error| McpError::invalid_request(error.to_string()))?
        .len();
    if size > MAX_MCP_TOOL_RESPONSE_BYTES {
        return Err(McpError::invalid_request(format!(
            "tool response exceeds {MAX_MCP_TOOL_RESPONSE_BYTES} bytes"
        )));
    }
    Ok(result)
}

#[cfg_attr(not(all(unix, target_os = "linux")), allow(dead_code))]
fn tool_request(name: &str, arguments: Value) -> Result<McpRequest, McpError> {
    match name {
        "remember" => {
            let metadata = arguments
                .get("metadata")
                .cloned()
                .unwrap_or_else(|| json!({}));
            if !metadata.is_object() {
                return Err(McpError::invalid_request("metadata must be an object"));
            }
            Ok(McpRequest::Remember {
                store: required_string(&arguments, "store")?,
                content: required_string(&arguments, "content")?,
                metadata,
            })
        }
        "recall" => Ok(McpRequest::Recall {
            store: required_string(&arguments, "store")?,
            query: required_string(&arguments, "query")?,
            top_k: optional_usize(&arguments, "top_k", 5)?,
            include_preview: optional_bool(&arguments, "include_preview", false)?,
        }),
        "search" => {
            let mode = optional_string(&arguments, "mode", "hybrid")?;
            if !matches!(mode.as_str(), "text" | "vector" | "hybrid") {
                return Err(McpError::invalid_request(
                    "mode must be one of text, vector, or hybrid",
                ));
            }
            Ok(McpRequest::Search {
                store: required_string(&arguments, "store")?,
                query: required_string(&arguments, "query")?,
                mode,
                top_k: optional_usize(&arguments, "top_k", 5)?,
                include_preview: optional_bool(&arguments, "include_preview", false)?,
            })
        }
        other => Err(McpError::invalid_request(format!(
            "unsupported tool: {other}"
        ))),
    }
}

#[cfg(all(unix, target_os = "linux"))]
fn tool_schema(name: &str, description: &str, input_schema: Value) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema
    })
}

#[cfg(all(unix, target_os = "linux"))]
fn product_memory_tools() -> Vec<Value> {
    vec![
        tool_schema(
            "remember",
            "Store a memory in a local .wax file",
            json!({
                "type": "object",
                "properties": {
                    "store": {"type": "string"},
                    "content": {"type": "string"},
                    "metadata": {"type": "object"}
                },
                "required": ["store", "content"]
            }),
        ),
        tool_schema(
            "recall",
            "Recall memory from a local .wax file with hybrid search",
            json!({
                "type": "object",
                "properties": {
                    "store": {"type": "string"},
                    "query": {"type": "string"},
                    "top_k": {"type": "integer", "default": 5, "minimum": 0, "maximum": MAX_MCP_SEARCH_TOP_K},
                    "include_preview": {"type": "boolean", "default": false, "description": "When true, previews are capped per hit."},
                    "max_preview_bytes": {"type": "integer", "const": MAX_MCP_PREVIEW_BYTES, "readOnly": true}
                },
                "required": ["store", "query"]
            }),
        ),
        tool_schema(
            "search",
            "Search memory from a local .wax file",
            json!({
                "type": "object",
                "properties": {
                    "store": {"type": "string"},
                    "query": {"type": "string"},
                    "mode": {"type": "string", "enum": ["text", "vector", "hybrid"], "default": "hybrid"},
                    "top_k": {"type": "integer", "default": 5, "minimum": 0, "maximum": MAX_MCP_SEARCH_TOP_K},
                    "include_preview": {"type": "boolean", "default": false, "description": "When true, previews are capped per hit."},
                    "max_preview_bytes": {"type": "integer", "const": MAX_MCP_PREVIEW_BYTES, "readOnly": true}
                },
                "required": ["store", "query"]
            }),
        ),
    ]
}

#[cfg(not(all(unix, target_os = "linux")))]
fn product_memory_tools() -> Vec<Value> {
    Vec::new()
}

#[cfg_attr(not(all(unix, target_os = "linux")), allow(dead_code))]
fn required_string(arguments: &Value, key: &str) -> Result<String, McpError> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| McpError::invalid_request(format!("missing string argument: {key}")))
}

#[cfg_attr(not(all(unix, target_os = "linux")), allow(dead_code))]
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

#[cfg_attr(not(all(unix, target_os = "linux")), allow(dead_code))]
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

#[cfg_attr(not(all(unix, target_os = "linux")), allow(dead_code))]
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

fn write_response<W: Write>(writer: &mut W, response: JsonRpcMessageResponse) -> io::Result<()> {
    let response = if let Err(error) = validate_message_response_size(&response) {
        let fallback = oversized_response_fallback(&response, error);
        if validate_message_response_size(&fallback).is_ok() {
            fallback
        } else {
            minimal_oversized_response_fallback(&response)
        }
    } else {
        response
    };
    serde_json::to_writer(&mut *writer, &response)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

fn validate_message_response_size(response: &JsonRpcMessageResponse) -> Result<(), McpError> {
    let size = serde_json::to_vec(response)
        .map_err(|error| McpError::invalid_request(error.to_string()))?
        .len();
    if size > MAX_MCP_TOOL_RESPONSE_BYTES {
        return Err(McpError::invalid_request(format!(
            "JSON-RPC response exceeds {MAX_MCP_TOOL_RESPONSE_BYTES} bytes"
        )));
    }
    Ok(())
}

fn minimal_oversized_response_fallback(
    response: &JsonRpcMessageResponse,
) -> JsonRpcMessageResponse {
    let error = JsonRpcResponse::error(
        Value::Null,
        -32602,
        format!("JSON-RPC response exceeds {MAX_MCP_TOOL_RESPONSE_BYTES} bytes"),
        None,
    );
    match response {
        JsonRpcMessageResponse::Single(_) => error.into(),
        JsonRpcMessageResponse::Batch(_) => JsonRpcMessageResponse::Batch(vec![error]),
    }
}

fn oversized_response_fallback(
    response: &JsonRpcMessageResponse,
    error: McpError,
) -> JsonRpcMessageResponse {
    match response {
        JsonRpcMessageResponse::Single(response) => {
            JsonRpcResponse::from_mcp_error(response.id.clone(), error).into()
        }
        JsonRpcMessageResponse::Batch(_) => {
            JsonRpcMessageResponse::Batch(vec![JsonRpcResponse::from_mcp_error(Value::Null, error)])
        }
    }
}

impl From<JsonRpcResponse> for JsonRpcMessageResponse {
    fn from(response: JsonRpcResponse) -> Self {
        Self::Single(response)
    }
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
        let json_rpc_code = match error.code() {
            McpErrorCode::InvalidRequest => -32602,
            McpErrorCode::Storage | McpErrorCode::SessionNotFound => -32000,
        };
        Self::error(
            id,
            json_rpc_code,
            error.message().to_owned(),
            Some(json!({ "code": error.code() })),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        handle_json_rpc, handle_json_rpc_message, parse_json_rpc_request, run_json_rpc_stdio,
        tool_request, tool_success_result, validate_message_response_size, write_response,
        JsonRpcMessageResponse, JsonRpcRequest, JsonRpcRequestId, JsonRpcResponse,
        McpLifecycleState, MAX_JSON_RPC_ID_STRING_BYTES, MAX_JSON_RPC_STDIO_FRAME_BYTES,
        MAX_MCP_TOOL_RESPONSE_BYTES,
    };
    use serde_json::{json, Value};
    use wax_v2_mcp::{
        McpErrorCode, McpResponse, McpSearchHit, WaxMcpSurface, MAX_MCP_PREVIEW_BYTES,
    };

    #[test]
    fn json_rpc_notifications_do_not_get_error_responses() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Uninitialized;
        let response = handle_json_rpc(
            &mut surface,
            &mut lifecycle,
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
    fn tool_request_defaults_previews_off_for_mcp_store_searches() {
        let request = tool_request(
            "recall",
            json!({"store":"/tmp/private/agent.wax","query":"alpha"}),
        )
        .unwrap();

        assert!(matches!(
            request,
            wax_v2_mcp::McpRequest::Recall {
                include_preview: false,
                ..
            }
        ));
    }

    #[test]
    fn tool_success_result_exposes_full_payload_in_2024_11_05_text_content() {
        let response = McpResponse::SearchResults {
            hits: vec![McpSearchHit {
                doc_id: "doc-1".to_owned(),
                preview: Some("x".repeat(MAX_MCP_PREVIEW_BYTES)),
            }],
        };

        let result = tool_success_result(&response).unwrap();

        let text_payload = result
            .pointer("/content/0/text")
            .and_then(Value::as_str)
            .expect("text content should contain serialized tool payload");
        let parsed_text_payload: Value =
            serde_json::from_str(text_payload).expect("text content should be JSON");
        assert_eq!(
            parsed_text_payload.pointer("/hits/0/doc_id"),
            Some(&json!("doc-1"))
        );
        assert_eq!(
            parsed_text_payload.pointer("/hits/0/preview"),
            Some(&json!("x".repeat(MAX_MCP_PREVIEW_BYTES)))
        );
        assert_eq!(
            result.pointer("/structuredContent/hits/0/preview"),
            Some(&json!("x".repeat(MAX_MCP_PREVIEW_BYTES)))
        );
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
    fn json_rpc_accepts_explicit_null_id() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Ready;
        let request =
            parse_json_rpc_request(json!({"jsonrpc":"2.0","id":null,"method":"tools/list"}))
                .unwrap();

        let response = handle_json_rpc(&mut surface, &mut lifecycle, request)
            .expect("null id request responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded.get("id"), Some(&Value::Null));
        assert!(encoded.get("result").is_some());
    }

    #[test]
    fn json_rpc_accepts_string_and_number_ids() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Ready;
        let request =
            parse_json_rpc_request(json!({"jsonrpc":"2.0","id":"req-1","method":"tools/list"}))
                .unwrap();

        let response = handle_json_rpc(&mut surface, &mut lifecycle, request)
            .expect("string id request responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded.get("id"), Some(&json!("req-1")));
        assert!(encoded.get("result").is_some());

        let request =
            parse_json_rpc_request(json!({"jsonrpc":"2.0","id":7,"method":"tools/list"})).unwrap();
        let response = handle_json_rpc(&mut surface, &mut lifecycle, request)
            .expect("number id request responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded.get("id"), Some(&json!(7)));
        assert!(encoded.get("result").is_some());

        let request =
            parse_json_rpc_request(json!({"jsonrpc":"2.0","id":1.5,"method":"tools/list"}))
                .unwrap();
        let response = handle_json_rpc(&mut surface, &mut lifecycle, request)
            .expect("fractional number id request responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded.get("id"), Some(&json!(1.5)));
        assert!(encoded.get("result").is_some());
    }

    #[test]
    fn json_rpc_rejects_oversized_string_ids_before_dispatch() {
        let error = parse_json_rpc_request(json!({
            "jsonrpc": "2.0",
            "id": "i".repeat(MAX_JSON_RPC_ID_STRING_BYTES + 1),
            "method": "tools/call",
            "params": {
                "name": "remember",
                "arguments": {"store": "agent.wax", "content": "alpha"}
            }
        }))
        .unwrap_err();

        assert_eq!(error.id, Some(Value::Null));
        assert_eq!(error.code, -32600);
        assert!(error.message.contains("JSON-RPC string id must be <="));
    }

    #[test]
    fn json_rpc_rejects_container_ids() {
        for id in [json!({}), json!([]), json!(true)] {
            let error =
                parse_json_rpc_request(json!({"jsonrpc":"2.0","id":id,"method":"tools/list"}))
                    .unwrap_err();

            assert_eq!(error.id, Some(Value::Null));
            assert_eq!(error.code, -32600);
        }
    }

    #[test]
    fn json_rpc_accepts_positional_params_at_envelope_layer() {
        let request = parse_json_rpc_request(json!({
            "jsonrpc": "2.0",
            "id": "positional",
            "method": "tools/list",
            "params": []
        }))
        .unwrap();

        assert!(request.params.is_array());
    }

    #[test]
    fn invalid_json_rpc_notifications_do_not_get_error_responses() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Ready;

        let response = handle_json_rpc_message(
            &mut surface,
            &mut lifecycle,
            json!({"jsonrpc":"2.0","method":"tools/list","params":[]}),
        );

        assert!(response.is_none());
    }

    #[test]
    fn null_params_notifications_do_not_advance_lifecycle() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Initializing;

        let response = handle_json_rpc_message(
            &mut surface,
            &mut lifecycle,
            json!({"jsonrpc":"2.0","method":"notifications/initialized","params":null}),
        );

        assert!(response.is_none());
        assert_eq!(lifecycle, McpLifecycleState::Initializing);
    }

    #[test]
    fn null_params_requests_are_invalid() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Ready;

        let response = handle_json_rpc_message(
            &mut surface,
            &mut lifecycle,
            json!({"jsonrpc":"2.0","id":"list","method":"tools/list","params":null}),
        )
        .expect("request responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded.pointer("/id"), Some(&json!("list")));
        assert_eq!(encoded.pointer("/error/code"), Some(&json!(-32602)));
    }

    #[test]
    fn json_rpc_batch_returns_responses_for_requests_only() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Ready;
        let response = handle_json_rpc_message(
            &mut surface,
            &mut lifecycle,
            json!([
                {"jsonrpc":"2.0","id":"list","method":"tools/list"},
                {"jsonrpc":"2.0","method":"notifications/initialized"},
                {"jsonrpc":"2.0","id":{},"method":"tools/list"}
            ]),
        )
        .expect("batch has request responses");
        let JsonRpcMessageResponse::Batch(responses) = response else {
            panic!("expected batch response");
        };

        assert_eq!(responses.len(), 2);
        let encoded = serde_json::to_value(responses).unwrap();
        assert_eq!(encoded.pointer("/0/id"), Some(&json!("list")));
        assert_eq!(encoded.pointer("/1/id"), Some(&Value::Null));
        assert_eq!(encoded.pointer("/1/error/code"), Some(&json!(-32600)));
    }

    #[test]
    fn json_rpc_batch_response_size_is_capped() {
        let mut responses = Vec::new();
        let large_result = json!({
            "content": [{"type": "text", "text": "ok"}],
            "structuredContent": {
                "result": "search_results",
                "hits": [{
                    "doc_id": "doc-1",
                    "preview": "x".repeat(16 * 1024)
                }]
            }
        });
        for id in 0..100 {
            responses.push(JsonRpcResponse::ok(json!(id), large_result.clone()));
        }

        let error =
            validate_message_response_size(&JsonRpcMessageResponse::Batch(responses)).unwrap_err();

        assert_eq!(error.code(), &McpErrorCode::InvalidRequest);
        assert!(error.message().contains("JSON-RPC response exceeds"));
    }

    #[test]
    fn oversized_batch_response_preserves_batch_envelope() {
        let large_result = json!({
            "content": [{"type": "text", "text": "x".repeat(MAX_MCP_TOOL_RESPONSE_BYTES)}],
        });
        let mut output = Vec::new();

        write_response(
            &mut output,
            JsonRpcMessageResponse::Batch(vec![JsonRpcResponse::ok(
                json!("request-1"),
                large_result,
            )]),
        )
        .unwrap();

        let encoded: Value = serde_json::from_slice(&output).unwrap();
        assert!(encoded.is_array());
        assert_eq!(encoded.pointer("/0/id"), Some(&Value::Null));
        assert_eq!(encoded.pointer("/0/error/code"), Some(&json!(-32602)));
    }

    #[test]
    fn oversized_single_response_preserves_request_id() {
        let large_result = json!({
            "content": [{"type": "text", "text": "x".repeat(MAX_MCP_TOOL_RESPONSE_BYTES)}],
        });
        let mut output = Vec::new();

        write_response(
            &mut output,
            JsonRpcResponse::ok(json!("request-1"), large_result).into(),
        )
        .unwrap();

        let encoded: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(encoded.get("id"), Some(&json!("request-1")));
        assert_eq!(encoded.pointer("/error/code"), Some(&json!(-32602)));
        assert_eq!(
            encoded.pointer("/error/message"),
            Some(&json!(format!(
                "JSON-RPC response exceeds {MAX_MCP_TOOL_RESPONSE_BYTES} bytes"
            )))
        );
    }

    #[test]
    fn oversized_single_response_fallback_does_not_recurse_on_huge_id() {
        let large_result = json!({
            "content": [{"type": "text", "text": "x".repeat(MAX_MCP_TOOL_RESPONSE_BYTES)}],
        });
        let huge_id = "i".repeat(MAX_MCP_TOOL_RESPONSE_BYTES);
        let mut output = Vec::new();

        write_response(
            &mut output,
            JsonRpcResponse::ok(json!(huge_id), large_result).into(),
        )
        .unwrap();

        let encoded: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(encoded.get("id"), Some(&Value::Null));
        assert_eq!(encoded.pointer("/error/code"), Some(&json!(-32602)));
        assert!(encoded
            .pointer("/error/message")
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("JSON-RPC response exceeds")));
    }

    #[test]
    fn json_rpc_notification_only_batch_has_no_response() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Uninitialized;
        let response = handle_json_rpc_message(
            &mut surface,
            &mut lifecycle,
            json!([{"jsonrpc":"2.0","method":"notifications/initialized"}]),
        );

        assert!(response.is_none());
    }

    #[test]
    fn initialize_notification_in_batch_gets_no_response() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Uninitialized;
        let response = handle_json_rpc_message(
            &mut surface,
            &mut lifecycle,
            json!([
                {"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}
            ]),
        );

        assert!(response.is_none());
        assert_eq!(lifecycle, McpLifecycleState::Uninitialized);
    }

    #[test]
    fn json_rpc_empty_batch_is_invalid_request() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Uninitialized;
        let response = handle_json_rpc_message(&mut surface, &mut lifecycle, json!([]))
            .expect("empty batch responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded.pointer("/id"), Some(&Value::Null));
        assert_eq!(encoded.pointer("/error/code"), Some(&json!(-32600)));
    }

    #[test]
    fn json_rpc_stdio_rejects_multiline_json_messages() {
        let mut surface = WaxMcpSurface::default();
        let input = br#"{
            "jsonrpc": "2.0",
            "id": "init",
            "method": "initialize",
            "params": {"protocolVersion": "2099-01-01", "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}}
        }
        {"jsonrpc":"2.0","id":"ping-1","method":"ping"}"#;
        let mut output = Vec::new();

        run_json_rpc_stdio(&input[..], &mut output, &mut surface);

        let lines = String::from_utf8(output).unwrap();
        let responses = lines
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert!(responses
            .iter()
            .any(|response| response.pointer("/error/code") == Some(&json!(-32700))));
        assert_eq!(
            responses.last().and_then(|response| response.get("id")),
            Some(&json!("ping-1"))
        );
        assert_eq!(
            responses.last().and_then(|response| response.get("result")),
            Some(&json!({}))
        );
    }

    #[test]
    fn json_rpc_stdio_rejects_multiple_messages_on_one_line() {
        let mut surface = WaxMcpSurface::default();
        let input = br#"{"jsonrpc":"2.0","id":"one","method":"ping"} {"jsonrpc":"2.0","id":"two","method":"ping"}
"#;
        let mut output = Vec::new();

        run_json_rpc_stdio(&input[..], &mut output, &mut surface);

        let lines = String::from_utf8(output).unwrap();
        let responses = lines
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].pointer("/error/code"), Some(&json!(-32700)));
    }

    #[test]
    fn json_rpc_stdio_rejects_oversized_frame() {
        let mut surface = WaxMcpSurface::default();
        let mut input = vec![b' '; MAX_JSON_RPC_STDIO_FRAME_BYTES + 1];
        input.push(b'\n');
        let mut output = Vec::new();

        run_json_rpc_stdio(&input[..], &mut output, &mut surface);

        let lines = String::from_utf8(output).unwrap();
        let responses = lines
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].pointer("/error/code"), Some(&json!(-32700)));
        assert!(responses[0]
            .pointer("/error/message")
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("exceeds")));
    }

    #[test]
    fn json_rpc_stdio_recovers_after_malformed_line() {
        let mut surface = WaxMcpSurface::default();
        let input = br#"bad
        {"jsonrpc":"2.0","id":"ping-1","method":"ping"}"#;
        let mut output = Vec::new();

        run_json_rpc_stdio(&input[..], &mut output, &mut surface);

        let lines = String::from_utf8(output).unwrap();
        let responses = lines
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].pointer("/error/code"), Some(&json!(-32700)));
        assert_eq!(responses[1].get("id"), Some(&json!("ping-1")));
        assert_eq!(responses[1].get("result"), Some(&json!({})));
    }

    #[test]
    fn initialized_notification_with_id_is_rejected() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Initializing;
        let request = parse_json_rpc_request(json!({
            "jsonrpc":"2.0",
            "id":"bad-init",
            "method":"notifications/initialized"
        }))
        .unwrap();

        let response =
            handle_json_rpc(&mut surface, &mut lifecycle, request).expect("request responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded.get("id"), Some(&json!("bad-init")));
        assert_eq!(encoded.pointer("/error/code"), Some(&json!(-32600)));
        assert_eq!(lifecycle, McpLifecycleState::Initializing);
    }

    #[test]
    fn batch_rejects_initialize_without_advancing_lifecycle() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Uninitialized;
        let response = handle_json_rpc_message(
            &mut surface,
            &mut lifecycle,
            json!([
                {"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}}},
                {"jsonrpc":"2.0","method":"notifications/initialized"},
                {"jsonrpc":"2.0","id":"list","method":"tools/list"}
            ]),
        )
        .expect("batch responds");
        let JsonRpcMessageResponse::Batch(responses) = response else {
            panic!("expected batch response");
        };
        let encoded = serde_json::to_value(&responses).unwrap();

        assert_eq!(responses.len(), 2);
        assert_eq!(encoded.pointer("/0/id"), Some(&json!("init")));
        assert_eq!(encoded.pointer("/0/error/code"), Some(&json!(-32600)));
        assert_eq!(
            encoded.pointer("/0/error/message"),
            Some(&json!("initialize must not be sent in a JSON-RPC batch"))
        );
        assert_eq!(encoded.pointer("/1/id"), Some(&json!("list")));
        assert_eq!(encoded.pointer("/1/error/code"), Some(&json!(-32002)));
        assert_eq!(lifecycle, McpLifecycleState::Uninitialized);
    }

    #[test]
    fn batch_initialize_error_does_not_drop_other_request_ids() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Uninitialized;
        let response = handle_json_rpc_message(
            &mut surface,
            &mut lifecycle,
            json!([
                {"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}}},
                {"jsonrpc":"2.0","id":"ping-1","method":"ping"}
            ]),
        )
        .expect("batch responds");
        let JsonRpcMessageResponse::Batch(responses) = response else {
            panic!("expected batch response");
        };
        let encoded = serde_json::to_value(&responses).unwrap();

        assert_eq!(responses.len(), 2);
        assert_eq!(encoded.pointer("/0/id"), Some(&json!("init")));
        assert_eq!(encoded.pointer("/0/error/code"), Some(&json!(-32600)));
        assert_eq!(encoded.pointer("/1/id"), Some(&json!("ping-1")));
        assert_eq!(encoded.pointer("/1/result"), Some(&json!({})));
        assert_eq!(lifecycle, McpLifecycleState::Uninitialized);
    }

    #[test]
    fn batch_tools_call_is_rejected_before_execution() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Ready;
        let response = handle_json_rpc_message(
            &mut surface,
            &mut lifecycle,
            json!([
                {"jsonrpc":"2.0","id":"remember-1","method":"tools/call","params":{"name":"remember","arguments":{"store":"agent.wax","content":"alpha"}}},
                {"jsonrpc":"2.0","id":"list","method":"tools/list"}
            ]),
        )
        .expect("batch responds");
        let JsonRpcMessageResponse::Batch(responses) = response else {
            panic!("expected batch response");
        };
        let encoded = serde_json::to_value(&responses).unwrap();

        assert_eq!(responses.len(), 2);
        assert_eq!(encoded.pointer("/0/id"), Some(&json!("remember-1")));
        assert_eq!(encoded.pointer("/0/error/code"), Some(&json!(-32600)));
        assert_eq!(
            encoded.pointer("/0/error/message"),
            Some(&json!("tools/call must not be sent in a JSON-RPC batch"))
        );
        assert_eq!(encoded.pointer("/1/id"), Some(&json!("list")));
        assert!(encoded.pointer("/1/result/tools").is_some());
        assert_eq!(lifecycle, McpLifecycleState::Ready);
    }

    #[test]
    fn json_rpc_ping_works_before_initialization() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Uninitialized;
        let request =
            parse_json_rpc_request(json!({"jsonrpc":"2.0","id":"ping-1","method":"ping"})).unwrap();

        let response =
            handle_json_rpc(&mut surface, &mut lifecycle, request).expect("ping responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded.get("id"), Some(&json!("ping-1")));
        assert_eq!(encoded.get("result"), Some(&json!({})));
        assert_eq!(lifecycle, McpLifecycleState::Uninitialized);
    }

    #[test]
    fn tools_are_rejected_until_initialization_completes() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Uninitialized;
        let request =
            parse_json_rpc_request(json!({"jsonrpc":"2.0","id":"list","method":"tools/list"}))
                .unwrap();

        let response =
            handle_json_rpc(&mut surface, &mut lifecycle, request).expect("request responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded.pointer("/error/code"), Some(&json!(-32002)));
    }

    #[cfg(not(all(unix, target_os = "linux")))]
    #[test]
    fn tools_call_is_rejected_on_unsupported_platforms() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Ready;
        let request = parse_json_rpc_request(json!({
            "jsonrpc": "2.0",
            "id": "remember-unsupported",
            "method": "tools/call",
            "params": {
                "name": "remember",
                "arguments": {"store": "agent.wax", "content": "alpha"}
            }
        }))
        .unwrap();

        let response = handle_json_rpc(&mut surface, &mut lifecycle, request)
            .expect("tools/call request responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded.get("id"), Some(&json!("remember-unsupported")));
        assert_eq!(encoded.pointer("/error/code"), Some(&json!(-32602)));
        assert!(encoded
            .pointer("/error/message")
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("supported only on Linux")));
    }

    #[test]
    fn initialize_requires_supported_protocol_version() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Uninitialized;
        let request = parse_json_rpc_request(json!({
            "jsonrpc":"2.0",
            "id":"init-1",
            "method":"initialize",
            "params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}}
        }))
        .unwrap();

        let response =
            handle_json_rpc(&mut surface, &mut lifecycle, request).expect("initialize responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(
            encoded.pointer("/result/protocolVersion"),
            Some(&json!("2024-11-05"))
        );
        #[cfg(all(unix, target_os = "linux"))]
        assert_eq!(
            encoded.pointer("/result/capabilities/tools"),
            Some(&json!({}))
        );
        #[cfg(not(all(unix, target_os = "linux")))]
        assert_eq!(encoded.pointer("/result/capabilities"), Some(&json!({})));
        assert_eq!(lifecycle, McpLifecycleState::Initializing);
    }

    #[test]
    fn initialize_negotiates_supported_protocol_version_for_mismatch() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Uninitialized;
        let request = parse_json_rpc_request(json!({
            "jsonrpc":"2.0",
            "id":"init-1",
            "method":"initialize",
            "params":{"protocolVersion":"2099-01-01","capabilities":{},"clientInfo":{"name":"test","version":"1"}}
        }))
        .unwrap();

        let response =
            handle_json_rpc(&mut surface, &mut lifecycle, request).expect("initialize responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(
            encoded.pointer("/result/protocolVersion"),
            Some(&json!("2024-11-05"))
        );
    }

    #[test]
    fn initialize_rejects_missing_or_non_string_protocol_version() {
        let mut surface = WaxMcpSurface::default();
        for params in [json!({}), json!({"protocolVersion":7})] {
            let mut lifecycle = McpLifecycleState::Uninitialized;
            let request = parse_json_rpc_request(json!({
                "jsonrpc":"2.0",
                "id":"init-1",
                "method":"initialize",
                "params":params
            }))
            .unwrap();

            let response = handle_json_rpc(&mut surface, &mut lifecycle, request)
                .expect("initialize responds");
            let encoded = serde_json::to_value(response).unwrap();

            assert_eq!(encoded.pointer("/error/code"), Some(&json!(-32602)));
            assert!(encoded.pointer("/result").is_none());
        }
    }

    #[test]
    fn initialize_rejects_missing_capabilities_or_client_info() {
        let mut surface = WaxMcpSurface::default();
        for params in [
            json!({"protocolVersion":"2024-11-05","clientInfo":{"name":"test","version":"1"}}),
            json!({"protocolVersion":"2024-11-05","capabilities":{}}),
            json!({"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test"}}),
        ] {
            let mut lifecycle = McpLifecycleState::Uninitialized;
            let request = parse_json_rpc_request(json!({
                "jsonrpc":"2.0",
                "id":"init-1",
                "method":"initialize",
                "params":params
            }))
            .unwrap();

            let response = handle_json_rpc(&mut surface, &mut lifecycle, request)
                .expect("initialize responds");
            let encoded = serde_json::to_value(response).unwrap();

            assert_eq!(encoded.pointer("/error/code"), Some(&json!(-32602)));
            assert_eq!(lifecycle, McpLifecycleState::Uninitialized);
        }
    }

    #[test]
    fn initialize_rejects_repeated_calls() {
        let mut surface = WaxMcpSurface::default();
        let mut lifecycle = McpLifecycleState::Initializing;
        let request = parse_json_rpc_request(json!({
            "jsonrpc":"2.0",
            "id":"init-2",
            "method":"initialize",
            "params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}}
        }))
        .unwrap();

        let response =
            handle_json_rpc(&mut surface, &mut lifecycle, request).expect("initialize responds");
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded.pointer("/error/code"), Some(&json!(-32600)));
        assert_eq!(lifecycle, McpLifecycleState::Initializing);
    }

    #[test]
    fn json_rpc_invalid_request_uses_supplied_id() {
        let error = parse_json_rpc_request(json!({"jsonrpc":"2.0","id":"req-1"})).unwrap_err();

        assert_eq!(error.id, Some(json!("req-1")));
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

    #[test]
    fn tool_request_rejects_non_object_metadata() {
        let error = super::tool_request(
            "remember",
            json!({"store":"memory.wax","content":"x","metadata":[]}),
        )
        .unwrap_err();

        assert!(error.message().contains("metadata must be an object"));
    }

    #[test]
    fn tool_request_rejects_unknown_search_mode() {
        let error = super::tool_request(
            "search",
            json!({"store":"memory.wax","query":"x","mode":"bogus"}),
        )
        .unwrap_err();

        assert!(error.message().contains("mode must be one of"));
    }
}
