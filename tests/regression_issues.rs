//! Focused repro tests for recent GitHub issues.
//!
//! These tests use only local translation code or a local mock upstream; they
//! do not require a real LLM, Codex Desktop, or an MCP server.

use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode},
    response::Response,
    routing::{get, post},
    Router,
};
use codex_relay::session::SessionStore;
use codex_relay::translate::to_chat_request;
use codex_relay::types::ResponsesRequest;
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const RELAY_BIN: &str = env!("CARGO_BIN_EXE_codex-relay");

fn fixture(name: &str) -> ResponsesRequest {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/fixtures/codex_0_128_0");
    p.push(name);
    let bytes = std::fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()));
    serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("parse {}: {e}", p.display()))
}

fn pick_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

#[test]
fn issue_6_namespace_tools_keep_namespace_when_flattened() {
    let req = fixture("with_namespace_tool.json");
    let chat = to_chat_request(&req, Vec::new(), &SessionStore::new());

    let names: Vec<String> = chat
        .tools
        .iter()
        .map(|t| {
            t.get("function")
                .and_then(|f| f.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        })
        .collect();

    assert!(
        names
            .iter()
            .any(|n| n == "mcp__codex_apps__github_add_comment_to_issue"),
        "namespace child tool should be flattened with its namespace prefix: {names:?}"
    );
}

#[derive(Clone)]
struct MockState {
    bodies: Arc<Mutex<Vec<Value>>>,
}

async fn models_handler() -> axum::Json<Value> {
    axum::Json(json!({"data": [{"id": "mock-model"}]}))
}

async fn chat_handler(State(state): State<MockState>, req: axum::extract::Request) -> Response {
    let bytes = match axum::body::to_bytes(req.into_body(), 1_000_000).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("bad body"))
                .unwrap();
        }
    };
    let body: Value = serde_json::from_slice(&bytes).expect("chat request json");
    state.bodies.lock().unwrap().push(body);

    let sse = concat!(
        "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"OK\"}}]}\n\n",
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":2,\"total_tokens\":9}}\n\n",
        "data: [DONE]\n\n",
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from(sse))
        .unwrap()
}

async fn spawn_mock_upstream() -> (u16, Arc<Mutex<Vec<Value>>>) {
    let port = pick_port();
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let state = MockState {
        bodies: bodies.clone(),
    };
    let app = Router::new()
        .route("/v1/models", get(models_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("bind mock upstream");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock upstream serve");
    });
    (port, bodies)
}

struct Relay {
    child: Child,
    port: u16,
}

impl Drop for Relay {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Relay {
    fn spawn(upstream: &str) -> Self {
        let port = pick_port();
        let child = Command::new(RELAY_BIN)
            .env("CODEX_RELAY_PORT", port.to_string())
            .env("CODEX_RELAY_UPSTREAM", upstream)
            .env("CODEX_RELAY_API_KEY", "")
            .env("RUST_LOG", "codex_relay=warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn codex-relay");

        let mut handle = Relay { child, port };
        handle.wait_ready();
        handle
    }

    fn wait_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(8);
        while Instant::now() < deadline {
            if std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(80));
        }
        panic!("relay did not become ready on :{}", self.port);
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{}", self.port, path)
    }
}

#[tokio::test]
async fn issue_5_streaming_completed_event_includes_usage() {
    let (upstream_port, bodies) = spawn_mock_upstream().await;
    let relay = Relay::spawn(&format!("http://127.0.0.1:{upstream_port}/v1"));

    let body = json!({
        "model": "mock-model",
        "instructions": "Answer briefly.",
        "input": "Say OK.",
        "tools": [],
        "stream": true
    });

    let resp = reqwest::Client::new()
        .post(relay.url("/v1/responses"))
        .json(&body)
        .send()
        .await
        .expect("POST /v1/responses");
    assert!(resp.status().is_success(), "status {}", resp.status());

    let mut events = resp.bytes_stream().eventsource();
    let mut completed: Option<Value> = None;
    let deadline = Instant::now() + Duration::from_secs(8);
    while let Some(ev) = tokio::time::timeout(deadline - Instant::now(), events.next())
        .await
        .expect("stream timeout")
    {
        let ev = ev.expect("sse parse");
        if ev.event == "response.completed" {
            completed = Some(serde_json::from_str(&ev.data).expect("completed json"));
            break;
        }
    }

    let completed = completed.expect("response.completed event");
    assert_eq!(
        completed["response"]["usage"],
        json!({"input_tokens": 7, "output_tokens": 2, "total_tokens": 9})
    );

    let request_bodies = bodies.lock().unwrap();
    let upstream_body = request_bodies.first().expect("upstream chat request");
    assert_eq!(
        upstream_body["stream_options"],
        json!({"include_usage": true}),
        "streaming Chat Completions requests must ask upstream to include usage"
    );
}
