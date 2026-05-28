//! Live image-input tests against the real Kimi (Moonshot) API.
//!
//! Empirically confirmed 2026-05-11: `kimi-k2.6` and `moonshot-v1-*-vision-*`
//! accept the standard OpenAI Chat Completions multimodal shape (content as a
//! parts array containing `{type:"image_url", image_url:{url}}`). DeepSeek
//! V4 Pro does not (see `compat_deepseek_live.rs::..._currently_rejected_*`),
//! so Kimi is currently our happy-path live verification for image forwarding.
//!
//! Gated on `MOONSHOT_API_KEY`. Each test is `#[ignore]`'d so the default
//! `cargo test` stays offline:
//!
//!     MOONSHOT_API_KEY=sk-... cargo test --test compat_kimi_live -- --ignored --test-threads=1

use serde_json::{json, Value};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const RELAY_BIN: &str = env!("CARGO_BIN_EXE_codex-relay");
const KIMI_UPSTREAM: &str = "https://api.moonshot.cn/v1";

/// 1×1 red PNG, base64-encoded. Tiny but valid; same fixture as the
/// DeepSeek wire-shape test for consistency.
const TINY_RED_PNG_B64: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";

fn moonshot_key() -> Option<String> {
    std::env::var("MOONSHOT_API_KEY")
        .ok()
        .filter(|s| !s.is_empty())
}

fn need_key() -> Option<String> {
    match moonshot_key() {
        Some(k) => Some(k),
        None => {
            eprintln!("skip: MOONSHOT_API_KEY not set");
            None
        }
    }
}

fn pick_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
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
    fn spawn(upstream: &str, key: &str) -> Self {
        let port = pick_port();
        let child = Command::new(RELAY_BIN)
            .env("CODEX_RELAY_PORT", port.to_string())
            .env("CODEX_RELAY_UPSTREAM", upstream)
            .env("CODEX_RELAY_API_KEY", key)
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

fn image_input_body(model: &str) -> Value {
    let data_url = format!("data:image/png;base64,{TINY_RED_PNG_B64}");
    json!({
        "model": model,
        "instructions": "Answer in one short sentence.",
        "input": [{
            "type": "message",
            "role": "user",
            "content": [
                {"type": "input_text", "text": "What do you see in this image? One short sentence."},
                {"type": "input_image", "image_url": data_url}
            ]
        }],
        "tools": [],
        "tool_choice": "auto",
        "parallel_tool_calls": false,
        "store": false,
        "stream": false,
        "include": []
    })
}

/// End-to-end happy path: a Codex-shaped `input_image` request goes through
/// the relay, hits the real Kimi-K2.6 vision endpoint, and comes back with
/// non-empty assistant text. Locks in that the relay's translation is
/// accepted by a real vision-capable upstream.
#[tokio::test]
#[ignore]
async fn live_kimi_k2_6_image_input_blocking() {
    let Some(key) = need_key() else { return };
    let relay = Relay::spawn(KIMI_UPSTREAM, &key);

    let resp: Value = reqwest::Client::new()
        .post(relay.url("/v1/responses"))
        .json(&image_input_body("kimi-k2.6"))
        .send()
        .await
        .expect("POST /v1/responses")
        .error_for_status()
        .expect("non-2xx (relay forwarded an unhappy upstream)")
        .json()
        .await
        .expect("json decode");

    assert_eq!(resp["object"].as_str(), Some("response"));
    assert_eq!(resp["model"].as_str(), Some("kimi-k2.6"));
    let output = resp["output"].as_array().expect("output array");
    assert!(!output.is_empty(), "empty output: {resp}");

    let text = output[0]["content"][0]["text"]
        .as_str()
        .expect("output_text present");
    assert!(
        !text.trim().is_empty(),
        "vision model returned empty text — relay likely dropped the image: {resp}"
    );

    let usage = &resp["usage"];
    assert!(
        usage["input_tokens"].as_u64().unwrap_or(0) > 0,
        "input_tokens: {usage}"
    );
    assert!(
        usage["output_tokens"].as_u64().unwrap_or(0) > 0,
        "output_tokens: {usage}"
    );

    eprintln!("✓ kimi-k2.6 vision response ({} chars): {text}", text.len());
}

/// Same as above but against the dedicated vision preview model, so we have
/// a backup pin in case the K2.6 generation rotates / changes behavior.
#[tokio::test]
#[ignore]
async fn live_kimi_v1_vision_preview_image_input_blocking() {
    let Some(key) = need_key() else { return };
    let relay = Relay::spawn(KIMI_UPSTREAM, &key);

    let resp: Value = reqwest::Client::new()
        .post(relay.url("/v1/responses"))
        .json(&image_input_body("moonshot-v1-32k-vision-preview"))
        .send()
        .await
        .expect("POST /v1/responses")
        .error_for_status()
        .expect("non-2xx")
        .json()
        .await
        .expect("json decode");

    let text = resp["output"][0]["content"][0]["text"]
        .as_str()
        .expect("output_text present");
    assert!(!text.trim().is_empty(), "empty: {resp}");
    eprintln!(
        "✓ moonshot-v1-32k-vision-preview ({} chars): {text}",
        text.len()
    );
}
