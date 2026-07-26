# Compatibility test version matrix

Fixtures and live tests in this directory pin the **versions known to work**.
Update this file whenever you regenerate fixtures or change the target model set.

## Codex CLI

| Fixture dir | Codex CLI version | Captured |
|---|---|---|
| `codex_0_128_0/` | 0.128.0 | 2026-05-07 |

Each fixture is a **minimized, hand-shrunk** representative of the real wire
format — capture method documented at the top of `tests/compat_codex_0_128.rs`.

To regenerate after a Codex upgrade:

1. Set `CODEX_RELAY_DEBUG_DUMP=/tmp/codex-dump` in the relay env, run a real
   `codex exec` against it, copy `inbound_*.json` to a new
   `tests/fixtures/codex_<major>_<minor>_<patch>/` folder.
2. Trim long strings and tool lists down to the smallest payload that still
   exercises the relevant feature.
3. Add an entry above and update `tests/compat_codex_0_128.rs` to load it.

## DeepSeek (live tests)

| Model | Wire shape | Notes |
|---|---|---|
| `deepseek-v4-pro` | Chat Completions | Reasoning model — emits `reasoning_content` |
| `deepseek-v4-flash` | Chat Completions | Non-reasoning, fast |

Live tests gated by `DEEPSEEK_API_KEY` env var. Run with:

```
DEEPSEEK_API_KEY=sk-... cargo test --test compat_deepseek_live -- --ignored
```

Note: as of 2026-05-11 DeepSeek's Chat Completions endpoint does **not**
accept multimodal image content parts — it rejects `image_url`, `input_image`,
and `image` with `"unknown variant ..., expected text"`. Image-input wire
shape is verified via a recording proxy (DeepSeek's 200/400 is ignored;
only the relay's outbound body is asserted).

## Kimi / Moonshot (live tests)

| Model | Wire shape | Notes |
|---|---|---|
| `kimi-k2.6` | Chat Completions | Accepts standard OpenAI multimodal content (verified 2026-05-11) |
| `moonshot-v1-32k-vision-preview` | Chat Completions | Dedicated vision preview, backup pin |

Used as our happy-path image-input verification since DeepSeek doesn't yet
support vision on its Chat Completions endpoint. Gated by `MOONSHOT_API_KEY`:

```
MOONSHOT_API_KEY=sk-... cargo test --test compat_kimi_live -- --ignored
```

Tests are `#[ignore]` so the default `cargo test` stays fully offline.

## codex-relay

Version pinned in `Cargo.toml`. Bump it when the relay's own request shape
changes (so consumers can detect a breaking change).
