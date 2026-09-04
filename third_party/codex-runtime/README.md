# Bundled native Codex CLI

Buzz release builds package an exact native Codex CLI so the reviewed ACP
adapter does not execute a mutable `codex` found on `PATH`.

`manifest.json` pins the Codex source commit, native npm package version and
per-platform package integrity. `scripts/bundle-codex-runtime.mjs` verifies
those pins, preserves the complete native vendor tree, stages the upstream
Apache-2.0 license, and writes payload checksums and provenance under
`desktop/src-tauri/bundle-resources/codex-cli/`.

The adapter build is intentionally separate. It consumes the verified CLI via
the `CODEX_PATH` environment variable configured by Buzz Desktop.

Generated payloads are ignored. Rebuild them for every target; never commit
generated binaries to this repository.

For macOS, every release lane runs
`scripts/verify-codex-macos-signing.py staged --target <rust-target>` before
Tauri builds the app. Signed lanes run the `app` phase again against the exact
`Buzz.app` extracted from the signing service. The tracked policy pins the
OpenAI Developer ID chain and team, hardened runtime, secure timestamp, thin
architecture, provenance checksums, and final
`Contents/Resources/codex-cli` placement. A Codex package update that changes
the payload inventory or signer must update that policy through review.
