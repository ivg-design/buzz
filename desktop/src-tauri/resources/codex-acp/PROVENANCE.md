# Buzz bundled Codex ACP adapter provenance

Buzz bundles a patched npm tarball instead of trusting an ambient `codex-acp`
binary when it installs the managed Codex runtime.

## Adapter

- Package: `@agentclientprotocol/codex-acp` 1.9.0
- Upstream repository: https://github.com/agentclientprotocol/codex-acp
- Upstream commit: `e31c8c369ec74f551d017d09abdb6d04d926dcab`
- Upstream npm git head: `67db0d3d4a8a9b4bd3040c4dfdfa0919e9d97be9`
- Upstream license: Apache-2.0; the package's `LICENSE` is retained inside the
  bundled tarball.
- Nemo patch: `nemo-system-prompt.patch`
- Patch SHA-256:
  `23d8aec1bb8cbb82ee45e3e105d8c44c19a24952202c9d5b5c34d2c75f284ec3`
- Patched source archive SHA-256:
  `d0822725759ce1ed89a932fbf2ef9b98dc3132a9e01bbe035483b7893793bc49`
- Compiled `dist/index.js` SHA-256:
  `80dddafac734af0a0db6977482a42b96633d1ebf2416d0be4bd6cf3669cf4c6e`
- Bundled tarball SHA-256:
  `5ba217a3afdba012f5f8e3e145f747d47eb4196c0283195edfb3c2212b388a4c`

The patch adds an explicit ACP v1 `systemPrompt` append capability and forwards
validated `_meta.systemPrompt.append` text to Codex app-server's
`thread/start.developerInstructions` field. Modified source is identified by
the adjacent patch, satisfying Apache-2.0 section 4(b).

The tarball was built from the pinned source with `npm ci --ignore-scripts`,
`npm run build`, and `npm pack --ignore-scripts`. Its published file set is the
upstream package file set plus `npm-shrinkwrap.json`; the shrinkwrap is the
source tree's lockfile renamed by `npm shrinkwrap`, so the transitive install is
pinned across macOS, Linux, and Windows rather than re-resolved from ranges.

## Codex CLI

- Package: `@openai/codex` 0.153.2 and its platform-specific optional packages
- Upstream repository: https://github.com/openai/codex
- Source tag: `rust-v0.153.2`
- Peeled source commit: `657a993cbee87acf52d14b758ce49dbd46d1b8eb`
- License: Apache-2.0, Copyright 2025 OpenAI
- Authoritative license source:
  https://github.com/openai/codex/blob/657a993cbee87acf52d14b758ce49dbd46d1b8eb/LICENSE
- Vendored license: `OPENAI-CODEX-LICENSE.txt`
- Vendored license SHA-256:
  `d17f227e4df5da1600391338865ce0f3055211760a36688f816941d58232d8dc`

The Codex npm packages declare Apache-2.0 but platform packages may omit a
standalone license file. Buzz therefore carries the authoritative license text
explicitly and materializes it beside the managed adapter artifact at install
time.
