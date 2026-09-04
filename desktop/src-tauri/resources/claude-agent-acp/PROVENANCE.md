# Buzz patched Claude ACP adapter provenance

- Package: `@agentclientprotocol/claude-agent-acp` 0.73.0
- Upstream: https://github.com/agentclientprotocol/claude-agent-acp.git
- Upstream commit: `ea7076c0bc324603e65d8c124b7573f158749969`
- Patch commit: `78baedaf9b9d57e30ec81afa91826a4cdb535756`
- License: Apache-2.0; the upstream license is retained in the tarball and copied as `APACHE-2.0.txt`.
- Patch: `nemo-job-policy.patch`
- Patch SHA-256: `f7413b155ac5da56efaa4c0d9f2e963a76bed229c7de29a4d343fb5f165d7f81`
- npm shrinkwrap SHA-256: `1d9ea207e4057964a8aadba4542090d1e162b60a834c145af3731f62674766cf`
- Compiled `dist/` tree SHA-256: `37ddade38946c1c1c464ab11bf21139532198f643a212c759578d8c165a8651a`
- Tarball SHA-256: `01c5d58734d77fcfc3779bd86e0fed5575fe1d8168e03e63a4ef138f1f2150e4`

The patch adds Buzz JobPolicyV1 advertisement, strict session policy and MCP validation, native-tool denial, and exact policy acknowledgement. It was built from the pinned source with `npm ci`, `npm run build`, `npm run check`, `npm run test:run`, `npm shrinkwrap --ignore-scripts`, and `npm pack --ignore-scripts`. The final validation passed 1,071 tests with 27 upstream skips. The shrinkwrap pins the complete npm dependency graph used when Desktop installs this app-private artifact.

Desktop installs this tarball into a dedicated versioned prefix and both Desktop and `buzz-acp` verify the complete installed package tree, including the nested Claude Agent SDK and target-specific native package, before JobPolicyV1 is enabled. Generated `.bin` launch wrappers are excluded because Buzz never executes them. Verified runtime-tree SHA-256 values are `a2040fe41ef0fd64789801a73165280594339194966d1bdbf8b874b006efc831` (macOS arm64), `d9a97f0eab8a57d20f3d1f8d1f9b84cb843a438b5309396e90db8ab17fe054e4` (macOS x64), `88586945dfd3353ca49659af7593d1a256addeb71e6d31bcea04e34640b7a619` (Windows x64), and `959224a2d434d25c352510aae19eb7db4be5496c4d6019d336cabd86c3fe01f1` (Windows arm64). Other targets fail closed.
