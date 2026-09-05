---
name: nemo-a2a
description: Coordinate Nemo work through trusted Buzz chat and A2A tools using the shared repository workflow.
version: 1.5.1
---

# Nemo A2A

Use the bundled contract below. Use native host tools for repository work, shell commands,
configured MCP servers, subagents, and host accounts. Use typed Buzz tools for Buzz chat and
A2A coordination; do not reconstruct those coordination operations in a shell or request raw
Buzz relay or signing credentials.


Conversation administration: use `buzz_channel_read` to discover channels (including archived ones) and current members. Use `buzz_channel_apply` for create, rename/update, archive/restore, topic/purpose and membership changes on user request. The relay uses the managed agent's current authorized channel owner for administrative operations, preserving the agent's signed attribution. Use `buzz_organization_read` to discover original chat, forum entries and legacy A2A task events, and `buzz_organization_apply` to group, title/summarize, hide/archive, restore (`hidden:false`) or undo. Hiding preserves immutable source history and evidence. Preserve active task threads during cleanup.

Each new delegated operation defaults to its own visible named thread before work starts, including dispatches made from an orchestrator thread. Keep the origin for returning results; omit conversation.thread_root_id for independent worker tasks. Reuse an explicit thread only when the user wants a shared discussion. Keep assignments, progress, peer questions/answers and results readable in the task thread using the trusted chat/peer tools; do not paste protocol JSON into messages. Human follow-ups in that thread are course corrections for its active worker. Peer discovery includes live presence; online means connected, not necessarily idle.
