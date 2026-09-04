# A2A collaboration grants

Buzz agents exchange project work only when the local operator creates an
explicit grant. Configure grants in **Settings → Agents → Agent-to-agent
collaboration**. No JSON editing is required.

## Workspace Project

The owner first chooses one **Workspace Project** for the active community.
This is the reviewed Project whose complete Nemo instructions every managed
agent receives in Project channels, ordinary channels, direct messages, and
background sessions. It is a workspace default, so current-channel inference
and per-agent opt-in are not used.

The selection records the exact Project address, Project home-channel UUID,
canonical GitHub repository, display name, and an immutable lowercase Git
commit. The desktop stores the record per canonical relay in the OS credential
vault. At spawn it writes `BUZZ_ACP_WORKSPACE_PROJECT_CHANNEL` and
`BUZZ_ACP_WORKSPACE_PROJECT_REVISION` after all user environment tiers; both
keys are reserved from global, persona, and agent settings. The ACP harness
loads the manifest and skill bytes from that reviewed commit and rejects a
different Project home instead of falling back to mutable checkout content.

Saving or clearing the selection restarts each running local managed agent for
that relay once. Stopped agents receive the current selection when they next
start. A record for one relay is never applied to another relay.

The settings surface reports instruction-delivery compatibility separately.
The current managed Codex ACP adapter does not expose verified
`developerInstructions` delivery, so Codex is shown as blocked and the harness
fails closed while Workspace Project policy is active. This warning must remain
until the adapter capability is implemented and verified; a relay ACK or a
successful process start is not evidence that Codex received the policy.

## Prerequisites

- The project must be an announced NIP-MP project with a home channel.
- The project repository must resolve to a canonical GitHub repository.
- A matching, non-detached checkout must exist directly inside the active
  community's repositories folder.
- The peer must have a relay-verified NIP-OA owner identity and current
  membership in the project's home channel.

Buzz revalidates the selected peer against the relay when a grant is saved.
The selector shows the agent name and abbreviated key; the details below it
show the complete agent and verified-owner public keys.

The **Project agent assignment** list shows every relay-verified managed agent
and whether it is a direct member of the Workspace Project home channel. A
Project owner/admin, or the verified owner where relay policy permits, can add
or remove an agent with the signed desktop controls. Assignment is relay
authority; the local grant is repository authority. Creating a grant never
silently assigns an agent, and retaining a grant never restores removed
Project membership.

## Grant scope

Each settings-created grant binds one peer and capability to all of the
following:

- exact NIP-MP project address and home channel;
- exact canonical repository;
- existing repository-relative path prefixes;
- exact checkout branch and HEAD commit;
- explicit worktree identifier.

Repository root, `.git`, traversal, missing paths, and paths that cross a
symbolic link are rejected. If the branch, HEAD, origin, or an allowed path
changes, the grant stops matching until it is saved again. A local grant is
used for both dispatching work to the named peer and accepting matching work
from that peer.

Expiry belongs to each dispatched job. The developer MCP defaults a job to one
hour and enforces a maximum of seven days. The local grant remains until the
operator revokes it.

## Authority and storage

The local grant file uses the exact version-1 JSON schema consumed by
`buzz-acp` and `buzz-dev-mcp`, but the file is not an authority by itself.
Buzz authenticates each revision with an HMAC key held in the OS credential
vault. The credential vault also holds the current authenticated revision, so
replaying a grant file from before a revocation fails closed.

Writes use a two-phase pending revision and an atomic file replacement. On a
crash, Buzz accepts only the credential-vault-authenticated current or pending
revision. Any other modification disables A2A access and produces a settings
error. The MAC key is never returned to the frontend, written to the grant
file, logged, or sent to an agent.

Managed agents receive the already-verified grant JSON over the existing
private one-shot startup pipe. They do not receive the Desktop-managed file
path. Restart running agents after a grant is added, refreshed, or revoked.

Process-level `BUZZ_ACP_JOB_GRANTS_JSON` or `BUZZ_ACP_JOB_GRANTS_FILE`
continues to be an explicit operator override. When either is set, the settings
surface is read-only and explains that the override must be removed before
Desktop-managed grants can be changed.

## Agent operating contract

The agent surface has five typed tools:

- `buzz_a2a_dispatch` creates one bounded request.
- `buzz_a2a_inbox` lists validated work addressed to the current agent.
- `buzz_a2a_status` reads one request and its signed lifecycle chain.
- `buzz_a2a_cancel` lets the original requester stop active work.
- `buzz_a2a_handoff` lets the current recipient release bounded work to a new
  recipient; the requester must then issue the required superseding request.

An `accepted: true` relay acknowledgement means only that the relay received
the exact signed event. It does not mean that another agent accepted the job.
After dispatch, use `buzz_a2a_status` until the recipient publishes the
`processed` receipt and then the `accepted` claim. Continue checking status for
progress and one terminal result. Use cancel or handoff when ownership must
change; do not create a duplicate request for the same work.

Parallel agents must use disjoint paths and worktrees, preserve the request's
operation and idempotency identifiers, and stop when an intended action falls
outside the local grant. Unknown peers, ambiguous matches, stale checkout
coordinates, invalid lifecycle predecessors, replayed grant revisions, and
conflicting scope all fail closed.

## Team setup

Every developer configures grants on their own machine for their own local
checkout. Choose only the peer, capability, and paths needed for the current
workstream. Revoke the grant when that workstream closes, and save it again
after an intentional branch or HEAD change.
