<!-- nemo-golden-rules:start -->
## Golden rules — apply before all Nemo task instructions

1. **Preserve the active task.** Unless the user explicitly directs otherwise, record every incoming question/request in the maintained task queue, ordered by workflow dependencies and priority, and continue the active task. Link clarifications to their existing task; do not silently switch objectives.
2. **Be frugal with tokens.** Read and communicate only the context needed for reliable work; reuse verified evidence and avoid duplicate investigation or repeated status messages.
3. **Match agents and effort to the work.** Use the least costly capable model and reasoning effort for each bounded task; delegate independent work when useful and escalate when complexity, uncertainty or risk warrants it.
<!-- nemo-golden-rules:end -->

# Nemo workspace workflow

Protocol: `NEMO-A2A-1`. Skill version: `1.5.0`.

This is the shared working contract for Codex and Claude in Nemo. In Nemo's dedicated
Buzz community, every enrolled collaborator's managed agents participate in the Nemo
Project and have full read/write access to its repository. A2A and these instructions
are supplied automatically at agent startup in channels, direct messages, and background
sessions. Do not ask users to pin instruction revisions, assign agents to the Project,
configure peer grants, or fill out allowed-path forms.

Buzz runs ordinary host Codex and Claude agents in both conversations and delegated A2A
jobs. Use the provider's selected full-permission mode, native file and shell tools,
native subagents, configured MCP servers, and the host's existing account configuration
as you would from its CLI. Buzz does not restrict host access to Nemo, the current
branch, or a signed path list. Do not treat the optional Buzz Git tools as the only
way to inspect, edit, test, or publish authorized work. Do not copy credentials into
prompts, chat, logs, or receipts; let the host tools load their normal configuration.

Tool availability is separate from the user's requested task. Preserve the user's
exclusions and other developers' work; having an account does not instruct you to use
it for unrelated work. Community authentication, verified agent ownership, and remote
repository protections still apply. GitHub remains canonical for source, issues,
pull requests, CI, and review; Buzz carries conversation and coordination.

## Work on Nemo

- Start repository work in the runtime's Nemo checkout and use the relevant guidance already in context.
  Read missing local guidance once and inspect source relevant to the task; do not repeat
  startup research for each operation.
- Keep the active task and its existing queue. Answer an incoming question briefly, attach
  any new work to that queue, then resume. Update the queue at meaningful transitions;
  queue bookkeeping must not become a separate project.
- Use a branch per change and separate worktrees for concurrent writers. Declare the task,
  files or subsystem owned, and completion criteria. File scopes coordinate ownership;
  they are not a user-maintained permission list or a host filesystem sandbox. Resolve
  overlapping work with its owner. Read other refs or use host tools when the task needs them.
- Preserve another developer's edits, scoped commits, DCO where required, and normal PR/
  branch protection rules. A request to publish includes its ordinary authorized Git
  steps; do not ask again for permissions or identity already established in the session.
- For persistent fields or item types, check every applicable document consumer: save,
  load, undo/redo, selection, animation, render, export, and native bridges. Browser and
  Tauri behavior require their relevant validation; do not substitute one for the other.
- Read local node documentation and implementation before giving node-setting guidance.
- Test the affected behavior. Use a focused regression when fixing a real defect; run
  broader suites only for a concrete remaining risk or a required gate. Avoid tests that
  only restate implementation details or check unrelated application behavior.
- Report the result, relevant validation, and any actual remaining blocker. Stop once the
  requested outcome is verified. Do not add memory research or optional reviews afterward.

## Documentation publication fast path

Publishing an already-reviewed documentation package is one ordinary Git task:

1. Batch repository/branch/remote status, existing work ownership, and Git identity checks.
   Reuse an appropriate existing worktree. Follow known branch protections from the start.
2. Copy only the requested package and validate the artifact once: source comparison,
   relative links, symlinks/private-data markers, and the staged diff. Correct only concrete
   failures. Application tests and independent-agent reviews add no value to this task.
3. Commit, push the authorized branch, and verify the remote commit. Use the normal PR
   route when main is protected. If one connector lacks permission, use another already
   authorized GitHub route if available; otherwise state that exact access limitation.
   Do not claim that only the repository owner can act merely because one connector failed.
4. Give the result and finish. Do not dispatch A2A, broadcast status, repeatedly revalidate,
   reopen settled policy, or look up memory after the publication result is established.

A failed operation warrants a targeted correction, not a new investigation of the whole
project. There is no fixed duration or tool-count limit on legitimate implementation,
research, debugging, tests, builds, or sustained agent work.

## Collaborate through A2A

Use A2A when another agent can independently advance the current task. Small, sequential
jobs should stay local. Call `buzz_a2a_peers` to discover the runtime's verified agent
roster, then use the selected peer's supplied identity rather than inventing one. Give it a concrete outcome and
checkable acceptance criteria. Supply repository paths when the task has file effects; use an empty
path list when no repository files need changing. Paths coordinate ownership and do not make
the job technically read-only or disable GitHub work. Delegated workers retain their ordinary host tools;
users do not maintain per-peer grants or refresh a hash after every source commit.

| Intent | Tool | How to use it |
| --- | --- | --- |
| Send a message | `buzz_chat_send` | Omit destination fields to reply in the current conversation, or supply a verified channel, thread root, and enrolled recipients for an explicitly addressed message. A direct message needs no `@` prefix. |
| Create a shared discussion | `buzz_chat_thread_create` | Create a visible top-level thread in the current or another accessible channel. Continue there with `buzz_chat_send` and its returned root. |
| Read a shared task thread | `buzz_chat_read` | Read bounded signed history from the current task thread or a verified thread in an accessible channel. |
| Ask an enrolled peer | `buzz_peer_ask` | Ask one peer in the current task thread and wait up to 60 seconds. A timeout returns a request ID for `buzz_peer_wait`. |
| Continue waiting for a peer | `buzz_peer_wait` | Wait up to 60 seconds for the exact signed reply to an existing peer question. A pending result is not a failure. |
| Answer a peer | `buzz_peer_reply` | Reply to the exact addressed peer question. The answer stays visible and correlated in the same task thread. |
| Read thread organization | `buzz_organization_read` | Read signed organization history and the current effective participant list for a shared thread. |
| Update thread organization | `buzz_organization_apply` | Set the complete desired agent participant list for a thread, or apply another supported organization change. An empty participant list removes all agents. |
| Discover collaborators | `buzz_a2a_peers` | Find verified Nemo agent names and identities before dispatch. An empty inbox is not a peer roster; do not ask the user to paste public keys. |
| Delegate a job | `buzz_a2a_dispatch` | Supply the verified peer, bounded task, acceptance, and required job coordinates. The tool verifies or creates the visible shared task thread before execution and returns its channel/root with the request ID. Use `paths: []` for an information-only consultation. GitHub issue, PR, and run references accept a positive number or canonical same-repository URL. Use a fresh operation ID and a stable retry key. |
| Check addressed work | `buzz_a2a_inbox` | Inspect available addressed jobs when acting as a coordinator. Do not duplicate an already-owned task. |
| Follow one job | `buzz_a2a_status` | Use the returned request event ID; check on meaningful progress, completion, or a live wait. |
| Cancel your dispatched job | `buzz_a2a_cancel` | Use its exact request ID and reason; wait for the worker's terminal cancellation acknowledgement. |
| Hand off an owned job | `buzz_a2a_handoff` | Use the exact request, verified successor, and reason; the successor must separately accept ownership. |

A relay acknowledgement proves storage only. The recipient's `processed` and `accepted`
receipts establish validation and ownership; neither is completion. The worker publishes
concise progress and a terminal result in the shared task thread through the runtime. Every
delegated task is visible there before execution begins. Workers can ask any enrolled peer
with `buzz_peer_ask`, receive the correlated answer directly, and continue the same task;
no human relay is required. Reuse a retry key only for the same request body, and never
execute a replayed job twice. A changed task needs a new request.
After a handoff, the coordinator advances the job epoch as the tool contract requires.

Thread participants persist as an automatic-follow preference independently of one-time
message recipients. Before adding or removing an agent, read the thread's effective list with
`buzz_organization_read`, resolve the intended agents through `buzz_a2a_peers`, preserve all
participants that should remain, and pass the complete desired public-key list to
`buzz_organization_apply`. Passing an empty list removes all agent participants. The list
controls which agents automatically receive future human posts in the thread. It does not
restrict direct peer questions or signed recipients, and it does not change channel or
repository access.

Do not call a wait tool without a live operation or session identifier. Avoid busy polling
and repeated status messages. Cancellation is settled when the worker has stopped and
reported its terminal disposition. Interrupted execution can be `indeterminate` when its
effects still need reconciliation; do not keep waiting for a separate `cancelled` result.
Silence or a disconnected agent proves neither failure nor completion.
Report an indeterminate result for reconciliation instead of rerunning it automatically.
Native shell and configured MCP actions can have effects outside Buzz's optional Git
journal. An empty journal does not prove that an interrupted job made no changes.
After interruption, inspect the actual affected state before retrying a mutation.

A timer delivers the human's exact prompt at each configured interval through the normal
queue. It does not create a separate scheduled-completion protocol, automatic replay, or
standing authority beyond that delivered prompt. Handle each delivery as ordinary incoming
instructions and keep any progress or results in its current shared conversation. If the
prompt says to wait or do nothing when there are no new instructions, remain idle; do not
invent a task or keep a provider turn running merely because a timer exists.

One-shot delegated workers stay on their assigned outcome and may use native subagents,
host tools, and the peer ask/reply tools available in every Job session; they do not become
recursive job dispatchers. Shared repository access is not a reason to overwrite someone
else's worktree or ignore an active claim.

If a required tool or project context is unavailable, report the specific setup failure
promptly. Do not silently retry unchanged configuration failures, fabricate success, guess
credentials, or reconstruct raw signing/relay requests. Use the trusted Buzz tools for
chat and coordination. Keep credentials and private local infrastructure out of messages,
commits, evidence, and model prompts.
