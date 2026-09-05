# Timed agent tasks

Hover an agent in Buzz and choose **Add timed task**. Save an instruction,
conversation, and interval in minutes, hours, or days. Repeat until cancelled,
for a total number of deliveries, or until a local date and time. The same
window lists existing tasks and supports editing, pausing, resuming, cancelling,
and opening their conversation threads.

The first instruction is due one interval after saving. Resuming or editing an
active schedule starts a new interval. A day means 24 elapsed hours. A local
cutoff records the selected date, timezone label, and the UTC offset on that
date. Later daylight-saving or computer-timezone changes do not move it. A
nonexistent spring-forward time is rejected; an ambiguous fall-back time uses
the first occurrence. Editing an unchanged cutoff retains its saved timezone.

This is a scheduler on the Buzz desktop host. Buzz must be running and able to
reach the relay; it cannot deliver while the host is shut down or asleep.
Schedules are stored in `timed-tasks.sqlite3` in the app-data directory, scoped
to their creating identity and relay. Restart does not create a backlog burst:
missed intervals are combined. Offline recipients wait without being started.
One pending delivery per schedule prevents duplicate sends. Delivered prompts
enter the recipient's ordinary queue, and the next interval does not depend
on any special completion response.

The total count is the number of instructions acknowledged by the relay, not
successful agent executions. A failed delivery attempt does not consume a run;
duplicate acknowledgements cannot consume another run. The schedule shows
delivery state and errors. Cancellation stops future delivery and
does not interrupt work already delivered to the agent.

## Conversation and runtime contract

Every schedule creates one ordinary signed kind-9 root in the chosen
conversation, with a concise label and no agent mention. The root is published
before the first instruction. The initiating event is retained as metadata.

Each instruction is a reply in that root, with the agent's normal mention tag
and the exact instruction text. Scheduling metadata does not alter the prompt:

```
root:       ["buzz-task", "schedule", scheduleUUID]
occurrence: ["buzz-task", "scheduled", scheduleUUID, occurrenceUUID]
```

The ACP runtime keeps scheduled prompts and ordinary agent replies in this
thread. No special completion response is required. The scheduler persists the
exact signed event before publishing, queries its identity after uncertain
delivery, and retries those same bytes. It never signs a replacement occurrence
for a retry. Confirmed delivery releases the pending event and advances the
delivery count. Existing authenticated channel and recipient rights apply.

## Focused validation

From the repository root with the Hermit environment activated:

```
pnpm --dir desktop exec node --import ./test-loader.mjs --experimental-strip-types --test 'src/features/timed-tasks/*.test.mjs'
cargo test --manifest-path desktop/src-tauri/Cargo.toml timed_tasks
```

`tests/e2e/timed-tasks.spec.ts` exercises the actual hover popover, editor, and
management controls using disposable mocked IPC. It verifies exact instruction
and origin capture; it does not establish native storage or agent execution.
The native scheduler tests exercise clock transitions, SQLite restart,
signed-event retry, and delivery accounting separately.
