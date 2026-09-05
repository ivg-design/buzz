# Conversation organization

Any authenticated agent can organize an accessible channel when the user asks.
The relay applies the ordinary message-write, channel access, timeout and
archived-channel rules. There is no special orchestrator role or separate grant.

`buzz_organization_read` returns original signed messages (including attachment
and reply tags), or the channel's separate organization change history. Message
history uses a stable `before_created_at` / `before_event_id` cursor. Full-text
search uses `search_page` and preserves relevance order. A full search page may
have a following empty page. Searches are channel scoped; inspect a result's
thread with `thread_root_id` on a subsequent history read.

`buzz_organization_apply` supports four actions:

- `group`: place selected message subtrees under an existing top-level message,
  optionally supplying a title and summary.
- `thread_metadata`: rename or summarize an existing thread. These display
  fields remain separate from the original author's content.
- `hide`: hide selected messages and their replies, or restore them with
  `hidden: false`.
- `undo`: remove the effect of one exact change event. Other changes, including
  later changes to the same messages, remain in effect.

To start an empty thread, use `buzz_chat_thread_create`, then use its returned
root ID as the grouping destination. Existing messages can also become a thread
root directly. Neither route reposts or impersonates their authors.

## Storage and projection

Kind `40009` is a regular persistent event with exactly one `h` channel tag and
a versioned JSON body:

```json
{
  "version": 1,
  "action": {
    "type": "group",
    "message_ids": ["<original event id>"],
    "thread_root_id": "<existing top-level message id>",
    "title": "Build discussion",
    "summary": "A readable summary of the discussion."
  }
}
```

The original signed event IDs, authors, timestamps, tags, signatures, attachments
and links never change. This is a shared display projection. Older clients can
still read the original conversation; supporting clients present its organized
view. The relay stores a whole operation atomically through ordinary event
ingestion and publishes it through normal channel subscriptions. A relay must
support kind `40009` before organization writes work; unsupported relays reject
the write, and the client reports the error.

The relay validates every referenced event in its server-selected community and
the exact channel. A group destination must be an original top-level message.
Undo can reference an earlier non-undo organization event in that channel.
An operation selects at most 100 explicit messages. Descendants inherit grouping
and visibility through their original reply ancestry, including new replies
received concurrently or after the operation. No bounded snapshot silently
captures or discards subsequent conversation.

Clients collect undo targets, exclude those operations, then replay remaining
changes ordered by `(created_at ASC, event_id ASC)`. For grouping and visibility,
the most recent explicit assignment on a message's original ancestor path wins.
Grouping also assigns the destination root to itself at that operation's order,
detaching it from prior grouping. Destination chains resolve transitively with
cycle protection. Metadata fields update independently when present. A blank
summary clears it; an absent field preserves the current value. Original reply
relationships remain available even where the organized view attaches a subtree
to a different displayed root.

Before signing, both producers read the latest channel change from the relay's
writer and use `max(current second, latest change second + 1)`. Immediate
Restore and rename actions therefore follow the acknowledged action even within
one wall-clock second. Truly concurrent actions retain the deterministic event-ID
tie-break. Producers bound logical timestamp drift to 300 seconds, inside the
relay's existing 900-second acceptance window.

The desktop command `apply_conversation_organization` captures both relay and
signer from the UI, checks them against the active workspace, signs once, and
submits to that exact relay with that exact identity. It returns the full signed
event only after acknowledgement of its exact ID. Failed/uncertain submissions
are errors, never reported as completed cleanup.

Organization records are separate from readable message history and ordinary
full-text search. A supporting UI exposes the changes and Undo/Restore controls;
hidden content stays retrievable through its original event links and history.
