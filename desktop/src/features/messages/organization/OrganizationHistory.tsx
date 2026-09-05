import * as React from "react";
import { History, Undo2 } from "lucide-react";
import { Button } from "@/shared/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/shared/ui/popover";
import { Markdown } from "@/shared/ui/markdown";
import { parseImetaTags } from "@/shared/ui/markdown/parseImeta";
import {
  resolveUserLabel,
  type UserProfileLookup,
} from "@/features/profile/lib/identity";
import type { useConversationOrganization } from "./useConversationOrganization";
import type { OrganizationAction, ThreadMetadata } from "./projection";

export type ConversationOrganization = ReturnType<
  typeof useConversationOrganization
>;

export function OrganizationThreadIntro({
  metadata,
}: {
  metadata?: ThreadMetadata;
}) {
  if (!metadata?.title && !metadata?.summary) return null;
  return (
    <div
      className="my-2 space-y-1 text-sm"
      data-testid="organization-thread-intro"
    >
      {metadata.title ? (
        <p className="font-semibold text-foreground">{metadata.title}</p>
      ) : null}
      {metadata.summary ? (
        <p className="whitespace-pre-wrap break-words text-muted-foreground">
          {metadata.summary}
        </p>
      ) : null}
    </div>
  );
}

function describeAction(action: OrganizationAction) {
  if (action.type === "group")
    return `Grouped ${action.message_ids.length} ${action.message_ids.length === 1 ? "message" : "messages"} into ${action.title ? `“${action.title}”` : "a thread"}`;
  if (action.type === "thread_metadata")
    return action.title
      ? `Updated thread “${action.title}”`
      : "Updated a thread summary";
  if (action.type === "hide")
    return `${action.hidden ? "Hid" : "Restored"} ${action.message_ids.length} ${action.message_ids.length === 1 ? "message" : "messages"} and their replies`;
  return "Undid an organization change";
}

/** Visible receipts and one-click undo; nothing is deleted or reposted. */
export function OrganizationHistory({
  organization,
  profiles,
  currentPubkey,
  channelId,
}: {
  organization: ConversationOrganization;
  profiles?: UserProfileLookup;
  currentPubkey?: string;
  channelId?: string;
}) {
  const [limit, setLimit] = React.useState(30);
  const records = [...organization.state.records].reverse();
  if (!records.length && !organization.error) return null;
  return (
    <div className="flex shrink-0 items-center gap-1 text-xs text-muted-foreground">
      {organization.error ? (
        <span role="alert" className="mr-auto text-destructive">
          Organization could not load or save.{" "}
          <button
            type="button"
            className="underline"
            onClick={organization.retry}
          >
            Retry
          </button>
        </span>
      ) : null}
      <Popover>
        <PopoverTrigger asChild>
          <Button
            variant="ghost"
            size="sm"
            className="h-8 w-8 p-0"
            title={`Organization history (${records.length} changes)`}
            data-testid="organization-history-open"
          >
            <History aria-hidden="true" className="h-3.5 w-3.5" />
            <span className="sr-only">Organization · {records.length}</span>
          </Button>
        </PopoverTrigger>
        <PopoverContent
          align="end"
          className="w-[min(30rem,calc(100vw-2rem))] p-0"
        >
          <div className="border-b border-border px-4 py-3">
            <h2 className="text-sm font-semibold">Organization history</h2>
            <p className="mt-1 text-xs text-muted-foreground">
              Original messages, authors, and links are preserved.
            </p>
          </div>
          <div className="max-h-[60vh] overflow-y-auto px-4">
            {organization.error ? (
              <p role="alert" className="py-3 text-sm text-destructive">
                {organization.error instanceof Error
                  ? organization.error.message
                  : String(organization.error)}
              </p>
            ) : null}
            {records.slice(0, limit).map(({ event, action, undone }) => {
              const selected =
                "message_ids" in action
                  ? organization.supplementalEvents.filter((e) =>
                      action.message_ids.includes(e.id),
                    )
                  : [];
              return (
                <div
                  key={event.id}
                  className="border-b border-border/50 py-3 last:border-0"
                >
                  <div className="flex items-start gap-3">
                    <div className="min-w-0 flex-1">
                      <p className="text-sm">{describeAction(action)}</p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        {resolveUserLabel({
                          pubkey: event.pubkey,
                          currentPubkey,
                          profiles,
                        })}{" "}
                        · {new Date(event.created_at * 1000).toLocaleString()}
                        {undone ? " · Undone" : ""}
                      </p>
                    </div>
                    {!undone && action.type !== "undo" ? (
                      <Button
                        variant="ghost"
                        size="sm"
                        disabled={organization.isSaving}
                        onClick={() => {
                          void organization
                            .apply({ type: "undo", change_event_id: event.id })
                            .catch(() => {});
                        }}
                        aria-label={`Undo: ${describeAction(action)}`}
                        className="h-7 gap-1 text-xs"
                      >
                        <Undo2 aria-hidden="true" className="h-3.5 w-3.5" />
                        {action.type === "hide" && action.hidden
                          ? "Restore"
                          : "Undo"}
                      </Button>
                    ) : null}
                  </div>
                  {selected.length ? (
                    <details className="mt-2 text-xs">
                      <summary className="cursor-pointer text-muted-foreground">
                        View original messages
                      </summary>
                      <div className="mt-2 space-y-3">
                        {selected.map((source) => (
                          <div
                            key={source.id}
                            className="rounded-md bg-muted/30 p-2"
                          >
                            <p className="mb-1 text-muted-foreground">
                              {resolveUserLabel({
                                pubkey: source.pubkey,
                                currentPubkey,
                                profiles,
                              })}{" "}
                              ·{" "}
                              {new Date(
                                source.created_at * 1000,
                              ).toLocaleString()}
                            </p>
                            <Markdown
                              content={source.content}
                              channelId={channelId}
                              messageId={source.id}
                              linkPreviewTags={source.tags}
                              imetaByUrl={parseImetaTags(source.tags)}
                            />
                          </div>
                        ))}
                      </div>
                    </details>
                  ) : null}
                </div>
              );
            })}
            {limit < records.length ? (
              <Button
                variant="ghost"
                size="sm"
                className="my-2 w-full"
                onClick={() => setLimit((value) => value + 30)}
              >
                Show older changes
              </Button>
            ) : null}
          </div>
        </PopoverContent>
      </Popover>
    </div>
  );
}
