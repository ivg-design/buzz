import * as React from "react";

import { useAppNavigation } from "@/app/navigation/useAppNavigation";
import { requestOpenSnapshotImport } from "@/features/agents/openSnapshotImportFromUrlEvent";
import type { ParsedMessageLink } from "@/features/messages/lib/messageLink";

/** Shared navigation callbacks supplied to interactive Markdown's runtime. */
export function useMarkdownNavigation() {
  const { goChannel, goAgents } = useAppNavigation();
  const onOpenChannel = React.useCallback(
    (channelId: string) => {
      void goChannel(channelId);
    },
    [goChannel],
  );
  const onOpenMessageLink = React.useCallback(
    (link: ParsedMessageLink) => {
      // Preserve the message-targeting navigation boundary before URL mutation.
      void goChannel(link.channelId, {
        messageId: link.messageId,
        threadRootId: link.threadRootId,
      });
    },
    [goChannel],
  );
  const onImportSnapshotFromUrl = React.useCallback(
    (fileBytes: number[], fileName: string, snapshotKind: "agent" | "team") => {
      requestOpenSnapshotImport({ fileBytes, fileName, snapshotKind });
      void goAgents();
    },
    [goAgents],
  );

  return { onOpenChannel, onOpenMessageLink, onImportSnapshotFromUrl };
}
