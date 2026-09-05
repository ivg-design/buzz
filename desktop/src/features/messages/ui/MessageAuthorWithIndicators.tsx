import type * as React from "react";

import { MessageAuthorIdentity } from "@/features/messages/ui/MessageHeader";
import { UserNameIndicators } from "@/features/user-status/ui/UserNameIndicators";

type MessageAuthorWithIndicatorsProps = {
  authorName: string;
  channelId?: string | null;
  originEventId?: string | null;
  children: React.ReactNode;
  ownerPubkey?: string | null;
  pubkey: string;
  role?: string;
};

export function MessageAuthorWithIndicators({
  authorName,
  channelId,
  originEventId,
  children,
  ownerPubkey,
  pubkey,
  role,
}: MessageAuthorWithIndicatorsProps) {
  return (
    <span className="inline-flex min-w-0 items-baseline gap-1">
      <MessageAuthorIdentity
        channelId={channelId}
        originEventId={originEventId}
        displayName={authorName}
        ownerPubkey={ownerPubkey}
        pubkey={pubkey}
        role={role}
      >
        {children}
      </MessageAuthorIdentity>
      <UserNameIndicators pubkey={pubkey} />
    </span>
  );
}
