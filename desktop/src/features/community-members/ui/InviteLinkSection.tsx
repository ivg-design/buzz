import { Check, ChevronDown } from "lucide-react";
import { motion, useReducedMotion } from "motion/react";
import * as React from "react";
import { toast } from "sonner";
import { useQueryClient } from "@tanstack/react-query";

import { pendingInvitesQueryKey } from "@/features/community-members/hooks";
import { mintInvite, type MintedInvite } from "@/shared/api/invites";
import { writeTextToClipboard } from "@/shared/lib/clipboard";
import { Button } from "@/shared/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from "@/shared/ui/dropdown-menu";
import { Input } from "@/shared/ui/input";
import { Spinner } from "@/shared/ui/spinner";

const TTL_OPTIONS: { label: string; value: number }[] = [
  { label: "1 day", value: 24 * 60 * 60 },
  { label: "3 days", value: 3 * 24 * 60 * 60 },
  { label: "7 days", value: 7 * 24 * 60 * 60 },
  { label: "30 days", value: 30 * 24 * 60 * 60 },
];

const MAX_USE_OPTIONS: { label: string; value: number }[] = [
  { label: "1 use", value: 1 },
  { label: "3 uses", value: 3 },
  { label: "5 uses", value: 5 },
  { label: "10 uses", value: 10 },
  { label: "25 uses", value: 25 },
];

export const DEFAULT_INVITE_TTL_SECS = TTL_OPTIONS[1].value;

type CopyStatus = "idle" | "copying" | "copied";
type GenerationStatus = "idle" | "generating" | "failed";

/**
 * Share-with-link footer for the community invite dialog.
 *
 * Minting is an explicit operator action. Merely opening the dialog or
 * changing an option never creates a durable bearer. A successful response
 * publishes the one-time bearer to local state before refreshing the
 * non-secret pending-invite metadata. The metadata refresh is best-effort and
 * also runs when this component was closed while the request was in flight.
 */
export function InviteLinkSection({
  onTtlSecsChange,
  ttlSecs,
}: {
  onTtlSecsChange: (ttlSecs: number) => void;
  ttlSecs: number;
}) {
  const [copyStatus, setCopyStatus] = React.useState<CopyStatus>("idle");
  const [generationStatus, setGenerationStatus] =
    React.useState<GenerationStatus>("idle");
  const [invite, setInvite] = React.useState<MintedInvite | null>(null);
  const [maxUses, setMaxUses] = React.useState(1);
  const generationRequestId = React.useRef(0);
  const generationInFlight = React.useRef(false);
  const mounted = React.useRef(true);
  const shouldReduceMotion = useReducedMotion();
  const queryClient = useQueryClient();
  const ttlLabel =
    TTL_OPTIONS.find((option) => option.value === ttlSecs)?.label ?? "3 days";
  const maxUsesLabel =
    MAX_USE_OPTIONS.find((option) => option.value === maxUses)?.label ??
    "1 use";
  const isGenerating = generationStatus === "generating";
  const hasGenerationFailed = generationStatus === "failed";
  const isWorking = isGenerating || copyStatus === "copying";
  const actionLabel = isGenerating
    ? "Creating…"
    : hasGenerationFailed
      ? "Retry"
      : invite
        ? copyStatus === "copied"
          ? "Copied"
          : "Copy link"
        : "Create link";
  const actionButtonWidth = isWorking
    ? "6.25rem"
    : actionLabel === "Create link"
      ? "6.5rem"
      : actionLabel === "Copied"
        ? "5.25rem"
        : "5.5rem";
  const actionButtonTransition = shouldReduceMotion
    ? { duration: 0 }
    : { duration: 0.12, ease: [0.77, 0, 0.175, 1] as const };

  React.useEffect(() => {
    // React StrictMode performs setup -> cleanup -> setup in development.
    // Reset the liveness flag on every setup so the real mount can consume a
    // successful explicit Create response.
    mounted.current = true;
    return () => {
      mounted.current = false;
      generationRequestId.current += 1;
    };
  }, []);

  React.useEffect(() => {
    if (copyStatus !== "copied") return;
    const resetTimer = window.setTimeout(() => setCopyStatus("idle"), 2000);
    return () => window.clearTimeout(resetTimer);
  }, [copyStatus]);

  function resetCreatedInvite() {
    generationRequestId.current += 1;
    setInvite(null);
    setGenerationStatus("idle");
    setCopyStatus("idle");
  }

  async function generateInviteLink() {
    if (generationInFlight.current) return;
    generationInFlight.current = true;
    const requestId = generationRequestId.current + 1;
    generationRequestId.current = requestId;
    setGenerationStatus("generating");
    setInvite(null);
    setCopyStatus("idle");

    try {
      const minted = await mintInvite({ ttlSecs, maxUses });
      if (mounted.current && generationRequestId.current === requestId) {
        // Never let a list-refresh failure discard the only copy of the
        // bearer returned by a successful mint.
        setInvite(minted);
        setGenerationStatus("idle");
      }
      void queryClient
        .invalidateQueries({ queryKey: pendingInvitesQueryKey })
        .catch(() => undefined);
    } catch {
      if (!mounted.current || generationRequestId.current !== requestId) return;
      setGenerationStatus("failed");
      toast.error("Couldn’t create an invite link.");
    } finally {
      generationInFlight.current = false;
    }
  }

  async function handleCopy() {
    if (!invite || isGenerating || copyStatus === "copying") return;
    setCopyStatus("copying");
    try {
      await writeTextToClipboard(invite.url);
      setCopyStatus("copied");
      toast.success("Invite link copied");
    } catch {
      setCopyStatus("idle");
      toast.error("Couldn’t copy the invite link. Try again.");
    }
  }

  function handleAction() {
    if (invite && !hasGenerationFailed) {
      void handleCopy();
    } else {
      void generateInviteLink();
    }
  }

  return (
    <section data-testid="community-invite-link-section">
      <div className="relative">
        <Input
          aria-label="Community invite link"
          className="h-11 pr-28 text-transparent caret-transparent selection:bg-transparent"
          data-invite-id={invite?.id}
          data-testid="invite-link-url"
          disabled={isGenerating}
          placeholder={
            hasGenerationFailed
              ? "Couldn’t create invite link"
              : isGenerating
                ? "Creating invite link…"
                : "Create a link when you’re ready"
          }
          readOnly
          value={invite?.url ?? ""}
        />
        {invite ? (
          <span
            aria-hidden="true"
            className="pointer-events-none absolute inset-y-0 left-3 right-28 flex items-center truncate text-sm text-muted-foreground"
            data-testid="invite-link-preview"
          >
            {invite.url}
          </span>
        ) : null}
        <motion.div
          animate={{ width: actionButtonWidth }}
          className="absolute right-1 top-1"
          initial={false}
          transition={actionButtonTransition}
        >
          <Button
            className="h-9 w-full px-3"
            data-copy-status={copyStatus}
            data-testid="copy-invite-link"
            disabled={isWorking}
            onClick={handleAction}
            size="sm"
            type="button"
          >
            {isWorking ? (
              <Spinner aria-hidden="true" className="h-4 w-4 border-2" />
            ) : copyStatus === "copied" ? (
              <Check aria-hidden="true" className="h-4 w-4" />
            ) : null}
            {actionLabel}
          </Button>
        </motion.div>
      </div>

      <div className="mt-3 space-y-3">
        <div className="flex items-center justify-between gap-4">
          <span className="text-sm font-medium">Expires after</span>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button
                aria-label="Choose invite expiry"
                className="h-8 shrink-0 gap-1.5 px-2 text-sm text-muted-foreground"
                data-testid="invite-link-ttl-trigger"
                disabled={isWorking}
                size="sm"
                type="button"
                variant="ghost"
              >
                {ttlLabel}
                <ChevronDown aria-hidden="true" className="h-3.5 w-3.5" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-40">
              <DropdownMenuRadioGroup
                onValueChange={(value) => {
                  resetCreatedInvite();
                  onTtlSecsChange(Number(value));
                }}
                value={String(ttlSecs)}
              >
                {TTL_OPTIONS.map((option) => (
                  <DropdownMenuRadioItem
                    data-testid={`invite-link-ttl-${option.value}`}
                    key={option.value}
                    value={String(option.value)}
                  >
                    {option.label}
                  </DropdownMenuRadioItem>
                ))}
              </DropdownMenuRadioGroup>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
        <div className="flex items-center justify-between gap-4">
          <span className="text-sm font-medium">Limit number of uses</span>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button
                aria-label="Choose maximum invite uses"
                className="h-8 shrink-0 gap-1.5 px-2 text-sm text-muted-foreground"
                data-testid="invite-link-max-uses-trigger"
                disabled={isWorking}
                size="sm"
                type="button"
                variant="ghost"
              >
                {maxUsesLabel}
                <ChevronDown aria-hidden="true" className="h-3.5 w-3.5" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-40">
              <DropdownMenuRadioGroup
                onValueChange={(value) => {
                  resetCreatedInvite();
                  setMaxUses(Number(value));
                }}
                value={String(maxUses)}
              >
                {MAX_USE_OPTIONS.map((option) => (
                  <DropdownMenuRadioItem
                    data-testid={`invite-link-max-uses-${option.value}`}
                    key={option.value}
                    value={String(option.value)}
                  >
                    {option.label}
                  </DropdownMenuRadioItem>
                ))}
              </DropdownMenuRadioGroup>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>
    </section>
  );
}
