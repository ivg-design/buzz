export type JobConversationEvent = {
  kind: number;
  content: string;
  tags?: string[][];
};

export type JobConversationProjection = {
  body: string;
  requestEventId: string | null;
  taskRoot: boolean;
  hidden: boolean;
  malformed: boolean;
};

/** Project one job protocol event into an ordinary chat body and task root. */
export function projectJobConversation(
  event: JobConversationEvent,
): JobConversationProjection | null;

export function isJobConversationKind(kind: number): boolean;
