import assert from "node:assert/strict";
import { after, test } from "node:test";
import { draftForTimedTask, localCutoff, timedTaskInput } from "./form.ts";

const previousZone = process.env.TZ;
process.env.TZ = "America/New_York";
after(() => {
  if (previousZone === undefined) delete process.env.TZ;
  else process.env.TZ = previousZone;
});
const now = Date.parse("2026-09-05T12:00:00Z");
const draft = {
  ...draftForTimedTask(undefined, "channel"),
  instruction: "  check @Every agent\n\nKeep `symbols` exactly.  ",
};

test("form preserves exact prompt, recipient and initiating event through editing", () => {
  const input = timedTaskInput(draft, "recipient", "origin", now);
  assert.equal(input.instruction, draft.instruction);
  assert.equal(input.originEventId, "origin");
  assert.deepEqual(input.interval, { value: 1, unit: "hours" });
  assert.deepEqual(input.repetition, { mode: "forever" });
  assert.deepEqual(
    timedTaskInput(draftForTimedTask(input), "recipient", "origin", now),
    input,
  );
});

test("positive whole intervals and count limits are validated before dispatch", () => {
  for (const intervalValue of [
    "0",
    "-1",
    "0.5",
    "",
    "Infinity",
    "99999999999999999",
  ]) {
    assert.throws(() =>
      timedTaskInput({ ...draft, intervalValue }, "recipient", null, now),
    );
  }
  assert.throws(() =>
    timedTaskInput(
      { ...draft, repeatMode: "count", count: "0" },
      "recipient",
      null,
      now,
    ),
  );
  assert.throws(() =>
    timedTaskInput({ ...draft, instruction: " \n " }, "recipient", null, now),
  );
  assert.deepEqual(
    timedTaskInput(
      {
        ...draft,
        intervalValue: "3",
        intervalUnit: "days",
        repeatMode: "count",
        count: "7",
      },
      "recipient",
      null,
      now,
    ).repetition,
    { mode: "count", count: 7 },
  );
});

test("local cutoff captures offset on chosen date across DST, rejects nonexistent time", () => {
  assert.equal(localCutoff("2026-10-30T10:00", now).utcOffsetMinutes, -240);
  assert.equal(localCutoff("2026-11-03T10:00", now).utcOffsetMinutes, -300);
  assert.throws(() => localCutoff("2027-03-14T02:30", now), /does not exist/);
  assert.throws(() => localCutoff("2027-02-30T12:00", now), /does not exist/);
  // At the repeated fall-back hour, the native local date chooser resolves the first occurrence.
  assert.equal(localCutoff("2026-11-01T01:30", now).utcOffsetMinutes, -240);
});

test("cutoff permits a full interval, and edits retain a saved foreign timezone", () => {
  assert.throws(
    () =>
      timedTaskInput(
        { ...draft, repeatMode: "until", localDateTime: "2026-09-05T08:30" },
        "recipient",
        null,
        now,
      ),
    /one full interval/,
  );
  const input = timedTaskInput(
    { ...draft, repeatMode: "until", localDateTime: "2026-09-06T12:00" },
    "recipient",
    null,
    now,
  );
  const saved = {
    ...input,
    repetition: {
      ...input.repetition,
      timeZone: "Europe/Paris",
      utcOffsetMinutes: 120,
    },
  };
  assert.deepEqual(
    timedTaskInput(draftForTimedTask(saved), "recipient", null, now, saved)
      .repetition,
    saved.repetition,
  );
});
