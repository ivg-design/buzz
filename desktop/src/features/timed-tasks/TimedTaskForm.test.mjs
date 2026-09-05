import assert from "node:assert/strict";
import { after, afterEach, before, test } from "node:test";
import { JSDOM } from "jsdom";
import { draftForTimedTask, timedTaskInput } from "./form.ts";

const dom = new JSDOM("<!doctype html><html><body></body></html>", {
  url: "http://localhost",
});
let React, render, fireEvent, cleanup, TimedTaskForm;
before(async () => {
  Object.assign(globalThis, {
    window: dom.window,
    document: dom.window.document,
    HTMLElement: dom.window.HTMLElement,
    IS_REACT_ACT_ENVIRONMENT: true,
  });
  React = (await import("react")).default;
  ({ render, fireEvent, cleanup } = await import("@testing-library/react"));
  ({ TimedTaskForm } = await import("./TimedTaskForm.tsx"));
});
afterEach(() => cleanup());
after(() => dom.window.close());

test("real editor selects count/end modes and submits unmodified instruction", () => {
  let submitted;
  function Fixture() {
    const [draft, setDraft] = React.useState(
      draftForTimedTask(undefined, "channel"),
    );
    return React.createElement(TimedTaskForm, {
      draft,
      onChange: setDraft,
      onSubmit: () => {
        submitted = timedTaskInput(draft, "recipient", "origin");
      },
      onCancel() {},
      channels: [{ id: "channel", name: "Fixture" }],
      timeZone: "America/New_York",
      pending: false,
      editing: false,
      error: null,
    });
  }
  const view = render(React.createElement(Fixture));
  const instruction =
    "  Check every available agent.\nKeep @name and whitespace.  ";
  fireEvent.change(view.getByLabelText("Instruction"), {
    target: { value: instruction },
  });
  fireEvent.change(view.getByLabelText("Send every"), {
    target: { value: "2" },
  });
  fireEvent.change(view.getByLabelText("Interval unit"), {
    target: { value: "minutes" },
  });
  fireEvent.change(view.getByLabelText("Repeat"), {
    target: { value: "until" },
  });
  assert.ok(view.getByLabelText("End date and time"));
  fireEvent.change(view.getByLabelText("Repeat"), {
    target: { value: "count" },
  });
  fireEvent.change(view.getByLabelText("Total runs"), {
    target: { value: "4" },
  });
  fireEvent.click(view.getByRole("button", { name: "Add timed task" }));
  assert.equal(submitted.instruction, instruction);
  assert.equal(submitted.originEventId, "origin");
  assert.deepEqual(submitted.interval, { value: 2, unit: "minutes" });
  assert.deepEqual(submitted.repetition, { mode: "count", count: 4 });
  assert.ok(view.getByText(/Timezone: America\/New_York/));
});

test("editing preserves the conversation and busy save disables controls", () => {
  const view = render(
    React.createElement(TimedTaskForm, {
      draft: draftForTimedTask(undefined, "channel"),
      onChange() {},
      onSubmit() {},
      onCancel() {},
      channels: [{ id: "channel", name: "Fixture" }],
      timeZone: "UTC",
      pending: true,
      editing: true,
      error: "Could not save",
    }),
  );
  assert.equal(view.getByLabelText("Conversation").disabled, true);
  assert.equal(view.getByRole("button", { name: "Saving…" }).disabled, true);
  assert.equal(view.getByRole("alert").textContent, "Could not save");
});
