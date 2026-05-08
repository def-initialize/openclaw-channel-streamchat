#!/usr/bin/env npx tsx
import assert from "node:assert/strict";
import { StreamingHandler } from "../src/streaming.js";
import { RunContextMap } from "../src/run-context.js";
import type { RunContext } from "../src/types.js";

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: unknown) => void;
} {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function rateLimitError(): Error & { status: number } {
  const err = new Error("Too many requests") as Error & { status: number };
  err.status = 429;
  return err;
}

function runContext(runId: string): RunContext {
  return {
    runId,
    channelType: "messaging",
    channelId: "user-1-main",
    threadParentId: null,
    inboundMessageId: `inbound-${runId}`,
    senderId: "user-1",
    responseMessageId: null,
  };
}

async function testFinalUpdateRetries(): Promise<void> {
  const events: Array<Record<string, unknown>> = [];
  const updates: Array<{ messageId: string; payload: unknown }> = [];

  const channel = {
    sendMessage: async () => ({ message: { id: "response-1" } }),
    sendEvent: async (event: Record<string, unknown>) => {
      events.push(event);
    },
  };

  let finalAttempts = 0;
  const client = {
    partialUpdateMessage: async (messageId: string, payload: unknown) => {
      updates.push({ messageId, payload });
      const set = (payload as { set?: { generating?: boolean } }).set;
      if (set?.generating === false && finalAttempts < 2) {
        finalAttempts++;
        throw rateLimitError();
      }
    },
  };

  const runContexts = new RunContextMap();
  const runCtx = runContext("run-1");
  runContexts.set(runCtx.runId, runCtx);

  const logMessages: string[] = [];
  const handler = new StreamingHandler({
    client: client as never,
    runContexts,
    log: {
      warn: (message: string) => logMessages.push(message),
      error: (message: string) => logMessages.push(message),
    },
  });

  await handler.onRunStarted(runCtx.runId, channel as never, runCtx);
  await handler.onTextChunk(runCtx.runId, "alpha ", 1);
  await handler.onTextChunk(runCtx.runId, "beta", 1);
  await handler.onRunCompleted(runCtx.runId);

  const finalUpdates = updates.filter(({ payload }) => {
    const set = (payload as { set?: { generating?: boolean } }).set;
    return set?.generating === false;
  });

  assert.equal(finalUpdates.length, 3, "final update should retry retryable failures");
  assert.deepEqual(finalUpdates.at(-1), {
    messageId: "response-1",
    payload: { set: { text: "alpha beta", generating: false } },
  });
  assert.equal(
    events.at(-1)?.type,
    "ai_indicator.clear",
    "indicator should clear after final text update succeeds",
  );
  assert.equal(logMessages.length, 0, "successful retry should not log a final failure");

  runContexts.delete(runCtx.runId);
}

async function testLateGeneratingIndicatorCannotAppendAfterFinal(): Promise<void> {
  const events: Array<Record<string, unknown>> = [];
  const updates: Array<{ messageId: string; payload: unknown }> = [];
  const generatingIndicator = deferred<void>();

  const channel = {
    sendMessage: async () => ({ message: { id: "response-2" } }),
    sendEvent: async (event: Record<string, unknown>) => {
      events.push(event);
      if (event.ai_state === "AI_STATE_GENERATING") {
        return generatingIndicator.promise;
      }
    },
  };

  const client = {
    partialUpdateMessage: async (messageId: string, payload: unknown) => {
      updates.push({ messageId, payload });
    },
    deleteMessage: async () => undefined,
  };

  const runContexts = new RunContextMap();
  const runCtx = runContext("run-2");
  runContexts.set(runCtx.runId, runCtx);

  const handler = new StreamingHandler({
    client: client as never,
    runContexts,
  });

  await handler.onRunStarted(runCtx.runId, channel as never, runCtx);

  const chunkPromise = handler.onTextChunk(runCtx.runId, "late text", 1);
  assert.equal(
    events.at(-1)?.ai_state,
    "AI_STATE_GENERATING",
    "test setup should block the generating indicator",
  );

  await handler.onRunCompleted(runCtx.runId);
  assert.deepEqual(updates.at(-1), {
    messageId: "response-2",
    payload: { set: { text: "late text", generating: false } },
  });

  generatingIndicator.resolve();
  await chunkPromise;
  await Promise.resolve();

  assert.deepEqual(
    updates.at(-1),
    {
      messageId: "response-2",
      payload: { set: { text: "late text", generating: false } },
    },
    "late onTextChunk continuation must not enqueue generating:true after final update",
  );

  runContexts.delete(runCtx.runId);
}

await testFinalUpdateRetries();
await testLateGeneratingIndicatorCannotAppendAfterFinal();

console.log("streaming final retry test passed");
