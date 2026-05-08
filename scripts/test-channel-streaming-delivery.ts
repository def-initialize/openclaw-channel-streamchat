#!/usr/bin/env npx tsx
import assert from "node:assert/strict";
import { handleStreamChatMessage } from "../src/channel.js";
import { setStreamChatRuntime } from "../src/runtime.js";
import { RunContextMap } from "../src/run-context.js";
import { StreamingHandler } from "../src/streaming.js";
import type { ResolvedAccount } from "../src/types.js";

const responseChannel = {
  sendMessage: async () => ({ message: { id: "response-1" } }),
  sendEvent: async () => undefined,
};

const chatRuntime = {
  getOrQueryChannel: async () => responseChannel,
};

const cfg = {
  channels: {
    streamchat: {
      apiKey: "key",
      botUserId: "bot-1",
      botUserToken: "token",
    },
  },
};

const account: ResolvedAccount = {
  accountId: "default",
  enabled: true,
  configured: true,
  apiKey: "key",
  botUserId: "bot-1",
  botUserToken: "token",
  dmPolicy: "open",
  ackReaction: "",
  doneReaction: "",
  streamingThrottle: 35,
};

async function dispatchTestMessage(
  streamingHandler: Parameters<typeof handleStreamChatMessage>[0]["streamingHandler"],
  inboundId: string,
): Promise<void> {
  await handleStreamChatMessage({
    cfg: cfg as never,
    accountId: "default",
    account,
    event: {
      user: { id: "user-1", name: "OpenClaw User" },
      channel_type: "messaging",
      channel_id: "user-1-main",
      channel: { cid: "messaging:user-1-main" },
      message: {
        id: inboundId,
        text: "stream a long answer",
        created_at: new Date().toISOString(),
      },
    } as never,
    chatRuntime: chatRuntime as never,
    streamingHandler,
    runContexts: new RunContextMap(),
  });
}

async function testCumulativePartialDelivery(): Promise<void> {
  const deliveredChunks: string[] = [];
  let completed = false;
  let capturedReplyOptions: Record<string, unknown> | undefined;

  const streamingHandler = {
    onRunStarted: async () => "response-1",
    onTextChunk: async (_runId: string, chunk: string) => {
      deliveredChunks.push(chunk);
    },
    onRunCompleted: async () => {
      completed = true;
    },
    onRunProgress: async () => undefined,
    onRunError: async () => undefined,
  };

  setStreamChatRuntime({
    channel: {
      routing: {
        resolveAgentRoute: () => ({
          agentId: "default",
          sessionKey: "agent:default:streamchat:channel:messaging:user-1-main",
          mainSessionKey: "agent:default:streamchat:channel:messaging:user-1-main",
        }),
      },
      session: {
        resolveStorePath: () => "/tmp/openclaw-session-store",
        recordInboundSession: async () => undefined,
      },
      reply: {
        finalizeInboundContext: (ctx: Record<string, unknown>) => ctx,
        dispatchReplyWithBufferedBlockDispatcher: async (params: {
          replyOptions?: Record<string, unknown>;
          dispatcherOptions: {
            deliver: (
              payload: { text?: string; isError?: boolean },
              info: { kind: string },
            ) => Promise<void>;
          };
        }) => {
          capturedReplyOptions = params.replyOptions;
          const onPartialReply = params.replyOptions?.onPartialReply as
            | ((payload: { text?: string }) => void)
            | undefined;

          onPartialReply?.({ text: "first partial" });
          onPartialReply?.({ text: "first partial plus more" });
          await params.dispatcherOptions.deliver(
            { text: "first partial plus more" },
            { kind: "block" },
          );
          await params.dispatcherOptions.deliver(
            { text: "first partial plus more" },
            { kind: "final" },
          );

          return { queuedFinal: true, counts: { tool: 0, block: 0, final: 1 } };
        },
      },
    },
  } as never);

  await dispatchTestMessage(streamingHandler as never, "inbound-1");

  assert.equal(
    capturedReplyOptions?.sourceReplyDeliveryMode,
    "automatic",
    "Stream Chat should use automatic source replies so text stays in the streaming placeholder",
  );
  assert.deepEqual(
    deliveredChunks,
    ["first partial", " plus more"],
    "final dispatcher text must not duplicate text already delivered by partial streaming",
  );
  assert.equal(completed, true, "run should complete and finalize the placeholder");
}

async function testNonPrefixFinalReplacesStreamedText(): Promise<void> {
  const updates: Array<{ messageId: string; payload: unknown }> = [];
  let responseId = 0;
  const responseChannelWithEvents = {
    sendMessage: async () => ({ message: { id: `replacement-response-${++responseId}` } }),
    sendEvent: async () => undefined,
  };
  const chatRuntimeWithReplacementChannel = {
    getOrQueryChannel: async () => responseChannelWithEvents,
  };
  const runContexts = new RunContextMap();
  const streamingHandler = new StreamingHandler({
    client: {
      partialUpdateMessage: async (messageId: string, payload: unknown) => {
        updates.push({ messageId, payload });
      },
      deleteMessage: async () => undefined,
    } as never,
    runContexts,
  });

  setStreamChatRuntime({
    channel: {
      routing: {
        resolveAgentRoute: () => ({
          agentId: "default",
          sessionKey: "agent:default:streamchat:channel:messaging:user-1-main",
          mainSessionKey: "agent:default:streamchat:channel:messaging:user-1-main",
        }),
      },
      session: {
        resolveStorePath: () => "/tmp/openclaw-session-store",
        recordInboundSession: async () => undefined,
      },
      reply: {
        finalizeInboundContext: (ctx: Record<string, unknown>) => ctx,
        dispatchReplyWithBufferedBlockDispatcher: async (params: {
          replyOptions?: Record<string, unknown>;
          dispatcherOptions: {
            deliver: (
              payload: { text?: string; isError?: boolean },
              info: { kind: string },
            ) => Promise<void>;
          };
        }) => {
          const onPartialReply = params.replyOptions?.onPartialReply as
            | ((payload: { text?: string }) => void)
            | undefined;

          onPartialReply?.({ text: "draft partial" });
          await params.dispatcherOptions.deliver(
            { text: "Authoritative final answer." },
            { kind: "final" },
          );

          return { queuedFinal: true, counts: { tool: 0, block: 0, final: 1 } };
        },
      },
    },
  } as never);

  await handleStreamChatMessage({
    cfg: cfg as never,
    accountId: "default",
    account,
    event: {
      user: { id: "user-1", name: "OpenClaw User" },
      channel_type: "messaging",
      channel_id: "user-1-main",
      channel: { cid: "messaging:user-1-main" },
      message: {
        id: "inbound-2",
        text: "stream a normalized answer",
        created_at: new Date().toISOString(),
      },
    } as never,
    chatRuntime: chatRuntimeWithReplacementChannel as never,
    streamingHandler,
    runContexts,
  });

  const finalUpdate = updates.findLast(({ payload }) => {
    const set = (payload as { set?: { generating?: boolean } }).set;
    return set?.generating === false;
  });

  assert.deepEqual(
    finalUpdate,
    {
      messageId: "replacement-response-1",
      payload: {
        set: {
          text: "Authoritative final answer.",
          generating: false,
        },
      },
    },
    "non-prefix final text should replace stale streamed partial text",
  );
  assert.equal(
    updates.some(({ payload }) => {
      const set = (payload as { set?: { text?: string } }).set;
      return set?.text === "draft partialAuthoritative final answer.";
    }),
    false,
    "non-prefix final text must not be appended after the stale partial",
  );
}

await testCumulativePartialDelivery();
await testNonPrefixFinalReplacesStreamedText();

console.log("channel streaming delivery test passed");
