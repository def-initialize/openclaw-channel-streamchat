import { randomUUID } from "node:crypto";
import type { Event } from "stream-chat";
import type {
  ChannelGatewayContext,
  ChannelLogSink,
  ChannelOutboundContext,
  OpenClawConfig,
} from "openclaw/plugin-sdk";
import { buildChannelConfigSchema } from "openclaw/plugin-sdk";
import { StreamChatConfigSchema } from "./config-schema.js";
import { getStreamChatRuntime } from "./runtime.js";
import { StreamChatClientRuntime } from "./stream-chat-runtime.js";
import { StreamingHandler } from "./streaming.js";
import { RunContextMap } from "./run-context.js";
import { buildEnvelope } from "./envelope.js";
import { safeAsync } from "./utils.js";
import type {
  ResolvedAccount,
  StreamChatChannelPlugin,
  RunContext,
} from "./types.js";
import {
  listStreamChatAccountIds,
  resolveStreamChatAccount,
} from "./types.js";

// Track which threads we've already seen (for first-in-thread detection)
const seenThreads = new Set<string>();

// Module-level registry of active gateway cleanup functions keyed by accountId.
// Allows startAccount to force-stop a stale connection if the framework calls
// startAccount again without having called stop() first (e.g. in-process reloads).
const activeGatewayCleanup = new Map<string, () => void>();

// ---------------------------------------------------------------------------
// Stream Chat target parsing
// ---------------------------------------------------------------------------

interface ParsedStreamChatTarget {
  channelType: string;
  channelId: string;
  cid: string;
}

const rejectedStreamChatTargetPrefixes = new Set([
  "streamchat",
  "channel",
  "group",
  "conversation",
  "room",
  "dm",
  "user",
  "thread",
]);

function getEventChannelCid(event: Event): string | undefined {
  const channel = event.channel as { cid?: unknown } | undefined;
  const cid = typeof channel?.cid === "string" ? channel.cid.trim() : "";
  return cid || undefined;
}

function buildStreamChatCid(channelType: string, channelId: string): string {
  return `${channelType}:${channelId}`;
}

function parseStreamChatTarget(
  raw?: string | null,
): ParsedStreamChatTarget | null {
  const cid = raw?.trim();
  if (!cid) return null;

  const separator = cid.indexOf(":");
  if (separator <= 0 || separator === cid.length - 1) return null;
  if (separator !== cid.lastIndexOf(":")) return null;

  const channelType = cid.slice(0, separator).trim();
  const channelId = cid.slice(separator + 1).trim();
  if (!channelType || !channelId) return null;
  if (rejectedStreamChatTargetPrefixes.has(channelType.toLowerCase())) {
    return null;
  }

  return {
    channelType,
    channelId,
    cid: buildStreamChatCid(channelType, channelId),
  };
}

function looksLikeStreamChatTarget(raw: string): boolean {
  return parseStreamChatTarget(raw) !== null;
}

// ---------------------------------------------------------------------------
// Reactions helper
// ---------------------------------------------------------------------------

async function addReaction(
  runtime: StreamChatClientRuntime,
  channelType: string,
  channelId: string,
  messageId: string,
  reactionType: string,
  log?: ChannelLogSink,
): Promise<void> {
  try {
    const channel = await runtime.getOrQueryChannel(channelType, channelId);
    await channel.sendReaction(messageId, { type: reactionType });
  } catch (err) {
    log?.warn?.(
      `[StreamChat] Failed to add reaction ${reactionType}: ${String(err)}`,
    );
  }
}

async function removeReaction(
  runtime: StreamChatClientRuntime,
  channelType: string,
  channelId: string,
  messageId: string,
  reactionType: string,
  log?: ChannelLogSink,
): Promise<void> {
  try {
    const channel = await runtime.getOrQueryChannel(channelType, channelId);
    await channel.deleteReaction(messageId, reactionType);
  } catch (err) {
    log?.warn?.(
      `[StreamChat] Failed to remove reaction ${reactionType}: ${String(err)}`,
    );
  }
}

// ---------------------------------------------------------------------------
// Inbound message handler
// ---------------------------------------------------------------------------

interface HandleMessageParams {
  cfg: OpenClawConfig;
  accountId: string;
  account: ResolvedAccount;
  event: Event;
  chatRuntime: StreamChatClientRuntime;
  streamingHandler: StreamingHandler;
  runContexts: RunContextMap;
  log?: ChannelLogSink;
}

export async function handleStreamChatMessage(params: HandleMessageParams): Promise<void> {
  const {
    cfg,
    accountId,
    account,
    event,
    chatRuntime,
    streamingHandler,
    runContexts,
    log,
  } = params;
  const rt = getStreamChatRuntime();

  const message = event.message;
  if (!message) return;

  // Bot echo prevention: skip our own messages and AI-generated messages
  if (event.user?.id === account.botUserId) return;
  if (message.ai_generated) return;

  const text = message.text?.trim();
  if (!text) return;

  const eventTarget = parseStreamChatTarget(getEventChannelCid(event));
  const channelType = event.channel_type ?? eventTarget?.channelType ?? "messaging";
  const channelId = event.channel_id ?? eventTarget?.channelId ?? "";
  if (!channelId) {
    log?.warn?.("[StreamChat] Dropping inbound message without channel id");
    return;
  }
  const streamTarget = eventTarget?.cid ?? buildStreamChatCid(channelType, channelId);
  const messageId = message.id;
  const senderId = event.user?.id ?? "unknown";
  const senderName = event.user?.name || senderId;

  // Determine thread and reply context
  const threadParentId = message.parent_id ?? null;
  const quotedMessageId = message.quoted_message_id ?? null;
  const quotedMessage = message.quoted_message ?? null;

  // Config-driven mock: reply with a static string and skip agent dispatch
  if (account.mockResponse) {
    const responseChannel = await chatRuntime.getOrQueryChannel(channelType, channelId);
    const msgPayload: Record<string, unknown> = { text: account.mockResponse, ai_generated: true };
    if (threadParentId) msgPayload.parent_id = threadParentId;
    await responseChannel.sendMessage(msgPayload as Parameters<typeof responseChannel.sendMessage>[0]);
    return;
  }

  // Resolve agent route
  // Use peer kind "channel" so the framework builds per-channel session keys:
  //   agent:<agentId>:streamchat:channel:<channelType>:<channelId>
  // This ensures each Stream Chat channel gets its own session (per action plan).
  const route = rt.channel.routing.resolveAgentRoute({
    cfg,
    channel: "streamchat",
    accountId,
    peer: { kind: "channel", id: streamTarget },
  });

  const storePath = rt.channel.session.resolveStorePath(
    cfg.session?.store,
    { agentId: route.agentId },
  );

  // Build envelope with thread/reply context
  let threadParentInfo: {
    id: string;
    text?: string;
    userId?: string;
    userName?: string;
  } | null = null;

  if (threadParentId) {
    // Try to get the parent message for context
    try {
      const channel = await chatRuntime.getOrQueryChannel(channelType, channelId);
      await channel.getReplies(threadParentId, { limit: 0 });
      // The parent message is embedded in the channel messages
      const state = channel.state;
      const parentMsg = state.messages.find((m) => m.id === threadParentId);
      threadParentInfo = {
        id: threadParentId,
        text: parentMsg?.text ?? undefined,
        userId: parentMsg?.user?.id ?? undefined,
        userName: parentMsg?.user?.name ?? undefined,
      };
    } catch {
      threadParentInfo = { id: threadParentId };
    }
  }

  let quotedInfo: {
    id: string;
    text?: string;
    userId?: string;
    userName?: string;
  } | null = null;

  if (quotedMessageId || quotedMessage) {
    quotedInfo = {
      id: quotedMessageId ?? quotedMessage?.id ?? "",
      text: quotedMessage?.text ?? undefined,
      userId: quotedMessage?.user?.id ?? undefined,
      userName: quotedMessage?.user?.name ?? undefined,
    };
  }

  const isFirstInThread = threadParentId
    ? !seenThreads.has(threadParentId)
    : false;
  if (threadParentId) seenThreads.add(threadParentId);

  const envelope = buildEnvelope({
    text,
    senderId,
    senderName,
    messageId,
    quotedMessage: quotedInfo,
    threadParent: threadParentInfo,
    isFirstInThread,
  });

  // Finalize inbound context
  const to = streamTarget;
  const fromLabel = `${senderName} (${senderId})`;

  const ctx = rt.channel.reply.finalizeInboundContext({
    Body: envelope.body,
    RawBody: text,
    CommandBody: envelope.commandBody,
    From: to,
    To: to,
    SessionKey: route.sessionKey,
    AccountId: accountId,
    ChatType: "channel" as const,
    ConversationLabel: fromLabel,
    SenderName: senderName,
    SenderId: senderId,
    Provider: "streamchat",
    Surface: "streamchat",
    MessageSid: messageId,
    Timestamp: message.created_at
      ? new Date(message.created_at).getTime()
      : Date.now(),
    OriginatingChannel: "streamchat",
    OriginatingTo: to,
  });

  // Record session
  await rt.channel.session.recordInboundSession({
    storePath,
    sessionKey: ctx.SessionKey || route.sessionKey,
    ctx,
    updateLastRoute: {
      sessionKey: route.mainSessionKey,
      channel: "streamchat",
      to,
      accountId,
    },
    onRecordError: (err: unknown) => {
      log?.error?.(
        `[StreamChat] Failed to record inbound session: ${String(err)}`,
      );
    },
  });

  log?.info?.(
    `[StreamChat] Inbound: from=${senderName} text="${text.slice(0, 50)}"`,
  );

  // Send ack reaction
  if (account.ackReaction) {
    safeAsync(
      () =>
        addReaction(
          chatRuntime,
          channelType,
          channelId,
          messageId,
          account.ackReaction,
          log,
        ),
      log,
      "ack reaction",
    );
  }

  // Create RunContext for delivery routing
  const runId = randomUUID();
  const runCtx: RunContext = {
    runId,
    channelType,
    channelId,
    threadParentId,
    inboundMessageId: messageId,
    senderId,
    responseMessageId: null,
  };
  runContexts.set(runId, runCtx);

  // Pre-create the placeholder message before dispatch so the message ID is
  // available when onPartialReply fires (which is called fire-and-forget by
  // OpenClaw and cannot safely do async work itself).
  const responseChannel = await chatRuntime.getOrQueryChannel(channelType, channelId);
  await streamingHandler.onRunStarted(runId, responseChannel, runCtx);

  let errorDelivered = false;

  // Track the text already pushed into StreamingHandler so cumulative token
  // snapshots and final/block dispatcher payloads do not duplicate each other.
  let streamedText = "";
  let sawPartialText = false;

  const appendTextDelta = (delta: string) => {
    if (!delta) return;
    streamedText += delta;
    void streamingHandler.onTextChunk(runId, delta, account.streamingThrottle);
  };

  const appendPartialText = (text: string) => {
    if (!text) return;

    if (text.startsWith(streamedText)) {
      appendTextDelta(text.slice(streamedText.length));
      return;
    }

    appendTextDelta(text);
  };

  const appendFinalText = (full: string) => {
    if (!full || full === streamedText) return;

    if (!streamedText || full.startsWith(streamedText)) {
      appendTextDelta(full.slice(streamedText.length));
      return;
    }

    log?.warn?.(
      "[StreamChat] Final text diverged from accumulated stream text; replacing streamed text",
    );
    streamedText = full;
    streamingHandler.replaceText(runId, full);
  };

  const replyOptions = {
    sourceReplyDeliveryMode: "automatic",
    onPartialReply: (payload: { text?: string }) => {
      if (payload.text) sawPartialText = true;
      appendPartialText(payload.text ?? "");
    },
  } as unknown as NonNullable<
    Parameters<
      typeof rt.channel.reply.dispatchReplyWithBufferedBlockDispatcher
    >[0]["replyOptions"]
  >;

  // Dispatch reply via the buffered block dispatcher.
  // onPartialReply fires for every streaming token (preview streaming).
  // deliver is called once per complete block; used here only for tool/error events.
  await rt.channel.reply.dispatchReplyWithBufferedBlockDispatcher({
    ctx,
    cfg,
    replyOptions,
    dispatcherOptions: {
      responsePrefix: "",
      deliver: async (
        payload: { text?: string; isError?: boolean },
        info: { kind: string },
      ) => {
        try {
          // Tool progress: update indicator to EXTERNAL_SOURCES
          if (info.kind === "tool") {
            await streamingHandler.onRunProgress(runId);
            return;
          }

          // Error: finalize with error state
          if (payload.isError) {
            await streamingHandler.onRunError(
              runId,
              payload.text || "Unknown error",
            );
            errorDelivered = true;
            return;
          }

          if (payload.text) {
            if (info.kind === "final") {
              appendFinalText(payload.text);
            } else if (!sawPartialText) {
              appendTextDelta(payload.text);
            }
          }
        } catch (err) {
          log?.error?.(
            `[StreamChat] Deliver failed: ${String(err)}`,
          );
          throw err;
        }
      },
    },
  });

  // Finalize after all deliveries complete
  if (!errorDelivered) {
    await streamingHandler.onRunCompleted(runId);
  }

  // Swap ack → done reaction
  if (account.ackReaction && account.doneReaction) {
    safeAsync(
      async () => {
        await removeReaction(
          chatRuntime,
          channelType,
          channelId,
          messageId,
          account.ackReaction,
          log,
        );
        await addReaction(
          chatRuntime,
          channelType,
          channelId,
          messageId,
          account.doneReaction,
          log,
        );
      },
      log,
      "reaction swap",
    );
  }

  runContexts.delete(runId);
}

// ---------------------------------------------------------------------------
// Channel plugin definition
// ---------------------------------------------------------------------------

export const streamchatPlugin: StreamChatChannelPlugin = {
  id: "streamchat",

  meta: {
    id: "streamchat",
    label: "Stream Chat",
    selectionLabel: "Stream Chat",
    docsPath: "/channels/streamchat",
    blurb: "Stream Chat messaging channel with AI streaming support.",
    aliases: ["sc"],
  },

  capabilities: {
    chatTypes: ["channel"],
    reactions: true,
    threads: true,
    media: false,
    nativeCommands: false,
    blockStreaming: false,
  },

  messaging: {
    normalizeTarget: (target: string) => target.trim() || undefined,
    inferTargetChatType: ({ to }: { to: string }) =>
      looksLikeStreamChatTarget(to) ? "channel" : undefined,
    targetResolver: {
      hint: "Use a Stream Chat CID, e.g. messaging:ai-test-channel",
      looksLikeId: looksLikeStreamChatTarget,
      resolveTarget: async ({
        input,
        normalized,
      }: {
        input: string;
        normalized: string;
      }) => {
        const target =
          parseStreamChatTarget(normalized) ?? parseStreamChatTarget(input);
        if (!target) return null;
        return {
          to: target.cid,
          kind: "channel" as const,
          display: target.cid,
          source: "normalized" as const,
        };
      },
    },
    formatTargetDisplay: ({ target, display }: { target: string; display?: string }) =>
      display ?? `#${target}`,
  } as unknown as NonNullable<StreamChatChannelPlugin["messaging"]>,

  reload: { configPrefixes: ["channels.streamchat"] },

  configSchema: buildChannelConfigSchema(StreamChatConfigSchema),

  config: {
    listAccountIds: (cfg: OpenClawConfig): string[] =>
      listStreamChatAccountIds(cfg),

    resolveAccount: (
      cfg: OpenClawConfig,
      accountId?: string | null,
    ): ResolvedAccount => resolveStreamChatAccount(cfg, accountId),

    defaultAccountId: () => "default",

    isConfigured: (account: ResolvedAccount): boolean =>
      Boolean(account.apiKey && account.botUserId && account.botUserToken),

    describeAccount: (account: ResolvedAccount) => ({
      accountId: account.accountId,
      name: account.botUserName || account.botUserId || undefined,
      enabled: account.enabled,
      configured: account.configured,
      running: false,
      lastStartAt: null,
      lastStopAt: null,
      lastError: null,
    }),
  },

  outbound: {
    deliveryMode: "direct",

    resolveTarget: ({ to }) => {
      const target = parseStreamChatTarget(to);
      if (!target) {
        return {
          ok: false,
          error: new Error(
            "Stream Chat target must be a CID like messaging:ai-test-channel.",
          ),
        };
      }
      return { ok: true, to: target.cid };
    },

    sendText: async (ctx: ChannelOutboundContext) => {
      const account = resolveStreamChatAccount(ctx.cfg, ctx.accountId);
      if (!account.configured) {
        throw new Error("StreamChat account not configured");
      }

      // We need to create a temporary client to send the outbound message.
      // In gateway mode, we reuse the running runtime, but for outbound-only
      // we create an ephemeral connection.
      const tempRuntime = new StreamChatClientRuntime(account);
      try {
        await tempRuntime.start();
        const target = parseStreamChatTarget(ctx.to);
        if (!target) {
          throw new Error(
            "Stream Chat target must be a CID like messaging:ai-test-channel.",
          );
        }
        const channel = await tempRuntime.getOrQueryChannel(
          target.channelType,
          target.channelId,
        );

        const msgPayload: Record<string, unknown> = { text: ctx.text };
        if (ctx.threadId) {
          msgPayload.parent_id = String(ctx.threadId);
        }

        const { message } = await channel.sendMessage(
          msgPayload as Parameters<typeof channel.sendMessage>[0],
        );

        return {
          channel: "streamchat" as const,
          messageId: message.id,
        };
      } finally {
        await tempRuntime.stop();
      }
    },
  },

  gateway: {
    startAccount: async (
      ctx: ChannelGatewayContext<ResolvedAccount>,
    ): Promise<void> => {
      const { cfg, accountId, account, log, abortSignal } = ctx;

      if (!account.configured) {
        throw new Error(
          "StreamChat not configured: apiKey, botUserId, and botUserToken are required",
        );
      }

      // Force-stop any stale runtime for this accountId that was never cleaned up
      // (can happen when the framework does an in-process reload without calling stop()).
      const staleCleanup = activeGatewayCleanup.get(accountId);
      if (staleCleanup) {
        log?.warn?.(
          `[StreamChat] Stale connection detected for account "${accountId}" — forcing cleanup before restart`,
        );
        staleCleanup();
      }

      const chatRuntime = new StreamChatClientRuntime(account, log);
      const runContexts = new RunContextMap();
      const streamingHandler = new StreamingHandler({
        client: chatRuntime.getClient(),
        runContexts,
        log,
      });

      // Connect and watch channels
      await chatRuntime.start();

      ctx.setStatus({
        ...ctx.getStatus(),
        running: true,
        lastStartAt: Date.now(),
        lastError: null,
      });

      // Listen for new messages
      const client = chatRuntime.getClient();
      const handleMessage = (event: Event) => {
        handleStreamChatMessage({
          cfg,
          accountId,
          account,
          event,
          chatRuntime,
          streamingHandler,
          runContexts,
          log,
        }).catch((err) => {
          log?.error?.(
            `[StreamChat] Message handler error: ${String(err)}`,
          );
        });
      };

      // Listen for force stop from client
      const handleAiStop = (event: Event) => {
        const messageId = (event as unknown as Record<string, unknown>).message_id as string | undefined;
        if (!messageId) return;
        const activeRun = runContexts.findByResponseMessageId(messageId);
        if (activeRun) {
          streamingHandler.onForceStop(activeRun.runId).catch((err) => {
            log?.warn?.(
              `[StreamChat] Force stop error: ${String(err)}`,
            );
          });
        }
      };

      client.on("message.new", handleMessage);
      client.on("ai_indicator.stop" as "user.watching.start", handleAiStop);

      log?.info?.(
        `[StreamChat] Gateway started for account "${accountId}"`,
      );

      // Stay pending until the abort signal fires or the caller stops the account.
      // Resolving early would cause the framework to interpret it as an unexpected
      // exit and schedule an auto-restart, accumulating live WebSocket connections.
      await new Promise<void>((resolve) => {
        // Idempotent via `stopped` guard — safe to call from both abort signal and
        // activeGatewayCleanup (which may fire on the next startAccount call).
        let stopped = false;
        const handleAbort = () => {
          if (stopped) return;
          stopped = true;
          client.off("message.new", handleMessage);
          client.off("ai_indicator.stop" as "user.watching.start", handleAiStop);
          activeGatewayCleanup.delete(accountId);
          chatRuntime.stop().catch((err) => {
            log?.error?.(
              `[StreamChat] Disconnect error: ${String(err)}`,
            );
          });
          ctx.setStatus({
            ...ctx.getStatus(),
            running: false,
            lastStopAt: Date.now(),
          });
          resolve();
        };

        activeGatewayCleanup.set(accountId, handleAbort);

        if (abortSignal) {
          abortSignal.addEventListener("abort", handleAbort, { once: true });
        }
      });
    },
  },

  status: {
    defaultRuntime: {
      accountId: "default",
      name: undefined,
      enabled: true,
      configured: false,
      running: false,
      lastStartAt: null,
      lastStopAt: null,
      lastError: null,
    },
  },
};
