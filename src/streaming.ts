import type { Channel, StreamChat } from "stream-chat";
import type { ChannelLogSink } from "openclaw/plugin-sdk";
import type { RunContext } from "./types.js";
import type { RunContextMap } from "./run-context.js";

// ---------------------------------------------------------------------------
// safeSendEvent — exponential backoff retry for indicator events
// ---------------------------------------------------------------------------

async function safeSendEvent(
  channel: Channel,
  event: Record<string, unknown>,
  log?: ChannelLogSink,
): Promise<void> {
  const maxAttempts = 5;
  let delay = 100;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await channel.sendEvent(event as unknown as Parameters<typeof channel.sendEvent>[0]);
      return;
    } catch (err: unknown) {
      const status =
        (err as { status?: number })?.status ??
        (err as { response?: { status?: number } })?.response?.status;
      const retryable = status === 429 || (status != null && status >= 500 && status < 600);
      if (!retryable || attempt === maxAttempts) {
        // Swallow in streaming context to avoid breaking generation flow
        log?.warn?.(
          `[StreamChat][safeSendEvent] Failed after ${attempt} attempt(s): ${String(err)}`,
        );
        return;
      }
      await new Promise((r) =>
        setTimeout(r, delay + Math.floor(Math.random() * 50)),
      );
      delay *= 2;
    }
  }
}

// ---------------------------------------------------------------------------
// StreamingHandler — manages AI streaming via partialUpdateMessage
// ---------------------------------------------------------------------------

export interface StreamingHandlerDeps {
  client: StreamChat;
  runContexts: RunContextMap;
  log?: ChannelLogSink;
}

interface ActiveStream {
  runCtx: RunContext;
  channel: Channel;
  messageId: string;
  accumulatedText: string;
  chunkCounter: number;
  indicatorState: string | undefined;
  lastUpdatePromise: Promise<void>;
  finalized: boolean;
}

export class StreamingHandler {
  private streams = new Map<string, ActiveStream>();
  private deps: StreamingHandlerDeps;

  constructor(deps: StreamingHandlerDeps) {
    this.deps = deps;
  }

  /**
   * Called when a new agent run starts. Creates the placeholder message
   * and sends the THINKING indicator.
   */
  async onRunStarted(
    runId: string,
    channel: Channel,
    runCtx: RunContext,
  ): Promise<string> {
    const { log } = this.deps;

    // Create empty AI message (optionally threaded)
    const msgPayload: Record<string, unknown> = {
      text: "",
      ai_generated: true,
    };
    if (runCtx.threadParentId) {
      msgPayload.parent_id = runCtx.threadParentId;
    }

    const { message } = await channel.sendMessage(
      msgPayload as Parameters<typeof channel.sendMessage>[0],
    );
    const messageId = message.id;

    // Record the response message ID in the run context
    this.deps.runContexts.setResponseMessageId(runId, messageId);

    // Send THINKING indicator
    await safeSendEvent(
      channel,
      {
        type: "ai_indicator.update",
        ai_state: "AI_STATE_THINKING",
        message_id: messageId,
      },
      log,
    );

    this.streams.set(runId, {
      runCtx,
      channel,
      messageId,
      accumulatedText: "",
      chunkCounter: 0,
      indicatorState: "AI_STATE_THINKING",
      lastUpdatePromise: Promise.resolve(),
      finalized: false,
    });

    return messageId;
  }

  /**
   * Called for each text chunk from the agent. Accumulates text and
   * periodically flushes via partialUpdateMessage with a throttle pattern:
   * - Early bursts: update on odd chunks < 8 (chunks 1, 3, 5, 7)
   * - Then every Nth chunk (configurable via streamingThrottle, default 15)
   */
  async onTextChunk(
    runId: string,
    chunk: string,
    streamingThrottle: number = 15,
  ): Promise<void> {
    const stream = this.streams.get(runId);
    if (!stream || stream.finalized) return;

    const { client } = this.deps;

    stream.accumulatedText += chunk;
    stream.chunkCounter++;
    const n = stream.chunkCounter;

    // Switch to GENERATING on first text chunk
    if (stream.indicatorState !== "AI_STATE_GENERATING") {
      stream.indicatorState = "AI_STATE_GENERATING";
      await safeSendEvent(
        stream.channel,
        {
          type: "ai_indicator.update",
          ai_state: "AI_STATE_GENERATING",
          message_id: stream.messageId,
        },
        this.deps.log,
      );
    }

    // Throttle: early bursts (odd chunks < 8) then every Nth
    const shouldUpdate =
      (n < 8 && n % 2 !== 0) || n % streamingThrottle === 0;

    if (shouldUpdate) {
      const text = stream.accumulatedText;
      const messageId = stream.messageId;
      stream.lastUpdatePromise = stream.lastUpdatePromise.then(() =>
        client
          .partialUpdateMessage(messageId, {
            set: { text, generating: true },
          })
          .then(() => undefined)
          .catch((err) => {
            this.deps.log?.warn?.(
              `[StreamChat][streaming] partialUpdate failed: ${String(err)}`,
            );
          }),
      );
    }
  }

  /**
   * Called when the agent invokes a tool. Updates the indicator to
   * EXTERNAL_SOURCES so the UI shows tool activity.
   */
  async onRunProgress(runId: string, _toolName?: string): Promise<void> {
    const stream = this.streams.get(runId);
    if (!stream || stream.finalized) return;

    if (stream.indicatorState !== "AI_STATE_EXTERNAL_SOURCES") {
      stream.indicatorState = "AI_STATE_EXTERNAL_SOURCES";
      await safeSendEvent(
        stream.channel,
        {
          type: "ai_indicator.update",
          ai_state: "AI_STATE_EXTERNAL_SOURCES",
          message_id: stream.messageId,
        },
        this.deps.log,
      );
    }
  }

  /**
   * Called when the agent run completes successfully.
   * Finalizes the message text and clears the indicator.
   */
  async onRunCompleted(runId: string): Promise<void> {
    const stream = this.streams.get(runId);
    if (!stream || stream.finalized) return;
    stream.finalized = true;

    const { client, log } = this.deps;

    // Wait for any in-flight partial updates
    await stream.lastUpdatePromise.catch(() => {});

    const finalText = stream.accumulatedText.trim();
    if (!finalText) {
      try {
        await client.deleteMessage(stream.messageId, { hardDelete: true });
      } catch (err) {
        log?.warn?.(
          `[StreamChat][streaming] Empty placeholder delete failed: ${String(err)}`,
        );
        try {
          await client.partialUpdateMessage(stream.messageId, {
            set: { text: "", generating: false },
          });
        } catch (fallbackErr) {
          log?.error?.(
            `[StreamChat][streaming] Empty placeholder fallback update failed: ${String(fallbackErr)}`,
          );
        }
      }

      await safeSendEvent(
        stream.channel,
        { type: "ai_indicator.clear", message_id: stream.messageId },
        log,
      );

      this.streams.delete(runId);
      return;
    }

    // Final update with complete text
    try {
      await client.partialUpdateMessage(stream.messageId, {
        set: {
          text: stream.accumulatedText || "(No response)",
          generating: false,
        },
      });
    } catch (err) {
      log?.error?.(
        `[StreamChat][streaming] Final update failed: ${String(err)}`,
      );
    }

    // Clear indicator
    await safeSendEvent(
      stream.channel,
      { type: "ai_indicator.clear", message_id: stream.messageId },
      log,
    );

    this.streams.delete(runId);
  }

  /**
   * Called when the agent run encounters an error.
   */
  async onRunError(runId: string, error: string): Promise<void> {
    const stream = this.streams.get(runId);
    if (!stream || stream.finalized) return;
    stream.finalized = true;

    const { client, log } = this.deps;

    // Wait for any in-flight partial updates
    await stream.lastUpdatePromise.catch(() => {});

    const errorText = stream.accumulatedText
      ? `${stream.accumulatedText}\n\n---\nError: ${error}`
      : `Error: ${error}`;

    try {
      await client.partialUpdateMessage(stream.messageId, {
        set: { text: errorText, generating: false },
      });
    } catch (err) {
      log?.error?.(
        `[StreamChat][streaming] Error update failed: ${String(err)}`,
      );
    }

    // Send error indicator
    await safeSendEvent(
      stream.channel,
      {
        type: "ai_indicator.update",
        ai_state: "AI_STATE_ERROR",
        message_id: stream.messageId,
      },
      log,
    );

    this.streams.delete(runId);
  }

  /**
   * Called when the user force-stops generation (ai_indicator.stop).
   * Does NOT overwrite the accumulated text.
   */
  async onForceStop(runId: string): Promise<void> {
    const stream = this.streams.get(runId);
    if (!stream || stream.finalized) return;
    stream.finalized = true;

    const { client, log } = this.deps;

    // Wait for any in-flight partial updates
    await stream.lastUpdatePromise.catch(() => {});

    // Just clear generating flag, don't touch text
    try {
      await client.partialUpdateMessage(stream.messageId, {
        set: { generating: false },
      });
    } catch (err) {
      log?.warn?.(
        `[StreamChat][streaming] Force stop update failed: ${String(err)}`,
      );
    }

    await safeSendEvent(
      stream.channel,
      { type: "ai_indicator.clear", message_id: stream.messageId },
      log,
    );

    this.streams.delete(runId);
  }

  getActiveStream(runId: string): { messageId: string } | undefined {
    const stream = this.streams.get(runId);
    if (!stream) return undefined;
    return { messageId: stream.messageId };
  }
}
