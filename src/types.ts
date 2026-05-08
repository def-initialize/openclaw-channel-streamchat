import type { ChannelPlugin, OpenClawConfig } from "openclaw/plugin-sdk";

// ---------------------------------------------------------------------------
// Channel configuration
// ---------------------------------------------------------------------------

export interface StreamChatChannelConfig {
  enabled?: boolean;
  apiKey?: string;
  botUserId?: string;
  botUserToken?: string;
  botUserName?: string;
  dmPolicy?: "open" | "pairing";
  ackReaction?: string;
  doneReaction?: string;
  streamingThrottle?: number;
  mockResponse?: string;
  accounts?: Record<string, StreamChatChannelConfig>;
}

export interface ResolvedAccount {
  accountId: string;
  enabled: boolean;
  configured: boolean;
  apiKey: string;
  botUserId: string;
  botUserToken: string;
  botUserName?: string;
  dmPolicy: "open" | "pairing";
  ackReaction: string;
  doneReaction: string;
  streamingThrottle: number;
  mockResponse?: string;
}

// ---------------------------------------------------------------------------
// Run context for outbound delivery routing
// ---------------------------------------------------------------------------

export interface RunContext {
  runId: string;
  channelType: string;
  channelId: string;
  threadParentId: string | null;
  inboundMessageId: string;
  senderId: string;
  responseMessageId: string | null;
}

// ---------------------------------------------------------------------------
// Envelope builder result
// ---------------------------------------------------------------------------

export interface EnvelopeResult {
  body: string;
  commandBody: string;
}

// ---------------------------------------------------------------------------
// Plugin type alias
// ---------------------------------------------------------------------------

export type StreamChatChannelPlugin = ChannelPlugin<ResolvedAccount>;

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

export function getStreamChatConfig(cfg: OpenClawConfig): StreamChatChannelConfig {
  return (cfg.channels as Record<string, unknown> | undefined)?.streamchat as StreamChatChannelConfig ?? {};
}

export function listStreamChatAccountIds(cfg: OpenClawConfig): string[] {
  const sc = getStreamChatConfig(cfg);
  const ids: string[] = [];
  if (sc.apiKey || sc.botUserId || sc.botUserToken) {
    ids.push("default");
  }
  if (sc.accounts) {
    ids.push(...Object.keys(sc.accounts));
  }
  if (ids.length === 0) ids.push("default");
  return ids;
}

export function resolveStreamChatAccount(
  cfg: OpenClawConfig,
  accountId?: string | null,
): ResolvedAccount {
  const id = accountId || "default";
  const sc = getStreamChatConfig(cfg);

  const base: StreamChatChannelConfig =
    id !== "default" && sc.accounts?.[id]
      ? { ...sc, ...sc.accounts[id] }
      : sc;

  return {
    accountId: id,
    enabled: base.enabled !== false,
    configured: Boolean(base.apiKey && base.botUserId && base.botUserToken),
    apiKey: base.apiKey ?? "",
    botUserId: base.botUserId ?? "",
    botUserToken: base.botUserToken ?? "",
    botUserName: base.botUserName,
    dmPolicy: base.dmPolicy ?? "open",
    ackReaction: base.ackReaction ?? "eyes",
    doneReaction: base.doneReaction ?? "white_check_mark",
    streamingThrottle: base.streamingThrottle ?? 35,
    mockResponse: base.mockResponse,
  };
}
