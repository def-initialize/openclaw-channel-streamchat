# openclaw-channel-streamchat

OpenClaw channel plugin for [Stream Chat](https://getstream.io/chat/). Connects as a bot user via WebSocket, normalizes inbound messages into OpenClaw envelope format, and delivers agent responses using Stream Chat's AI streaming pattern (`partialUpdateMessage` + `ai_indicator` events).

## Prerequisites

- OpenClaw `>= 2026.2.13`
- A Stream Chat application (API key + secret from the [Stream Dashboard](https://dashboard.getstream.io/))
- Node.js `>= 20`

## Setup

### 1. Install dependencies

```bash
cd openclaw-channel-streamchat
npm install
```

### 2. Provision the app

You have two options depending on your situation:

**Option A — Fresh app (recommended for first-time setup)**

Use `setup-app.ts` if you are starting from a new Stream Chat app. It creates the bot and test users, generates their tokens, creates a test channel, and writes both `~/.openclaw/openclaw.json` and `scripts/.env` automatically:

```bash
STREAM_API_KEY=your_api_key STREAM_API_SECRET=your_api_secret npx tsx scripts/setup-app.ts
```

After this, skip to step 4.

**Option B — Existing app (bot token only)**

Use `generate-bot-token.ts` if the app and channel already exist and you only need to mint or rotate the bot JWT. It prints the token to stdout — copy it into `~/.openclaw/openclaw.json` manually:

```bash
STREAM_API_KEY=your_api_key STREAM_API_SECRET=your_api_secret npx tsx scripts/generate-bot-token.ts
```

> **Note:** Pass the API secret inline as shown above. It is only needed by these two provisioning scripts and should not be stored in `scripts/.env`.

### 3. Configure OpenClaw (Option B only)

If you used Option B, add the channel config and plugin entry to `~/.openclaw/openclaw.json` manually:

```jsonc
{
  "channels": {
    "streamchat": {
      "enabled": true,
      "apiKey": "your_api_key",
      "botUserId": "openclaw-bot",
      "botUserToken": "<token from generate-bot-token.ts>",
      // Optional:
      "ackReaction": "eyes",              // reaction added when message is received (default: "eyes")
      "doneReaction": "white_check_mark", // reaction swapped in when response is done (default: "white_check_mark")
      "streamingThrottle": 35,            // partial-update every Nth chunk after chunks 1 and 5 (default: 35)
      "mockResponse": "hello"             // if set, reply with this string and skip agent dispatch (for testing)
    }
  },
  "plugins": {
    "load": {
      "paths": ["/absolute/path/to/openclaw-channel-streamchat"]
    },
    "entries": {
      "streamchat": { "enabled": true }
    }
  }
}
```

### 4. Restart the gateway

```bash
openclaw gateway restart
```

The plugin will connect to Stream Chat, watch all channels where the bot is a member, and start processing messages.

## Testing

All test scripts live in `scripts/` and load credentials from `scripts/.env` (populated by `setup-app.ts`). The plugin itself reads only from `~/.openclaw/openclaw.json` — `scripts/.env` is not used at runtime.

See `scripts/.env.example` for the expected variables. You can also pass any variable inline to override the file:

```bash
STREAM_API_KEY=... TEST_USER_TOKEN=... npx tsx scripts/chat-client.ts
```

### Discover channels

Lists all channels the test user belongs to:

```bash
npx tsx scripts/discover-channels.ts
```

### Interactive chat client

Connects as a test user, watches a channel, and lets you send messages interactively while printing incoming bot responses and AI indicator events:

```bash
# Auto-discover channels and use the first one
npx tsx scripts/chat-client.ts

# Specify a channel
npx tsx scripts/chat-client.ts myChannelId

# Send a single message
npx tsx scripts/chat-client.ts myChannelId "Hello bot"
```

Commands inside the interactive client:

| Command | Description |
|---------|-------------|
| `/thread <parentId> <text>` | Send a thread reply |
| `/quote <messageId> <text>` | Send a quoted reply |
| `/quit` | Disconnect and exit |

### Automated round-trip test

Sends a message and waits for the bot to respond, verifying the full streaming lifecycle (placeholder message, AI indicators, partial updates, final update):

```bash
npx tsx scripts/test-roundtrip.ts
```

Expected output:

```
[NEW MSG][chatgpt] [AI]: (no text)        # empty placeholder
[AI INDICATOR] AI_STATE_THINKING           # thinking indicator
[AI INDICATOR] AI_STATE_GENERATING         # generating indicator
[STREAMING] 2 + 2 = 4.                    # partial update
[FINAL] 2 + 2 = 4.                        # final update (generating: false)
[AI INDICATOR] cleared                     # indicator cleared

✓ Round-trip test PASSED — got bot response.
```

### Thread test

Sends a parent message, waits for the bot's response, then sends a thread reply and verifies the bot responds inside the thread:

```bash
npx tsx scripts/test-thread.ts
```

## Mock mode

Set `mockResponse` in `openclaw.json` to make the plugin reply with a static string instead of dispatching to the agent. Useful when the OpenClaw agent pipeline is unavailable or broken and you need to verify the channel plumbing end-to-end.

```jsonc
"channels": {
  "streamchat": {
    // ...
    "mockResponse": "hello"
  }
}
```

OpenClaw hot-reloads `channels.streamchat.*` config on save — no restart needed. Remove the field to restore normal agent dispatch.

## How it works

**Inbound flow:**
1. Bot receives `message.new` event via WebSocket
2. Plugin filters out bot's own messages and AI-generated messages
3. Builds an envelope with thread/reply context wrappers (`[Thread]`, `[Replying]`)
4. Dispatches to the OpenClaw agent pipeline

**Outbound flow (streaming):**
1. Creates an empty placeholder message with `ai_generated: true`
2. Sends `ai_indicator.update` with `AI_STATE_THINKING`
3. On first text chunk, switches to `AI_STATE_GENERATING`
4. Progressively updates the message via `partialUpdateMessage` with `generating: true`
5. On completion, sends final update with `generating: false` and clears the indicator

**Thread handling:**
- Thread replies include `parent_id` so the bot's response routes to the correct thread
- First message in a thread includes the parent message text for context
- Quoted replies are wrapped in `[Replying to ...]` envelopes
