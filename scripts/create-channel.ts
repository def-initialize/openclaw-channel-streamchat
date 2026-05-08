#!/usr/bin/env npx tsx
/**
 * Creates a messaging channel for a given user and adds the bot as a member.
 *
 * Usage:
 *   npx tsx scripts/create-channel.ts [channelId]
 *
 * Reads STREAM_API_KEY, BOT_USER_ID, TEST_USER_ID, TEST_USER_TOKEN from scripts/.env.
 * channelId defaults to a timestamp-based ID if not provided.
 */

import { config } from "dotenv";
config({ path: new URL(".env", import.meta.url).pathname });
import { StreamChat } from "stream-chat";

const apiKey = process.env.STREAM_API_KEY;
const botUserId = process.env.BOT_USER_ID;
const userId = process.env.TEST_USER_ID;
const userToken = process.env.TEST_USER_TOKEN;

if (!apiKey || !botUserId || !userId || !userToken) {
  console.error(
    "Error: STREAM_API_KEY, BOT_USER_ID, TEST_USER_ID, and TEST_USER_TOKEN must be set in .env",
  );
  process.exit(1);
}

const channelId = process.argv[2] || `channel-${Date.now()}`;
const channelType = "messaging";

const client = new StreamChat(apiKey, { allowServerSideConnect: true });
await client.connectUser({ id: userId }, userToken);

console.log(`Creating ${channelType}:${channelId} with members [${userId}, ${botUserId}]...`);

const channel = client.channel(channelType, channelId, {
  name: channelId,
  members: [userId, botUserId],
});
await channel.create();

await client.disconnectUser();

console.log(`Channel created: ${channelType}:${channelId}`);
process.exit(0);
