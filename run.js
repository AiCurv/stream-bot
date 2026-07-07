// run.js — GitHub Actions entry point
// Reads payload from payload.json (written by workflow), avoids shell quoting bugs
// Usage: node run.js <action>
//   action = metadata_scrape | stream_process | batch_process

import { readFileSync } from "fs";
import { scrapeMetadata, processStream, buildPlaylist } from "./stream_engine.js";
import { processBatch } from "./services/playlist.js";

const action = process.argv[2];
const payloadPath = process.argv[3] || "payload.json";

let p;
try {
  p = JSON.parse(readFileSync(payloadPath, "utf-8"));
} catch (e) {
  console.error("Failed to read payload:", e.message);
  process.exit(0);
}

console.log("[run] Action:", action, "| Chat:", p.chat_id, "| Magnet:", (p.magnet || "").slice(0, 50) + "...");

try {
  switch (action) {
    case "metadata_scrape":
      console.log("[run] Starting metadata scrape...");
      await scrapeMetadata(p.magnet, p.chat_id);
      console.log("[run] Metadata scrape complete.");
      break;

    case "stream_process":
      console.log("[run] Starting stream process, mode:", p.mode);
      if (p.mode === "playlist") {
        await buildPlaylist(p.magnet, p.chat_id);
      } else {
        await processStream(p.magnet, p.file_index, p.chat_id, p.host);
      }
      console.log("[run] Stream process complete.");
      break;

    case "batch_process":
      console.log("[run] Starting batch process, items:", (p.links || []).length);
      await processBatch(p.links, p.batch_type, p.chat_id);
      console.log("[run] Batch process complete.");
      break;

    default:
        console.error("Unknown action:", action);
  }
} catch (e) {
  console.error("Runner error:", e.message);
}

// ALWAYS exit 0 — errors are reported to user via Telegram
process.exit(0);