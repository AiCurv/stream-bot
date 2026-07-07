// services/playlist.js — Batch aggregator pipeline
// Handles multi-link batch processing: magnet arrays and direct video URL arrays
// Streams each item to Pixeldrain zero-disk, then compiles an Extended M3U8

import { execSync } from "child_process";
import { Readable } from "stream";
import parseTorrent from "parse-torrent";
import WebTorrent from "webtorrent";
import { streamToPixeldrain, getPixeldrainStreamUrl } from "./pixeldrain.js";

const BATCH_ITEM_TIMEOUT_MS = 600000;

/**
 * Process a batch of links and compile an M3U8 playlist.
 * @param {string[]} links - Array of magnet URIs or direct video URLs
 * @param {string} batchType - "BATCH_MAGNET" or "BATCH_VIDEO"
 * @param {number} chatId - Telegram chat ID
 */
export async function processBatch(links, batchType, chatId) {
  const entries = [];

  await sendTelegram("sendMessage", {
    chat_id: chatId,
    text: "Batch pipeline started. Processing " + links.length + " item(s)..."
  });

  for (let i = 0; i < links.length; i++) {
    const link = links[i];

    try {
      await sendTelegram("sendMessage", {
        chat_id: chatId,
        text: "[" + (i + 1) + "/" + links.length + "] Processing..."
      });

      let fileName, streamUrl;

      if (batchType === "BATCH_VIDEO") {
        // Direct video URL: fetch and stream to Pixeldrain
        fileName = extractFileName(link) || ("video_" + (i + 1) + ".mp4");
        const resp = await fetch(link);
        if (!resp.ok || !resp.body) throw new Error("HTTP " + resp.status);
        streamUrl = await streamToPixeldrain(resp.body, fileName);
      } else {
        // Magnet link: resolve torrent, stream selected/all files
        const result = await streamMagnetToPixeldrain(link, i, chatId);
        fileName = result.name;
        streamUrl = result.url;
      }

      entries.push({ name: fileName, url: streamUrl });

      await sendTelegram("sendMessage", {
        chat_id: chatId,
        text: "[" + (i + 1) + "/" + links.length + "] Done: " + fileName
      });
    } catch (err) {
      await sendTelegram("sendMessage", {
        chat_id: chatId,
        text: "[" + (i + 1) + "/" + links.length + "] FAILED: " + err.message
      });
    }
  }

  if (!entries.length) {
    await sendTelegram("sendMessage", {
      chat_id: chatId,
      text: "Batch complete. No items were successfully processed."
    });
    return;
  }

  // Build Extended M3U8
  let m3u = "#EXTM3U\n";
  for (const entry of entries) {
    m3u += "#EXTINF:-1," + entry.name + "\n";
    m3u += entry.url + "\n";
  }

  // Upload M3U8 to Pixeldrain
  const m3uStream = Readable.from([m3u]);
  const playlistUrl = await streamToPixeldrain(m3uStream, "batch_playlist.m3u8");

  // Send playlist as document to Telegram
  const buf = Buffer.from(m3u, "utf-8");
  const boundary = "----FormBoundary" + Math.random().toString(36).slice(2);
  const bodyParts = [];
  bodyParts.push(
    "--" + boundary,
    'Content-Disposition: form-data; name="document"; filename="batch_playlist.m3u8"',
    "Content-Type: audio/x-mpegurl",
    "",
    m3u,
    "--" + boundary,
    'Content-Disposition: form-data; name="chat_id"',
    "",
    String(chatId),
    "--" + boundary,
    'Content-Disposition: form-data; name="caption"',
    "",
    "Playlist ready! " + entries.length + "/" + links.length + " items processed.\n\nM3U8 URL: " + playlistUrl,
    "--" + boundary + "--"
  );
  const formData = bodyParts.join("\r\n");

  const tgUrl = "https://api.telegram.org/bot" + process.env.TELEGRAM_BOT_TOKEN + "/sendDocument";
  await fetch(tgUrl, {
    method: "POST",
    headers: { "Content-Type": "multipart/form-data; boundary=" + boundary },
    body: formData
  });
}

/**
 * Resolve a single magnet, stream the first (or only) video file to Pixeldrain.
 * Tries torrent cache first for faster resolution on restricted networks.
 * Returns { name, url }.
 */
async function streamMagnetToPixeldrain(magnet, index, chatId) {
  const hash = (magnet.match(/btih:([A-Fa-f0-9]{40})/i) || [])[1];
  let torrentInput = magnet;

  // Try torrent cache first
  if (hash) {
    try {
      for (const tpl of ["http://itorrents.org/torrent/{hash}.torrent"]) {
        const url = tpl.replace("{hash}", hash);
        const ac = new AbortController();
        const tm = setTimeout(() => ac.abort(), 10000);
        try {
          const resp = await fetch(url, { redirect: "follow", signal: ac.signal });
          clearTimeout(tm);
          if (resp.ok) {
            const buf = Buffer.from(await resp.arrayBuffer());
            if (buf.length > 0 && buf[0] === 0x64) {
              console.log("[batch] Cache hit for item", index + 1);
              torrentInput = buf;
              break;
            }
          }
        } catch (_) {}
          }
        }
      }
    } catch (_) {}
  }

  return new Promise((resolve, reject) => {
    let settled = false;
    const client = new WebTorrent();

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      client.destroy();
      reject(new Error("Magnet [" + (index + 1) + "] timed out"));
    }, BATCH_ITEM_TIMEOUT_MS);

    client.add(torrentInput, { announce: getTrackers() }, async (torrent) => {
      if (settled) { client.destroy(); return; }
      try {
        await new Promise((res) => torrent.once("ready", res));
        if (settled) { client.destroy(); return; }

        const file = pickVideoFile(torrent.files) || torrent.files[0];
        if (!file) {
          clearTimeout(timer);
          settled = true;
          client.destroy();
          reject(new Error("No files in torrent"));
          return;
        }

        const readStream = file.createReadStream();
        const url = await streamToPixeldrain(readStream, file.name);

        clearTimeout(timer);
        settled = true;
        client.destroy();
        resolve({ name: file.name, url });
      } catch (err) {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          client.destroy();
          reject(err);
        }
      }
    });

    client.on("error", (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      client.destroy();
      reject(err);
    });
  });
}

function pickVideoFile(files) {
  const videoExts = [".mp4", ".mkv", ".webm", ".avi", ".mov", ".flv", ".wmv"];
  let best = null;
  let bestSize = 0;
  for (const f of files) {
    const ext = f.name.substring(f.name.lastIndexOf(".")).toLowerCase();
    if (videoExts.includes(ext) && f.length > bestSize) {
      best = f;
      bestSize = f.length;
    }
  }
  return best;
}

function extractFileName(url) {
  try {
    const pathname = new URL(url).pathname;
    const parts = pathname.split("/");
    return parts[parts.length - 1] || null;
  } catch (_) {
    return null;
  }
}

async function sendTelegram(method, payload) {
  const url = "https://api.telegram.org/bot" + process.env.TELEGRAM_BOT_TOKEN + "/" + method;
  const body = JSON.stringify(payload);
  try {
    const result = execSync(
      'curl -s -X POST -H "Content-Type: application/json" -d \'' + body.replace(/'/g, "'\\''") + '\' "' + url + '"',
      { timeout: 15000, encoding: "utf-8" }
    );
    const data = JSON.parse(result);
    if (data.ok) {
      console.log("[telegram]", method, "ok");
    } else {
      console.error("[telegram] API error:", data.error_code, data.description);
    }
    return data;
  } catch (e) {
    console.error("[telegram] curl failed:", e.message.slice(0, 200));
    return { ok: false, error: e.message };
  }
}

function getTrackers() {
  return [
    "wss://tracker.openwebtorrent.com",
    "wss://tracker.btorrent.xyz",
    "wss://tracker.fastcast.nz",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.dler.org:6969/announce",
    "udp://tracker-udp.gbitt.info:80/announce",
    "udp://public.popcorn-tracker.org:6969/announce",
    "udp://opentracker.i2p.rocks:6969/announce",
    "udp://retracker.lanta-net.ru:2710/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://tracker.pirateparty.gr:6969/announce",
    "udp://tracker.coppersurfer.tk:6969/announce",
    "udp://p4p.arenabg.com:1337/announce",
    "http://tracker.openbittorrent.com:80/announce",
    "http://openbittorrent.com:80/announce",
    "http://tracker.openbittorrent.com/announce",
    "http://tracker1.bt7z.com:8080/announce",
    "http://tracker.torrent.eu.org/announce"
  ];
}