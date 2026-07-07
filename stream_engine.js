// stream_engine.js — Zero-disk torrent streaming engine
// Runs on GitHub Actions ubuntu-latest (Node.js v20)
// Uses webtorrent to create in-memory read streams piped directly to Pixeldrain
// 45-second metadata timeout with graceful exit (code 0)

import WebTorrent from "webtorrent";
import { streamToPixeldrain, getPixeldrainStreamUrl } from "./services/pixeldrain.js";

const METADATA_TIMEOUT_MS = 45000;
const STREAM_TIMEOUT_MS = 600000;
const PLAYLIST_TIMEOUT_MS = 1800000;

/**
 * Scrape metadata from a magnet/torrent and reply with an inline keyboard.
 * 45-second hard timeout — notifies user gracefully on DHT failure, exits 0.
 */
export async function scrapeMetadata(magnet, chatId) {
  let settled = false;

  return new Promise((resolve) => {
    const client = new WebTorrent();

    const timer = setTimeout(async () => {
      if (settled) return;
      settled = true;
      client.destroy();
      try {
        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Torrent resolution timed out. No active seeders found on DHT network."
        });
      } catch (_) {}
      resolve();
    }, METADATA_TIMEOUT_MS);

    client.add(magnet, { announce: getTrackers() }, async (torrent) => {
      if (settled) { client.destroy(); return; }
      try {
        await new Promise((res) => torrent.once("ready", res));
        if (settled) { client.destroy(); return; }

        clearTimeout(timer);

        const files = torrent.files.map((f, idx) => ({
          name: f.name,
          length: f.length,
          index: idx
        }));

        const inlineKeyboard = files.slice(0, 30).map((f) => [{
          text: formatFileSize(f.length) + " \u2014 " + f.name,
          callback_data: "STREAM:" + torrent.infoHash + ":" + f.index + ":pixeldrain"
        }]);

        if (files.length > 1) {
          inlineKeyboard.push([{
            text: "Build Full Playlist (M3U)",
            callback_data: "BUILD_PLAYLIST:" + torrent.infoHash + ":pixeldrain"
          }]);
        }

        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Found " + files.length + " file(s) in torrent:\n" +
                "Name: " + torrent.name + "\n" +
                "Size: " + formatFileSize(torrent.length) + "\n\n" +
                "Select a file to stream:",
          reply_markup: { inline_keyboard: inlineKeyboard }
        });

        settled = true;
        client.destroy();
        resolve();
      } catch (err) {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          client.destroy();
          try {
            await sendTelegram("sendMessage", {
              chat_id: chatId,
              text: "Error reading torrent metadata: " + err.message
            });
          } catch (_) {}
        }
        resolve();
      }
    });

    client.on("error", async (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      client.destroy();
      try {
        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Torrent error: " + err.message
        });
      } catch (_) {}
      resolve();
    });
  });
}

/**
 * Stream a specific file from a torrent directly to Pixeldrain (zero-disk).
 */
export async function processStream(magnet, fileIndex, chatId, host = "pixeldrain") {
  let settled = false;

  return new Promise((resolve) => {
    const client = new WebTorrent();

    const timer = setTimeout(async () => {
      if (settled) return;
      settled = true;
      client.destroy();
      try {
        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Stream process timed out after 10 minutes."
        });
      } catch (_) {}
      resolve();
    }, STREAM_TIMEOUT_MS);

    client.add(magnet, { announce: getTrackers() }, async (torrent) => {
      if (settled) { client.destroy(); return; }
      try {
        await new Promise((res) => torrent.once("ready", res));
        if (settled) { client.destroy(); return; }

        const file = torrent.files[fileIndex];
        if (!file) {
          clearTimeout(timer);
          settled = true;
          client.destroy();
          await sendTelegram("sendMessage", { chat_id: chatId, text: "File index " + fileIndex + " not found." });
          resolve();
          return;
        }

        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Streaming: " + file.name + "\nSize: " + formatFileSize(file.length) + "\n\nConnecting to peers..."
        });

        // ZERO DISK: readStream -> HTTP PUT body
        const readStream = file.createReadStream();
        const directUrl = await streamToPixeldrain(readStream, file.name);

        clearTimeout(timer);
        settled = true;
        client.destroy();

        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Stream ready!\n\nFile: " + file.name + "\nDirect URL:\n" + directUrl + "\n\nStreaming URL (206 Partial Content):\n" + getPixeldrainStreamUrl(file.name)
        });
        resolve();
      } catch (err) {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          client.destroy();
          try {
            await sendTelegram("sendMessage", {
              chat_id: chatId,
              text: "Stream error: " + err.message
            });
          } catch (_) {}
        }
        resolve();
      }
    });

    client.on("error", async (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      client.destroy();
      try {
        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Torrent stream error: " + err.message
        });
      } catch (_) {}
      resolve();
    });
  });
}

/**
 * Build an Extended M3U playlist by uploading all files sequentially from a single torrent.
 */
export async function buildPlaylist(magnet, chatId) {
  let settled = false;

  return new Promise((resolve) => {
    const client = new WebTorrent();

    const timer = setTimeout(async () => {
      if (settled) return;
      settled = true;
      client.destroy();
      try {
        await sendTelegram("sendMessage", { chat_id: chatId, text: "Playlist build timed out." });
      } catch (_) {}
      resolve();
    }, PLAYLIST_TIMEOUT_MS);

    client.add(magnet, { announce: getTrackers() }, async (torrent) => {
      if (settled) { client.destroy(); return; }
      try {
        await new Promise((res) => torrent.once("ready", res));
        if (settled) { client.destroy(); return; }

        const files = torrent.files;
        const urls = [];

        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Building playlist for " + files.length + " file(s)..."
        });

        for (let i = 0; i < files.length; i++) {
          if (settled) break;
          await sendTelegram("sendMessage", {
            chat_id: chatId,
            text: "Uploading [" + (i + 1) + "/" + files.length + "]: " + files[i].name
          });
          const readStream = files[i].createReadStream();
          const directUrl = await streamToPixeldrain(readStream, files[i].name);
          const streamUrl = getPixeldrainStreamUrl(files[i].name);
          urls.push({ name: files[i].name, url: streamUrl, length: files[i].length });
        }

        if (settled) { client.destroy(); return; }

        let m3u = "#EXTM3U\n";
        for (const entry of urls) {
          m3u += "#EXTINF:-1," + entry.name + "\n";
          m3u += entry.url + "\n";
        }

        const { Readable } = await import("stream");
        const m3uStream = Readable.from([m3u]);
        const playlistUrl = await streamToPixeldrain(m3uStream, torrent.name + ".m3u");

        clearTimeout(timer);
        settled = true;
        client.destroy();

        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Playlist ready!\n\nTorrent: " + torrent.name + "\nFiles: " + urls.length + "\n\nM3U Playlist URL:\n" + playlistUrl
        });
        resolve();
      } catch (err) {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          client.destroy();
          try {
            await sendTelegram("sendMessage", { chat_id: chatId, text: "Playlist error: " + err.message });
          } catch (_) {}
        }
        resolve();
      }
    });

    client.on("error", async (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      client.destroy();
      try {
        await sendTelegram("sendMessage", { chat_id: chatId, text: "Playlist torrent error: " + err.message });
      } catch (_) {}
      resolve();
    });
  });
}

// --- HELPERS ---

async function sendTelegram(method, payload) {
  const url = "https://api.telegram.org/bot" + process.env.TELEGRAM_BOT_TOKEN + "/" + method;
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const data = await response.json();
  if (!data.ok) {
    console.error("[telegram] API error:", data.error_code, data.description);
  } else {
    console.log("[telegram]", method, "ok");
  }
  return data;
}

function formatFileSize(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + " KB";
  if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + " MB";
  return (bytes / 1073741824).toFixed(2) + " GB";
}

function getTrackers() {
  return [
    // WSS trackers (most reliable on CI/GitHub Actions)
    "wss://tracker.openwebtorrent.com",
    "wss://tracker.btorrent.xyz",
    "wss://tracker.fastcast.nz",
    // High-reliability UDP trackers
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
    // HTTP trackers (fallback, work everywhere)
    "http://tracker.openbittorrent.com:80/announce",
    "http://openbittorrent.com:80/announce",
    "http://tracker.openbittorrent.com/announce",
    "http://tracker1.bt7z.com:8080/announce",
    "http://tracker.torrent.eu.org/announce"
  ];
}