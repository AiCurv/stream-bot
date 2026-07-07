// stream_engine.js — Zero-disk torrent streaming engine
// Runs on GitHub Actions ubuntu-latest (Node.js v20)
// Uses webtorrent to create in-memory read streams piped directly to Pixeldrain
// Requires: TELEGRAM_BOT_TOKEN, PIXELDRAIN_API_KEY environment variables

import WebTorrent from "webtorrent";
import { streamToPixeldrain, getPixeldrainStreamUrl } from "./services/pixeldrain.js";

/**
 * Scrape metadata from a magnet/torrent and reply with an inline keyboard.
 */
export async function scrapeMetadata(magnet, chatId) {
  return new Promise((resolve, reject) => {
    const client = new WebTorrent();
    client.add(magnet, { announce: getTrackers() }, async (torrent) => {
      try {
        await new Promise((res) => torrent.once("ready", res));

        const files = torrent.files.map((f, idx) => ({
          name: f.name,
          length: f.length,
          index: idx
        }));

        const inlineKeyboard = files.slice(0, 30).map((f) => [{
          text: formatFileSize(f.length) + " — " + f.name,
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

        client.destroy();
        resolve();
      } catch (err) {
        client.destroy();
        reject(err);
      }
    });

    setTimeout(() => {
      client.destroy();
      reject(new Error("Metadata scrape timed out"));
    }, 60000);
  });
}

/**
 * Stream a specific file from a torrent directly to Pixeldrain (zero-disk).
 * Creates a readStream from webtorrent and pipes it to the upload stream.
 */
export async function processStream(magnet, fileIndex, chatId, host = "pixeldrain") {
  return new Promise((resolve, reject) => {
    const client = new WebTorrent();
    client.add(magnet, { announce: getTrackers() }, async (torrent) => {
      try {
        await new Promise((res) => torrent.once("ready", res));

        const file = torrent.files[fileIndex];
        if (!file) {
          await sendTelegram("sendMessage", { chat_id: chatId, text: "File index " + fileIndex + " not found." });
          client.destroy();
          return resolve();
        }

        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Streaming: " + file.name + "\nSize: " + formatFileSize(file.length) + "\n\nConnecting to peers..."
        });

        // ZERO DISK: readStream -> HTTP PUT body (never touches disk)
        const readStream = file.createReadStream();
        const directUrl = await streamToPixeldrain(readStream, file.name);

        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Stream ready!\n\nFile: " + file.name + "\nDirect URL:\n" + directUrl + "\n\nStreaming URL (206 Partial Content):\n" + getPixeldrainStreamUrl(file.name)
        });

        client.destroy();
        resolve();
      } catch (err) {
        client.destroy();
        reject(err);
      }
    });

    setTimeout(() => {
      client.destroy();
      reject(new Error("Stream process timed out"));
    }, 600000);
  });
}

/**
 * Build an Extended M3U playlist by uploading all files sequentially.
 */
export async function buildPlaylist(magnet, chatId) {
  return new Promise((resolve, reject) => {
    const client = new WebTorrent();
    client.add(magnet, { announce: getTrackers() }, async (torrent) => {
      try {
        await new Promise((res) => torrent.once("ready", res));

        const files = torrent.files;
        const urls = [];

        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Building playlist for " + files.length + " file(s)..."
        });

        for (let i = 0; i < files.length; i++) {
          await sendTelegram("sendMessage", {
            chat_id: chatId,
            text: "Uploading [" + (i + 1) + "/" + files.length + "]: " + files[i].name
          });

          // ZERO DISK per file: stream directly from torrent to Pixeldrain
          const readStream = files[i].createReadStream();
          const directUrl = await streamToPixeldrain(readStream, files[i].name);
          const streamUrl = getPixeldrainStreamUrl(files[i].name);
          urls.push({ name: files[i].name, url: streamUrl, length: files[i].length });
        }

        // Format valid Extended M3U
        let m3u = "#EXTM3U\n";
        for (const entry of urls) {
          const duration = Math.ceil(entry.length / (1024 * 1024 * 5));
          m3u += "#EXTINF:" + duration + "," + entry.name + "\n";
          m3u += entry.url + "\n";
        }

        // Upload the M3U file itself
        const { Readable } = await import("stream");
        const m3uStream = Readable.from([m3u]);
        const playlistUrl = await streamToPixeldrain(m3uStream, torrent.name + ".m3u");

        await sendTelegram("sendMessage", {
          chat_id: chatId,
          text: "Playlist ready!\n\nTorrent: " + torrent.name + "\nFiles: " + urls.length + "\n\nM3U Playlist URL:\n" + playlistUrl
        });

        client.destroy();
        resolve();
      } catch (err) {
        client.destroy();
        reject(err);
      }
    });

    setTimeout(() => {
      client.destroy();
      reject(new Error("Playlist build timed out"));
    }, 1800000);
  });
}

// --- HELPERS ---

async function sendTelegram(method, payload) {
  // Exact string concatenation for Telegram API endpoint
  const url = "https://api.telegram.org/bot" + process.env.TELEGRAM_BOT_TOKEN + "/" + method;
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  return response.json();
}

function formatFileSize(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + " KB";
  if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + " MB";
  return (bytes / 1073741824).toFixed(2) + " GB";
}

function getTrackers() {
  return [
    "wss://tracker.openwebtorrent.com",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://opentracker.i2p.rocks:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce"
  ];
}