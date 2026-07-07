// api/webhook.js — Vercel Serverless Function
// Security: ALLOWED_USER_ID check at absolute entry point
// Secrets loaded from environment variables
// Stateful in-memory session cache for batch processing

// --- VOLATILE SESSION CACHE ---
const sessions = {};

function getSession(chatId) {
  if (!sessions[chatId]) {
    sessions[chatId] = { state: "IDLE", items: [] };
  }
  return sessions[chatId];
}

function clearSession(chatId) {
  delete sessions[chatId];
}

export default async function handler(req, res) {
  try {
    const ALLOWED_USER_ID = process.env.ALLOWED_USER_ID;
    const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
    const GITHUB_TOKEN = process.env.PAT_TOKEN;
    const GITHUB_USERNAME = process.env.GITHUB_USERNAME || "aicurv";
    const GITHUB_REPO = process.env.GITHUB_REPO_NAME || "stream-bot";

    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;

    // --- SECURITY GATE ---
    const userId = (body.message && body.message.from && String(body.message.from.id))
                || (body.callback_query && body.callback_query.from && String(body.callback_query.from.id));

    if (!userId || userId !== ALLOWED_USER_ID) {
      return res.status(403).send("Forbidden");
    }

    // --- CALLBACK QUERY ---
    if (body.callback_query) {
      const data = body.callback_query.data || "";
      const chatId = body.callback_query.message.chat.id;

      await sendTelegram(BOT_TOKEN, "answerCallbackQuery", {
        callback_query_id: body.callback_query.id
      });

      // Finish & Compile Playlist button
      if (data === "FINISH_BATCH") {
        const session = getSession(chatId);
        const items = session.items;
        const batchType = session.state;

        if (!items.length) {
          await sendTelegram(BOT_TOKEN, "sendMessage", {
            chat_id: chatId,
            text: "No links collected. Batch cancelled."
          });
          clearSession(chatId);
          return res.status(200).send("OK");
        }

        clearSession(chatId);

        await sendTelegram(BOT_TOKEN, "sendMessage", {
          chat_id: chatId,
          text: "Compiling batch playlist with " + items.length + " item(s)... Dispatching to runner."
        });

        await dispatchGitHub(GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO, "batch_process", {
          links: items,
          batch_type: batchType,
          chat_id: chatId
        });
        return res.status(200).send("OK");
      }

      // Cancel batch
      if (data === "CANCEL_BATCH") {
        const chatId2 = body.callback_query.message.chat.id;
        clearSession(chatId2);
        await sendTelegram(BOT_TOKEN, "sendMessage", {
          chat_id: chatId2,
          text: "Batch cancelled."
        });
        return res.status(200).send("OK");
      }

      // Stream file selection from inline keyboard
      if (data.startsWith("STREAM:")) {
        const parts = data.split(":");
        const magnetHash = parts[1];
        const fileIndex = parseInt(parts[2], 10);
        const host = parts[3] || "pixeldrain";

        await sendTelegram(BOT_TOKEN, "answerCallbackQuery", {
          callback_query_id: body.callback_query.id,
          text: "Streaming dispatch started..."
        });

        await dispatchGitHub(GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO, "stream_process", {
          magnet: "magnet:?xt=urn:btih:" + magnetHash,
          file_index: fileIndex,
          chat_id: chatId,
          host: host,
          mode: "single"
        });
        return res.status(200).send("OK");
      }

      // Build full torrent playlist
      if (data.startsWith("BUILD_PLAYLIST:")) {
        const parts = data.split(":");
        const magnetHash = parts[1];
        const host = parts[2] || "pixeldrain";

        await sendTelegram(BOT_TOKEN, "answerCallbackQuery", {
          callback_query_id: body.callback_query.id,
          text: "Playlist dispatch started..."
        });

        await dispatchGitHub(GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO, "stream_process", {
          magnet: "magnet:?xt=urn:btih:" + magnetHash,
          chat_id: chatId,
          host: host,
          mode: "playlist"
        });
        return res.status(200).send("OK");
      }

      return res.status(200).send("OK");
    }

    // --- MESSAGE HANDLING ---
    if (body.message) {
      const text = (body.message.text || "").trim();
      const chatId = body.message.chat.id;
      const messageId = body.message.message_id;
      const session = getSession(chatId);

      // /start
      if (text === "/start") {
        await sendTelegram(BOT_TOKEN, "sendMessage", {
          chat_id: chatId,
          parse_mode: "HTML",
          text: "Stream Bot is online.\n\n" +
                "Send a <b>magnet link</b> for single-file streaming.\n" +
                "/magnet_playlist — Batch magnet links into one M3U playlist.\n" +
                "/video_links — Batch direct video URLs (MP4/MKV) into one M3U playlist."
        });
        return res.status(200).send("OK");
      }

      // /magnet_playlist — enter batch magnet mode
      if (text === "/magnet_playlist") {
        session.state = "BATCH_MAGNET";
        session.items = [];
        await sendTelegram(BOT_TOKEN, "sendMessage", {
          chat_id: chatId,
          text: "Batch Magnet Mode active. Send your magnet links one by one. Tap 'Finish' when done.",
          reply_markup: {
            inline_keyboard: [
              [{ text: "Finish & Compile Playlist", callback_data: "FINISH_BATCH" }],
              [{ text: "Cancel", callback_data: "CANCEL_BATCH" }]
            ]
          }
        });
        return res.status(200).send("OK");
      }

      // /video_links — enter batch video URL mode
      if (text === "/video_links") {
        session.state = "BATCH_VIDEO";
        session.items = [];
        await sendTelegram(BOT_TOKEN, "sendMessage", {
          chat_id: chatId,
          text: "Batch Video Mode active. Send direct video URLs (MP4/MKV) one by one. Tap 'Finish' when done.",
          reply_markup: {
            inline_keyboard: [
              [{ text: "Finish & Compile Playlist", callback_data: "FINISH_BATCH" }],
              [{ text: "Cancel", callback_data: "CANCEL_BATCH" }]
            ]
          }
        });
        return res.status(200).send("OK");
      }

      // --- BATCH MODE: intercept all text, validate, collect ---
      if (session.state === "BATCH_MAGNET") {
        if (text.startsWith("magnet:") || text.includes(".torrent")) {
          session.items.push(text);
          // Delete user's message to keep chat clean
          await sendTelegram(BOT_TOKEN, "deleteMessage", { chat_id: chatId, message_id: messageId }).catch(() => {});
          await sendTelegram(BOT_TOKEN, "sendMessage", {
            chat_id: chatId,
            text: "Added magnet [" + session.items.length + "]. Send more or tap Finish."
          });
        } else {
          await sendTelegram(BOT_TOKEN, "sendMessage", {
            chat_id: chatId,
            text: "Please send a valid magnet link. Ignored: " + (text.length > 40 ? text.slice(0, 40) + "..." : text)
          });
        }
        return res.status(200).send("OK");
      }

      if (session.state === "BATCH_VIDEO") {
        const validVideo = /\.(mp4|mkv|webm|avi|mov|flv)(\?|$)/i.test(text) || text.startsWith("http");
        if (validVideo) {
          session.items.push(text);
          await sendTelegram(BOT_TOKEN, "deleteMessage", { chat_id: chatId, message_id: messageId }).catch(() => {});
          await sendTelegram(BOT_TOKEN, "sendMessage", {
            chat_id: chatId,
            text: "Added video link [" + session.items.length + "]. Send more or tap Finish."
          });
        } else {
          await sendTelegram(BOT_TOKEN, "sendMessage", {
            chat_id: chatId,
            text: "Please send a valid video URL (MP4/MKV/WebM). Ignored."
          });
        }
        return res.status(200).send("OK");
      }

      // --- SINGLE MODE: direct magnet link ---
      if (text.startsWith("magnet:") || text.includes(".torrent")) {
        await dispatchGitHub(GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO, "metadata_scrape", {
          magnet: text,
          chat_id: chatId
        });
        await sendTelegram(BOT_TOKEN, "sendMessage", {
          chat_id: chatId,
          text: "Metadata scrape dispatched. Fetching file tree..."
        });
        return res.status(200).send("OK");
      }

      // Fallback
      await sendTelegram(BOT_TOKEN, "sendMessage", {
        chat_id: chatId,
        text: "Send a magnet link or use /magnet_playlist or /video_links for batch mode."
      });
      return res.status(200).send("OK");
    }

    return res.status(200).send("OK");
  } catch (err) {
    console.error("Webhook error:", err);
    return res.status(500).send("Internal Server Error");
  }
}

// --- HELPERS ---

async function sendTelegram(botToken, method, payload) {
  const url = "https://api.telegram.org/bot" + botToken + "/" + method;
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  return response.json();
}

async function dispatchGitHub(githubToken, username, repo, action, payload) {
  const url = "https://api.github.com/repos/" + username + "/" + repo + "/dispatches";
  await fetch(url, {
    method: "POST",
    headers: {
      "Authorization": "Bearer " + githubToken,
      "Accept": "application/vnd.github.v3+json",
      "Content-Type": "application/json",
      "X-GitHub-Api-Version": "2022-11-28"
    },
    body: JSON.stringify({ event_type: action, client_payload: payload })
  });
}