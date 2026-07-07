// api/webhook.js — Vercel Serverless Function
// Security: ALLOWED_USER_ID check at absolute entry point
// Secrets are loaded from environment variables (set in Vercel dashboard)

export default async function handler(req, res) {
  try {
    const ALLOWED_USER_ID = process.env.ALLOWED_USER_ID;
    const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
    const GITHUB_TOKEN = process.env.PAT_TOKEN;
    const GITHUB_USERNAME = process.env.GITHUB_USERNAME || "aicurv";
    const GITHUB_REPO = process.env.GITHUB_REPO_NAME || "stream-bot";

    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;

    // --- SECURITY GATE: extract user ID from message or callback_query ---
    const userId = (body.message && body.message.from && String(body.message.from.id))
                || (body.callback_query && body.callback_query.from && String(body.callback_query.from.id));

    if (!userId || userId !== ALLOWED_USER_ID) {
      return res.status(403).send("Forbidden");
    }

    // --- ROUTING ---
    if (body.message) {
      const text = (body.message.text || "").trim();
      const chatId = body.message.chat.id;

      if (text.startsWith("/start")) {
        await sendTelegram(BOT_TOKEN, "sendMessage", {
          chat_id: chatId,
          text: "Stream Bot is online.\n\nSend me a magnet link or .torrent file to begin.\nUse /playlist <magnet> for an M3U playlist."
        });
      } else if (text.startsWith("/playlist ")) {
        const magnet = text.slice(9).trim();
        if (!magnet) {
          return await sendTelegram(BOT_TOKEN, "sendMessage", { chat_id: chatId, text: "Usage: /playlist <magnet_uri>" });
        }
        await dispatchGitHub(GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO, "stream_process", {
          magnet,
          chat_id: chatId,
          mode: "playlist",
          callback: "playlist_ready"
        });
        await sendTelegram(BOT_TOKEN, "sendMessage", { chat_id: chatId, text: "Playlist build dispatched. Processing..." });
      } else if (text.startsWith("magnet:") || text.includes(".torrent")) {
        await dispatchGitHub(GITHUB_TOKEN, GITHUB_USERNAME, GITHUB_REPO, "metadata_scrape", { magnet: text, chat_id: chatId });
        await sendTelegram(BOT_TOKEN, "sendMessage", { chat_id: chatId, text: "Metadata scrape dispatched. Fetching file tree..." });
      } else {
        await sendTelegram(BOT_TOKEN, "sendMessage", { chat_id: chatId, text: "Send a magnet link to get started." });
      }
    }

    // --- CALLBACK QUERY (inline keyboard press) ---
    if (body.callback_query) {
      const data = body.callback_query.data || "";
      const chatId = body.callback_query.message.chat.id;

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
          mode: "single",
          callback: "stream_ready"
        });
      }
    }

    return res.status(200).send("OK");
  } catch (err) {
    console.error("Webhook error:", err);
    return res.status(500).send("Internal Server Error");
  }
}

// --- HELPERS ---

async function sendTelegram(botToken, method, payload) {
  // Exact string concatenation for Telegram API endpoint (avoids URL parsing bugs)
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
    body: JSON.stringify({
      event_type: action,
      client_payload: payload
    })
  });
}