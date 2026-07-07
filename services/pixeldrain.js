// services/pixeldrain.js — Pixeldrain storage driver
// Streams binary data directly to Pixeldrain via HTTP PUT (zero-disk)
// Requires PIXELDRAIN_API_KEY environment variable

/**
 * Upload a readable stream to Pixeldrain and return the direct file URL.
 * Uses HTTP PUT with streaming — no temp files on disk.
 *
 * @param {import("stream").Readable} readStream - The file read stream (e.g. from webtorrent)
 * @param {string} fileName - Name of the file (will be URI-encoded)
 * @returns {Promise<string>} The direct Pixeldrain URL for the uploaded file
 */
export async function streamToPixeldrain(readStream, fileName) {
  const apiKey = process.env.PIXELDRAIN_API_KEY;
  const encodedName = encodeURIComponent(fileName);
  const uploadUrl = "https://pixeldrain.com/api/file/" + encodedName;

  const response = await fetch(uploadUrl, {
    method: "PUT",
    headers: {
      "Content-Type": "application/octet-stream",
      "Authorization": "Bearer " + apiKey
    },
    body: readStream,
    duplex: "half" // Required for streaming request bodies in Node.js fetch
  });

  if (!response.ok) {
    const errText = await response.text();
    throw new Error("Pixeldrain upload failed (" + response.status + "): " + errText);
  }

  const result = await response.json();
  const fileId = result.id || result.name;
  return "https://pixeldrain.com/api/file/" + fileId;
}

/**
 * Get the streaming playback URL (HTTP 206 Partial Content) for a file on Pixeldrain.
 *
 * @param {string} fileId - The Pixeldrain file ID
 * @returns {string} Direct streaming URL
 */
export function getPixeldrainStreamUrl(fileId) {
  return "https://pixeldrain.com/api/file/" + fileId;
}