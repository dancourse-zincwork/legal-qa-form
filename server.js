const express = require("express");
const path = require("path");
const http = require("http");
const https = require("https");

const app = express();
const PORT = process.env.PORT || 3000;

const QUERY_URL = process.env.N8N_WEBHOOK_URL || "https://zincwork.app.n8n.cloud/webhook/legal-qa-test";
const INGEST_URL = process.env.N8N_INGEST_URL || "https://zincwork.app.n8n.cloud/webhook/legal-qa-ingest-test";
const QDRANT_URL = process.env.QDRANT_URL || "http://maglev.proxy.rlwy.net:26454";
const QDRANT_KEY = process.env.QDRANT_API_KEY || "";

app.use(express.json({ limit: "5mb" }));
app.use(express.static(path.join(__dirname, "public")));

function proxyRequest(targetUrl, payload, timeoutMs) {
  return new Promise((resolve, reject) => {
    const url = new URL(targetUrl);
    const transport = url.protocol === "https:" ? https : http;
    const body = JSON.stringify(payload);

    const options = {
      hostname: url.hostname,
      port: url.port || (url.protocol === "https:" ? 443 : 80),
      path: url.pathname + url.search,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body),
      },
      timeout: timeoutMs,
    };

    if (url.hostname.includes("rlwy.net") && QDRANT_KEY) {
      options.headers["api-key"] = QDRANT_KEY;
    }

    const req = transport.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          const parsed = JSON.parse(data);
          resolve(Array.isArray(parsed) ? parsed[0] : parsed);
        } catch {
          reject(new Error("Invalid response from upstream"));
        }
      });
    });

    req.on("error", reject);
    req.on("timeout", () => {
      req.destroy();
      reject(new Error(`Request timed out (${timeoutMs / 1000}s)`));
    });

    req.write(body);
    req.end();
  });
}

// Ask a question
app.post("/api/ask", async (req, res) => {
  const { question } = req.body;
  if (!question || question.trim().length < 5) {
    return res.status(400).json({ error: "Question too short" });
  }
  try {
    const result = await proxyRequest(QUERY_URL, {
      question: question.trim(),
      channel: "qa-form",
      user: "form-user",
    }, 300000);
    res.json(result);
  } catch (err) {
    console.error("Query error:", err.message);
    res.status(502).json({ error: err.message });
  }
});

// Ingest a document
app.post("/api/ingest", async (req, res) => {
  const { title, type, repo, content } = req.body;
  if (!title || !content) {
    return res.status(400).json({ error: "Title and content required" });
  }
  try {
    const result = await proxyRequest(INGEST_URL, {
      title: title.trim(),
      document_type: type || "policy",
      repo: repo || "general",
      content: content.trim(),
      source_url: `form://${repo || "general"}/${title.trim()}`,
      source: "qa-form",
    }, 180000);
    res.json(result);
  } catch (err) {
    console.error("Ingest error:", err.message);
    res.status(502).json({ error: err.message });
  }
});

// Browse documents from Qdrant
app.get("/api/documents", async (req, res) => {
  try {
    const allDocs = [];
    let offset = null;
    const maxPages = 10;

    for (let page = 0; page < maxPages; page++) {
      const payload = {
        limit: 100,
        with_payload: true,
        with_vector: false,
      };
      if (offset) payload.offset = offset;

      const result = await proxyRequest(
        `${QDRANT_URL}/collections/legal_docs/points/scroll`,
        payload,
        15000
      );

      const points = result.points || result.result?.points || [];
      allDocs.push(...points);

      const nextOffset = result.next_page_offset || result.result?.next_page_offset;
      if (!nextOffset || points.length === 0) break;
      offset = nextOffset;
    }

    // Group by repo
    const repos = {};
    for (const doc of allDocs) {
      const p = doc.payload || {};
      const sourceUrl = p.source_url || "";
      let repo = "ungrouped";
      if (sourceUrl.startsWith("form://")) {
        const parts = sourceUrl.replace("form://", "").split("/");
        repo = parts[0] || "general";
      } else if (sourceUrl.startsWith("gdrive://")) {
        repo = "google-drive";
      } else if (p.repo) {
        repo = p.repo;
      }

      if (!repos[repo]) repos[repo] = [];
      repos[repo].push({
        id: doc.id,
        title: p.title || "Untitled",
        type: p.document_type || "unknown",
        jurisdictions: p.jurisdictions || [],
        topics: p.topics || [],
        chunk_index: p.chunk_index,
        total_chunks: p.total_chunks,
      });
    }

    res.json({ repos, total_chunks: allDocs.length });
  } catch (err) {
    console.error("Documents error:", err.message);
    res.status(502).json({ error: err.message });
  }
});

app.get("/health", (_req, res) => res.json({ status: "ok" }));

app.listen(PORT, () => {
  console.log(`Legal QA Form running on port ${PORT}`);
});
