const express = require("express");
const path = require("path");
const http = require("http");
const https = require("https");
const { Client } = require("pg");

const app = express();
const PORT = process.env.PORT || 3000;

const QUERY_URL = process.env.N8N_WEBHOOK_URL || "https://zincwork.app.n8n.cloud/webhook/legal-qa-test";
const INGEST_URL = process.env.N8N_INGEST_URL || "https://zincwork.app.n8n.cloud/webhook/legal-qa-ingest-test";
const QDRANT_URL = process.env.QDRANT_URL || "http://qdrant.railway.internal:6333";
const QDRANT_KEY = process.env.QDRANT_API_KEY || "";
const PG_URL = process.env.DATABASE_URL || "";

app.use(express.json({ limit: "5mb" }));
app.use(express.static(path.join(__dirname, "public")));

// ── PostgreSQL setup ──
let db = null;
async function getDb() {
  if (db) return db;
  if (!PG_URL) return null;
  db = new Client({ connectionString: PG_URL, ssl: { rejectUnauthorized: false } });
  await db.connect();
  // Create query_log table if not exists
  await db.query(`
    CREATE TABLE IF NOT EXISTS query_log (
      id SERIAL PRIMARY KEY,
      question TEXT NOT NULL,
      answer TEXT,
      confidence TEXT,
      verdict TEXT,
      quality_score REAL,
      category TEXT,
      complexity TEXT,
      citation_count INT DEFAULT 0,
      routing TEXT,
      processing_time_s REAL,
      feedback TEXT,
      source_versions JSONB DEFAULT '{}',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      stale BOOLEAN DEFAULT FALSE,
      stale_reason TEXT
    )
  `);
  return db;
}
getDb().catch(e => console.error("DB init:", e.message));

// ── HTTP proxy helper ──
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

    if (QDRANT_KEY) {
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

// ── Ask a question ──
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

    // Log to PostgreSQL
    try {
      const pg = await getDb();
      if (pg) {
        await pg.query(
          `INSERT INTO query_log (question, answer, confidence, verdict, quality_score, category, complexity, citation_count, routing, processing_time_s)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
          [
            question.trim(),
            result.answer || "",
            result.confidence || "MEDIUM",
            result.judge_verdict || "NEEDS_EDIT",
            result.judge_quality || 0,
            result.category || "general",
            result.complexity || "complex",
            result.citation_count || 0,
            result.routing || "human_review",
            parseFloat(result.processing_time_s) || 0,
          ]
        );
      }
    } catch (dbErr) {
      console.error("DB log error:", dbErr.message);
    }

    res.json(result);
  } catch (err) {
    console.error("Query error:", err.message);
    res.status(502).json({ error: err.message });
  }
});

// ── Ingest a document ──
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

    // Mark cached answers as potentially stale when KB changes
    try {
      const pg = await getDb();
      if (pg) {
        await pg.query(
          `UPDATE query_log SET stale = TRUE, stale_reason = $1 WHERE stale = FALSE`,
          [`KB updated: ${title.trim()} ingested at ${new Date().toISOString()}`]
        );
      }
    } catch (dbErr) {
      console.error("DB stale-mark error:", dbErr.message);
    }

    res.json(result);
  } catch (err) {
    console.error("Ingest error:", err.message);
    res.status(502).json({ error: err.message });
  }
});

// ── Feedback ──
app.post("/api/feedback", async (req, res) => {
  const { question, feedback } = req.body;
  try {
    const pg = await getDb();
    if (pg && question) {
      await pg.query(
        `UPDATE query_log SET feedback = $1 WHERE question = $2 AND feedback IS NULL ORDER BY created_at DESC LIMIT 1`,
        [feedback, question]
      );
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Memory (query log) ──
app.get("/api/memory", async (req, res) => {
  try {
    const pg = await getDb();
    if (!pg) {
      return res.json({ entries: [], total: 0, message: "Database not connected" });
    }
    const { rows } = await pg.query(
      `SELECT id, question, answer, confidence, verdict, quality_score, category, complexity,
              citation_count, routing, processing_time_s, feedback, stale, stale_reason, created_at
       FROM query_log ORDER BY created_at DESC LIMIT 100`
    );
    const stats = await pg.query(
      `SELECT COUNT(*) as total,
              COUNT(*) FILTER (WHERE stale = TRUE) as stale_count,
              COUNT(*) FILTER (WHERE feedback = 'up') as thumbs_up,
              COUNT(*) FILTER (WHERE feedback = 'down') as thumbs_down,
              AVG(quality_score) as avg_quality,
              AVG(processing_time_s) as avg_time
       FROM query_log`
    );
    res.json({
      entries: rows,
      stats: stats.rows[0],
      total: parseInt(stats.rows[0].total),
    });
  } catch (err) {
    console.error("Memory error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Browse documents from Qdrant ──
app.get("/api/documents", async (req, res) => {
  try {
    const allPoints = [];
    let offset = null;
    const maxPages = 20;

    for (let page = 0; page < maxPages; page++) {
      const payload = { limit: 100, with_payload: true, with_vector: false };
      if (offset) payload.offset = offset;

      const result = await proxyRequest(
        `${QDRANT_URL}/collections/legal_docs/points/scroll`,
        payload,
        15000
      );

      const points = result.points || result.result?.points || [];
      allPoints.push(...points);

      const nextOffset = result.next_page_offset || result.result?.next_page_offset;
      if (!nextOffset || points.length === 0) break;
      offset = nextOffset;
    }

    // Group by title to count chunks per document
    const docMap = {};
    for (const pt of allPoints) {
      const p = pt.payload || {};
      const title = p.title || "Untitled";
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

      const key = `${repo}::${title}`;
      if (!docMap[key]) {
        docMap[key] = {
          title,
          repo,
          document_type: p.document_type || "unknown",
          jurisdictions: p.jurisdictions || [],
          topics: p.topics || [],
          chunks: 0,
        };
      }
      docMap[key].chunks++;
    }

    // Build repos array and documents array
    const repoMap = {};
    const documents = [];
    for (const doc of Object.values(docMap)) {
      if (!repoMap[doc.repo]) {
        repoMap[doc.repo] = { name: doc.repo, doc_count: 0, total_chunks: 0 };
      }
      repoMap[doc.repo].doc_count++;
      repoMap[doc.repo].total_chunks += doc.chunks;
      documents.push(doc);
    }

    const repos = Object.values(repoMap).sort((a, b) => b.doc_count - a.doc_count);

    res.json({
      repos,
      documents,
      total_documents: documents.length,
      total_chunks: allPoints.length,
    });
  } catch (err) {
    console.error("Documents error:", err.message);
    res.status(502).json({ error: err.message });
  }
});

app.get("/health", (_req, res) => res.json({ status: "ok" }));

app.listen(PORT, () => {
  console.log(`Legal QA Form running on port ${PORT}`);
});
