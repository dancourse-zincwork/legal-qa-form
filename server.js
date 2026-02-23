const express = require("express");
const path = require("path");
const http = require("http");
const https = require("https");
const { Client } = require("pg");

const app = express();
const PORT = process.env.PORT || 3000;

const QUERY_URL = process.env.N8N_WEBHOOK_URL || "https://zincwork.app.n8n.cloud/webhook/legal-qa-test";
const QDRANT_URL = process.env.QDRANT_URL || "http://qdrant.railway.internal:6333";
const QDRANT_KEY = process.env.QDRANT_API_KEY || "";
const OPENAI_KEY = process.env.OPENAI_API_KEY || "";
const PG_URL = process.env.DATABASE_URL || "";
const EMBED_MODEL = "text-embedding-3-small";
const CHUNK_SIZE = 1500;
const CHUNK_OVERLAP = 200;
const COLLECTION = "legal_docs";

app.use(express.json({ limit: "5mb" }));
app.use(express.static(path.join(__dirname, "public")));

// ── PostgreSQL setup ──
let db = null;
let dbFailed = false;
async function getDb() {
  if (db) return db;
  if (dbFailed || !PG_URL) return null;
  try {
    const client = new Client({ connectionString: PG_URL, ssl: false });
    await client.connect();
    await client.query(`
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
    db = client;
    console.log("PostgreSQL connected");
    return db;
  } catch (e) {
    console.error("DB connect failed:", e.message);
    dbFailed = true;
    return null;
  }
}
getDb();

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

// ── OpenAI embedding ──
function openaiEmbed(texts) {
  return new Promise((resolve, reject) => {
    if (!OPENAI_KEY) return reject(new Error("OPENAI_API_KEY not configured"));
    const body = JSON.stringify({ model: EMBED_MODEL, input: texts });
    const req = https.request({
      hostname: "api.openai.com",
      path: "/v1/embeddings",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${OPENAI_KEY}`,
        "Content-Length": Buffer.byteLength(body),
      },
      timeout: 60000,
    }, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.error) return reject(new Error(parsed.error.message));
          const sorted = parsed.data.sort((a, b) => a.index - b.index);
          resolve(sorted.map((d) => d.embedding));
        } catch {
          reject(new Error("Invalid response from OpenAI"));
        }
      });
    });
    req.on("error", reject);
    req.on("timeout", () => { req.destroy(); reject(new Error("OpenAI timeout")); });
    req.write(body);
    req.end();
  });
}

// ── Qdrant upsert ──
function qdrantUpsert(points) {
  return new Promise((resolve, reject) => {
    const url = new URL(`${QDRANT_URL}/collections/${COLLECTION}/points`);
    const transport = url.protocol === "https:" ? https : http;
    const body = JSON.stringify({ points });
    const opts = {
      hostname: url.hostname,
      port: url.port || (url.protocol === "https:" ? 443 : 80),
      path: url.pathname + "?wait=true",
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body),
        "api-key": QDRANT_KEY,
      },
      timeout: 60000,
    };
    const req = transport.request(opts, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          const parsed = JSON.parse(data);
          resolve(parsed);
        } catch {
          reject(new Error("Invalid response from Qdrant"));
        }
      });
    });
    req.on("error", reject);
    req.on("timeout", () => { req.destroy(); reject(new Error("Qdrant timeout")); });
    req.write(body);
    req.end();
  });
}

// ── Text chunking ──
function chunkText(text) {
  if (text.length <= CHUNK_SIZE) return [text];
  const chunks = [];
  const paragraphs = text.split(/\n\n+/);
  let current = "";

  for (const para of paragraphs) {
    if (current.length + para.length + 2 <= CHUNK_SIZE) {
      current = current ? current + "\n\n" + para : para;
    } else {
      if (current) chunks.push(current.trim());
      if (para.length > CHUNK_SIZE) {
        // Split long paragraph by sentences
        const sentences = para.split(/(?<=[.!?])\s+/);
        current = "";
        for (const sent of sentences) {
          if (current.length + sent.length + 1 <= CHUNK_SIZE) {
            current = current ? current + " " + sent : sent;
          } else {
            if (current) chunks.push(current.trim());
            current = sent;
          }
        }
      } else {
        // Add overlap from previous chunk
        if (chunks.length > 0 && CHUNK_OVERLAP > 0) {
          const prev = chunks[chunks.length - 1];
          const tail = prev.slice(-CHUNK_OVERLAP);
          current = tail + "\n\n" + para;
        } else {
          current = para;
        }
      }
    }
  }
  if (current.trim()) chunks.push(current.trim());
  return chunks.length > 0 ? chunks : [text];
}

// ── UUID v4 generator ──
function uuid4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
  });
}

// ── Direct ingest: chunk → embed → upsert ──
async function ingestDocument(title, content, docType, repo) {
  const start = Date.now();
  const docId = `doc_${Math.floor(Date.now() / 1000)}_${uuid4().slice(0, 8)}`;

  const chunks = chunkText(content);

  // Embed in batches of 50
  const allVectors = [];
  let totalTokens = 0;
  for (let i = 0; i < chunks.length; i += 50) {
    const batch = chunks.slice(i, i + 50);
    const vectors = await openaiEmbed(batch);
    allVectors.push(...vectors);
    totalTokens += batch.reduce((sum, c) => sum + Math.ceil(c.length / 4), 0);
  }

  // Build Qdrant points
  const points = chunks.map((chunk, idx) => ({
    id: uuid4(),
    vector: allVectors[idx],
    payload: {
      text: chunk,
      document_id: docId,
      title,
      document_type: docType,
      source_url: `form://${repo}/${title}`,
      chunk_index: idx,
      jurisdictions: [],
      topics: [],
      repo,
    },
  }));

  await qdrantUpsert(points);

  return {
    title,
    document_id: docId,
    chunks_stored: chunks.length,
    embedding_tokens: totalTokens,
    qdrant_status: "ok",
    processing_time_s: ((Date.now() - start) / 1000).toFixed(1),
  };
}

// ── Ingest a document (direct embed → Qdrant) ──
app.post("/api/ingest", async (req, res) => {
  const { title, type, document_type, repo, content } = req.body;
  if (!title || !content) {
    return res.status(400).json({ error: "Title and content required" });
  }
  if (content.trim().length < 50) {
    return res.status(400).json({ error: "Content too short (min 50 chars)" });
  }
  try {
    const docType = document_type || type || "guidance";
    const result = await ingestDocument(
      title.trim(),
      content.trim(),
      docType,
      (repo || "general").trim()
    );

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

// ── Batch ingest multiple documents ──
app.post("/api/ingest-batch", async (req, res) => {
  const { documents } = req.body;
  if (!documents || !Array.isArray(documents) || documents.length === 0) {
    return res.status(400).json({ error: "No documents provided" });
  }

  const results = [];
  const errors = [];

  for (const doc of documents) {
    const title = (doc.title || "").trim();
    const content = (doc.content || "").trim();
    const docType = doc.document_type || doc.type || "guidance";
    const repo = (doc.repo || "general").trim();

    if (!title || content.length < 50) {
      errors.push({ title: title || "(no title)", error: "Title required and content min 50 chars" });
      continue;
    }

    try {
      const result = await ingestDocument(title, content, docType, repo);
      results.push(result);
    } catch (err) {
      errors.push({ title, error: err.message.slice(0, 200) });
    }
  }

  // Mark cached answers as stale
  if (results.length > 0) {
    try {
      const pg = await getDb();
      if (pg) {
        await pg.query(
          `UPDATE query_log SET stale = TRUE, stale_reason = $1 WHERE stale = FALSE`,
          [`KB batch update: ${results.length} docs ingested at ${new Date().toISOString()}`]
        );
      }
    } catch (dbErr) {
      console.error("DB stale-mark error:", dbErr.message);
    }
  }

  res.json({
    ingested: results.length,
    errors: errors.length,
    total: documents.length,
    results,
    error_details: errors,
  });
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
