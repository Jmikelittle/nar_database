/**
 * app.js – NAR Address Search
 *
 * Queries province-partitioned Parquet files in data/parquet/ using
 * DuckDB-wasm running entirely in the browser.
 *
 * Expected file layout (relative to the page):
 *   data/parquet/province=AB/part-0.parquet
 *   data/parquet/province=BC/part-0.parquet
 *   data/parquet/_metadata.json
 */

// ---------------------------------------------------------------------------
// DuckDB-wasm bootstrap
// ---------------------------------------------------------------------------

// Import DuckDB-wasm ES modules directly
import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm';

const DUCKDB_BUNDLES = {
  mvp: {
    mainModule:   "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-mvp.wasm",
    mainWorker:   "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-mvp.worker.js",
  },
  eh: {
    mainModule:   "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-eh.wasm",
    mainWorker:   "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-eh.worker.js",
  },
};

// Build the base URL for parquet files (handle GitHub Pages URLs)
// For GitHub Pages: https://username.github.io/nar_database/data/parquet/
// For local serving: http://localhost:8000/data/parquet/
const getParquetBaseUrl = () => {
  const { protocol, hostname, pathname } = window.location;
  
  // If on localhost, use relative path
  if (hostname === 'localhost' || hostname === '127.0.0.1') {
    return 'data/parquet/';
  }
  
  // For GitHub Pages or other hosts, build full URL
  const pathParts = pathname.split('/').filter(Boolean);
  let basePath = '/';
  
  // If we're in a subdirectory repo (e.g., /nar_database/), include it
  if (pathParts.length > 0) {
    basePath = '/' + pathParts[0] + '/';
  }
  
  return `${protocol}//${hostname}${basePath}data/parquet/`;
};

const PARQUET_BASE_URL = getParquetBaseUrl();
const METADATA_URL = `${PARQUET_BASE_URL}_metadata.json`;

// Cache for province mapping (alpha/numeric code -> full name)
let provinceMapping = null;

/**
 * Load province mapping from DRSprovinces.json or use fallback
 * Maps: NT -> Northwest Territories, 61 -> Northwest Territories, etc.
 */
async function loadProvinceMapping() {
  if (provinceMapping) return provinceMapping;
  
  const mapping = {};
  
  try {
    // Try to fetch the hardcoded mapping from the site root
    // The DRSprovinces.json is in src/, so we include the mapping in this file
    // Format: { code: name } for both alpha and numeric codes
    
    // Hardcoded mapping of province codes to full English names
    // Data source: Statistics Canada DRS provinces
    const provinces = [
      { alpha: "NL", numeric: "10", name: "Newfoundland and Labrador" },
      { alpha: "PE", numeric: "11", name: "Prince Edward Island" },
      { alpha: "NS", numeric: "12", name: "Nova Scotia" },
      { alpha: "NB", numeric: "13", name: "New Brunswick" },
      { alpha: "QC", numeric: "24", name: "Quebec" },
      { alpha: "ON", numeric: "35", name: "Ontario" },
      { alpha: "MB", numeric: "46", name: "Manitoba" },
      { alpha: "SK", numeric: "47", name: "Saskatchewan" },
      { alpha: "AB", numeric: "48", name: "Alberta" },
      { alpha: "BC", numeric: "59", name: "British Columbia" },
      { alpha: "YT", numeric: "60", name: "Yukon" },
      { alpha: "NT", numeric: "61", name: "Northwest Territories" },
      { alpha: "NU", numeric: "62", name: "Nunavut" },
    ];
    
    // Build bidirectional mapping: both alpha and numeric codes map to full name
    for (const prov of provinces) {
      mapping[prov.alpha] = prov.name;
      mapping[prov.numeric] = prov.name;
    }
    
    provinceMapping = mapping;
    console.log("✓ Province mapping loaded:", Object.keys(mapping).length, "entries");
    return mapping;
  } catch (err) {
    console.error("Error loading province mapping:", err);
    return {};
  }
}

/**
 * Convert a province code (alpha or numeric) to full English name
 * E.g., "NT" -> "Northwest Territories", "61" -> "Northwest Territories"
 */
async function getFullProvinceName(code) {
  const mapping = await loadProvinceMapping();
  return mapping[code] || code; // Return code if not found
}

// Cache for loaded metadata
let metadataCache = null;

async function loadMetadata() {
  if (metadataCache) return metadataCache;
  
  console.log("📥 Fetching metadata from:", METADATA_URL);
  const resp = await fetch(METADATA_URL);
  if (!resp.ok) {
    throw new Error(`Failed to fetch metadata: ${resp.status}`);
  }
  
  metadataCache = await resp.json();
  console.log("✓ Metadata loaded:", metadataCache);
  return metadataCache;
}

// Build actual file URLs from metadata for DuckDB
async function getParquetFileUrls() {
  const meta = await loadMetadata();
  const files = meta.parquet_files || [];
  
  return files.map(file => {
    // Parquet files path is like: province=NT/part-0.parquet
    // Ensure it's a relative path first
    const relativePath = file.replace(/^.*data\/parquet\//, '');
    const url = `${PARQUET_BASE_URL}${relativePath}`;
    console.log("📄 Parquet file URL:", url);
    return url;
  });
}

// For DuckDB queries, create a UNION ALL of all parquet files
async function buildParquetScanQuery() {
  const files = await getParquetFileUrls();
  if (files.length === 0) {
    throw new Error("No Parquet files found in metadata");
  }
  
  // Read each parquet file and union them
  const selects = files.map(file => `SELECT * FROM read_parquet('${file}')`);
  return selects.join(' UNION ALL ');
}

let db   = null;
let conn = null;

// ---------------------------------------------------------------------------
// UI helpers
// ---------------------------------------------------------------------------

/** @type {HTMLElement} */
const banner          = document.getElementById("status-banner");
/** @type {HTMLElement} */
const statsSection    = document.getElementById("stats-section");
/** @type {HTMLElement} */
const resultsSection  = document.getElementById("results-section");
/** @type {HTMLTableElement} */
const resultsTable    = document.getElementById("results-table");
/** @type {HTMLElement} */
const resultsCount    = document.getElementById("results-count");
/** @type {HTMLButtonElement} */
const btnDownloadCsv  = document.getElementById("btn-download-csv");

function setBanner(msg, type = "info") {
  banner.textContent = msg;
  banner.className   = `banner banner-${type}`;
  banner.classList.remove("hidden");
}

function hideBanner() {
  banner.classList.add("hidden");
}

function setLoading(isLoading) {
  document.querySelectorAll(".btn-primary").forEach((btn) => {
    btn.disabled = isLoading;
  });
}

// ---------------------------------------------------------------------------
// Tab switching
// ---------------------------------------------------------------------------

document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((t) => {
      t.classList.remove("active");
      t.setAttribute("aria-selected", "false");
    });
    document.querySelectorAll(".tab-content").forEach((c) =>
      c.classList.remove("active")
    );
    tab.classList.add("active");
    tab.setAttribute("aria-selected", "true");
    document.getElementById(`tab-${tab.dataset.tab}`).classList.add("active");
  });
});

// ---------------------------------------------------------------------------
// Render results table
// ---------------------------------------------------------------------------

/** @type {Array<Object>} */
let lastResults = [];

/**
 * Render rows into the results table.
 * @param {Array<Object>} rows
 */
function renderResults(rows) {
  lastResults = rows;

  if (!rows || rows.length === 0) {
    setBanner("ℹ️ No addresses found matching your criteria.", "info");
    resultsSection.classList.add("hidden");
    return;
  }

  hideBanner();
  resultsSection.classList.remove("hidden");
  resultsCount.textContent = `${rows.length.toLocaleString()} row(s)`;

  const thead = resultsTable.querySelector("thead");
  const tbody = resultsTable.querySelector("tbody");

  const columns = Object.keys(rows[0]);

  // Header
  thead.innerHTML = `<tr>${columns
    .map((c) => `<th>${c.replace(/_/g, " ")}</th>`)
    .join("")}</tr>`;

  // Body
  tbody.innerHTML = rows
    .map(
      (row) =>
        `<tr>${columns.map((c) => `<td>${row[c] ?? ""}</td>`).join("")}</tr>`
    )
    .join("");
}

// ---------------------------------------------------------------------------
// CSV download
// ---------------------------------------------------------------------------

btnDownloadCsv.addEventListener("click", () => {
  if (!lastResults.length) return;

  const columns = Object.keys(lastResults[0]);
  const csvRows = [
    columns.join(","),
    ...lastResults.map((row) =>
      columns
        .map((c) => {
          const val = String(row[c] ?? "").replace(/"/g, '""');
          return `"${val}"`;
        })
        .join(",")
    ),
  ];
  const blob = new Blob([csvRows.join("\n")], { type: "text/csv" });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement("a");
  a.href     = url;
  a.download = "nar_results.csv";
  a.click();
  URL.revokeObjectURL(url);
});

// ---------------------------------------------------------------------------
// Province selectors
// ---------------------------------------------------------------------------

async function populateProvinceSelectors(provinces) {
  const selects = [
    document.getElementById("input-province-city"),
    document.getElementById("input-province-browse"),
  ];
  
  // Ensure province mapping is loaded
  await loadProvinceMapping();
  
  for (const sel of selects) {
    // Keep the placeholder option, add provinces
    const placeholder = sel.querySelector("option");
    sel.innerHTML = "";
    sel.appendChild(placeholder);
    for (const prov of provinces) {
      const opt   = document.createElement("option");
      opt.value   = prov;
      // Display full province name while keeping code as value
      opt.textContent = await getFullProvinceName(prov);
      sel.appendChild(opt);
    }
  }
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

async function loadStatistics() {
  // Load metadata JSON (fast, no SQL needed)
  try {
    const meta = await loadMetadata();
    
    document.getElementById("stat-total").textContent =
      (meta.total_rows ?? 0).toLocaleString();
    const provinces = Object.keys(meta.provinces ?? {}).sort();
    document.getElementById("stat-provinces").textContent = provinces.length;
    await populateProvinceSelectors(provinces);
    statsSection.classList.remove("hidden");

    // Get city count from DuckDB (heavier query, run async)
    loadCityCount();
    return;
  } catch (err) {
    console.warn("⚠️ Could not load metadata:", err.message);
    // fall through to SQL-based stats
  }

  // Fallback: query DuckDB directly (slower)
  try {
    console.log("📊 Running DuckDB query on parquet files...");
    const scanQuery = await buildParquetScanQuery();
    const result = await conn.query(`
      SELECT
        COUNT(*)                       AS total_rows,
        COUNT(DISTINCT province)       AS province_count,
        COUNT(DISTINCT city)           AS city_count
      FROM (${scanQuery})
    `);
    const row = result.toArray()[0];
    document.getElementById("stat-total").textContent =
      Number(row.total_rows).toLocaleString();
    document.getElementById("stat-provinces").textContent =
      Number(row.province_count);
    document.getElementById("stat-cities").textContent =
      Number(row.city_count).toLocaleString();
    statsSection.classList.remove("hidden");

    // Province list for selectors
    const provResult = await conn.query(`
      SELECT DISTINCT province
      FROM (${scanQuery})
      WHERE province IS NOT NULL
      ORDER BY province
    `);
    const provinces = provResult.toArray().map((r) => r.province);
    await populateProvinceSelectors(provinces);
  } catch (err) {
    console.error("❌ Could not load statistics:", err);
    setBanner("⚠️ Error loading statistics. Check browser console.", "error");
  }
}

async function loadCityCount() {
  try {
    const scanQuery = await buildParquetScanQuery();
    const result = await conn.query(`
      SELECT COUNT(DISTINCT city) AS city_count
      FROM (${scanQuery})
    `);
    const row = result.toArray()[0];
    document.getElementById("stat-cities").textContent =
      Number(row.city_count).toLocaleString();
  } catch (err) {
    console.error("Could not load city count:", err);
  }
}

// ---------------------------------------------------------------------------
// Search handlers
// ---------------------------------------------------------------------------

/**
 * Run a DuckDB prepared statement with positional parameters and return rows.
 * @param {string} sql   - SQL with $1, $2, … placeholders for user values.
 * @param {...*}   params - Values to bind.
 * @returns {Promise<Array<Object>>}
 */
async function runPrepared(sql, ...params) {
  const stmt   = await conn.prepare(sql);
  const result = await stmt.query(...params);
  await stmt.close();
  return result.toArray().map((row) => {
    const obj = {};
    for (const key of Object.keys(row)) {
      obj[key] = row[key];
    }
    return obj;
  });
}

// Postal code search
document.getElementById("btn-postal").addEventListener("click", async () => {
  const raw    = document.getElementById("input-postal").value.trim().toUpperCase().replace(/\s/g, "");
  if (!raw) { setBanner("⚠️ Please enter a postal code.", "error"); return; }

  setLoading(true);
  setBanner("🔍 Searching…", "info");
  try {
    const scanQuery = await buildParquetScanQuery();
    const rows = await runPrepared(
      `SELECT *
       FROM (${scanQuery})
       WHERE UPPER(REPLACE(postal_code, ' ', '')) = $1
       LIMIT 500`,
      raw
    );
    renderResults(rows);
  } catch (err) {
    setBanner(`❌ Query error: ${err.message}`, "error");
  } finally {
    setLoading(false);
  }
});

// Allow Enter key on postal input
document.getElementById("input-postal").addEventListener("keydown", (e) => {
  if (e.key === "Enter") document.getElementById("btn-postal").click();
});

// City + province search
document.getElementById("btn-city").addEventListener("click", async () => {
  const city     = document.getElementById("input-city").value.trim();
  const province = document.getElementById("input-province-city").value.trim();

  if (!city && !province) {
    setBanner("⚠️ Please enter at least a city or select a province.", "error");
    return;
  }

  setLoading(true);
  setBanner("🔍 Searching…", "info");
  try {
    const scanQuery = await buildParquetScanQuery();
    let sql;
    let params;
    if (city && province) {
      sql    = `SELECT * FROM (${scanQuery})
                WHERE LOWER(city) LIKE LOWER($1) AND province = $2
                LIMIT 500`;
      params = [`${city}%`, province];
    } else if (city) {
      sql    = `SELECT * FROM (${scanQuery})
                WHERE LOWER(city) LIKE LOWER($1)
                LIMIT 500`;
      params = [`${city}%`];
    } else {
      sql    = `SELECT * FROM (${scanQuery})
                WHERE province = $1
                LIMIT 500`;
      params = [province];
    }
    const rows = await runPrepared(sql, ...params);
    renderResults(rows);
  } catch (err) {
    setBanner(`❌ Query error: ${err.message}`, "error");
  } finally {
    setLoading(false);
  }
});

// Browse by province
document.getElementById("btn-province").addEventListener("click", async () => {
  const province = document.getElementById("input-province-browse").value;
  const limit    = parseInt(document.getElementById("input-limit").value, 10) || 50;

  if (!province) {
    setBanner("⚠️ Please select a province.", "error");
    return;
  }

  setLoading(true);
  setBanner("🔍 Loading…", "info");
  try {
    const scanQuery = await buildParquetScanQuery();
    const rows = await runPrepared(
      `SELECT * FROM (${scanQuery})
       WHERE province = $1
       LIMIT $2`,
      province,
      limit
    );
    renderResults(rows);
  } catch (err) {
    setBanner(`❌ Query error: ${err.message}`, "error");
  } finally {
    setLoading(false);
  }
});

// ---------------------------------------------------------------------------
// DuckDB-wasm initialisation
// ---------------------------------------------------------------------------

async function initDuckDB() {
  setBanner("⏳ Initialising DuckDB-wasm…", "info");

  try {
    // DuckDB is now imported as ES module above
    console.log("🚀 DuckDB module loaded. Available exports:", Object.keys(duckdb).slice(0, 15));
    console.log("📁 Parquet base URL:", PARQUET_BASE_URL);
    console.log("📑 Metadata URL:", METADATA_URL);
    
    // Select appropriate bundle based on browser capabilities
    const bundle = await duckdb.selectBundle(DUCKDB_BUNDLES);
    console.log("✓ Selected bundle:", bundle.mainModule.split('/').pop());
    
    // Create worker with the worker script
    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" })
    );
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();

    // Initialize DuckDB
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule);
    URL.revokeObjectURL(worker_url);

    conn = await db.connect();
    setBanner("✅ DuckDB ready. Loading dataset statistics…", "success");

    await loadStatistics();
    hideBanner();
  } catch (err) {
    setBanner(
      `❌ Failed to load DuckDB-wasm: ${err.message}`,
      "error"
    );
    console.error("❌ DuckDB initialization error:", err);
    console.error("DuckDB module:", typeof duckdb);
    console.error("Available exports:", Object.keys(duckdb || {}));
  }
}

// Initialize DuckDB when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initDuckDB);
} else {
  initDuckDB();
}
