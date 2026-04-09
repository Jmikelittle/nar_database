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

const DUCKDB_BUNDLES = {
  mvp: {
    mainModule:   "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/dist/duckdb-mvp.wasm",
    mainWorker:   "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/dist/duckdb-browser-mvp.worker.js",
  },
  eh: {
    mainModule:   "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/dist/duckdb-eh.wasm",
    mainWorker:   "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/dist/duckdb-browser-eh.worker.js",
  },
};

const PARQUET_GLOB = "data/parquet/**/*.parquet";
const METADATA_URL = "data/parquet/_metadata.json";

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
  for (const sel of selects) {
    // Keep the placeholder option, add provinces
    const placeholder = sel.querySelector("option");
    sel.innerHTML = "";
    sel.appendChild(placeholder);
    for (const prov of provinces) {
      const opt   = document.createElement("option");
      opt.value   = prov;
      opt.textContent = prov;
      sel.appendChild(opt);
    }
  }
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

async function loadStatistics() {
  // Try metadata JSON first (fast, no SQL needed)
  try {
    const resp = await fetch(METADATA_URL);
    if (resp.ok) {
      const meta = await resp.json();
      document.getElementById("stat-total").textContent =
        (meta.total_rows ?? 0).toLocaleString();
      const provinces = Object.keys(meta.provinces ?? {}).sort();
      document.getElementById("stat-provinces").textContent = provinces.length;
      await populateProvinceSelectors(provinces);
      statsSection.classList.remove("hidden");

      // Get city count from DuckDB (heavier query, run async)
      loadCityCount();
      return;
    }
  } catch (_) {
    // fall through to SQL-based stats
  }

  // Fallback: query DuckDB directly
  try {
    const result = await conn.query(`
      SELECT
        COUNT(*)                       AS total_rows,
        COUNT(DISTINCT province)       AS province_count,
        COUNT(DISTINCT city)           AS city_count
      FROM parquet_scan('${PARQUET_GLOB}')
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
      FROM parquet_scan('${PARQUET_GLOB}')
      WHERE province IS NOT NULL
      ORDER BY province
    `);
    const provinces = provResult.toArray().map((r) => r.province);
    await populateProvinceSelectors(provinces);
  } catch (err) {
    console.warn("Could not load statistics:", err);
  }
}

async function loadCityCount() {
  try {
    const result = await conn.query(`
      SELECT COUNT(DISTINCT city) AS city_count
      FROM parquet_scan('${PARQUET_GLOB}')
    `);
    const row = result.toArray()[0];
    document.getElementById("stat-cities").textContent =
      Number(row.city_count).toLocaleString();
  } catch (_) {}
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
    const rows = await runPrepared(
      `SELECT *
       FROM parquet_scan('${PARQUET_GLOB}')
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
    let sql;
    let params;
    if (city && province) {
      sql    = `SELECT * FROM parquet_scan('${PARQUET_GLOB}')
                WHERE LOWER(city) LIKE LOWER($1) AND province = $2
                LIMIT 500`;
      params = [`${city}%`, province];
    } else if (city) {
      sql    = `SELECT * FROM parquet_scan('${PARQUET_GLOB}')
                WHERE LOWER(city) LIKE LOWER($1)
                LIMIT 500`;
      params = [`${city}%`];
    } else {
      sql    = `SELECT * FROM parquet_scan('${PARQUET_GLOB}')
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
    const rows = await runPrepared(
      `SELECT * FROM parquet_scan('${PARQUET_GLOB}')
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
    // duckdb is exposed globally by the blocking bundle script
    const bundle = await duckdb.selectBundle(DUCKDB_BUNDLES);

    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" })
    );
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();

    db   = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule);
    URL.revokeObjectURL(worker_url);

    conn = await db.connect();
    setBanner("✅ DuckDB ready. Loading dataset statistics…", "success");

    await loadStatistics();
    hideBanner();
  } catch (err) {
    setBanner(
      `❌ Failed to load DuckDB-wasm: ${err.message}. ` +
      "Ensure the Parquet files have been uploaded to docs/data/parquet/.",
      "error"
    );
    console.error(err);
  }
}

initDuckDB();
