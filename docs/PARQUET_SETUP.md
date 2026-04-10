# Parquet Setup Guide

This document explains how to generate province-partitioned Parquet files from
the Statistics Canada National Address Register and publish them to GitHub Pages
so the browser-based DuckDB-wasm interface can query them.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.8+ | Available in GitHub Codespaces by default |
| `pyarrow` | Installed via the `[parquet]` extra |
| A GitHub Codespace (or local clone) | At least 5 GB of free disk during processing |

---

## Step 1 – Open a Codespace

1. Go to your repository on GitHub.
2. Click **Code → Codespaces → Create codespace on main**.
3. Wait for the environment to start.

---

## Step 2 – Install dependencies

```bash
pip install -e ".[parquet]"
```

This installs the NAR database package together with `pyarrow` and
`fastparquet`.

---

## Step 3 – Generate Parquet files

### Option A – Download automatically from Statistics Canada

```bash
nar-db init-parquet --auto-latest
```

The command will:
1. Detect the latest ZIP version from the Statistics Canada homepage.
2. Download the ZIP (~1 GB) to `data/raw/`.
3. Extract the CSV files to `data/raw/extracted/`.
4. Stream-convert each CSV to Parquet with **snappy** compression.
5. Write partitioned Parquet files directly to `docs/data/parquet/`.
6. **Delete** the extracted CSVs and the original ZIP automatically (unless `--keep-csv`).

Temporary working files in `data/` are ignored by git. Only `docs/data/parquet/` needs to be committed.

### Option B – Use a local ZIP file

If you have already downloaded the ZIP manually:

```bash
nar-db init-parquet --local-zip /path/to/202507.zip
```
nar-db init-parquet --local-zip /path/to/202507.zip
```

### Option C – Keep intermediate CSV files

Add `--keep-csv` if you also want to build the SQLite database afterwards:

```bash
nar-db init-parquet --keep-csv
nar-db process   # optional: create SQLite database from the same CSVs
```

---

## Step 4 – Commit to GitHub

Parquet files are already in `docs/` where GitHub Pages expects them:

```bash
git add docs/data/parquet/
git commit -m "Update NAR parquet data (version 202507)"
git push
```

**That's it!** The data is live on GitHub Pages immediately.

### Optional: Clean up temporary files

The `data/` directory contains temporary working files (CSVs, ZIPs) and is safely ignored by git:

```bash
rm -rf data/  # Safe to delete; all files are in .gitignore
```

### Querying Parquet Files

The Parquet files are designed to be queried in the browser via DuckDB-wasm. If you need to query the data programmatically with Python, consider using the SQLite workflow instead. See:
- **[SQLite Setup Guide](../SQLITE_SETUP.md)** for creating a queryable local database
- **[Usage Guide](usage.md)** for complete Python API documentation

---

## Step 5 – Commit and push

```bash
git add docs/data/parquet
git commit -m "Update NAR data to $(date +%Y%m)"
git push
```

GitHub Actions (if enabled) will deploy `docs/` to GitHub Pages automatically.
Otherwise, enable GitHub Pages manually:

1. **Settings → Pages**
2. Source: **Deploy from a branch**
3. Branch: `main`, folder: `/docs`
4. Click **Save**

Your search interface is now live at
`https://<your-username>.github.io/<repo-name>/`.

---

## File sizes (approximate)

| File type | Approx. size |
|---|---|
| Original ZIP from Statistics Canada | ~900 MB – 1.1 GB |
| Extracted CSVs | ~1.0 – 1.3 GB |
| Parquet files (snappy, all provinces) | **~200 – 300 MB** |

The size reduction comes from snappy compression and DuckDB's columnar storage.

---

## Updating data regularly

Statistics Canada updates the NAR monthly.  To refresh:

```bash
# In your Codespace
nar-db init-parquet --auto-latest
cp -r data/parquet docs/data/parquet
git add docs/data/parquet
git commit -m "Update NAR data to $(date +%Y%m)"
git push
```

---

## Troubleshooting

### `ImportError: No module named 'pyarrow'`

Run `pip install 'nar-database[parquet]'` or `pip install pyarrow`.

### Download fails with a 404 error

The Statistics Canada URL pattern changes with each release.  Use
`--local-zip` with a manually downloaded file, or check
<https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022022001-eng.htm>
for the current URL.

### "No addresses found" in the browser

1. Make sure the Parquet files are inside `docs/data/parquet/` (not
   `data/parquet/`).
2. Check that GitHub Pages is serving the `docs/` folder.
3. Open the browser console and look for network errors – the Parquet files
   must be publicly accessible.

### Out-of-disk-space error during extraction

The ZIP and CSVs together need about 2 GB of temporary space.  Codespaces
machines typically have 30–60 GB available, so this should not be an issue.
If you are running locally, free up space first.

### DuckDB-wasm fails to load

The browser requires access to the DuckDB CDN.  If you are behind a corporate
proxy or offline, host the DuckDB WASM bundles locally and update the URLs in
`docs/js/app.js`.
