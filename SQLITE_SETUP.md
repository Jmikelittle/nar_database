# SQLite Database Setup Guide

This guide explains how to create a local SQLite database from the Statistics Canada National Address Register (NAR) dataset for high-performance queries.

---

## Overview

The SQLite workflow downloads the NAR dataset, processes the CSV files, and creates an optimized local database file. This approach is ideal for:

- **Fast local queries** - SQL-based searches with indexed fields
- **Offline access** - No internet required after initial download
- **Python integration** - Direct database access via Python API
- **Large result sets** - Efficient handling of millions of records

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.8+ | Required for the nar-database package |
| ~5 GB free disk space | For temporary CSV files and final database |
| Internet connection | For initial dataset download (~1 GB) |

---

## Installation

### Option 1: Install from PyPI (when published)

```bash
pip install nar-database
```

### Option 2: Install from source

```bash
git clone https://github.com/Jmikelittle/nar_database.git
cd nar_database
pip install -e .
```

---

## Quick Start - One Command

The simplest way to set up everything:

```bash
nar-db init
```

This single command will:
1. Download the latest NAR ZIP file from Statistics Canada (~1 GB)
2. Extract 49 CSV files to `data/raw/extracted/`
3. Process and clean the data
4. Create SQLite database with indexes at `data/database/nar.db`
5. Display summary statistics

**Expected time**: 10-30 minutes depending on your internet speed and CPU.

---

## Step-by-Step Process

If you prefer more control, you can run each step individually:

### Step 1: Download the Dataset

```bash
# Download default version (202507 - July 2025)
nar-db download

# Or download a specific version
nar-db download --version "202412"

# Or auto-detect and download the latest version
nar-db download --auto-latest
```

The ZIP file (~1 GB) will be downloaded to `data/raw/` and automatically extracted to `data/raw/extracted/`.

### Step 2: Process CSVs and Create Database

```bash
nar-db process
```

This command:
- Reads all 49 CSV files (27 Address + 22 Location files)
- Maps columns to standardized schema
- Translates province codes (numeric → alpha: 10→NL, 35→ON, etc.)
- Creates SQLite database with optimized indexes
- Saves to `data/database/nar.db`

### Step 3: Verify the Database

```bash
nar-db stats
```

Shows statistics like:
- Total addresses
- Records by province
- Database file size
- Index information

---

## Querying the Database

### Command Line Interface

**Search by postal code:**
```bash
nar-db query --postal-code "K1A0A6"
```

**Search by city and province:**
```bash
nar-db query --city "Ottawa" --province "ON" --limit 10
```

**Search by street name:**
```bash
nar-db query --street "Main" --province "ON" --limit 5
```

### Python API

```python
from nar_database import NARDatabase

# Initialize database connection
db = NARDatabase()

# Query by postal code
addresses = db.query_by_postal_code("K1A0A6")
for addr in addresses:
    print(f"{addr['street_name']} {addr['street_number']}, {addr['city']}")

# Query by city and province
addresses = db.query_by_city_province("Ottawa", "ON", limit=100)

# Custom SQL query
import sqlite3
conn = sqlite3.connect("data/database/nar.db")
cursor = conn.execute("SELECT * FROM addresses WHERE province = 'ON' LIMIT 5")
results = cursor.fetchall()
```

---

## Database Schema

The SQLite database contains the following main fields:

| Field | Type | Description | Indexed |
|---|---|---|---|
| `postal_code` | TEXT | Canadian postal code (e.g., K1A0A6) | ✓ |
| `province` | TEXT | Province code (e.g., ON, QC, BC) | ✓ |
| `city` | TEXT | Municipality/city name | ✓ |
| `street_name` | TEXT | Street name | ✓ |
| `street_number` | TEXT | Civic/street number | |
| `street_type` | TEXT | Street type (Ave, Rd, St, etc.) | |
| `unit_number` | TEXT | Apartment/unit number | |
| `latitude` | REAL | Latitude coordinate | |
| `longitude` | REAL | Longitude coordinate | |

For complete field definitions, see [docs/data_dictionary.md](docs/data_dictionary.md).

---

## Data Management

### File Locations

- **Raw data**: `data/raw/` (ZIP files and extracted CSVs)
- **Database**: `data/database/nar.db`
- **Processed data**: `data/processed/` (intermediate files)

### Git Ignore

All files in the `data/` directory are excluded from version control via `.gitignore`. This ensures:
- No large data files are committed to the repository
- Each user downloads their own copy
- Database files remain local to each installation

### Updating Data

To update with a newer NAR release:

```bash
# Download new version
nar-db download --version "YYYYMM"

# Rebuild database (overwrites existing)
nar-db process
```

Or use the all-in-one command:
```bash
nar-db init --version "YYYYMM"
```

### Cleaning Up

To remove temporary files while keeping the database:

```bash
# Remove CSV files (keep database)
rm -rf data/raw/
rm -rf data/processed/

# Or remove everything and start fresh
rm -rf data/
nar-db init
```

---

## Performance Optimization

The SQLite database is optimized for common query patterns:

- **Indexes** created on: postal_code, province, city, street_name
- **Bulk inserts** used during database creation
- **WAL mode** enabled for better concurrency
- **Analyze** run after data load for query optimization

Typical query performance:
- Postal code lookup: < 10ms
- City/province search: 50-200ms (depending on result size)
- Full table scan: 1-5 seconds (millions of records)

---

## Troubleshooting

### Download Fails

```bash
# Check your internet connection
# Try a different version
nar-db download --version "202412"

# Or use a local ZIP file if you have one
nar-db init --local-zip /path/to/nar_dataset.zip
```

### Database Creation Fails

```bash
# Check disk space (need ~5 GB during processing)
df -h

# Check file permissions
ls -la data/

# Remove partial database and try again
rm -rf data/database/
nar-db process
```

### Slow Queries

```bash
# Verify indexes exist
sqlite3 data/database/nar.db "SELECT * FROM sqlite_master WHERE type='index';"

# Run ANALYZE to update statistics
sqlite3 data/database/nar.db "ANALYZE;"
```

---

## Advanced Usage

### Custom Database Location

```python
from nar_database import NARDatabase
from pathlib import Path

db = NARDatabase(data_dir=Path("/custom/path"))
db.process()
```

### Batch Processing

```python
from nar_database.processor import NARProcessor
from pathlib import Path

processor = NARProcessor(data_dir=Path("data"))
processor.process_csv_files(batch_size=10000)
```

### Database Migration

```bash
# Export to CSV
sqlite3 data/database/nar.db <<EOF
.headers on
.mode csv
.output addresses_export.csv
SELECT * FROM addresses;
EOF
```

---

## Next Steps

- **Parquet Export**: See [docs/PARQUET_SETUP.md](docs/PARQUET_SETUP.md) for browser-queryable format
- **API Reference**: See [docs/usage.md](docs/usage.md) for detailed Python API
- **Data Dictionary**: See [docs/data_dictionary.md](docs/data_dictionary.md) for field definitions

---

## Support

- **GitHub Repository**: https://github.com/Jmikelittle/nar_database
- **Issues**: https://github.com/Jmikelittle/nar_database/issues
- **Documentation**: https://github.com/Jmikelittle/nar_database/tree/main/docs
