# NAR Database - AI Agent Background

## Project Overview
This document provides background information for AI agents working on the NAR Database project.

The **NAR Database** project is a Python package that processes Statistics Canada's National Address Register (NAR) open dataset into queryable formats. The package offers two primary output options:

1. **SQLite Database** - High-performance local database for SQL queries
2. **Parquet Files** - Browser-queryable partitioned files via DuckDB-wasm

The package handles ~1GB of compressed data containing **49 CSV files** (27 Address files + 22 Location files) while keeping data out of version control.

## Current Project Status
✅ **Completed Features:**
- Full project structure with Python packaging (pyproject.toml, setup.py, requirements.txt)
- Data downloader with version detection and auto-latest functionality
- CSV processor with automatic column mapping and province code translation
- SQLite database creation with optimized indexing
- Parquet exporter with province partitioning for browser queries
- CLI interface with progress tracking and comprehensive commands
- Documentation (README.md, SQLITE_SETUP.md, docs/PARQUET_SETUP.md)
- Git configuration to exclude data files
- Test framework structure

⚠️ **Known Limitations:**
- Limited test coverage (needs expansion with real data scenarios)
- Version auto-detection depends on Statistics Canada page structure
- No incremental update mechanism (full re-download required)
- No geographic/radius search capabilities

## Key Implementation Tasks for Copilot

## Data Source Information

**Statistics Canada NAR Dataset:**
- **Homepage**: https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022022001-eng.htm
- **License**: Open Government License - Canada
- **Base URL Pattern**: `https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/{VERSION}.zip`
- **Version Format**: YYYYMM (e.g., 202507 = July 2025)
- **Dataset Size**: ~1GB compressed ZIP file
- **Content**: 49 CSV files (27 Address files + 22 Location files)

**Recent Versions:**
- 202507 (July 2025) - Default in code as of last update
- 202412 (December 2024)
- Older: 2024.zip, 2023.zip, 2022.zip (different naming format)

**Note**: As of April 2026, newer versions may be available. The package supports:
- Manual version selection: `--version "YYYYMM"`
- Auto-detection: `--auto-latest` (scrapes homepage for latest version)
- Default: Hardcoded to 202507 (may need update)

## Package Architecture

### Core Modules

**downloader.py** - Data acquisition
- Downloads NAR ZIP files from Statistics Canada
- Supports version selection (manual or auto-detect)
- Handles extraction to `data/raw/extracted/`
- Progress tracking with tqdm

**processor.py** - CSV processing
- Automatic column mapping from NAR field names to standardized schema
- Province code translation (numeric → alpha codes using DRSprovinces.json)
- Streaming/chunked processing for memory efficiency
- Handles both Address and Location CSV files

**database.py** - SQLite database management
- Schema creation with optimized indexes
- Bulk insert operations for performance
- Query methods (by postal code, city/province)
- Database statistics and metadata

**parquet_exporter.py** - Parquet file generation
- Streaming CSV-to-Parquet conversion
- Province-based partitioning (`province=XX/`)
- Snappy compression for browser compatibility
- Direct output to `docs/data/parquet/` for GitHub Pages

**cli.py** - Command-line interface
- Commands: init, download, process, query, stats, init-parquet
- Progress indicators and user feedback
- Support for both SQLite and Parquet workflows

## Directory Structure
```
nar_database/
├── src/nar_database/          # Main package code
│   ├── __init__.py           # Package initialization
│   ├── downloader.py         # Download & extract NAR data
│   ├── processor.py          # CSV processing & cleaning
│   ├── database.py           # SQLite database management
│   ├── parquet_exporter.py   # Parquet file generation
│   ├── cli.py                # Command-line interface
│   └── DRSprovinces.json     # Province code mapping (numeric → alpha)
├── tests/                     # Unit tests
├── docs/                      # Documentation & GitHub Pages
│   ├── PARQUET_SETUP.md      # Instructions for Parquet workflow
│   └── data/parquet/         # Published Parquet files (git-tracked)
├── data/                      # Local working data (git-ignored)
│   ├── raw/                  # Downloaded ZIPs and extracted CSVs
│   ├── processed/            # Intermediate processed files
│   └── database/             # Generated SQLite databases
├── SQLITE_SETUP.md           # Instructions for SQLite workflow
├── COPILOT_INSTRUCTIONS.md   # This file - AI agent background
├── README.md                 # Project overview & quick start
├── pyproject.toml            # Modern Python packaging
├── setup.py                  # Backward compatibility
├── requirements.txt          # Dependencies
└── .gitignore                # Excludes data/ directory
```

## Development Workflow
```bash
# 1. Setup development environment
pip install -e .[dev]

# 2. Run tests
pytest tests/

# 3. Test CLI commands
nar-db --help
nar-db init

# 4. Format code
black src/ tests/

# 5. Check types
mypy src/
```

## Key Design Principles

### Data Management
- **Never commit data files** - All CSV, ZIP, and database files are git-ignored
- **Local-only processing** - Users download and process data on their machines
- **Efficient processing** - Use chunked processing for large CSV files
- **Database optimization** - Create indexes for common query patterns

### User Experience
- **Simple initialization** - One command (`nar-db init`) sets everything up
- **Multiple interfaces** - Both CLI and Python API
- **Clear error messages** - Helpful feedback for common issues
- **Progress indicators** - Show download and processing progress

### Code Quality
- **Type hints** - Use throughout for better IDE support
- **Error handling** - Graceful handling of network, file, and data errors
- **Testing** - Unit tests for all major components
- **Documentation** - Clear docstrings and usage examples

## Common AI Agent Tasks

### Understanding the Data Flow
1. **Download**: ZIP from Statistics Canada → `data/raw/`
2. **Extract**: ZIP → CSV files in `data/raw/extracted/Addresses/` and `data/raw/extracted/Locations/`
3. **Process**:
   - **SQLite path**: CSVs → SQLite database in `data/database/`
   - **Parquet path**: CSVs → Partitioned Parquet in `docs/data/parquet/`
4. **Query**: SQLite via SQL or Parquet via DuckDB-wasm in browser

### Debugging Tips
- Check `DRSprovinces.json` for province code mappings
- CSV files use numeric province codes (10, 11, 12, etc.)
- The processor translates: 10→NL, 11→PE, 12→NS, 13→NB, 24→QC, 35→ON, etc.
- Address files contain unit-level data; Location files contain building/site data
- All temporary data goes to `data/` (git-ignored)
- Only `docs/data/parquet/` is committed for GitHub Pages

### Version Management
- Default version hardcoded in code: "202507" (July 2025)
- As of April 2026, this may be outdated - check for newer YYYYMM versions
- Use `--auto-latest` for automatic detection (scrapes Statistics Canada page)
- Manual selection: `--version "YYYYMM"`

## User Workflows

### SQLite Workflow (Local High-Performance Queries)
```bash
pip install nar-database
nar-db init                          # Download + process + create DB
nar-db query --postal-code "K1A0A6"  # Query the database
nar-db stats                         # View statistics
```
See [SQLITE_SETUP.md](SQLITE_SETUP.md) for detailed instructions.

### Parquet Workflow (Browser-Queryable via GitHub Pages)
```bash
pip install nar-database[parquet]
nar-db init-parquet --auto-latest    # Download + convert to Parquet
git add docs/data/parquet/
git commit -m "Update NAR parquet data"
git push                             # Publish to GitHub Pages
```
See [docs/PARQUET_SETUP.md](docs/PARQUET_SETUP.md) for detailed instructions.

## Documentation Structure

- **README.md** - High-level project overview, installation, and quick start
- **COPILOT_INSTRUCTIONS.md** (this file) - AI agent background and context
- **SQLITE_SETUP.md** - Detailed SQLite database creation instructions
- **docs/PARQUET_SETUP.md** - Detailed Parquet export instructions
- **docs/data_dictionary.md** - Field definitions and data schema
- **docs/usage.md** - Detailed API reference and usage examples

## Repository Information
- **Owner**: Jmikelittle
- **Repo**: nar_database 
- **GitHub Pages**: Hosts Parquet files for DuckDB-wasm querying
- **License**: MIT
