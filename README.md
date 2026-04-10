# NAR Database

A Python package to download and process Statistics Canada's National Address Register (NAR) open dataset into queryable formats.

## Overview

The **National Address Register** is Canada's authoritative source of civic addresses. This package provides tools to transform the NAR dataset (~1 GB ZIP with 49 CSV files) into two powerful query formats:

### 🗄️ **SQLite Database** - High-Performance Local Queries
- Fast SQL-based searches with optimized indexes
- Offline access after initial download
- Perfect for data analysis, integration, and batch processing
- **[→ Full SQLite Setup Guide](SQLITE_SETUP.md)**

### 📦 **Parquet Files** - Browser-Queryable via DuckDB
- Province-partitioned files hosted on GitHub Pages
- Query directly in the browser with DuckDB-wasm
- No installation required for end users
- **[→ Full Parquet Setup Guide](docs/PARQUET_SETUP.md)**

## 📚 Documentation

| Document | Description |
|----------|-------------|
| **[SQLite Setup Guide](SQLITE_SETUP.md)** | Create local database for SQL queries |
| **[Parquet Setup Guide](docs/PARQUET_SETUP.md)** | Export to browser-queryable format |
| [Data Dictionary](docs/data_dictionary.md) | Complete field definitions and mappings |
| [Usage Guide](docs/usage.md) | Detailed API reference and examples |

## 🔗 Links

- **GitHub Repository**: https://github.com/Jmikelittle/nar_database
- **Data Source**: [Statistics Canada NAR](https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022022001-eng.htm)
- **License**: MIT (Data: Open Government License - Canada)

## 🚀 Quick Start

### Installation

```bash
# Install from PyPI (when published)
pip install nar-database

# Or install from source
git clone https://github.com/Jmikelittle/nar_database.git
cd nar_database
pip install -e .
```

### Choose Your Workflow

#### Option 1: SQLite Database (Recommended for Local Use)

Create a high-performance local database:

```bash
# One command to download, process, and create database
nar-db init

# Query the database
nar-db query --postal-code "K1A0A6"
nar-db query --city "Ottawa" --province "ON" --limit 5
nar-db stats
```

**[→ See complete SQLite setup guide](SQLITE_SETUP.md)**

#### Option 2: Parquet Files (Recommended for Public Access)

Create browser-queryable partitioned files:

```bash
# Install with Parquet support
pip install -e ".[parquet]"

# Download and convert to Parquet format
nar-db init-parquet --auto-latest

# Publish to GitHub Pages
git add docs/data/parquet/
git commit -m "Update NAR parquet data"
git push
```

**[→ See complete Parquet setup guide](docs/PARQUET_SETUP.md)**

## 🐍 Python API

Use the package programmatically:

```python
from nar_database import NARDatabase

# Initialize database
db = NARDatabase()

# Query addresses
addresses = db.query_by_postal_code("K1A0A6")
addresses = db.query_by_city_province("Ottawa", "ON")

# Get statistics
stats = db.get_stats()
print(f"Total addresses: {stats['total_addresses']:,}")
```

**[→ See full API documentation](docs/usage.md)**

## 📁 Project Structure

```
nar_database/
├── src/nar_database/           # Main package
│   ├── downloader.py          # Data download and extraction
│   ├── processor.py           # CSV processing and cleaning
│   ├── database.py            # SQLite database management
│   ├── parquet_exporter.py    # Parquet file generation
│   └── cli.py                 # Command-line interface
├── docs/                      # Documentation
│   ├── PARQUET_SETUP.md       # Parquet export guide
│   └── data/parquet/          # Published Parquet files (git-tracked)
├── data/                      # Local working data (git-ignored)
│   ├── raw/                   # Downloaded ZIPs and CSVs
│   └── database/              # SQLite database files
├── SQLITE_SETUP.md            # SQLite setup guide
├── COPILOT_INSTRUCTIONS.md    # AI agent background
└── README.md                  # This file
```

## 💾 Data Management

- **Dataset**: 49 CSV files (27 Address + 22 Location files)
- **Size**: ~1 GB compressed, ~3 GB extracted
- **Git Strategy**: All data files in `data/` are excluded via `.gitignore`
- **User Downloads**: Each user downloads their own copy locally
- **Published Data**: Only `docs/data/parquet/` is committed (for GitHub Pages)

### Default File Locations
- SQLite database: `./data/database/nar.db`
- Raw CSVs: `./data/raw/extracted/`
- Parquet files: `./docs/data/parquet/province=XX/`

## ⚙️ Requirements

- **Python**: 3.8 or higher
- **Core dependencies**: requests, pandas, click, tqdm
- **Optional dependencies** (for Parquet): pyarrow, fastparquet

## 🛠️ Development

```bash
# Clone repository
git clone https://github.com/Jmikelittle/nar_database.git
cd nar_database

# Install with development dependencies
pip install -e .[dev]

# Run tests
pytest

# Format code
black src/ tests/
```

## 📄 Data Source

- **Provider**: Statistics Canada
- **Dataset**: National Address Register (NAR)
- **URL**: https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022022001-eng.htm
- **License**: Open Government License - Canada
- **Format**: ZIP file containing 49 CSV files (~1 GB compressed)
- **Update Frequency**: Quarterly (check Statistics Canada for latest version)

## 📜 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

The NAR data is provided by Statistics Canada under the Open Government License - Canada.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Add tests for new functionality
4. Ensure all tests pass (`pytest`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## ✨ Features

- ✅ Automatic download from Statistics Canada (version detection)
- ✅ SQLite database with optimized indexing
- ✅ Parquet export with province partitioning
- ✅ CLI with progress tracking and comprehensive commands
- ✅ Python API for programmatic access
- ✅ Automatic column mapping and data cleaning
- ✅ Province code translation (numeric → alpha)
- ✅ Support for both local and remote data sources

## 🗺️ Roadmap

- [ ] Geographic/radius search capabilities
- [ ] Data validation and quality reporting
- [ ] Incremental updates (delta downloads)
- [ ] Export functionality (GeoJSON, KML)
- [ ] Jupyter notebook examples
- [ ] Web interface for Parquet data querying
- [ ] Performance benchmarks and optimization

---

**Questions or Issues?**  
Open an issue at https://github.com/Jmikelittle/nar_database/issues
