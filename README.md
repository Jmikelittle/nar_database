# NAR Database

A Python package to download and process Statistics Canada's National Address Register (NAR) open dataset into a local SQLite database.

## Overview

The National Address Register is Canada's authoritative source of civic addresses. This package provides tools to:

- Download the latest NAR dataset (1GB+ ZIP file with 49 CSV files)
- Process and clean the address data (Address files contain unit-level data)
- Create and populate a local SQLite database with optimized performance
- Query addresses by postal code, city/province, or street name

## Documentation

- **[Data Dictionary](docs/data_dictionary.md)** - Complete field definitions and mappings
- **[Usage Guide](docs/usage.md)** - Detailed usage examples and API reference

## Installation

```bash
pip install nar-database
```

Or install from source:

```bash
git clone https://github.com/GCOrgName/nar_database.git
cd nar_database
pip install -e .
```

## Quick Start

### Initialize Database
```bash
# Download data and create database in one command
nar-db init

# Or step by step:
nar-db download    # Download and extract CSV files
nar-db process     # Process CSVs and create database
```

### Query the Database
```bash
# Search by postal code
nar-db query --postal-code "K1A0A6"

# Search by city and province
nar-db query --city "Ottawa" --province "ON" --limit 5

# Show database statistics
nar-db stats
```

## Python API

```python
from nar_database import NARDatabase

# Initialize
db = NARDatabase()

# Query addresses
addresses = db.query_by_postal_code("K1A0A6")
addresses = db.query_by_city_province("Ottawa", "ON")

# Get statistics
stats = db.get_stats()
print(f"Total addresses: {stats['total_addresses']:,}")
```

## Project Structure

```
nar_database/
├── src/nar_database/           # Main package
│   ├── downloader.py          # Data download and extraction
│   ├── processor.py           # CSV processing and cleaning
│   ├── database.py            # SQLite database management
│   └── cli.py                 # Command-line interface
├── tests/                     # Unit tests
├── data/                      # Local data (git-ignored)
│   ├── raw/                   # Downloaded ZIP and CSVs
│   └── database/              # SQLite database
└── docs/                      # Documentation
```

## Data Management

- All data files are excluded from git (see `.gitignore`)
- Users download data locally using this package
- Database files are created in `./data/database/` by default
- Raw CSV files are stored in `./data/raw/` by default

## Requirements

- Python 3.8+
- Dependencies: requests, pandas, click, tqdm

## Development

```bash
# Clone repository
git clone https://github.com/GCOrgName/nar_database.git
cd nar_database

# Install in development mode
pip install -e .[dev]

# Run tests
pytest

# Format code
black src/ tests/
```

## Data Source

- **Source**: Statistics Canada Open Data Portal
- **Dataset**: National Address Register (NAR)
- **License**: Open Government License - Canada
- **Format**: ZIP file containing 27 CSV files (~1GB compressed)

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Roadmap

- [ ] Find actual NAR download URL from Statistics Canada
- [ ] Implement automatic column mapping based on actual CSV structure
- [ ] Add geographic queries (radius search)
- [ ] Add data validation and quality checks
- [ ] Implement incremental updates
- [ ] Add export functionality (GeoJSON, KML)
- [ ] Create Jupyter notebook examples
