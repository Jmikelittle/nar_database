# NAR Database Implementation Instructions for GitHub Copilot

## Project Overview
This project creates a Python package that downloads Statistics Canada's National Address Register (NAR) open dataset and converts it into a local SQLite database. The package handles ~1GB of data with 27 CSV files while keeping data out of version control.

## Current Project Status
✅ **Completed:**
- Basic project structure created
- Core module files implemented (downloader.py, processor.py, database.py, cli.py)
- Configuration files ready (pyproject.toml, requirements.txt, setup.py)
- Documentation and usage guides written
- Git ignore configured to exclude data files
- Test framework structure established

⚠️ **Needs Implementation:**
- Find actual Statistics Canada NAR download URL
- Test and fix column mapping based on real CSV structure
- Implement error handling improvements
- Add data validation
- Create working tests with mock data

## Key Implementation Tasks for Copilot

### 1. **COMPLETED: NAR Data Source Found ✅**
**Statistics Canada NAR Dataset Information:**
- **Homepage**: https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022022001-eng.htm
- **Current Download URL**: https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/202507.zip (July 2025)
- **Base URL Pattern**: `https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/{VERSION}.zip`
- **Version Format**: YYYYMM (ISO 8601 format)

**Available Versions:**
- July 2025: `202507.zip`
- December 2024: `202412.zip`  
- June 2024: `2024.zip` (different format)
- 2023: `2023.zip`
- 2022: `2022.zip`

**Implementation Status:**
- ✅ Updated `downloader.py` with actual URLs
- ✅ Added version detection and auto-latest features
- ✅ Added both hardcoded and smart version detection options
- ✅ Updated CLI to support `--version` and `--auto-latest` flags

**Usage Examples:**
```bash
# Use current default version (202507 - July 2025)
nar-db download

# Download specific version
nar-db download --version "202412"

# Auto-detect and download latest version
nar-db download --auto-latest

# Initialize with specific version
nar-db init --version "202507"
```

### 2. **Column Mapping Customization**
The `processor.py` contains generic column mappings that need customization:
```python
# In src/nar_database/processor.py, method standardize_column_names()
# Lines ~60-100 contain placeholder mappings
```
**Task:** Once you have access to real NAR CSV files, update the column mapping logic to match the actual field names used in the Statistics Canada dataset.

### 3. **Error Handling Improvements**
**Task:** Add robust error handling for:
- Network timeouts during download
- Corrupted ZIP files
- Invalid CSV formats
- Database connection issues
- Disk space constraints

### 4. **Import Fixes**
Several imports show lint errors due to missing packages:
```python
# These will be resolved when dependencies are installed:
from tqdm import tqdm
import click
import pandas as pd
```
**Task:** Ensure all required dependencies are properly installed when testing.

### 5. **Testing with Real Data**
**Task:** 
- Download actual NAR dataset for testing
- Validate CSV structure matches expectations
- Test end-to-end workflow: download → process → query
- Verify performance with large dataset

### 6. **CLI Enhancement**
Current CLI in `src/nar_database/cli.py` has basic functionality.
**Task:** Add features like:
- Progress indicators for long-running operations
- Verbose/quiet modes
- Configuration file support
- Data validation reports

## Directory Structure Reference
```
nar_database/
├── src/nar_database/          # Main package code
│   ├── __init__.py           # Package initialization
│   ├── downloader.py         # Download & extract NAR data
│   ├── processor.py          # CSV processing & cleaning
│   ├── database.py           # SQLite database management
│   └── cli.py               # Command-line interface
├── tests/                    # Unit tests
├── docs/                     # Documentation
├── data/                     # Local data (git-ignored)
├── pyproject.toml           # Modern Python packaging
├── setup.py                 # Backward compatibility
├── requirements.txt         # Dependencies
├── .gitignore              # Excludes data files
└── README.md               # Project documentation
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

## Next Steps Priority

1. **MEDIUM PRIORITY:** Test with real data to validate CSV structure assumptions
2. **MEDIUM PRIORITY:** Fix CLI import/syntax errors (click, pathlib dependencies)
3. **MEDIUM PRIORITY:** Improve error handling and user feedback  
4. **MEDIUM PRIORITY:** Add comprehensive tests
5. **LOW PRIORITY:** Performance optimizations and advanced features

## Version Strategy Recommendation

**For immediate use:** Use the hardcoded approach with version "202507" (July 2025) as it's the most recent.

**For future releases:** The auto-detection feature can scrape the homepage to find newer versions, but it's more complex and could break if the webpage changes.

**Both approaches are implemented:**
- Default: Uses hardcoded "202507" 
- Option 1: `--version "YYYYMM"` for specific versions
- Option 2: `--auto-latest` for automatic detection

## Package Distribution
Once implemented and tested:
```bash
# Build package
python -m build

# Upload to PyPI (when ready)
twine upload dist/*
```

## Integration with GitHub
The project is already set up in the `nar_database` repository. All code changes should be committed to version control, but data files will be excluded by `.gitignore`.

## Expected Usage Flow
```bash
# User installs package
pip install nar-database

# User initializes database
nar-db init  # Downloads ~1GB, processes 27 CSVs, creates SQLite DB

# User queries data
nar-db query --postal-code "K1A0A6"
nar-db query --city "Ottawa" --province "ON"
```

This structure provides a solid foundation for creating a professional-quality Python package for Canadian address data processing.
