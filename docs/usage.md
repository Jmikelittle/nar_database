# NAR Database Usage Guide

## Installation

### From PyPI (once published)
```bash
pip install nar-database
```

### From Source
```bash
git clone https://github.com/GCOrgName/nar_database.git
cd nar_database
pip install -e .
```

## Command Line Usage

### Initialize Database (Recommended First Step)
```bash
# Download data and create database in one command
nar-db init

# With custom data directory
nar-db init --data-dir /path/to/data

# Force re-download of data
nar-db init --force-download
```

### Step-by-Step Process
```bash
# 1. Download NAR dataset
nar-db download

# 2. Process CSV files and create database
nar-db process
```

### Query the Database
```bash
# Search by postal code
nar-db query --postal-code "K1A0A6"

# Search by city and province
nar-db query --city "Ottawa" --province "ON"

# Limit results
nar-db query --city "Toronto" --province "ON" --limit 20

# Show database statistics
nar-db stats
```

## Python API Usage

### Basic Usage
```python
from nar_database import NARDatabase
from nar_database.downloader import NARDownloader
from nar_database.processor import NARProcessor

# Initialize database
db = NARDatabase()

# Create database schema
db.create_schema()

# Download data (if not already downloaded)
downloader = NARDownloader()
csv_files = downloader.download_and_extract()

# Process CSV files
processor = NARProcessor()
for csv_file in csv_files:
    for chunk in processor.process_csv_file(csv_file):
        db.insert_addresses_batch(chunk)
```

### Querying Data
```python
from nar_database import NARDatabase

# Initialize database connection
db = NARDatabase()

# Query by postal code
addresses = db.query_by_postal_code("K1A0A6")
for addr in addresses:
    print(f"{addr['street_number']} {addr['street_name']}, {addr['city']}")

# Query by city and province
addresses = db.query_by_city_province("Ottawa", "ON")

# Query by coordinates (within 1km radius)
addresses = db.query_by_coordinates(45.4215, -75.6972, radius=0.01)

# Get database statistics
stats = db.get_stats()
print(f"Total addresses: {stats['total_addresses']:,}")
print(f"Unique cities: {stats['unique_cities']:,}")
```

### Advanced Usage
```python
import pandas as pd
from nar_database import NARDatabase

# Initialize database
db = NARDatabase()

# Custom SQL queries
with db.get_connection() as conn:
    # Find all addresses on "Main Street"
    df = pd.read_sql("""
        SELECT * FROM addresses 
        WHERE street_name LIKE '%Main%'
        LIMIT 100
    """, conn)
    
    # Count addresses by province
    province_counts = pd.read_sql("""
        SELECT province, COUNT(*) as count 
        FROM addresses 
        GROUP BY province 
        ORDER BY count DESC
    """, conn)
```

## Configuration

### Data Directory Structure
```
data/                          # Default data directory
├── raw/                       # Downloaded and extracted files
│   ├── nar_dataset.zip       # Original download
│   └── extracted/            # Extracted CSV files
│       ├── file1.csv
│       ├── file2.csv
│       └── ...
├── processed/                 # Cleaned/processed data (optional)
└── database/                  # SQLite database
    └── nar.db                # Main database file
```

### Custom Paths
```python
from pathlib import Path
from nar_database import NARDatabase
from nar_database.downloader import NARDownloader

# Custom data directory
data_dir = Path("/custom/data/path")
downloader = NARDownloader(data_dir)

# Custom database path
db_path = Path("/custom/db/nar.db")
database = NARDatabase(db_path)
```

## Error Handling

### Common Issues

**1. Download Fails**
```python
try:
    downloader = NARDownloader()
    csv_files = downloader.download_and_extract()
except Exception as e:
    print(f"Download failed: {e}")
    # Check internet connection and URL
```

**2. Processing Errors**
```python
try:
    processor = NARProcessor()
    processor.process_all_csv_files()
except FileNotFoundError:
    print("CSV files not found. Run download first.")
except Exception as e:
    print(f"Processing error: {e}")
```

**3. Database Errors**
```python
try:
    db = NARDatabase()
    results = db.query_by_postal_code("K1A0A6")
except Exception as e:
    print(f"Database error: {e}")
    # Check if database exists and is properly initialized
```

## Performance Tips

### Large Dataset Handling
```python
# Process in chunks to manage memory
processor = NARProcessor()
database = NARDatabase()

for csv_file in csv_files:
    print(f"Processing {csv_file.name}...")
    
    for chunk in processor.process_csv_file(csv_file, chunk_size=5000):
        database.insert_addresses_batch(chunk, batch_size=5000)
```

### Database Optimization
```python
# The database automatically creates indexes for common queries:
# - postal_code
# - city, province
# - street_name
# - latitude, longitude

# For custom queries, you can add additional indexes
with db.get_connection() as conn:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_custom ON addresses(street_type, city)")
```

## Data Updates

### Updating with New Data
```bash
# Re-download and update database
nar-db init --force-download

# Or update specific components
nar-db download --force
nar-db process  # Will update existing database
```

### Backup Database
```bash
# Backup database file
cp data/database/nar.db data/database/nar_backup.db

# Or export to CSV
sqlite3 data/database/nar.db ".mode csv" ".output addresses_backup.csv" "SELECT * FROM addresses;"
```

## Troubleshooting

### Check System Requirements
- Python 3.8 or higher
- At least 2GB free disk space
- Stable internet connection for download

### Verify Installation
```bash
nar-db --version
```

### Debug Mode
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Your NAR database code here
```

### Clean Installation
```bash
# Remove data and start fresh
rm -rf data/
nar-db init
```
