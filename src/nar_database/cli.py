"""
Command Line Interface for NAR Database
"""

import click
from pathlib import Path
from datetime import datetime
from .downloader import NARDownloader
from .processor import NARProcessor
from .database import NARDatabase


@click.group()
@click.version_option()
def main():
    """National Address Register Database Tool
    
    Download and process Statistics Canada's National Address Register
    into a local SQLite database.
    """
    pass


@main.command()
@click.option('--data-dir', type=click.Path(path_type=Path), 
              help='Directory to store data (default: ./data)')
@click.option('--version', type=str, 
              help='Specific version to download (e.g., "202507")')
@click.option('--auto-latest', is_flag=True, 
              help='Automatically detect and download the latest version')
@click.option('--force', is_flag=True, help='Force re-download even if file exists')
def download(data_dir: Path, version: str, auto_latest: bool, force: bool):
    """Download the NAR dataset"""
    if version and auto_latest:
        click.echo("❌ Error: Cannot specify both --version and --auto-latest", err=True)
        raise click.Abort()
    
    downloader = NARDownloader(data_dir, version)
    
    try:
        csv_files = downloader.download_and_extract(
            force_download=force, 
            auto_detect_latest=auto_latest
        )
        click.echo(f"✅ Successfully downloaded and extracted {len(csv_files)} CSV files")
        click.echo(f"📦 Version: {downloader.version}")
        
        for csv_file in csv_files[:5]:  # Show first 5 files
            click.echo(f"   - {csv_file.name}")
        if len(csv_files) > 5:
            click.echo(f"   ... and {len(csv_files) - 5} more files")
            
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


@main.command()
@click.option('--data-dir', type=click.Path(path_type=Path), 
              help='Directory containing data (default: ./data)')
@click.option('--db-path', type=click.Path(path_type=Path), 
              help='Path to database file (default: ./data/database/nar.db)')
def process(data_dir: Path, db_path: Path):
    """Process CSV files and create the database"""
    processor = NARProcessor(data_dir)
    database = NARDatabase(db_path)
    
    try:
        # Create database schema
        click.echo("Creating database schema...")
        database.create_schema()
        
        # Process CSV files
        click.echo("Processing CSV files...")
        csv_files = processor.discover_csv_files()
        
        total_records = 0
        for csv_file in csv_files:
            click.echo(f"Processing {csv_file.name}...")
            
            for chunk in processor.process_csv_file(csv_file):
                database.insert_addresses_batch(chunk)
                total_records += len(chunk)
        
        # Update metadata
        database.update_metadata('total_records', str(total_records))
        database.update_metadata('last_processed', datetime.now().isoformat())
        
        click.echo(f"✅ Successfully processed {total_records} address records")
        
        # Show statistics
        db_stats = database.get_stats()
        click.echo("\\n📊 Database Statistics:")
        click.echo(f"   Total addresses: {db_stats['total_addresses']:,}")
        click.echo(f"   Unique postal codes: {db_stats['unique_postal_codes']:,}")
        click.echo(f"   Unique cities: {db_stats['unique_cities']:,}")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


@main.command()
@click.option('--data-dir', type=click.Path(path_type=Path), 
              help='Directory to store data (default: ./data)')
@click.option('--db-path', type=click.Path(path_type=Path), 
              help='Path to database file (default: ./data/database/nar.db)')
@click.option('--version', type=str, 
              help='Specific version to download (e.g., "202507")')
@click.option('--auto-latest', is_flag=True, 
              help='Automatically detect and download the latest version')
@click.option('--force-download', is_flag=True, help='Force re-download of data')
def init(data_dir: Path, db_path: Path, version: str, auto_latest: bool, force_download: bool):
    """Initialize the NAR database (download + process)"""
    if version and auto_latest:
        click.echo("❌ Error: Cannot specify both --version and --auto-latest", err=True)
        raise click.Abort()
    
    click.echo("🚀 Initializing NAR Database...")
    
    # Download data
    click.echo("\\n1. Downloading data...")
    downloader = NARDownloader(data_dir, version)
    csv_files = downloader.download_and_extract(
        force_download=force_download,
        auto_detect_latest=auto_latest
    )
    click.echo(f"✅ Downloaded {len(csv_files)} CSV files")
    click.echo(f"📦 Version: {downloader.version}")
    
    # Process data
    click.echo("\\n2. Processing data...")
    processor = NARProcessor(data_dir)
    database = NARDatabase(db_path)
    
    # Create schema
    database.create_schema()
    
    # Process all files
    total_records = 0
    for csv_file in csv_files:
        click.echo(f"Processing {csv_file.name}...")
        
        for chunk in processor.process_csv_file(csv_file):
            database.insert_addresses_batch(chunk)
            total_records += len(chunk)
    
    # Update metadata
    database.update_metadata('total_records', str(total_records))
    database.update_metadata('last_processed', datetime.now().isoformat())
    database.update_metadata('version', downloader.version)
    
    click.echo("\\n✅ Initialization complete!")
    click.echo(f"   Database: {database.db_path}")
    click.echo(f"   Version: {downloader.version}")
    click.echo(f"   Total records: {total_records:,}")


@main.command()
@click.option('--db-path', type=click.Path(path_type=Path), 
              help='Path to database file (default: ./data/database/nar.db)')
@click.option('--postal-code', help='Search by postal code')
@click.option('--city', help='City name')
@click.option('--province', help='Province code (e.g., ON, BC)')
@click.option('--limit', default=10, help='Maximum number of results (default: 10)')
def query(db_path: Path, postal_code: str, city: str, province: str, limit: int):
    """Query the NAR database"""
    database = NARDatabase(db_path)
    
    try:
        results = []
        
        if postal_code:
            results = database.query_by_postal_code(postal_code)
        elif city and province:
            results = database.query_by_city_province(city, province)
        elif city or province:
            click.echo("❌ Error: When using city/province, both must be provided", err=True)
            raise click.Abort()
        else:
            click.echo("❌ Error: Must provide either postal-code or city+province", err=True)
            raise click.Abort()
        
        if not results:
            click.echo("No addresses found matching your criteria")
            return
        
        click.echo(f"Found {len(results)} addresses (showing first {min(limit, len(results))}):\\n")
        
        for i, row in enumerate(results[:limit]):
            click.echo(f"{i+1}. {row['street_number']} {row['street_name']} {row['street_type']}")
            if row['unit_number']:
                click.echo(f"   Unit: {row['unit_number']}")
            click.echo(f"   {row['city']}, {row['province']} {row['postal_code']}")
            if row['latitude'] and row['longitude']:
                click.echo(f"   Coordinates: {row['latitude']}, {row['longitude']}")
            click.echo("")
            
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


@main.command()
@click.option('--db-path', type=click.Path(path_type=Path), 
              help='Path to database file (default: ./data/database/nar.db)')
def stats(db_path: Path):
    """Show database statistics"""
    database = NARDatabase(db_path)
    
    try:
        db_statistics = database.get_stats()
        
        click.echo("📊 NAR Database Statistics\\n")
        click.echo(f"Total addresses: {db_statistics['total_addresses']:,}")
        click.echo(f"Unique postal codes: {db_statistics['unique_postal_codes']:,}")
        click.echo(f"Unique cities: {db_statistics['unique_cities']:,}")
        
        click.echo("\\nAddresses by province:")
        for province, count in sorted(db_statistics['addresses_by_province'].items()):
            click.echo(f"  {province}: {count:,}")
        
        # Show metadata
        version = database.get_metadata('version')
        if version:
            click.echo(f"\\nDataset version: {version}")
        
        last_processed = database.get_metadata('last_processed')
        if last_processed:
            click.echo(f"Last processed: {last_processed}")
            
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    main()
