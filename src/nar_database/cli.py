"""
Command Line Interface for NAR Database
"""

import click
from pathlib import Path
from datetime import datetime
from .downloader import NARDownloader
from .processor import NARProcessor
from .database import NARDatabase
from .parquet_exporter import NARParquetExporter


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
@click.option('--local-zip', type=click.Path(exists=True, path_type=Path),
              help='Path to local ZIP file to use instead of downloading')
@click.option('--auto-latest', is_flag=True, 
              help='Automatically detect and download the latest version')
@click.option('--force', is_flag=True, help='Force re-download even if file exists')
def download(data_dir: Path, version: str, local_zip: Path, auto_latest: bool, force: bool):
    """Download the NAR dataset"""
    if version and auto_latest:
        click.echo("❌ Error: Cannot specify both --version and --auto-latest", err=True)
        raise click.Abort()
    
    if local_zip and (version or auto_latest):
        click.echo("❌ Error: Cannot specify --local-zip with --version or --auto-latest", err=True)
        raise click.Abort()
    
    downloader = NARDownloader(data_dir, version, local_zip)
    
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
        
        # Show timing information
        end_time = datetime.now()
        total_duration = end_time - start_time
        click.echo(f"\\n🕒 Total elapsed time: {total_duration}")
        click.echo(f"⏰ Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
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
@click.option('--local-zip', type=click.Path(exists=True, path_type=Path),
              help='Path to local ZIP file to use instead of downloading')
@click.option('--auto-latest', is_flag=True, 
              help='Automatically detect and download the latest version')
@click.option('--force-download', is_flag=True, help='Force re-download of data')
def init(data_dir: Path, db_path: Path, version: str, local_zip: Path, auto_latest: bool, force_download: bool):
    """Initialize the NAR database (download + process)"""
    
    # Record start time
    start_time = datetime.now()
    click.echo(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if version and auto_latest:
        click.echo("❌ Error: Cannot specify both --version and --auto-latest", err=True)
        raise click.Abort()
    
    if local_zip and (version or auto_latest):
        click.echo("❌ Error: Cannot specify --local-zip with --version or --auto-latest", err=True)
        raise click.Abort()
    
    click.echo("🚀 Initializing NAR Database...")
    
    # Download data
    click.echo("\\n1. Downloading data...")
    downloader = NARDownloader(data_dir, version, local_zip)
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
@click.option('--local-zip', type=click.Path(exists=True, path_type=Path),
              help='Path to local ZIP file to test with')
def test_local(local_zip: Path):
    """Test processing with a local ZIP file (quick validation)"""
    if not local_zip:
        click.echo("❌ Error: Must provide --local-zip path", err=True)
        raise click.Abort()
    
    click.echo(f"🧪 Testing with local ZIP file: {local_zip}")
    
    try:
        # Set up test data directory  
        test_data_dir = Path("test_data")
        downloader = NARDownloader(test_data_dir, local_zip_path=local_zip)
        
        # Extract and analyze
        click.echo("1. Extracting ZIP file...")
        zip_path = downloader.use_local_zip()
        extract_dir = downloader.extract(zip_path)
        csv_files = downloader.list_csv_files(extract_dir)
        
        click.echo(f"✅ Found {len(csv_files)} CSV files")
        
        # Analyze first few CSV files
        click.echo("\\n2. Analyzing CSV structure...")
        processor = NARProcessor(test_data_dir)
        
        for i, csv_file in enumerate(csv_files[:3]):  # Test first 3 files only
            click.echo(f"\\nAnalyzing {csv_file.name}:")
            analysis = processor.analyze_csv_structure(csv_file)
            
            if 'error' in analysis:
                click.echo(f"   ❌ Error: {analysis['error']}")
            else:
                click.echo(f"   📊 Columns: {analysis['num_columns']}")
                click.echo(f"   📋 Sample rows: {analysis['sample_rows']}")
                click.echo(f"   📈 Estimated total: {analysis['estimated_total_rows']}")
                click.echo("   🏷️  Column names:")
                for col in analysis['columns'][:10]:  # Show first 10 columns
                    click.echo(f"      - {col}")
                if len(analysis['columns']) > 10:
                    click.echo(f"      ... and {len(analysis['columns']) - 10} more")
        
        click.echo("\\n✅ Local ZIP test completed successfully!")
        click.echo("💡 Use 'nar-db init --local-zip <path>' to process the full dataset")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


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


@main.command()
@click.option('--local-zip', type=click.Path(exists=True, path_type=Path), 
              help='Use a local ZIP file instead of downloading')
@click.option('--sample-size', type=int, 
              help='Process only N records per file (for testing)')
@click.option('--max-workers', type=int, 
              help='Number of parallel workers (default: auto)')
def init_fast(local_zip: Path, sample_size: int, max_workers: int):
    """Initialize NAR database with optimized high-performance processing"""
    
    # Record start time
    start_time = datetime.now()
    click.echo(f"🚀 Initializing NAR Database (FAST MODE)...")
    click.echo(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if sample_size:
        click.echo(f"🧪 Sample mode: Processing {sample_size:,} records per file")
    
    # Import optimized processor
    try:
        from .processor_optimized import NARProcessorOptimized
        from .database import NARDatabase
        from .downloader import NARDownloader
    except ImportError as e:
        click.echo(f"❌ Error importing modules: {e}")
        return
    
    try:
        # Step 1: Download/Extract
        step1_start = datetime.now()
        click.echo(f"\\n1. Downloading data... ({step1_start.strftime('%H:%M:%S')})")
        downloader = NARDownloader()
        
        if local_zip:
            version = downloader.use_local_zip(local_zip)
            extract_dir = downloader.extract()
            csv_files = downloader.list_csv_files(extract_dir)
        else:
            csv_files = downloader.download_and_extract()
            version = downloader.version
        
        step1_end = datetime.now()
        step1_duration = step1_end - step1_start
        click.echo(f"✅ Downloaded {len(csv_files)} CSV files (took {step1_duration})")
        click.echo(f"📦 Version: {version}")
        
        # Step 2: Process with optimizations
        step2_start = datetime.now()
        click.echo(f"\\n2. Processing data (OPTIMIZED)... ({step2_start.strftime('%H:%M:%S')})")
        processor = NARProcessorOptimized()
        database = NARDatabase()
        
        # Create database schema
        database.create_schema()
        
        total_processed = 0
        chunk_count = 0
        
        # Process with I/O-optimized settings (your system handles large batches well)
        chunk_size = 75000  # Optimal for your I/O performance
        
        for chunk in processor.process_csv_parallel(
            csv_files, 
            max_workers=max_workers,
            chunk_size=chunk_size, 
            sample_size=sample_size
        ):
            chunk_count += 1
            batch_start = datetime.now()
            database.insert_addresses_batch(chunk, batch_size=75000)
            batch_end = datetime.now()
            batch_duration = batch_end - batch_start
            total_processed += len(chunk)
            click.echo(f"📊 Batch {chunk_count}: Inserted {len(chunk):,} records in {batch_duration} (Total: {total_processed:,})")
        
        step2_end = datetime.now()
        step2_duration = step2_end - step2_start
        total_duration = step2_end - start_time
        
        click.echo(f"\\n✅ Database initialized successfully!")
        click.echo(f"📈 Total records processed: {total_processed:,}")
        click.echo(f"💾 Database location: data/database/nar.db")
        click.echo(f"⏱️  Processing time: {step2_duration}")
        click.echo(f"🕒 Total elapsed time: {total_duration}")
        click.echo(f"⏰ Finished at: {step2_end.strftime('%Y-%m-%d %H:%M:%S')}")
        click.echo(f"🎯 Use 'python -m nar_database.cli query' commands to search the data")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


@main.command("export-parquet")
@click.option(
    "--data-dir",
    type=click.Path(path_type=Path),
    help="Base data directory (default: ./data)",
)
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    help="Output Parquet file path (default: <data-dir>/parquet_output/nar_addresses.parquet)",
)
def export_parquet(data_dir: Path, output: Path):
    """Export processed NAR data to a single Parquet file"""
    exporter = NARParquetExporter(data_dir)
    try:
        parquet_path = exporter.export_full(output)
        click.echo(f"✅ Parquet file written to: {parquet_path}")
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


@main.command("export-parquet-by-province")
@click.option(
    "--data-dir",
    type=click.Path(path_type=Path),
    help="Base data directory (default: ./data)",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    help="Output directory for partitioned Parquet files (default: <data-dir>/parquet_output)",
)
def export_parquet_by_province(data_dir: Path, output_dir: Path):
    """Export processed NAR data to Parquet files partitioned by province"""
    exporter = NARParquetExporter(data_dir)
    try:
        created = exporter.export_by_province(output_dir)
        click.echo(f"✅ Wrote {len(created)} province partition(s):")
        for p in created:
            click.echo(f"   {p}")
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    main()
