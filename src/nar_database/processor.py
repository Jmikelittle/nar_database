"""
CSV processor module for NAR Database
Handles reading, cleaning, and processing CSV files from the NAR dataset
"""

import csv
from pathlib import Path
from typing import List, Dict, Any, Iterator, Optional
import pandas as pd
from tqdm import tqdm
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import sqlite3


class NARProcessor:
    """Processes CSV files from the National Address Register dataset"""
    
    def __init__(self, data_dir: Optional[Path] = None):
        """
        Initialize the processor
        
        Args:
            data_dir: Directory containing the data. Defaults to ./data/
        """
        self.data_dir = data_dir or Path("data")
        self.raw_dir = self.data_dir / "raw" / "extracted"
        self.processed_dir = self.data_dir / "processed"
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def discover_csv_files(self) -> List[Path]:
        """Find all CSV files in the raw data directory"""
        if not self.raw_dir.exists():
            raise FileNotFoundError(f"Raw data directory not found: {self.raw_dir}")
        
        csv_files = list(self.raw_dir.glob("**/*.csv"))
        csv_files.sort()
        
        print(f"Found {len(csv_files)} CSV files to process:")
        for csv_file in csv_files:
            print(f"  - {csv_file.name}")
        
        return csv_files
    
    def analyze_csv_structure(self, csv_path: Path) -> Dict[str, Any]:
        """
        Analyze the structure of a CSV file
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Dictionary with file analysis results
        """
        try:
            # Read first few rows to analyze structure
            df_sample = pd.read_csv(csv_path, nrows=100)
            
            analysis = {
                'file_path': str(csv_path),
                'file_name': csv_path.name,
                'columns': list(df_sample.columns),
                'num_columns': len(df_sample.columns),
                'sample_rows': len(df_sample),
                'data_types': df_sample.dtypes.to_dict(),
            }
            
            # Try to estimate total rows (can be slow for large files)
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    total_rows = sum(1 for _ in f) - 1  # Subtract header
                analysis['estimated_total_rows'] = total_rows
            except Exception:
                analysis['estimated_total_rows'] = 'Unknown'
            
            return analysis
            
        except Exception as e:
            return {
                'file_path': str(csv_path),
                'file_name': csv_path.name,
                'error': str(e)
            }
    
    def standardize_column_names(self, columns: List[str]) -> Dict[str, str]:
        """
        Create a mapping from original column names to standardized names
        Based on actual NAR CSV structure (Address and Location files)
        
        Args:
            columns: List of original column names
            
        Returns:
            Dictionary mapping original -> standardized names
        """
        # Official NAR column mappings based on real data analysis
        standard_mapping = {}
        
        for col in columns:
            col_upper = col.upper().strip()
            
            # Primary identifiers
            if col_upper == 'ADDR_GUID':
                standard_mapping[col] = 'address_id'
            elif col_upper == 'LOC_GUID':
                standard_mapping[col] = 'location_id'
            
            # Address components
            elif col_upper == 'CIVIC_NO':
                standard_mapping[col] = 'street_number'
            elif col_upper == 'CIVIC_NO_SUFFIX':
                standard_mapping[col] = 'street_number_suffix'
            elif col_upper == 'APT_NO_LABEL':
                standard_mapping[col] = 'unit_number'
            
            # Official street information
            elif col_upper == 'OFFICIAL_STREET_NAME':
                standard_mapping[col] = 'street_name'
            elif col_upper == 'OFFICIAL_STREET_TYPE':
                standard_mapping[col] = 'street_type'
            elif col_upper == 'OFFICIAL_STREET_DIR':
                standard_mapping[col] = 'street_direction'
            
            # Mailing address components
            elif col_upper == 'MAIL_STREET_NAME':
                standard_mapping[col] = 'mail_street_name'
            elif col_upper == 'MAIL_STREET_TYPE':
                standard_mapping[col] = 'mail_street_type'
            elif col_upper == 'MAIL_STREET_DIR':
                standard_mapping[col] = 'mail_street_direction'
            elif col_upper == 'MAIL_MUN_NAME':
                standard_mapping[col] = 'mail_city'
            elif col_upper == 'MAIL_PROV_ABVN':
                standard_mapping[col] = 'mail_province'
            elif col_upper == 'MAIL_POSTAL_CODE':
                standard_mapping[col] = 'postal_code'
            
            # Geographic information
            elif col_upper == 'PROV_CODE':
                standard_mapping[col] = 'province_code'
            elif col_upper == 'CSD_ENG_NAME':
                standard_mapping[col] = 'city'
            elif col_upper == 'CSD_FRE_NAME':
                standard_mapping[col] = 'city_french'
            
            # Coordinates (from Location files)
            elif col_upper == 'BG_LATITUDE':
                standard_mapping[col] = 'latitude'
            elif col_upper == 'BG_LONGITUDE':
                standard_mapping[col] = 'longitude'
            
            # Federal electoral district
            elif col_upper == 'FED_ENG_NAME':
                standard_mapping[col] = 'federal_district'
            elif col_upper == 'FED_FRE_NAME':
                standard_mapping[col] = 'federal_district_french'
            
            # Economic region
            elif col_upper == 'ER_ENG_NAME':
                standard_mapping[col] = 'economic_region'
            elif col_upper == 'ER_FRE_NAME':
                standard_mapping[col] = 'economic_region_french'
            
            # Keep other columns with cleaned names
            else:
                # Convert to lowercase and replace spaces/special chars with underscores
                standard_name = col.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
                standard_mapping[col] = standard_name
        
        return standard_mapping
    
    def process_csv_parallel(self, csv_files: List[Path], max_workers: Optional[int] = None, 
                           chunk_size: int = 50000, sample_size: int = None) -> Iterator[List[Dict[str, Any]]]:
        """
        Process multiple CSV files in parallel using multiple CPU cores
        
        Args:
            csv_files: List of CSV file paths to process
            max_workers: Number of parallel workers (default: CPU count)
            chunk_size: Records per chunk for processing
            sample_size: If specified, only process this many records per file (for testing)
            
        Yields:
            List of processed address records
        """
        if max_workers is None:
            max_workers = min(mp.cpu_count(), len(csv_files))
        
        print(f"🚀 Processing {len(csv_files)} CSV files with {max_workers} parallel workers")
        print(f"📦 Chunk size: {chunk_size:,} records")
        if sample_size:
            print(f"🧪 Sample mode: {sample_size:,} records per file")
            
        # Process files in parallel
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all files for processing
            future_to_file = {
                executor.submit(self._process_single_file_optimized, csv_file, chunk_size, sample_size): csv_file
                for csv_file in csv_files
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_file):
                csv_file = future_to_file[future]
                try:
                    file_results = future.result()
                    for chunk in file_results:
                        if chunk:  # Only yield non-empty chunks
                            yield chunk
                except Exception as e:
                    print(f"❌ Error processing {csv_file.name}: {e}")
    
    def _process_single_file_optimized(self, csv_path: Path, chunk_size: int = 50000, 
                                     sample_size: int = None) -> List[List[Dict[str, Any]]]:
        """
        Optimized processing of a single CSV file using vectorized pandas operations
        
        Args:
            csv_path: Path to the CSV file
            chunk_size: Number of records per chunk
            sample_size: Maximum records to process (for testing)
            
        Returns:
            List of processed record chunks
        """
        results = []
        
        try:
            # Read file with optimized pandas settings
            read_kwargs = {
                'chunksize': chunk_size,
                'dtype': str,  # Read all as strings to avoid type inference overhead
                'na_filter': False,  # Don't convert to NaN
                'low_memory': False,  # Use more memory for speed
            }
            
            if sample_size:
                read_kwargs['nrows'] = sample_size
            
            chunk_num = 0
            total_processed = 0
            
            for chunk_df in pd.read_csv(csv_path, **read_kwargs):
                chunk_num += 1
                
                # Standardize column names
                chunk_df.columns = [self.standardize_column_names({col: col})[col] 
                                  for col in chunk_df.columns]
                
                # Vectorized cleaning operations
                processed_chunk = self._clean_chunk_vectorized(chunk_df, csv_path.name)
                
                if not processed_chunk.empty:
                    # Convert to list of dictionaries
                    records = processed_chunk.to_dict('records')
                    total_processed += len(records)
                    results.append(records)
                    print(f"  📊 {csv_path.name} chunk {chunk_num}: {len(records):,} records")
                
                # Break if we've hit the sample limit
                if sample_size and total_processed >= sample_size:
                    break
                    
        except Exception as e:
            print(f"❌ Error processing {csv_path.name}: {e}")
        
        return results
    
    def _clean_chunk_vectorized(self, chunk_df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """
        Vectorized cleaning of a data chunk - much faster than row-by-row processing
        
        Args:
            chunk_df: DataFrame chunk to clean
            source_file: Name of source file
            
        Returns:
            Cleaned DataFrame
        """
        # Create a copy to avoid modifying original
        df = chunk_df.copy()
        
        # Add source file column
        df['source_file'] = source_file
        
        # Vectorized string cleaning
        string_columns = ['address_id', 'street_number', 'street_name', 'street_type', 
                         'street_direction', 'unit_number', 'city', 'province']
        
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        
        # Special formatting
        if 'street_name' in df.columns:
            df['street_name'] = df['street_name'].str.title()
        if 'street_type' in df.columns:
            df['street_type'] = df['street_type'].str.upper()
        if 'street_direction' in df.columns:
            df['street_direction'] = df['street_direction'].str.upper()
        if 'city' in df.columns:
            df['city'] = df['city'].str.title()
        if 'province' in df.columns:
            df['province'] = df['province'].str.upper()
            
        # Clean postal codes vectorized
        if 'postal_code' in df.columns:
            df['postal_code'] = df['postal_code'].apply(self.clean_postal_code)
        
        # Convert coordinates to numeric
        for coord_col in ['latitude', 'longitude']:
            if coord_col in df.columns:
                df[coord_col] = pd.to_numeric(df[coord_col], errors='coerce')
        
        # Filter out invalid records (must have street name and postal code)
        df = df.dropna(subset=['street_name', 'postal_code'])
        df = df[
            (df['street_name'].str.strip() != '') & 
            (df['postal_code'].str.strip() != '')
        ]
        
        return df

    def process_csv_files(self, csv_files: List[Path], chunk_size: int = 10000) -> Iterator[List[Dict[str, Any]]]:
        """
        Process a CSV file in chunks
        
        Args:
            csv_path: Path to the CSV file
            chunk_size: Number of rows to process at once
            
        Yields:
            Chunks of processed address records
        """
        print(f"Processing {csv_path.name}...")
        
        try:
            # Analyze file structure first
            analysis = self.analyze_csv_structure(csv_path)
            if 'error' in analysis:
                print(f"Error analyzing {csv_path.name}: {analysis['error']}")
                return
            
            column_mapping = self.standardize_column_names(analysis['columns'])
            
            # Process file in chunks
            chunk_iter = pd.read_csv(csv_path, chunksize=chunk_size)
            
            for chunk_num, chunk_df in enumerate(chunk_iter):
                # Rename columns to standard names
                chunk_df = chunk_df.rename(columns=column_mapping)
                
                # Clean and process data
                processed_records = []
                
                for _, row in chunk_df.iterrows():
                    record = self.clean_address_record(row, csv_path.name)
                    if record:  # Only add valid records
                        processed_records.append(record)
                
                if processed_records:
                    print(f"  Processed chunk {chunk_num + 1}: {len(processed_records)} records")
                    yield processed_records
                    
        except Exception as e:
            print(f"Error processing {csv_path.name}: {e}")
    
    def clean_address_record(self, row: pd.Series, source_file: str) -> Optional[Dict[str, Any]]:
        """
        Clean and validate an individual address record
        
        Args:
            row: Pandas Series representing one row
            source_file: Name of the source CSV file
            
        Returns:
            Cleaned address dictionary or None if invalid
        """
        try:
            # Extract and clean fields
            record = {
                'address_id': str(row.get('address_id', '')).strip(),
                'street_number': str(row.get('street_number', '')).strip(),
                'street_name': str(row.get('street_name', '')).strip().title(),
                'street_type': str(row.get('street_type', '')).strip().upper(),
                'street_direction': str(row.get('street_direction', '')).strip().upper(),
                'unit_number': str(row.get('unit_number', '')).strip(),
                'postal_code': self.clean_postal_code(str(row.get('postal_code', ''))),
                'city': str(row.get('city', '')).strip().title(),
                'province': str(row.get('province', '')).strip().upper(),
                'latitude': self.safe_float(row.get('latitude')),
                'longitude': self.safe_float(row.get('longitude')),
                'source_file': source_file
            }
            
            # Basic validation - must have at least street name and postal code
            if not record['street_name'] or not record['postal_code']:
                return None
            
            return record
            
        except Exception as e:
            print(f"Error cleaning record: {e}")
            return None
    
    def clean_postal_code(self, postal_code: str) -> str:
        """Clean Canadian postal code while preserving original CSV format"""
        if not postal_code or postal_code.lower() in ['nan', 'null', '']:
            return ''
        
        # Convert to uppercase and remove any existing spaces (in case they exist)
        clean_code = postal_code.strip().upper().replace(' ', '')
        
        # Validate Canadian postal code format: A1A1A1 (6 chars, alternating letter-digit)
        if len(clean_code) == 6 and clean_code.isalnum():
            # Keep original CSV format (no space): A1A1A1
            return clean_code
        
        # Return as-is if format is unexpected (for debugging)
        return clean_code
    
    def safe_float(self, value) -> Optional[float]:
        """Safely convert value to float"""
        if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'null']:
            return None
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def process_all_csv_files(self) -> int:
        """
        Process all CSV files in the raw data directory
        
        Returns:
            Total number of records processed
        """
        csv_files = self.discover_csv_files()
        total_records = 0
        
        print(f"Starting to process {len(csv_files)} CSV files...")
        
        for csv_file in csv_files:
            file_record_count = 0
            
            for chunk in self.process_csv_file(csv_file):
                file_record_count += len(chunk)
                total_records += len(chunk)
            
            print(f"Completed {csv_file.name}: {file_record_count} records")
        
        print(f"Total records processed: {total_records}")
        return total_records
