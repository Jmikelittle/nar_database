"""
CSV processor module for NAR Database
Handles reading, cleaning, and processing CSV files from the NAR dataset
"""

import csv
from pathlib import Path
from typing import List, Dict, Any, Iterator, Optional
import pandas as pd
from tqdm import tqdm


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
        
        Args:
            columns: List of original column names
            
        Returns:
            Dictionary mapping original -> standardized names
        """
        # This mapping will need to be customized based on actual NAR CSV structure
        # These are common address field mappings
        standard_mapping = {}
        
        for col in columns:
            col_lower = col.lower().strip()
            
            # Address ID fields
            if any(term in col_lower for term in ['id', 'identifier', 'key']):
                standard_mapping[col] = 'address_id'
            
            # Street number
            elif any(term in col_lower for term in ['street_number', 'civic_number', 'house_number']):
                standard_mapping[col] = 'street_number'
            
            # Street name
            elif any(term in col_lower for term in ['street_name', 'street']):
                standard_mapping[col] = 'street_name'
            
            # Street type
            elif any(term in col_lower for term in ['street_type', 'type']):
                standard_mapping[col] = 'street_type'
            
            # Direction
            elif any(term in col_lower for term in ['direction', 'dir']):
                standard_mapping[col] = 'street_direction'
            
            # Unit/apartment
            elif any(term in col_lower for term in ['unit', 'apt', 'apartment', 'suite']):
                standard_mapping[col] = 'unit_number'
            
            # Postal code
            elif any(term in col_lower for term in ['postal', 'postal_code', 'postcode']):
                standard_mapping[col] = 'postal_code'
            
            # City
            elif any(term in col_lower for term in ['city', 'municipality', 'town']):
                standard_mapping[col] = 'city'
            
            # Province
            elif any(term in col_lower for term in ['province', 'prov', 'state']):
                standard_mapping[col] = 'province'
            
            # Coordinates
            elif any(term in col_lower for term in ['latitude', 'lat']):
                standard_mapping[col] = 'latitude'
            elif any(term in col_lower for term in ['longitude', 'lon', 'lng']):
                standard_mapping[col] = 'longitude'
            
            else:
                # Keep original name if no mapping found
                standard_mapping[col] = col.lower().replace(' ', '_')
        
        return standard_mapping
    
    def process_csv_file(self, csv_path: Path, chunk_size: int = 10000) -> Iterator[List[Dict[str, Any]]]:
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
        """Clean and format Canadian postal code"""
        if not postal_code or postal_code.lower() in ['nan', 'null', '']:
            return ''
        
        # Remove spaces and convert to uppercase
        clean_code = postal_code.strip().upper().replace(' ', '')
        
        # Canadian postal code format: A1A1A1
        if len(clean_code) == 6 and clean_code.isalnum():
            return f"{clean_code[:3]} {clean_code[3:]}"
        
        return clean_code  # Return as-is if format is unexpected
    
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
