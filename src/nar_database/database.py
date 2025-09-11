"""
Database module for NAR Database
Handles SQLite database creation and management for the National Address Register
"""

import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd
from contextlib import contextmanager


class NARDatabase:
    """Manages SQLite database for National Address Register data"""
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize the database manager
        
        Args:
            db_path: Path to SQLite database file. Defaults to ./data/database/nar.db
        """
        if db_path is None:
            db_path = Path("data") / "database" / "nar.db"
        
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def create_schema(self):
        """Create the database schema for NAR data"""
        schema_sql = """
        -- Main addresses table
        CREATE TABLE IF NOT EXISTS addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address_id TEXT UNIQUE,
            street_number TEXT,
            street_name TEXT,
            street_type TEXT,
            street_direction TEXT,
            unit_number TEXT,
            postal_code TEXT,
            city TEXT,
            province TEXT,
            latitude REAL,
            longitude REAL,
            source_file TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_postal_code ON addresses(postal_code);
        CREATE INDEX IF NOT EXISTS idx_city_province ON addresses(city, province);
        CREATE INDEX IF NOT EXISTS idx_street_name ON addresses(street_name);
        CREATE INDEX IF NOT EXISTS idx_coordinates ON addresses(latitude, longitude);
        
        -- Metadata table to track data versions and updates
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        with self.get_connection() as conn:
            conn.executescript(schema_sql)
        
        print(f"Database schema created: {self.db_path}")
    
    def insert_addresses_batch(self, addresses: List[Dict[str, Any]], batch_size: int = 10000):
        """
        Insert addresses in batches for efficient processing
        
        Args:
            addresses: List of address dictionaries
            batch_size: Number of records to insert at once
        """
        insert_sql = """
        INSERT OR REPLACE INTO addresses 
        (address_id, street_number, street_name, street_type, street_direction,
         unit_number, postal_code, city, province, latitude, longitude, source_file)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        with self.get_connection() as conn:
            for i in range(0, len(addresses), batch_size):
                batch = addresses[i:i + batch_size]
                data_tuples = [
                    (
                        addr.get('address_id'),
                        addr.get('street_number'),
                        addr.get('street_name'),
                        addr.get('street_type'),
                        addr.get('street_direction'),
                        addr.get('unit_number'),
                        addr.get('postal_code'),
                        addr.get('city'),
                        addr.get('province'),
                        addr.get('latitude'),
                        addr.get('longitude'),
                        addr.get('source_file')
                    )
                    for addr in batch
                ]
                
                conn.executemany(insert_sql, data_tuples)
                print(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")
    
    def query_by_postal_code(self, postal_code: str) -> List[sqlite3.Row]:
        """Query addresses by postal code"""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM addresses WHERE postal_code = ?",
                (postal_code.upper().replace(' ', ''),)
            )
            return cursor.fetchall()
    
    def query_by_city_province(self, city: str, province: str) -> List[sqlite3.Row]:
        """Query addresses by city and province"""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM addresses WHERE city = ? AND province = ?",
                (city.upper(), province.upper())
            )
            return cursor.fetchall()
    
    def query_by_coordinates(self, lat: float, lon: float, radius: float = 0.01) -> List[sqlite3.Row]:
        """
        Query addresses within a coordinate radius
        
        Args:
            lat: Latitude
            lon: Longitude
            radius: Search radius in degrees (approximately 1km ≈ 0.01 degrees)
        """
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM addresses 
                WHERE latitude BETWEEN ? AND ? 
                AND longitude BETWEEN ? AND ?
                """,
                (lat - radius, lat + radius, lon - radius, lon + radius)
            )
            return cursor.fetchall()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        with self.get_connection() as conn:
            stats = {}
            
            # Total addresses
            cursor = conn.execute("SELECT COUNT(*) FROM addresses")
            stats['total_addresses'] = cursor.fetchone()[0]
            
            # Unique postal codes
            cursor = conn.execute("SELECT COUNT(DISTINCT postal_code) FROM addresses")
            stats['unique_postal_codes'] = cursor.fetchone()[0]
            
            # Unique cities
            cursor = conn.execute("SELECT COUNT(DISTINCT city) FROM addresses")
            stats['unique_cities'] = cursor.fetchone()[0]
            
            # Provinces
            cursor = conn.execute("SELECT province, COUNT(*) FROM addresses GROUP BY province")
            stats['addresses_by_province'] = dict(cursor.fetchall())
            
            return stats
    
    def update_metadata(self, key: str, value: str):
        """Update metadata key-value pair"""
        with self.get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
                (key, value)
            )
    
    def get_metadata(self, key: str) -> Optional[str]:
        """Get metadata value by key"""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT value FROM metadata WHERE key = ?", (key,))
            result = cursor.fetchone()
            return result[0] if result else None
