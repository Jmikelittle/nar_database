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
        Initialize the database manager with performance optimizations
        
        Args:
            db_path: Path to SQLite database file. Defaults to ./data/database/nar.db
        """
        if db_path is None:
            db_path = Path("data") / "database" / "nar.db"
        
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Apply performance optimizations
        self._optimize_database()
    
    def _optimize_database(self):
        """Apply SQLite performance optimizations for bulk operations"""
        with self.get_connection() as conn:
            # Performance optimizations for SSD and bulk inserts
            conn.execute("PRAGMA journal_mode = WAL")          # Write-Ahead Logging
            conn.execute("PRAGMA synchronous = NORMAL")        # Faster than FULL
            conn.execute("PRAGMA cache_size = 1000000")        # Large cache (1GB)
            conn.execute("PRAGMA temp_store = memory")         # Use RAM for temp tables
            conn.execute("PRAGMA mmap_size = 268435456")       # Memory-mapped I/O (256MB)
            conn.execute("PRAGMA page_size = 4096")            # Optimize for SSD
        
        print(f"🚀 Database optimized for high-performance operations")
    
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
        """Create the database schema for NAR data with all Address CSV fields"""
        schema_sql = """
        -- Main addresses table with all NAR fields
        CREATE TABLE IF NOT EXISTS addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Core identifiers
            loc_guid TEXT,
            addr_guid TEXT UNIQUE,
            
            -- Physical address components  
            apt_no_label TEXT,
            civic_no TEXT,
            civic_no_suffix TEXT,
            official_street_name TEXT,
            official_street_type TEXT,
            official_street_dir TEXT,
            
            -- Administrative/Geographic
            prov_code TEXT,
            csd_eng_name TEXT,
            csd_fre_name TEXT, 
            csd_type_eng_code TEXT,
            csd_type_fre_code TEXT,
            
            -- Mailing address components
            mail_street_name TEXT,
            mail_street_type TEXT,
            mail_street_dir TEXT,
            mail_mun_name TEXT,
            mail_prov_abvn TEXT,
            mail_postal_code TEXT,
            
            -- Dominion Land Survey coordinates
            bg_dls_lsd TEXT,
            bg_dls_qtr TEXT,
            bg_dls_sctn TEXT,
            bg_dls_twnshp TEXT,
            bg_dls_rng TEXT,
            bg_dls_mrd TEXT,
            
            -- Spatial coordinates
            bg_x REAL,
            bg_y REAL,
            
            -- Building information
            bu_n_civic_add TEXT,
            bu_use TEXT,
            
            -- Processing metadata
            source_file TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_mail_postal_code ON addresses(mail_postal_code);
        CREATE INDEX IF NOT EXISTS idx_official_street_name ON addresses(official_street_name);
        CREATE INDEX IF NOT EXISTS idx_csd_eng_name ON addresses(csd_eng_name);
        CREATE INDEX IF NOT EXISTS idx_prov_code ON addresses(prov_code);
        CREATE INDEX IF NOT EXISTS idx_addr_guid ON addresses(addr_guid);
        CREATE INDEX IF NOT EXISTS idx_loc_guid ON addresses(loc_guid);
        CREATE INDEX IF NOT EXISTS idx_coordinates ON addresses(bg_x, bg_y);
        
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
    
    def insert_addresses_batch(self, addresses: List[Dict[str, Any]], batch_size: int = 50000):
        """
        Insert addresses in batches for efficient processing
        
        Args:
            addresses: List of address dictionaries
            batch_size: Number of records to insert at once
        """
        insert_sql = """
        INSERT OR REPLACE INTO addresses 
        (loc_guid, addr_guid, apt_no_label, civic_no, civic_no_suffix, 
         official_street_name, official_street_type, official_street_dir,
         prov_code, csd_eng_name, csd_fre_name, csd_type_eng_code, csd_type_fre_code,
         mail_street_name, mail_street_type, mail_street_dir, mail_mun_name, 
         mail_prov_abvn, mail_postal_code, bg_dls_lsd, bg_dls_qtr, bg_dls_sctn,
         bg_dls_twnshp, bg_dls_rng, bg_dls_mrd, bg_x, bg_y, bu_n_civic_add, 
         bu_use, source_file)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        with self.get_connection() as conn:
            for i in range(0, len(addresses), batch_size):
                batch = addresses[i:i + batch_size]
                data_tuples = [
                    (
                        addr.get('LOC_GUID'),
                        addr.get('ADDR_GUID'), 
                        addr.get('APT_NO_LABEL'),
                        addr.get('CIVIC_NO'),
                        addr.get('CIVIC_NO_SUFFIX'),
                        addr.get('OFFICIAL_STREET_NAME'),
                        addr.get('OFFICIAL_STREET_TYPE'),
                        addr.get('OFFICIAL_STREET_DIR'),
                        addr.get('PROV_CODE'),
                        addr.get('CSD_ENG_NAME'),
                        addr.get('CSD_FRE_NAME'),
                        addr.get('CSD_TYPE_ENG_CODE'),
                        addr.get('CSD_TYPE_FRE_CODE'),
                        addr.get('MAIL_STREET_NAME'),
                        addr.get('MAIL_STREET_TYPE'),
                        addr.get('MAIL_STREET_DIR'),
                        addr.get('MAIL_MUN_NAME'),
                        addr.get('MAIL_PROV_ABVN'),
                        addr.get('MAIL_POSTAL_CODE'),
                        addr.get('BG_DLS_LSD'),
                        addr.get('BG_DLS_QTR'),
                        addr.get('BG_DLS_SCTN'),
                        addr.get('BG_DLS_TWNSHP'),
                        addr.get('BG_DLS_RNG'),
                        addr.get('BG_DLS_MRD'),
                        addr.get('BG_X'),
                        addr.get('BG_Y'),
                        addr.get('BU_N_CIVIC_ADD'),
                        addr.get('BU_USE'),
                        addr.get('source_file')
                    )
                    for addr in batch
                ]
                
                conn.executemany(insert_sql, data_tuples)
                print(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")
    
    def query_by_postal_code(self, postal_code: str) -> List[sqlite3.Row]:
        """Query addresses by postal code (handles both spaced and non-spaced input)"""
        with self.get_connection() as conn:
            # Normalize input: remove spaces and convert to uppercase
            clean_postal = postal_code.upper().replace(' ', '')
            
            cursor = conn.execute(
                "SELECT * FROM addresses WHERE mail_postal_code = ?",
                (clean_postal,)
            )
            return cursor.fetchall()
    
    def query_by_city_province(self, city: str, province: str) -> List[sqlite3.Row]:
        """Query addresses by city and province"""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM addresses WHERE csd_eng_name = ? AND mail_prov_abvn = ?",
                (city, province.upper())
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
