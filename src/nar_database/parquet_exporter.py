"""
Parquet export module for NAR Database
Converts processed CSV data to Parquet format for efficient web-based querying
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd


class NARParquetExporter:
    """Exports NAR data to Parquet format for DuckDB-wasm web queries"""

    # NAR CSV columns to include in Parquet output
    NAR_COLUMNS = [
        "LOC_GUID",
        "ADDR_GUID",
        "APT_NO_LABEL",
        "CIVIC_NO",
        "CIVIC_NO_SUFFIX",
        "OFFICIAL_STREET_NAME",
        "OFFICIAL_STREET_TYPE",
        "OFFICIAL_STREET_DIR",
        "PROV_CODE",
        "CSD_ENG_NAME",
        "CSD_FRE_NAME",
        "MAIL_STREET_NAME",
        "MAIL_STREET_TYPE",
        "MAIL_STREET_DIR",
        "MAIL_MUN_NAME",
        "MAIL_PROV_ABVN",
        "MAIL_POSTAL_CODE",
        "BG_X",
        "BG_Y",
        "BU_USE",
    ]

    def __init__(self, data_dir: Optional[Path] = None):
        """
        Initialize the exporter.

        Args:
            data_dir: Base data directory. Defaults to ./data/
        """
        self.data_dir = Path(data_dir) if data_dir else Path("data")
        self.raw_dir = self.data_dir / "raw" / "extracted"
        self.parquet_dir = self.data_dir / "parquet_output"

    def discover_csv_files(self) -> List[Path]:
        """Find all CSV files in the raw data directory."""
        if not self.raw_dir.exists():
            raise FileNotFoundError(
                f"Raw data directory not found: {self.raw_dir}. "
                "Run 'nar-db download' first."
            )

        csv_files = sorted(self.raw_dir.glob("**/*.csv"))
        return csv_files

    def _read_csv_file(self, csv_path: Path) -> pd.DataFrame:
        """
        Read a NAR CSV file and return a DataFrame with relevant columns.

        Args:
            csv_path: Path to the CSV file.

        Returns:
            DataFrame with NAR columns (only those present in the file).
        """
        df = pd.read_csv(
            csv_path,
            dtype=str,
            na_filter=False,
            low_memory=False,
        )

        # Keep only columns that are present in the file
        present_cols = [c for c in self.NAR_COLUMNS if c in df.columns]
        df = df[present_cols]

        return df

    def export_full(self, output_path: Optional[Path] = None) -> Path:
        """
        Export all NAR CSV files to a single Parquet file.

        Args:
            output_path: Path for the output Parquet file.
                         Defaults to data/parquet_output/nar_addresses.parquet

        Returns:
            Path to the created Parquet file.
        """
        if output_path is None:
            self.parquet_dir.mkdir(parents=True, exist_ok=True)
            output_path = self.parquet_dir / "nar_addresses.parquet"
        else:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

        csv_files = self.discover_csv_files()
        if not csv_files:
            raise FileNotFoundError("No CSV files found. Run 'nar-db download' first.")

        print(f"Exporting {len(csv_files)} CSV file(s) to Parquet...")

        chunks: List[pd.DataFrame] = []
        for csv_path in csv_files:
            print(f"  Reading {csv_path.name}...")
            df = self._read_csv_file(csv_path)
            if not df.empty:
                chunks.append(df)

        if not chunks:
            raise ValueError("No data found in CSV files.")

        combined = pd.concat(chunks, ignore_index=True)
        total_rows = len(combined)
        print(f"Writing {total_rows:,} records to {output_path}...")

        combined.to_parquet(output_path, compression="snappy", index=False)

        # Write metadata sidecar
        self._write_metadata(
            self.parquet_dir / "metadata.json",
            schema=list(combined.columns),
            total_rows=total_rows,
            files_processed=[f.name for f in csv_files],
        )

        print(f"✅ Parquet export complete: {output_path}")
        return output_path

    def export_by_province(
        self, output_dir: Optional[Path] = None
    ) -> List[Path]:
        """
        Export NAR data partitioned by province.

        Creates one Parquet file per province under:
            <output_dir>/province=<CODE>/addresses.parquet

        Args:
            output_dir: Root directory for partitioned output.
                        Defaults to data/parquet_output/

        Returns:
            List of paths to created Parquet files.
        """
        if output_dir is None:
            output_dir = self.parquet_dir
        else:
            output_dir = Path(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)

        csv_files = self.discover_csv_files()
        if not csv_files:
            raise FileNotFoundError("No CSV files found. Run 'nar-db download' first.")

        print(f"Reading {len(csv_files)} CSV file(s)...")
        chunks: List[pd.DataFrame] = []
        for csv_path in csv_files:
            print(f"  Reading {csv_path.name}...")
            df = self._read_csv_file(csv_path)
            if not df.empty:
                chunks.append(df)

        if not chunks:
            raise ValueError("No data found in CSV files.")

        combined = pd.concat(chunks, ignore_index=True)

        # Determine the province column name (prefer PROV_CODE)
        province_col = None
        for candidate in ("PROV_CODE", "MAIL_PROV_ABVN"):
            if candidate in combined.columns:
                province_col = candidate
                break

        if province_col is None:
            raise ValueError(
                "No province column found in CSV data. "
                "Expected PROV_CODE or MAIL_PROV_ABVN."
            )

        created_files: List[Path] = []
        provinces = combined[province_col].str.strip().str.upper().unique()
        provinces = [p for p in sorted(provinces) if p]

        print(f"Writing Parquet files for {len(provinces)} province(s)...")
        all_schema: Optional[List[str]] = None
        files_info: dict = {}

        for province in provinces:
            province_df = combined[
                combined[province_col].str.strip().str.upper() == province
            ].copy()

            if province_df.empty:
                continue

            province_dir = output_dir / f"province={province}"
            province_dir.mkdir(parents=True, exist_ok=True)
            parquet_path = province_dir / "addresses.parquet"

            print(f"  Writing {len(province_df):,} records for {province}...")
            province_df.to_parquet(parquet_path, compression="snappy", index=False)
            created_files.append(parquet_path)

            if all_schema is None:
                all_schema = list(province_df.columns)
            files_info[province] = len(province_df)

        # Write metadata
        total_rows = sum(files_info.values())
        self._write_metadata(
            output_dir / "metadata.json",
            schema=all_schema or [],
            total_rows=total_rows,
            files_processed=[f.name for f in csv_files],
            provinces=files_info,
        )

        print(f"✅ Partitioned Parquet export complete: {output_dir}")
        print(f"   {len(created_files)} partition(s), {total_rows:,} total records")
        return created_files

    def _write_metadata(
        self,
        metadata_path: Path,
        schema: List[str],
        total_rows: int,
        files_processed: List[str],
        provinces: Optional[dict] = None,
    ) -> None:
        """Write a JSON metadata file alongside the Parquet output."""
        metadata: dict = {
            "schema": schema,
            "total_rows": total_rows,
            "files_processed": files_processed,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "compression": "snappy",
        }
        if provinces is not None:
            metadata["provinces"] = provinces

        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)
