"""
Parquet exporter module for NAR Database.
Converts CSV files to partitioned Parquet format for efficient web queries via DuckDB-wasm.
"""

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Optional

import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

from .processor import NARProcessor


class NARParquetExporter:
    """
    Exports NAR CSV data to partitioned Parquet files.

    Streams data in chunks to minimise memory usage, partitions output by
    province, and optionally removes the source CSV files and ZIP archive
    after a successful export.
    """

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        parquet_dir: Optional[Path] = None,
        chunk_size: int = 50_000,
    ):
        """
        Initialise the exporter.

        Args:
            data_dir:    Root data directory (default: ./data).
            parquet_dir: Output directory for Parquet files
                         (default: ./data/parquet).
            chunk_size:  Number of rows per processing chunk.
        """
        if not PYARROW_AVAILABLE:
            raise ImportError(
                "pyarrow is required for Parquet export. "
                "Install it with: pip install 'nar-database[parquet]'"
            )

        self.data_dir = data_dir or Path("data")
        self.parquet_dir = parquet_dir or (self.data_dir / "parquet")
        self.chunk_size = chunk_size
        self.processor = NARProcessor(self.data_dir)

        self.parquet_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def export_csv_files(
        self,
        csv_files: List[Path],
        *,
        keep_csv: bool = False,
        zip_path: Optional[Path] = None,
        progress_callback=None,
    ) -> dict:
        """
        Convert a list of CSV files to province-partitioned Parquet files.

        Args:
            csv_files:         List of CSV paths to process.
            keep_csv:          When False (default) CSV files are deleted after
                               a successful export.
            zip_path:          Optional ZIP archive to delete after export.
            progress_callback: Optional callable(message: str) for progress
                               reporting.

        Returns:
            Summary dict with keys: provinces, total_rows, parquet_files,
            parquet_dir, metadata_path.
        """

        def _log(msg: str) -> None:
            if progress_callback:
                progress_callback(msg)

        _log(f"Exporting {len(csv_files)} CSV file(s) to Parquet…")
        _log(f"Output directory: {self.parquet_dir}")

        # Collect all processed data grouped by province so we can write
        # complete per-province files without keeping everything in RAM.
        province_writers: dict[str, pq.ParquetWriter] = {}
        province_row_counts: dict[str, int] = {}
        parquet_files: list[Path] = []
        arrow_schema: Optional[pa.Schema] = None

        try:
            for csv_path in csv_files:
                _log(f"  Processing {csv_path.name}…")
                for chunk_df in self._stream_csv(csv_path):
                    if chunk_df.empty:
                        continue

                    # Determine province column
                    province_col = self._province_column(chunk_df)

                    for province, province_df in self._split_by_province(
                        chunk_df, province_col
                    ):
                        table = pa.Table.from_pandas(
                            province_df, preserve_index=False
                        )
                        if arrow_schema is None:
                            arrow_schema = table.schema

                        if province not in province_writers:
                            prov_dir = self.parquet_dir / f"province={province}"
                            prov_dir.mkdir(parents=True, exist_ok=True)
                            out_path = prov_dir / "part-0.parquet"
                            parquet_files.append(out_path)
                            province_writers[province] = pq.ParquetWriter(
                                str(out_path),
                                table.schema,
                                compression="snappy",
                            )
                            province_row_counts[province] = 0

                        province_writers[province].write_table(table)
                        province_row_counts[province] += len(province_df)

        finally:
            for writer in province_writers.values():
                writer.close()

        total_rows = sum(province_row_counts.values())
        _log(
            f"✅ Exported {total_rows:,} rows across "
            f"{len(province_row_counts)} province(s)"
        )

        # Write metadata
        metadata_path = self._write_metadata(
            province_row_counts, arrow_schema, total_rows
        )
        _log(f"📄 Metadata written to {metadata_path}")

        # Cleanup
        if not keep_csv:
            _log("🗑️  Removing temporary CSV files…")
            self._delete_csv_files(csv_files)
            if zip_path and zip_path.exists():
                _log(f"🗑️  Removing ZIP archive {zip_path.name}…")
                zip_path.unlink()

        return {
            "provinces": sorted(province_row_counts.keys()),
            "province_row_counts": province_row_counts,
            "total_rows": total_rows,
            "parquet_files": parquet_files,
            "parquet_dir": self.parquet_dir,
            "metadata_path": metadata_path,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _stream_csv(self, csv_path: Path) -> Iterator[pd.DataFrame]:
        """Read a CSV file in chunks and yield standardised DataFrames."""
        read_kwargs = {
            "chunksize": self.chunk_size,
            "dtype": str,
            "na_filter": False,
            "low_memory": False,
        }
        col_map = None
        try:
            for chunk_df in pd.read_csv(csv_path, **read_kwargs):
                if col_map is None:
                    col_map = self.processor.standardize_column_names(
                        list(chunk_df.columns)
                    )
                chunk_df = chunk_df.rename(columns=col_map)
                chunk_df = self.processor._clean_chunk_vectorized(
                    chunk_df, csv_path.name
                )
                yield chunk_df
        except Exception as exc:
            print(f"❌ Error reading {csv_path.name}: {exc}")

    def _province_column(self, df: pd.DataFrame) -> str:
        """Return the name of the province column present in *df*."""
        for candidate in ("province", "province_code", "mail_province"):
            if candidate in df.columns:
                return candidate
        return "province"

    def _split_by_province(
        self, df: pd.DataFrame, province_col: str
    ) -> Iterator[tuple[str, pd.DataFrame]]:
        """Yield (province_label, subset_df) for each province in *df*."""
        if province_col not in df.columns:
            yield ("UNKNOWN", df)
            return

        for province, group in df.groupby(province_col, sort=False):
            label = str(province).strip().upper() if province else "UNKNOWN"
            if not label:
                label = "UNKNOWN"
            yield (label, group.reset_index(drop=True))

    def _write_metadata(
        self,
        province_row_counts: dict,
        arrow_schema: Optional["pa.Schema"],
        total_rows: int,
    ) -> Path:
        """Write a JSON metadata file alongside the Parquet directory."""
        schema_fields = []
        if arrow_schema is not None:
            for field in arrow_schema:
                schema_fields.append(
                    {"name": field.name, "type": str(field.type)}
                )

        metadata = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "total_rows": total_rows,
            "provinces": province_row_counts,
            "schema": schema_fields,
            "compression": "snappy",
            "partition_column": "province",
        }

        metadata_path = self.parquet_dir / "_metadata.json"
        metadata_path.write_text(json.dumps(metadata, indent=2))
        return metadata_path

    def _delete_csv_files(self, csv_files: List[Path]) -> None:
        """Delete CSV files and their parent directories when empty."""
        deleted = 0
        for csv_path in csv_files:
            try:
                if csv_path.exists():
                    csv_path.unlink()
                    deleted += 1
                # Remove parent directory if empty
                parent = csv_path.parent
                if parent.exists() and not any(parent.iterdir()):
                    shutil.rmtree(parent, ignore_errors=True)
            except OSError as exc:
                print(f"  Warning: could not delete {csv_path}: {exc}")

        # Also try to remove the extracted/ directory if now empty
        extracted_dir = self.data_dir / "raw" / "extracted"
        if extracted_dir.exists():
            try:
                shutil.rmtree(extracted_dir)
            except OSError:
                pass

        print(f"  Deleted {deleted} CSV file(s)")
