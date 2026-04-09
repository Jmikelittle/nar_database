"""
Tests for the NARParquetExporter module
"""

import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from nar_database.parquet_exporter import NARParquetExporter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_csv(directory: Path, filename: str, prov_col: str = "PROV_CODE") -> Path:
    """Write a minimal NAR-like CSV file and return its path."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    df = pd.DataFrame(
        {
            "ADDR_GUID": ["guid-1", "guid-2", "guid-3"],
            "CIVIC_NO": ["10", "20", "30"],
            "OFFICIAL_STREET_NAME": ["Main St", "Elm Ave", "Oak Blvd"],
            "OFFICIAL_STREET_TYPE": ["ST", "AVE", "BLVD"],
            prov_col: ["ON", "BC", "ON"],
            "MAIL_POSTAL_CODE": ["K1A0A6", "V5K0A1", "K2P1J6"],
            "CSD_ENG_NAME": ["Ottawa", "Vancouver", "Ottawa"],
        }
    )
    df.to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_data_dir(tmp_path: Path):
    """Return a temporary data directory with a sample CSV file."""
    raw_extracted = tmp_path / "raw" / "extracted"
    _make_csv(raw_extracted, "sample.csv")
    yield tmp_path
    shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.fixture()
def exporter(tmp_data_dir: Path) -> NARParquetExporter:
    return NARParquetExporter(tmp_data_dir)


# ---------------------------------------------------------------------------
# NARParquetExporter.__init__
# ---------------------------------------------------------------------------

class TestNARParquetExporterInit:
    def test_default_data_dir(self):
        exp = NARParquetExporter()
        assert exp.data_dir == Path("data")

    def test_custom_data_dir(self, tmp_path: Path):
        exp = NARParquetExporter(tmp_path)
        assert exp.data_dir == tmp_path
        assert exp.raw_dir == tmp_path / "raw" / "extracted"
        assert exp.parquet_dir == tmp_path / "parquet_output"


# ---------------------------------------------------------------------------
# discover_csv_files
# ---------------------------------------------------------------------------

class TestDiscoverCsvFiles:
    def test_discovers_csv(self, exporter: NARParquetExporter):
        files = exporter.discover_csv_files()
        assert len(files) == 1
        assert files[0].suffix == ".csv"

    def test_raises_if_raw_dir_missing(self, tmp_path: Path):
        exp = NARParquetExporter(tmp_path / "nonexistent")
        with pytest.raises(FileNotFoundError, match="Raw data directory not found"):
            exp.discover_csv_files()


# ---------------------------------------------------------------------------
# export_full
# ---------------------------------------------------------------------------

class TestExportFull:
    def test_creates_parquet_file(self, exporter: NARParquetExporter, tmp_path: Path):
        out = tmp_path / "output" / "full.parquet"
        result = exporter.export_full(out)
        assert result == out
        assert out.exists()

    def test_default_output_path(self, exporter: NARParquetExporter):
        result = exporter.export_full()
        assert result == exporter.parquet_dir / "nar_addresses.parquet"
        assert result.exists()

    def test_parquet_readable(self, exporter: NARParquetExporter, tmp_path: Path):
        out = tmp_path / "full.parquet"
        exporter.export_full(out)
        df = pd.read_parquet(out)
        assert len(df) == 3
        assert "OFFICIAL_STREET_NAME" in df.columns

    def test_metadata_written(self, exporter: NARParquetExporter):
        exporter.export_full()
        meta_path = exporter.parquet_dir / "metadata.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert "schema" in meta
        assert "total_rows" in meta
        assert meta["total_rows"] == 3
        assert meta["compression"] == "snappy"

    def test_raises_if_no_csv_files(self, tmp_path: Path):
        # Create raw dir but no CSV files
        (tmp_path / "raw" / "extracted").mkdir(parents=True)
        exp = NARParquetExporter(tmp_path)
        with pytest.raises(FileNotFoundError, match="No CSV files found"):
            exp.export_full()


# ---------------------------------------------------------------------------
# export_by_province
# ---------------------------------------------------------------------------

class TestExportByProvince:
    def test_creates_partitions(self, exporter: NARParquetExporter, tmp_path: Path):
        out_dir = tmp_path / "partitioned"
        files = exporter.export_by_province(out_dir)
        # Two provinces: ON and BC
        assert len(files) == 2
        partitions = {f.parent.name for f in files}
        assert "province=ON" in partitions
        assert "province=BC" in partitions

    def test_default_output_dir(self, exporter: NARParquetExporter):
        files = exporter.export_by_province()
        assert all(f.is_relative_to(exporter.parquet_dir) for f in files)

    def test_partition_content(self, exporter: NARParquetExporter, tmp_path: Path):
        out_dir = tmp_path / "partitioned"
        files = exporter.export_by_province(out_dir)

        on_file = next(f for f in files if "province=ON" in str(f))
        df_on = pd.read_parquet(on_file)
        assert len(df_on) == 2  # Two ON records in the fixture

        bc_file = next(f for f in files if "province=BC" in str(f))
        df_bc = pd.read_parquet(bc_file)
        assert len(df_bc) == 1

    def test_metadata_written(self, exporter: NARParquetExporter, tmp_path: Path):
        out_dir = tmp_path / "partitioned"
        exporter.export_by_province(out_dir)
        meta = json.loads((out_dir / "metadata.json").read_text())
        assert "provinces" in meta
        assert meta["provinces"]["ON"] == 2
        assert meta["provinces"]["BC"] == 1
        assert meta["total_rows"] == 3

    def test_raises_if_no_csv_files(self, tmp_path: Path):
        (tmp_path / "raw" / "extracted").mkdir(parents=True)
        exp = NARParquetExporter(tmp_path)
        with pytest.raises(FileNotFoundError, match="No CSV files found"):
            exp.export_by_province()

    def test_fallback_to_mail_prov_abvn(self, tmp_path: Path):
        """Should use MAIL_PROV_ABVN when PROV_CODE is absent."""
        raw_dir = tmp_path / "raw" / "extracted"
        _make_csv(raw_dir, "sample.csv", prov_col="MAIL_PROV_ABVN")
        exp = NARParquetExporter(tmp_path)
        files = exp.export_by_province()
        assert len(files) == 2
