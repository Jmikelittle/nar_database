"""
Tests for the NAR downloader module
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch
from nar_database.downloader import NARDownloader


class TestNARDownloader:
    """Test cases for NARDownloader"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.test_data_dir = Path("test_data")
        self.downloader = NARDownloader(self.test_data_dir)
    
    def test_init(self):
        """Test downloader initialization"""
        assert self.downloader.data_dir == self.test_data_dir
        assert self.downloader.raw_dir == self.test_data_dir / "raw"
        assert self.downloader.processed_dir == self.test_data_dir / "processed"
    
    def test_init_default_dir(self):
        """Test downloader with default directory"""
        downloader = NARDownloader()
        assert downloader.data_dir == Path("data")
    
    @patch('nar_database.downloader.requests.get')
    def test_download_success(self, mock_get):
        """Test successful download"""
        # Mock response
        mock_response = Mock()
        mock_response.headers = {'content-length': '1000'}
        mock_response.iter_content.return_value = [b'test data chunk']
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Ensure directory exists
        self.downloader.raw_dir.mkdir(parents=True, exist_ok=True)
        
        result = self.downloader.download()
        
        assert result == self.downloader.raw_dir / "nar_dataset.zip"
        assert mock_get.called
    
    def test_discover_csv_files_empty(self):
        """Test CSV file discovery with no files"""
        extract_dir = self.test_data_dir / "raw" / "extracted"
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        csv_files = self.downloader.list_csv_files(extract_dir)
        assert csv_files == []
    
    def teardown_method(self):
        """Clean up after tests"""
        import shutil
        if self.test_data_dir.exists():
            shutil.rmtree(self.test_data_dir)
