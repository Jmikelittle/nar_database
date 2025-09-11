"""
Download module for NAR Database
Handles downloading and extracting the Statistics Canada NAR dataset
"""

import os
import zipfile
from pathlib import Path
from typing import Optional
import requests
from tqdm import tqdm


class NARDownloader:
    """Downloads and extracts Statistics Canada's National Address Register dataset"""
    
    # Base URL for NAR downloads
    NAR_BASE_URL = "https://www150.statcan.gc.ca/n1/pub/46-26-0002/2022001/"
    
    # Current known version (July 2025)
    DEFAULT_VERSION = "202507"
    
    # Homepage URL for checking available versions
    NAR_HOMEPAGE_URL = "https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022022001-eng.htm"
    
    def __init__(self, data_dir: Optional[Path] = None, version: Optional[str] = None):
        """
        Initialize the downloader
        
        Args:
            data_dir: Directory to store downloaded data. Defaults to ./data/
            version: Specific version to download (e.g., "202507"). If None, uses latest known.
        """
        self.data_dir = data_dir or Path("data")
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.version = version or self.DEFAULT_VERSION
        
        # Create directories if they don't exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def get_download_url(self, version: Optional[str] = None) -> str:
        """
        Get the download URL for a specific version
        
        Args:
            version: Version string (e.g., "202507"). If None, uses instance version.
            
        Returns:
            Full download URL for the ZIP file
        """
        version_to_use = version or self.version
        return f"{self.NAR_BASE_URL}{version_to_use}.zip"
    
    def find_latest_version(self) -> str:
        """
        Attempt to find the latest available version by checking the homepage
        
        Returns:
            Latest version string found, or default version if detection fails
        """
        try:
            import re
            response = requests.get(self.NAR_HOMEPAGE_URL, timeout=10)
            response.raise_for_status()
            
            # Look for version patterns in the format YYYYMM.zip
            version_pattern = r'(\d{6})\.zip'
            versions = re.findall(version_pattern, response.text)
            
            if versions:
                # Return the latest version (highest number)
                latest = max(versions)
                print(f"Detected latest version: {latest}")
                return latest
            else:
                print(f"Could not detect latest version, using default: {self.DEFAULT_VERSION}")
                return self.DEFAULT_VERSION
                
        except Exception as e:
            print(f"Error detecting latest version: {e}")
            print(f"Using default version: {self.DEFAULT_VERSION}")
            return self.DEFAULT_VERSION
    def download(self, force_download: bool = False, auto_detect_latest: bool = False) -> Path:
        """
        Download the NAR dataset ZIP file
        
        Args:
            force_download: If True, download even if file already exists
            auto_detect_latest: If True, attempt to detect and download latest version
            
        Returns:
            Path to the downloaded ZIP file
        """
        # Determine version to download
        if auto_detect_latest:
            version_to_download = self.find_latest_version()
        else:
            version_to_download = self.version
        
        download_url = self.get_download_url(version_to_download)
        zip_filename = f"nar_dataset_{version_to_download}.zip"
        zip_path = self.raw_dir / zip_filename
        
        if zip_path.exists() and not force_download:
            print(f"ZIP file already exists: {zip_path}")
            return zip_path
        
        print(f"Downloading National Address Register dataset...")
        print(f"Version: {version_to_download}")
        print(f"URL: {download_url}")
        
        try:
            response = requests.get(download_url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Get file size for progress bar
            total_size = int(response.headers.get('content-length', 0))
            
            with open(zip_path, 'wb') as file:
                if total_size > 0:
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                file.write(chunk)
                                pbar.update(len(chunk))
                else:
                    # Fallback if no content-length header
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)
            
            # Store the version that was actually downloaded
            self.version = version_to_download
            print(f"Download completed: {zip_path}")
            return zip_path
            
        except requests.RequestException as e:
            if zip_path.exists():
                zip_path.unlink()  # Remove incomplete file
            raise Exception(f"Failed to download NAR dataset from {download_url}: {e}")
    
    def extract(self, zip_path: Optional[Path] = None) -> Path:
        """
        Extract the ZIP file contents
        
        Args:
            zip_path: Path to ZIP file. If None, uses default location
            
        Returns:
            Path to the extracted directory
        """
        if zip_path is None:
            # Look for the most recent downloaded file
            zip_files = list(self.raw_dir.glob("nar_dataset_*.zip"))
            if zip_files:
                zip_path = max(zip_files, key=lambda p: p.stat().st_mtime)
                print(f"Using most recent ZIP file: {zip_path}")
            else:
                raise FileNotFoundError(f"No NAR ZIP files found in {self.raw_dir}")
        
        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP file not found: {zip_path}")
        
        extract_dir = self.raw_dir / "extracted"
        extract_dir.mkdir(exist_ok=True)
        
        print(f"Extracting {zip_path} to {extract_dir}...")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_file:
                zip_file.extractall(extract_dir)
            
            print(f"Extraction completed: {extract_dir}")
            return extract_dir
            
        except zipfile.BadZipFile as e:
            raise Exception(f"Invalid ZIP file: {e}")
    
    def list_csv_files(self, extract_dir: Optional[Path] = None) -> list[Path]:
        """
        List all CSV files in the extracted directory
        
        Args:
            extract_dir: Directory containing extracted files
            
        Returns:
            List of CSV file paths
        """
        if extract_dir is None:
            extract_dir = self.raw_dir / "extracted"
        
        if not extract_dir.exists():
            raise FileNotFoundError(f"Extract directory not found: {extract_dir}")
        
        csv_files = list(extract_dir.glob("**/*.csv"))
        csv_files.sort()
        
        print(f"Found {len(csv_files)} CSV files:")
        for csv_file in csv_files:
            print(f"  - {csv_file.name}")
        
        return csv_files
    
    def download_and_extract(self, force_download: bool = False, auto_detect_latest: bool = False) -> list[Path]:
        """
        Convenience method to download and extract in one step
        
        Args:
            force_download: If True, download even if file already exists
            auto_detect_latest: If True, attempt to detect and download latest version
            
        Returns:
            List of CSV file paths
        """
        zip_path = self.download(force_download=force_download, auto_detect_latest=auto_detect_latest)
        extract_dir = self.extract(zip_path)
        csv_files = self.list_csv_files(extract_dir)
        return csv_files
