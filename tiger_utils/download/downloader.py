"""
downloader.py - Download logic and parallelization for TIGER/Line downloads
"""

from pathlib import Path
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
import time
try:
    import cloudscraper
    _scraper = cloudscraper.create_scraper()
    _use_cloudscraper = True
except ImportError:
    import requests
    _scraper = requests
    _use_cloudscraper = False
from tiger_utils.utils.logger import get_logger, setup_logger
from .progress_manager import DownloadState, DownloadStateDB
from .url_patterns import construct_url, get_county_list, DATASET_TYPES, STATES, COUNTY_LEVEL_TYPES

setup_logger()
logger = get_logger()

def download_file(url: str, output_path: Path, retries: int = 8, timeout: int = 60, 
                 state=None, state_fips: str = None) -> tuple:
    """
    Download a file with enhanced retry logic and partial download resume support.
    Returns (success: bool, url: str, message: str)
    """
    base_delay = 2
    max_delay = 60
    if output_path.exists():
        logger.info(f"File already exists: {output_path}")
        if state:
            state.mark_completed(url, str(output_path), state_fips, output_path.stat().st_size)
        return (True, url, "Already exists")
    temp_path = output_path.with_suffix('.tmp')
    resume_pos = 0
    last_exception = None
    if temp_path.exists():
        resume_pos = temp_path.stat().st_size
        logger.info(f"Resuming partial download: {temp_path} at {resume_pos} bytes")
    logger.info(f"Preparing to download: {url} -> {output_path}")
    browser_headers = {
        "User-Agent": "TIGERDownloader/1.0 (Research/Educational Use)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive"
    }
    for attempt in range(retries):
        logger.info(f"Attempt {attempt+1}/{retries} for {url}")
        try:
            headers = dict(browser_headers)
            if resume_pos > 0:
                headers['Range'] = f'bytes={resume_pos}-'
            # Use cloudscraper if available, else requests
            if _use_cloudscraper:
                logger.info(f"Using cloudscraper for: {url}")
                r = _scraper.get(url, stream=True, timeout=timeout, headers=headers)
            else:
                logger.info(f"Using requests for: {url}")
                r = _scraper.get(url, stream=True, timeout=timeout, headers=headers)
            r.raise_for_status()
            mode = 'ab' if resume_pos > 0 else 'wb'
            with open(temp_path, mode) as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            temp_path.rename(output_path)
            logger.info(f"Downloaded: {output_path}")
            if state:
                state.mark_completed(url, str(output_path), state_fips, output_path.stat().st_size)
            return (True, url, "Downloaded")
        except Exception as e:
            last_exception = e
            logger.warning(f"Download failed (attempt {attempt+1}/{retries}) for {url}: {e}")
            time.sleep(min(base_delay * (2 ** attempt), max_delay))
    if state:
        state.mark_failed(url, str(output_path), str(last_exception), state_fips)
    return (False, url, f"Failed after {retries} attempts")

def download_county_data(state_fips: str, year: int, output_dir: Path, 
                         dataset_types: List[str], parallel: int = 4, 
                         timeout: int = 60, state=None, 
                         discover_files: bool = False):
    """
    Download county-level data for a state.
    """
    logger.info(f"Starting county data download for state {state_fips}, year {year}, datasets: {dataset_types}")
    counties = get_county_list(state_fips, year)
    logger.info(f"Found {len(counties)} counties for state {state_fips}")
    download_tasks = []
    for dataset_type in dataset_types:
        logger.info(f"Preparing download tasks for dataset type: {dataset_type}")
        for county_fips in counties:
            url = construct_url(year, state_fips, county_fips, dataset_type)
            filename = os.path.basename(url)
            output_path = output_dir / state_fips / filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Task: {url} -> {output_path}")
            download_tasks.append((url, output_path, dataset_type, county_fips))
    successful = 0
    failed = 0
    not_found = 0
    logger.info(f"Starting parallel downloads with {parallel} workers")
    with ThreadPoolExecutor(max_workers=parallel) as executor:
        future_to_task = {executor.submit(download_file, url, output_path, 8, timeout, state, state_fips): (url, output_path) for url, output_path, _, _ in download_tasks}
        for future in as_completed(future_to_task):
            url, output_path = future_to_task[future]
            try:
                success, _, msg = future.result()
                if success:
                    logger.info(f"Download succeeded: {url}")
                    successful += 1
                else:
                    logger.info(f"Download failed: {url} ({msg})")
                    # Optionally, check msg for 'not found' or 404
                    if 'not found' in msg.lower() or '404' in msg:
                        not_found += 1
                    else:
                        failed += 1
            except Exception as e:
                logger.error(f"Error downloading {url}: {e}")
                failed += 1
    logger.info(f"Download summary for {state_fips}: Successful: {successful}, Failed: {failed}, Not found: {not_found}")
    return successful, failed, not_found
