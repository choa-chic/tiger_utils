"""
downloader.py - Download logic and parallelization for TIGER/Line downloads
"""

from pathlib import Path
import os
from typing import List, Iterable
import time
import httpx
import asyncio
from tiger_utils.utils.logger import get_logger, setup_logger
from .progress_manager import DownloadState, DownloadStateDB
from .url_patterns import construct_url, DATASET_TYPES, STATES, COUNTY_LEVEL_TYPES
from .discover import get_county_list

setup_logger()
logger = get_logger()

async def download_file(url: str, output_path: Path, retries: int = 8, timeout: int = 60, 
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
            logger.info(f"Using httpx (async) for: {url}")
            mode = 'ab' if resume_pos > 0 else 'wb'
            async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=True) as client:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    with open(temp_path, mode) as f:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
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
            await asyncio.sleep(min(base_delay * (2 ** attempt), max_delay))
    if state:
        state.mark_failed(url, str(output_path), str(last_exception), state_fips)
    return (False, url, f"Failed after {retries} attempts")

async def download_county_data(state_fips: str, year: int, output_dir: Path, 
                               dataset_types: List[str], parallel: int = 8, 
                               timeout: int = 60, state=None, 
                               discover_files: bool = False):
    """
    Download county-level data for a state.
    """
    logger.info(f"Starting county data download for state {state_fips}, year {year}, datasets: {dataset_types}")
    # Use 'EDGES' as the default dataset_type for county list scraping
    counties = get_county_list(state_fips, year, dataset_types[0] if dataset_types else 'EDGES')
    logger.info(f"Found {len(counties)} counties for state {state_fips}")
    download_tasks = []
    # Only honor discovered URLs when explicitly requested; otherwise download everything
    discovered_urls = None
    if discover_files and state is not None:
        if hasattr(state, 'data'):
            discovered_urls = set(state.data.get('discovered_urls', {}).get(state_fips, []))
        elif hasattr(state, 'get_pending_urls'):
            try:
                discovered_urls = set(state.get_pending_urls(state_fips))
            except Exception:
                discovered_urls = None
    for dataset_type in dataset_types:
        logger.info(f"Preparing download tasks for dataset type: {dataset_type}")
        for county_fips in counties:
            url = construct_url(year, state_fips, county_fips, dataset_type)
            if discover_files and discovered_urls is not None and url not in discovered_urls:
                logger.debug(f"Skipping non-discovered URL: {url}")
                continue
            filename = os.path.basename(url)
            output_path = output_dir / state_fips / filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Task: {url} -> {output_path}")
            download_tasks.append((url, output_path, dataset_type, county_fips))
    successful = 0
    failed = 0
    not_found = 0
    logger.info(f"Starting parallel downloads with {parallel} workers (asyncio)")
    sem = asyncio.Semaphore(parallel)

    async def sem_download_file(*args, **kwargs):
        async with sem:
            return await download_file(*args, **kwargs)

    tasks = [sem_download_file(url, output_path, 8, timeout, state, state_fips) for url, output_path, _, _ in download_tasks]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, result in enumerate(results):
        url, output_path, _, _ = download_tasks[i]
        if isinstance(result, Exception):
            logger.error(f"Error downloading {url}: {result}")
            failed += 1
        else:
            success, _, msg = result
            if success:
                logger.info(f"Download succeeded: {url}")
                successful += 1
            else:
                logger.info(f"Download failed: {url} ({msg})")
                if 'not found' in msg.lower() or '404' in msg:
                    not_found += 1
                else:
                    failed += 1
    logger.info(f"Download summary for {state_fips}: Successful: {successful}, Failed: {failed}, Not found: {not_found}")
    return successful, failed, not_found


async def download_discovered_urls(state_fips: str, urls: Iterable[str], output_dir: Path,
                                   parallel: int = 8, timeout: int = 60, state=None):
    """
    Download the provided URLs for a state; intended for previously discovered-but-missing files.
    """
    urls = list(dict.fromkeys(urls))  # de-duplicate while preserving order
    logger.info(f"Starting discovered downloads for state {state_fips}; {len(urls)} pending")
    download_tasks = []
    for url in urls:
        filename = os.path.basename(url)
        output_path = output_dir / state_fips / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        download_tasks.append((url, output_path))
    sem = asyncio.Semaphore(parallel)

    async def sem_download_file(*args, **kwargs):
        async with sem:
            return await download_file(*args, **kwargs)

    tasks = [sem_download_file(url, output_path, 8, timeout, state, state_fips) for url, output_path in download_tasks]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    successful = 0
    failed = 0
    not_found = 0
    for i, result in enumerate(results):
        url, output_path = download_tasks[i]
        if isinstance(result, Exception):
            logger.error(f"Error downloading {url}: {result}")
            failed += 1
        else:
            success, _, msg = result
            if success:
                logger.info(f"Download succeeded: {url}")
                successful += 1
            else:
                logger.info(f"Download failed: {url} ({msg})")
                if 'not found' in msg.lower() or '404' in msg:
                    not_found += 1
                else:
                    failed += 1
    logger.info(f"Discovered download summary for {state_fips}: Successful: {successful}, Failed: {failed}, Not found: {not_found}")
    return successful, failed, not_found
