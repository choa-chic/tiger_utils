"""
downloader.py - Unified download logic for TIGER/Line data (by level: county, state, national)
"""

from pathlib import Path
from typing import List, Iterable, Literal
import asyncio
import httpx
from tiger_utils.utils.logger import get_logger, setup_logger
from .progress_manager import DownloadState, DownloadStateDB
from .url_patterns import construct_url, DATASET_TYPES, STATES
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
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/zip,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive"
    }
    for attempt in range(retries):
        logger.info(f"Attempt {attempt+1}/{retries} for {url}")
        try:
            headers = dict(browser_headers)
            if resume_pos > 0:
                headers['Range'] = f'bytes={resume_pos}-'
            mode = 'ab' if resume_pos > 0 else 'wb'
            async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=True) as client:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    with temp_path.open(mode) as f:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            f.write(chunk)
            temp_path.rename(output_path)
            file_size = output_path.stat().st_size
            logger.info(f"Successfully downloaded: {output_path} ({file_size} bytes)")
            if state:
                state.mark_completed(url, str(output_path), state_fips, file_size)
            return (True, url, f"Downloaded {file_size} bytes")
        except Exception as e:
            last_exception = e
            logger.warning(f"Attempt {attempt+1} failed: {e}")
            if state and temp_path.exists():
                state.mark_partial(url, str(temp_path), temp_path.stat().st_size, state_fips)
            delay = min(base_delay * (2 ** attempt), max_delay)
            if attempt < retries - 1:
                logger.info(f"Retrying in {delay}s...")
                await asyncio.sleep(delay)
    if state:
        state.mark_failed(url, str(output_path), str(last_exception), state_fips)
    return (False, url, f"Failed after {retries} attempts: {last_exception}")

async def download_urls(urls: Iterable[str], output_dir: Path, parallel: int = 8,
                        timeout: int = 60, state=None, state_fips: str = None) -> List[tuple]:
    """
    Download a list of URLs in parallel.
    Returns list of (success, url, message) tuples.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    tasks = []
    for url in urls:
        filename = url.split('/')[-1]
        output_path = output_dir / filename
        tasks.append(download_file(url, output_path, timeout=timeout, state=state, state_fips=state_fips))
    results = []
    for coro in asyncio.as_completed(tasks, timeout=None):
        result = await coro
        results.append(result)
        completed = sum(1 for r in results if r[0])
        logger.info(f"Progress: {completed}/{len(tasks)} completed")
    return results

async def download_data(state_fips: str, year: int, output_dir: Path,
                        dataset_types: List[str], level: Literal["county", "state", "national"] = "county",
                        parallel: int = 8, timeout: int = 60, state=None) -> List[tuple]:
    """
    Unified downloader that handles county, state, and national level downloads.
    Uses construct_url() to generate proper URLs for each level.
    
    Args:
        state_fips: State FIPS code (ignored for national level)
        year: Year to download
        output_dir: Output directory
        dataset_types: List of dataset types (EDGES, ADDR, COUNTY, STATE, etc.)
        level: Download level - "county", "state", or "national"
        parallel: Number of parallel downloads
        timeout: Request timeout in seconds
        state: DownloadState or DownloadStateDB for tracking
    
    Returns:
        List of (success, url, message) tuples
    """
    output_dir = output_dir / str(year) / state_fips
    output_dir.mkdir(parents=True, exist_ok=True)
    urls = []
    if level == "county":
        counties = get_county_list(state_fips, year, dataset_types[0], timeout)
        for county_fips in counties:
            for dataset_type in dataset_types:
                url = construct_url(year, state_fips, county_fips, dataset_type)
                urls.append(url)
        logger.info(f"Discovered {len(urls)} files for {len(counties)} counties in state {state_fips}")
    elif level == "state":
        for dataset_type in dataset_types:
            url = construct_url(year, state_fips, "", dataset_type)
            urls.append(url)
        logger.info(f"Generated {len(urls)} state-level URLs for state {state_fips}")
    elif level == "national":
        for dataset_type in dataset_types:
            url = construct_url(year, "", "", dataset_type)
            urls.append(url)
        logger.info(f"Generated {len(urls)} national-level URLs")
    else:
        raise ValueError(f"Invalid level: {level}. Must be 'county', 'state', or 'national'")
    return await download_urls(urls, output_dir, parallel, timeout, state, state_fips)

async def download_county_data(state_fips: str, year: int, output_dir: Path,
                               dataset_types: List[str], parallel: int = 8,
                               timeout: int = 60, state=None) -> List[tuple]:
    """
    Convenience wrapper for county-level downloads.
    """
    return await download_data(state_fips, year, output_dir, dataset_types,
                               level="county", parallel=parallel, timeout=timeout, state=state)