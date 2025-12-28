"""
discover.py - Directory scraping and file discovery logic for TIGER/Line downloads
"""


import sys
import time
from pathlib import Path
from typing import List, Dict, Set
from bs4 import BeautifulSoup
from tiger_utils.utils.logger import get_logger, setup_logger
import requests
import functools

setup_logger()
logger = get_logger()


def discover_state_files(state_fips: str, year: int, dataset_types: List[str], timeout: int = 30) -> Dict[str, Set[str]]:
    """
    Backward-compatible wrapper for single-state discovery using the efficient multi-state function.
    Args:
        state_fips: State FIPS code
        year: Year to download
        dataset_types: List of dataset types to discover
        timeout: Request timeout in seconds
    Returns:
        Dictionary mapping dataset type to set of discovered URLs for the given state
    """
    multi = discover_state_files_multi([state_fips], year, dataset_types, timeout)
    # Return {dataset_type: set_of_urls} for the single state
    return {dtype: multi[dtype].get(state_fips, set()) for dtype in dataset_types}
"""
discover.py - Directory scraping and file discovery logic for TIGER/Line downloads
"""


# cache to prevent redundant requests
@functools.lru_cache(maxsize=128)
def scrape_directory(url: str, timeout: int = 30) -> Set[str]:
    """
    Scrape a Census Bureau directory page to discover available files.
    Args:
        url: Directory URL to scrape
        timeout: Request timeout in seconds
    Returns:
        Set of file URLs found in the directory
    """
    try:
        logger.info(f"Requesting directory listing: {url}")
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        logger.info(f"Parsing HTML for links at: {url}")
        soup = BeautifulSoup(resp.text, "html.parser")
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href and not href.startswith("../"):
                links.add(href)
        logger.info(f"Found {len(links)} links in directory: {url}")
        return links
    except Exception as e:
        logger.warning(f"Failed to scrape {url}: {e}")
        return set()

def discover_state_files_multi(states_fips: List[str], year: int, dataset_types: List[str], timeout: int = 30) -> Dict[str, Dict[str, Set[str]]]:
    """
    Efficiently discover all available files for multiple states by scraping Census Bureau directories only once per dataset type.
    Args:
        states_fips: List of state FIPS codes
        year: Year to download
        dataset_types: List of dataset types to discover
        timeout: Request timeout in seconds
    Returns:
        Dictionary mapping dataset type to {state_fips: set of discovered URLs}
    """
    discovered = {dataset_type: {} for dataset_type in dataset_types}
    base_url = f"https://www2.census.gov/geo/tiger/TIGER{year}"
    logger.info(f"Starting multi-state discovery for states {states_fips}, year {year}, datasets: {dataset_types}")
    for dataset_type in dataset_types:
        dir_url = f"{base_url}/{dataset_type}/"
        logger.info(f"Scraping directory for dataset type: {dataset_type} at {dir_url}")
        links = scrape_directory(dir_url, timeout=timeout)
        # Group links by state FIPS
        state_map = {state: set() for state in states_fips}
        for l in links:
            # Example: tl_2025_06001_edges.zip or tl_2025_06001_addr.zip
            parts = l.split('_')
            if len(parts) >= 3 and parts[0] == f"tl" and parts[1] == str(year):
                state_county = parts[2]
                for state in states_fips:
                    if state_county.startswith(state):
                        state_map[state].add(f"{dir_url}{l}")
        for state, files in state_map.items():
            logger.info(f"Found {len(files)} candidate files for state {state} in {dataset_type}")
            discovered[dataset_type][state] = files
    logger.info(f"Multi-state discovery complete for year {year}")
    return discovered

def get_county_list(state_fips: str, year: int = 2025, dataset_type: str = 'EDGES', timeout: int = 30) -> list:
    """
    Scrape the Census directory for the given year/state/type and extract county FIPS codes from filenames.
    Returns a sorted list of unique 3-digit county FIPS codes for the state.
    """
    base_url = f"https://www2.census.gov/geo/tiger/TIGER{year}/{dataset_type}/"
    links = scrape_directory(base_url, timeout=timeout)
    county_fips_set = set()
    for l in links:
        # Example: tl_2025_06001_edges.zip or tl_2025_06001_addr.zip
        parts = l.split('_')
        if len(parts) >= 3 and parts[0] == 'tl' and parts[1] == str(year):
            state_county = parts[2]
            if state_county.startswith(state_fips) and len(state_county) == 5:
                county_fips = state_county[2:]
                if county_fips.isdigit():
                    county_fips_set.add(county_fips)
    return sorted(county_fips_set)