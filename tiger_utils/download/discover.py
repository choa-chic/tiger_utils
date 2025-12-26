"""
discover.py - Directory scraping and file discovery logic for TIGER/Line downloads
"""

import sys
import time
from pathlib import Path
from typing import List, Dict, Set
from html.parser import HTMLParser
from tiger_utils.utils.logger import get_logger, setup_logger
import requests

setup_logger()
logger = get_logger()

class DirectoryParser(HTMLParser):
    """Parse HTML directory listings to extract file links."""
    def __init__(self):
        super().__init__()
        self.links = set()
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for attr, value in attrs:
                if attr == "href" and value and not value.startswith("../"):
                    self.links.add(value)
    def handle_endtag(self, tag):
        pass

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
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        parser = DirectoryParser()
        parser.feed(resp.text)
        return parser.links
    except Exception as e:
        logger.warning(f"Failed to scrape {url}: {e}")
        return set()

def discover_state_files(state_fips: str, year: int, dataset_types: List[str], timeout: int = 30) -> Dict[str, Set[str]]:
    """
    Discover all available files for a state by scraping Census Bureau directories.
    Args:
        state_fips: State FIPS code
        year: Year to download
        dataset_types: List of dataset types to discover
        timeout: Request timeout in seconds
    Returns:
        Dictionary mapping dataset type to set of discovered URLs
    """
    discovered = {}
    base_url = f"https://www2.census.gov/geo/tiger/TIGER{year}"
    for dataset_type in dataset_types:
        dir_url = f"{base_url}/{dataset_type}/"
        links = scrape_directory(dir_url, timeout=timeout)
        # Filter links for this state
        state_links = set(l for l in links if l.startswith(f"tl_{year}_{state_fips}"))
        discovered[dataset_type] = {f"{dir_url}{l}" for l in state_links}
    return discovered
