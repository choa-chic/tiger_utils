"""
progress_manager.py - Download state tracking for TIGER/Line downloads (JSON and DuckDB)
"""

import os
import time
from pathlib import Path
from typing import Dict, List, Set, Optional
from tiger_utils.utils.logger import get_logger, setup_logger

setup_logger()
logger = get_logger()

class DownloadState:
    """Track download state for resuming interrupted downloads (JSON backend)."""
    def __init__(self, state_file: Path):
        self.state_file = Path(state_file)
        self.data = self._load()
    
    def _load(self) -> Dict:
        if self.state_file.exists():
            import json
            with open(self.state_file, "r") as f:
                return json.load(f)
        return self._default_state()
    
    def _default_state(self) -> Dict:
        return {
            "files": {},
            "completed": [],
            "failed": [],
            "states": {},
            "discovered_urls": {}
        }
    
    def save(self):
        import json
        with open(self.state_file, "w") as f:
            json.dump(self.data, f, indent=2)
    
    def mark_completed(self, url: str, output_path: str, state_fips: str = None, file_size: int = None):
        self.data["files"][output_path] = {
            "url": url,
            "status": "completed",
            "state_fips": state_fips,
            "size": file_size,
            "timestamp": time.time()
        }
        if url not in self.data["completed"]:
            self.data["completed"].append(url)
        if url in self.data["failed"]:
            self.data["failed"].remove(url)
        self.save()
    
    def mark_failed(self, url: str, output_path: str, error: str, state_fips: str = None):
        self.data["files"][output_path] = {
            "url": url,
            "status": "failed",
            "state_fips": state_fips,
            "error": error,
            "timestamp": time.time()
        }
        if url not in self.data["failed"]:
            self.data["failed"].append(url)
        self.save()
    
    def mark_partial(self, url: str, output_path: str, bytes_downloaded: int, state_fips: str = None):
        self.data["files"][output_path] = {
            "url": url,
            "status": "partial",
            "state_fips": state_fips,
            "bytes_downloaded": bytes_downloaded,
            "timestamp": time.time()
        }
        self.save()
    
    def get_partial_size(self, output_path: str) -> int:
        entry = self.data["files"].get(output_path)
        if entry and entry.get("status") == "partial":
            return entry.get("bytes_downloaded", 0)
        return 0
    
    def is_completed(self, output_path: str) -> bool:
        entry = self.data["files"].get(output_path)
        return entry and entry.get("status") == "completed"
    
    def set_discovered_urls(self, state_fips: str, urls: Set[str]):
        self.data["discovered_urls"][state_fips] = list(urls)
        self.save()
    
    def get_pending_urls(self, state_fips: str) -> List[str]:
        discovered = set(self.data["discovered_urls"].get(state_fips, []))
        completed = set(self.data["completed"])
        failed = set(self.data["failed"])
        return list(discovered - completed - failed)
    
    def get_urls_for_state(self, state_fips: str) -> Dict[str, List[str]]:
        completed = [
            url for url in self.data["completed"]
            if self.data["files"].get(url, {}).get("state_fips") == state_fips
        ]
        failed = [
            url for url in self.data["failed"]
            if self.data["files"].get(url, {}).get("state_fips") == state_fips
        ]
        return {"completed": completed, "failed": failed}
    
    def get_download_progress(self, state_fips: str) -> Dict:
        discovered = len(self.data["discovered_urls"].get(state_fips, []))
        urls = self.get_urls_for_state(state_fips)
        pending = self.get_pending_urls(state_fips)
        return {
            "discovered": discovered,
            "completed": len(urls["completed"]),
            "failed": len(urls["failed"]),
            "pending": len(pending),
            "pending_urls": pending[:10]
        }

def sync_state_with_filesystem(output_dir: Path, download_state, state_list):
    """
    Scan the output directory for downloaded files and ensure the state database is consistent.
    Mark files as completed in the state if they exist and are not already marked.
    Optionally, warn about files marked as completed in the state but missing on disk.
    """
    logger.info("=" * 70)
    logger.info("Synchronizing state database with file system...")
    logger.info(f"Output directory: {output_dir}")
    updated = 0
    missing = 0
    
    for state_fips in state_list:
        logger.info(f"\n--- Checking state: {state_fips} ---")
        discovered_urls = set()
        
        # Get discovered URLs from either JSON or DuckDB backend
        if hasattr(download_state, "data"):
            logger.debug(f"Using JSON backend for state {state_fips}")
            discovered_urls = set(
                download_state.data.get("discovered_urls", {}).get(state_fips, [])
            )
        elif hasattr(download_state, "get_pending_urls"):
            logger.debug(f"Using DuckDB backend for state {state_fips}")
            try:
                # Get ALL discovered URLs (not just pending)
                discovered = download_state.conn.execute(
                    "SELECT url FROM discovered_urls WHERE state_fips = ?",
                    [state_fips]
                ).fetchall()
                discovered_urls = set(row[0] for row in discovered)
            except Exception as e:
                logger.warning(f"Failed to get discovered URLs for {state_fips}: {e}")
                discovered_urls = set()
        
        logger.info(f"Discovered {len(discovered_urls)} URLs for state {state_fips}")
        
        # Check discovered files on disk
        for url in discovered_urls:
            filename = os.path.basename(url)
            file_path = output_dir / state_fips / filename
            logger.debug(f"Checking file: {file_path} (from URL: {url})")
            
            if file_path.exists():
                logger.debug(f"File exists: {file_path}")
                if not download_state.is_completed(str(file_path)):
                    logger.info(f"Marking as completed in state: {file_path}")
                    download_state.mark_completed(
                        url, str(file_path),
                        state_fips=state_fips,
                        file_size=file_path.stat().st_size
                    )
                    updated += 1
                else:
                    logger.debug(f"Already marked as completed: {file_path}")
            else:
                logger.debug(f"File does not exist: {file_path}")
        
        # Check for files marked completed but missing on disk
        if hasattr(download_state, "data") and "files" in download_state.data:
            logger.debug(f"Checking for missing files in JSON backend for state {state_fips}")
            for output_path, entry in download_state.data["files"].items():
                if (entry.get("status") == "completed" and
                    entry.get("state_fips") == state_fips):
                    if not os.path.exists(output_path):
                        logger.warning(
                            f"File marked as completed but missing: {output_path}"
                        )
                        missing += 1
        
        elif hasattr(download_state, "conn"):
            logger.debug(f"Checking for missing files in DuckDB backend for state {state_fips}")
            try:
                completed = download_state.conn.execute(
                    "SELECT path FROM files WHERE state_fips = ? AND status = 'completed'",
                    [state_fips]
                ).fetchall()
                for row in completed:
                    file_path = row[0]
                    if not os.path.exists(file_path):
                        logger.warning(
                            f"File marked as completed but missing: {file_path}"
                        )
                        missing += 1
            except Exception as e:
                logger.warning(f"Failed to check missing files for {state_fips}: {e}")
    
    logger.info(
        f"Synchronization complete. {updated} file(s) marked as completed. "
        f"{missing} missing file(s) found."
    )
    logger.info("=" * 70)

# DuckDB-based state tracking (DownloadStateDB)
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

if DUCKDB_AVAILABLE:
    class DownloadStateDB:
        """Track download state using DuckDB for better scalability and query capabilities."""
        
        def __init__(self, db_path: Path):
            self.db_path = Path(db_path)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(str(self.db_path))
            self._create_schema()
        
        def _create_schema(self):
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    path VARCHAR PRIMARY KEY,
                    url VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    state_fips VARCHAR,
                    size BIGINT,
                    bytes_downloaded BIGINT,
                    error VARCHAR,
                    timestamp DOUBLE NOT NULL
                )
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON files(status)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_state ON files(state_fips)")
            
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS states (
                    state_fips VARCHAR PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    completed INTEGER DEFAULT 0,
                    failed INTEGER DEFAULT 0,
                    discovered INTEGER DEFAULT 0,
                    last_updated DOUBLE
                )
            """)
            
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS discovered_urls (
                    state_fips VARCHAR NOT NULL,
                    url VARCHAR NOT NULL,
                    discovered_at DOUBLE NOT NULL,
                    PRIMARY KEY (state_fips, url)
                )
            """)
            
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS url_lists (
                    url VARCHAR PRIMARY KEY,
                    list_type VARCHAR NOT NULL,
                    added_at DOUBLE NOT NULL
                )
            """)
        
        def mark_completed(self, url: str, output_path: str, state_fips: str = None,
                          file_size: int = None):
            timestamp = time.time()
            self.conn.execute("""
                INSERT INTO files (path, url, status, state_fips, size, bytes_downloaded, error, timestamp)
                VALUES (?, ?, 'completed', ?, ?, NULL, NULL, ?)
                ON CONFLICT (path) DO UPDATE SET
                    status = 'completed',
                    url = EXCLUDED.url,
                    state_fips = EXCLUDED.state_fips,
                    size = EXCLUDED.size,
                    bytes_downloaded = NULL,
                    error = NULL,
                    timestamp = EXCLUDED.timestamp
            """, [output_path, url, state_fips, file_size, timestamp])
            
            self.conn.execute("""
                INSERT INTO url_lists (url, list_type, added_at)
                VALUES (?, 'completed', ?)
                ON CONFLICT (url) DO UPDATE SET list_type = 'completed', added_at = EXCLUDED.added_at
            """, [url, timestamp])
            
            self.conn.execute(
                "DELETE FROM url_lists WHERE url = ? AND list_type = 'failed'",
                [url]
            )
            
            if state_fips:
                self._update_state_stats(state_fips, "completed")
        
        def mark_failed(self, url: str, output_path: str, error: str, state_fips: str = None):
            timestamp = time.time()
            self.conn.execute("""
                INSERT INTO files (path, url, status, state_fips, size, bytes_downloaded, error, timestamp)
                VALUES (?, ?, 'failed', ?, NULL, NULL, ?, ?)
                ON CONFLICT (path) DO UPDATE SET
                    status = 'failed',
                    url = EXCLUDED.url,
                    state_fips = EXCLUDED.state_fips,
                    error = EXCLUDED.error,
                    timestamp = EXCLUDED.timestamp
            """, [output_path, url, state_fips, error, timestamp])
            
            self.conn.execute("""
                INSERT INTO url_lists (url, list_type, added_at)
                VALUES (?, 'failed', ?)
                ON CONFLICT (url) DO UPDATE SET list_type = 'failed', added_at = EXCLUDED.added_at
            """, [url, timestamp])
            
            if state_fips:
                self._update_state_stats(state_fips, "failed")
        
        def mark_partial(self, url: str, output_path: str, bytes_downloaded: int,
                        state_fips: str = None):
            timestamp = time.time()
            self.conn.execute("""
                INSERT INTO files (path, url, status, state_fips, size, bytes_downloaded, error, timestamp)
                VALUES (?, ?, 'partial', ?, NULL, ?, NULL, ?)
                ON CONFLICT (path) DO UPDATE SET
                    status = 'partial',
                    url = EXCLUDED.url,
                    state_fips = EXCLUDED.state_fips,
                    bytes_downloaded = EXCLUDED.bytes_downloaded,
                    timestamp = EXCLUDED.timestamp
            """, [output_path, url, state_fips, bytes_downloaded, timestamp])
            
            if state_fips:
                self._ensure_state_exists(state_fips)
        
        def get_partial_size(self, output_path: str) -> int:
            result = self.conn.execute("""
                SELECT bytes_downloaded FROM files
                WHERE path = ? AND status = 'partial'
            """, [output_path]).fetchone()
            return result[0] if result and result[0] is not None else 0
        
        def is_completed(self, output_path: str) -> bool:
            result = self.conn.execute("""
                SELECT 1 FROM files WHERE path = ? AND status = 'completed'
            """, [output_path]).fetchone()
            return result is not None
        
        def _ensure_state_exists(self, state_fips: str):
            self.conn.execute("""
                INSERT INTO states (state_fips, name, completed, failed, discovered, last_updated)
                VALUES (?, ?, 0, 0, 0, ?)
                ON CONFLICT (state_fips) DO NOTHING
            """, [state_fips, f"State {state_fips}", time.time()])
        
        def _update_state_stats(self, state_fips: str, status: str):
            timestamp = time.time()
            self._ensure_state_exists(state_fips)
            
            if status == "completed":
                self.conn.execute("""
                    UPDATE states SET completed = completed + 1, last_updated = ?
                    WHERE state_fips = ?
                """, [timestamp, state_fips])
            elif status == "failed":
                self.conn.execute("""
                    UPDATE states SET failed = failed + 1, last_updated = ?
                    WHERE state_fips = ?
                """, [timestamp, state_fips])
        
        def get_summary(self) -> Dict:
            result = self.conn.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN list_type = 'completed' THEN 1 END) as completed,
                    COUNT(CASE WHEN list_type = 'failed' THEN 1 END) as failed
                FROM url_lists
            """).fetchone()
            if result:
                return {
                    "total": result[0],
                    "completed": result[1],
                    "failed": result[2]
                }
            return {"total": 0, "completed": 0, "failed": 0}
        
        def get_state_summary(self, state_fips: str = None) -> Dict:
            if state_fips:
                result = self.conn.execute("""
                    SELECT * FROM states WHERE state_fips = ?
                """, [state_fips]).fetchone()
                if result:
                    keys = [desc[0] for desc in self.conn.description]
                    return dict(zip(keys, result))
                return {}
            else:
                results = self.conn.execute("SELECT * FROM states").fetchall()
                keys = [desc[0] for desc in self.conn.description]
                return [dict(zip(keys, row)) for row in results]
        
        def list_states_requested(self) -> List[str]:
            results = self.conn.execute(
                "SELECT state_fips FROM states ORDER BY state_fips"
            ).fetchall()
            return [row[0] for row in results]
        
        def get_urls_for_state(self, state_fips: str) -> Dict[str, List[str]]:
            completed = self.conn.execute("""
                SELECT url FROM files
                WHERE state_fips = ? AND status = 'completed'
            """, [state_fips]).fetchall()
            failed = self.conn.execute("""
                SELECT url FROM files
                WHERE state_fips = ? AND status = 'failed'
            """, [state_fips]).fetchall()
            return {
                "completed": [row[0] for row in completed],
                "failed": [row[0] for row in failed]
            }
        
        def set_discovered_urls(self, state_fips: str, urls: Set[str]):
            timestamp = time.time()
            self._ensure_state_exists(state_fips)
            
            if urls:
                self.conn.executemany(
                    """
                    INSERT INTO discovered_urls (state_fips, url, discovered_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT (state_fips, url) DO NOTHING
                    """,
                    [(state_fips, url, timestamp) for url in urls]
                )
            
            count = len(urls)
            self.conn.execute("""
                UPDATE states SET discovered = ?, last_updated = ?
                WHERE state_fips = ?
            """, [count, timestamp, state_fips])
        
        def get_pending_urls(self, state_fips: str) -> List[str]:
            results = self.conn.execute("""
                SELECT d.url
                FROM discovered_urls d
                LEFT JOIN url_lists u ON d.url = u.url AND u.list_type IN ('completed', 'failed')
                WHERE d.state_fips = ? AND u.url IS NULL
            """, [state_fips]).fetchall()
            return [row[0] for row in results]
        
        def get_download_progress(self, state_fips: str) -> Dict:
            summary = self.get_state_summary(state_fips)
            urls = self.get_urls_for_state(state_fips)
            pending = self.get_pending_urls(state_fips)
            return {
                "discovered": summary.get("discovered", 0) if summary else 0,
                "completed": len(urls["completed"]),
                "failed": len(urls["failed"]),
                "pending": len(pending),
                "pending_urls": pending[:10]
            }
        
        def close(self):
            if hasattr(self, "conn") and self.conn:
                self.conn.close()
        
        def __enter__(self):
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            self.close()
        
        def export_to_json(self, json_path: Path) -> None:
            import json
            
            files_data = {}
            files = self.conn.execute("SELECT * FROM files").fetchall()
            keys = [desc[0] for desc in self.conn.description]
            for row in files:
                files_data[row[0]] = dict(zip(keys, row))
            
            completed = [
                row[0] for row in self.conn.execute(
                    "SELECT url FROM url_lists WHERE list_type = 'completed'"
                ).fetchall()
            ]
            failed = [
                row[0] for row in self.conn.execute(
                    "SELECT url FROM url_lists WHERE list_type = 'failed'"
                ).fetchall()
            ]
            
            states_data = {}
            states = self.conn.execute("SELECT * FROM states").fetchall()
            state_keys = [desc[0] for desc in self.conn.description]
            for row in states:
                states_data[row[0]] = dict(zip(state_keys, row))
            
            discovered_urls = {}
            discovered = self.conn.execute(
                "SELECT state_fips, url FROM discovered_urls"
            ).fetchall()
            for state_fips, url in discovered:
                discovered_urls.setdefault(state_fips, []).append(url)
            
            data = {
                "files": files_data,
                "completed": completed,
                "failed": failed,
                "states": states_data,
                "discovered_urls": discovered_urls
            }
            
            with open(json_path, "w") as f:
                json.dump(data, f, indent=2)