"""
schema_cache.py
Fast schema inference and caching for TIGER/Line files.
"""
import re
import duckdb
import zipfile
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Cache: {file_pattern: (columns, dtypes)}
_SCHEMA_CACHE: Dict[str, Tuple[list, dict]] = {}

def get_file_pattern(filename: str) -> Optional[str]:
    """
    Extract TIGER/Line file pattern: tl_YYYY_SSCCC_FEAT -> FEAT
    
    Args:
        filename: TIGER/Line filename
        
    Returns:
        Feature type (e.g., "edges", "addr") or None if invalid
    """
    pattern = r'^tl_\d{4}_\d{2,5}_([a-z0-9]+)\.(shp|dbf)$'
    match = re.match(pattern, filename.lower())
    return match.group(1) if match else None

def infer_schema_from_shp(shp_path: str) -> Tuple[list, dict]:
    """
    Infer schema from SHP file using DuckDB's spatial extension.
    This is FAST - DuckDB reads just the header, not all data.
    Supports both regular files and files inside ZIP archives.
    
    Args:
        shp_path: Path to SHP file (or /vsizip/ path for files in ZIP)
        
    Returns:
        Tuple of (column_names, column_types)
    """
    conn = duckdb.connect(':memory:')
    conn.execute("INSTALL spatial; LOAD spatial;")
    
    # Use LIMIT 0 to get schema without reading data
    result = conn.execute(f"SELECT * FROM st_read('{shp_path}') LIMIT 0")
    columns = [desc[0] for desc in result.description]
    dtypes = {desc[0]: str(desc[1]) for desc in result.description}
    
    conn.close()
    return columns, dtypes

def infer_schema_from_dbf(dbf_path: str) -> Tuple[list, dict]:
    """
    Infer schema from DBF file using DuckDB.
    Supports both regular files and files inside ZIP archives.
    
    Args:
        dbf_path: Path to DBF file (or /vsizip/ path for files in ZIP)
        
    Returns:
        Tuple of (column_names, column_types)
    """
    conn = duckdb.connect(':memory:')
    conn.execute("INSTALL spatial; LOAD spatial;")
    
    # Read DBF file and get schema
    result = conn.execute(f"SELECT * FROM st_read('{dbf_path}') LIMIT 0")
    columns = [desc[0] for desc in result.description]
    dtypes = {desc[0]: str(desc[1]) for desc in result.description}
    
    conn.close()
    return columns, dtypes

def infer_schema_from_zip(zip_path: str, file_ext: str = '.shp') -> Optional[Tuple[list, dict]]:
    """
    Infer schema from a file inside a ZIP archive without extraction.
    Uses DuckDB's /vsizip/ virtual file system.
    
    Args:
        zip_path: Path to ZIP file
        file_ext: Extension to look for ('.shp' or '.dbf')
        
    Returns:
        Tuple of (column_names, column_types) or None if no matching file found
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Find first file with matching extension
            target_file = None
            for name in zf.namelist():
                if name.lower().endswith(file_ext.lower()):
                    target_file = name
                    break
            
            if not target_file:
                return None
            
            # Use DuckDB's virtual file system: /vsizip/{zip_path}/{file_in_zip}
            vsi_path = f"/vsizip/{zip_path}/{target_file}"
            
            if file_ext.lower() == '.shp':
                return infer_schema_from_shp(vsi_path)
            elif file_ext.lower() == '.dbf':
                return infer_schema_from_dbf(vsi_path)
            else:
                return None
    except Exception:
        return None

def get_or_infer_schema(file_path: str, use_cache: bool = True) -> Tuple[list, dict]:
    """
    Get schema for a file, using cache if available.
    
    Args:
        file_path: Path to SHP or DBF file
        use_cache: If True, use cached schema for same file patterns
        
    Returns:
        Tuple of (column_names, column_types)
    """
    path = Path(file_path)
    file_pattern = get_file_pattern(path.name)
    
    # Check cache
    if use_cache and file_pattern and file_pattern in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[file_pattern]
    
    # Infer schema
    if path.suffix.lower() == '.shp':
        columns, dtypes = infer_schema_from_shp(str(path))
    elif path.suffix.lower() == '.dbf':
        columns, dtypes = infer_schema_from_dbf(str(path))
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")
    
    # Cache it
    if use_cache and file_pattern:
        _SCHEMA_CACHE[file_pattern] = (columns, dtypes)
    
    return columns, dtypes

def batch_infer_schemas_from_zips(zip_paths: List[Path], max_workers: int = 4) -> Dict[str, Tuple[list, dict]]:
    """
    Infer schemas directly from ZIP files without extraction.
    This is much faster than extracting first!
    
    Args:
        zip_paths: List of ZIP file paths
        max_workers: Number of parallel workers
        
    Returns:
        Dict mapping file patterns to (columns, dtypes)
    """
    unique_patterns = {}
    
    # Find one representative ZIP per pattern
    for zip_path in zip_paths:
        # Extract pattern from ZIP filename (e.g., tl_2021_13001_edges.zip -> edges)
        pattern = get_file_pattern(Path(zip_path).name)
        if pattern and pattern not in unique_patterns:
            # Skip if already cached
            if pattern in _SCHEMA_CACHE:
                continue
            unique_patterns[pattern] = zip_path
    
    if not unique_patterns:
        return {}
    
    # Infer schemas in parallel
    results = {}
    
    def infer_from_zip(args):
        pattern, zip_path = args
        try:
            # Try .shp first (for edges files), then .dbf (for addr/featnames)
            result = infer_schema_from_zip(str(zip_path), '.shp')
            if result is None:
                result = infer_schema_from_zip(str(zip_path), '.dbf')
            return pattern, result
        except Exception as e:
            print(f"Warning: Failed to infer schema from {zip_path}: {e}")
            return pattern, None
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(infer_from_zip, (pattern, path))
            for pattern, path in unique_patterns.items()
        ]
        
        for future in as_completed(futures):
            try:
                pattern, result = future.result()
                if result:
                    columns, dtypes = result
                    results[pattern] = (columns, dtypes)
                    _SCHEMA_CACHE[pattern] = (columns, dtypes)
            except Exception as e:
                print(f"Warning: Schema inference failed: {e}")
    
    return results

def batch_infer_schemas(file_paths: List[Path], max_workers: int = 4, use_cache: bool = True) -> Dict[str, Tuple[list, dict]]:
    """
    Infer schemas for multiple files in parallel (optimization for initial discovery).
    
    Args:
        file_paths: List of file paths
        max_workers: Number of parallel workers
        use_cache: If True, skip patterns already in cache
        
    Returns:
        Dict mapping file patterns to (columns, dtypes)
    """
    unique_patterns = {}
    
    # Find one representative file per pattern
    for path in file_paths:
        pattern = get_file_pattern(Path(path).name)
        if pattern and pattern not in unique_patterns:
            # Skip if already cached (unless use_cache=False)
            if use_cache and pattern in _SCHEMA_CACHE:
                continue
            unique_patterns[pattern] = path
    
    if not unique_patterns:
        return {}
    
    # Infer schemas in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(get_or_infer_schema, str(path), use_cache=False): pattern
            for pattern, path in unique_patterns.items()
        }
        
        for future in as_completed(futures):
            pattern = futures[future]
            try:
                columns, dtypes = future.result()
                results[pattern] = (columns, dtypes)
                _SCHEMA_CACHE[pattern] = (columns, dtypes)
            except Exception as e:
                print(f"Warning: Failed to infer schema for {pattern}: {e}")
    
    return results

def save_cache_to_file(cache_path: str):
    """Save schema cache to JSON file."""
    cache_data = {
        pattern: {
            'columns': cols,
            'dtypes': dtypes
        }
        for pattern, (cols, dtypes) in _SCHEMA_CACHE.items()
    }
    Path(cache_path).write_text(json.dumps(cache_data, indent=2))

def load_cache_from_file(cache_path: str) -> bool:
    """
    Load schema cache from JSON file.
    
    Returns:
        True if cache was loaded, False if file doesn't exist
    """
    global _SCHEMA_CACHE
    if Path(cache_path).exists():
        try:
            cache_data = json.loads(Path(cache_path).read_text())
            _SCHEMA_CACHE = {
                pattern: (data['columns'], data['dtypes'])
                for pattern, data in cache_data.items()
            }
            return True
        except Exception as e:
            print(f"Warning: Failed to load schema cache: {e}")
            return False
    return False

def clear_cache():
    """Clear in-memory schema cache."""
    global _SCHEMA_CACHE
    _SCHEMA_CACHE = {}

def get_cache_stats() -> Dict[str, int]:
    """Get statistics about current cache."""
    return {
        'patterns_cached': len(_SCHEMA_CACHE),
        'patterns': list(_SCHEMA_CACHE.keys())
    }