
import duckdb
from pathlib import Path
from typing import Optional
import re
from tiger_utils.utils.logger import get_logger

logger = get_logger()

def parse_tiger_filename(filename: str):
	m = re.match(r"tl_(\d{4})_(\d{2})(\d{3})_(\w+)", filename)
	if not m:
		logger.warning(f"Filename does not match TIGER/Line pattern: {filename}")
		return None
	return {
		"year": m.group(1),
		"state": m.group(2),
		"county": m.group(3),
		"layer": m.group(4)
	}

def find_parquet_files(input_dir: Path, year: Optional[str] = None, state: Optional[str] = None, county: Optional[str] = None, layer: Optional[str] = None):
	"""Find all tl_*.parquet files recursively in input_dir, optionally filtered."""
	files = list(input_dir.rglob("tl_*.parquet"))
	filtered = []
	for pq_path in files:
		info = parse_tiger_filename(pq_path.stem)
		if not info:
			continue
		if year and info["year"] != year:
			continue
		if state and info["state"] != state:
			continue
		if county and info["county"] != county:
			continue
		if layer and info["layer"].lower() != layer.lower():
			continue
		filtered.append((pq_path, info))
	return filtered

def register_parquet_views(
	parquet_root: Path,
	duckdb_path: Path,
	year: str,
	state: Optional[str] = None,
	county: Optional[str] = None,
	layer: Optional[str] = None,
):
	"""
	Register Parquet files as DuckDB views (virtual tables) using glob patterns.
	"""
	pq_dir = parquet_root / year
	pq_files = find_parquet_files(pq_dir, year, state, county, layer)
	if not pq_files:
		logger.warning(f"No Parquet files found in {pq_dir} for year={year}, state={state}, county={county}, layer={layer}")
		return
	duckdb_path.parent.mkdir(parents=True, exist_ok=True)
	
	con = duckdb.connect(str(duckdb_path))
	# Find all unique layers
	from os.path import commonpath
	from collections import defaultdict
	# Group files by layer
	layer_to_files = defaultdict(list)
	for pq_path, info in pq_files:
		layer_to_files[info['layer'].lower()].append(pq_path)

	for layer, files in layer_to_files.items():
		view_name = f"tiger_{layer}"
		# Find deepest common directory for this layer's files
		file_dirs = [str(pq_path.parent) for pq_path in files]
		common_dir = commonpath(file_dirs)
		pq_pattern = str(Path(common_dir) / f"tl_{year}_*_{layer}.parquet")
		pq_pattern = pq_pattern.replace('\\', '/')  # Ensure forward slashes for DuckDB
		sql = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM parquet_scan('{pq_pattern}');"
		logger.info(f"Registering view {view_name} using pattern {pq_pattern}")
		con.execute(sql)
	con.close()
	logger.info(f"DuckDB pattern-matched views created in {duckdb_path}")
