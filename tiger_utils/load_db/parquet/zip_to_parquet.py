

import os
import re
import subprocess
from pathlib import Path
from typing import Optional, List
from tiger_utils.utils.logger import get_logger

logger = get_logger()

def parse_tiger_filename(filename: str):
	"""
	Parse TIGER/Line filename: tl_YYYY_SSCCC_layer.zip
	Returns dict with year, state, county, layer.
	"""
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

def find_tiger_zips(input_dir: Path) -> List[Path]:
	"""Find all tl_*.zip files recursively in input_dir."""
	zips = list(input_dir.rglob("tl_*.zip"))
	logger.info(f"Found {len(zips)} zip files in {input_dir}")
	return zips

def ogr2ogr_shapefile_to_parquet(zip_path: Path, output_path: Path, layer: str):
	"""
	Use ogr2ogr to convert shapefile in zip to GeoParquet using GDAL VFS.
	"""
	# Try .shp first, then .dbf if .shp is missing
	stem = zip_path.stem
	shp_name = f"{stem}.shp"
	dbf_name = f"{stem}.dbf"
	vfs_shp = f"/vsizip/{zip_path.as_posix()}/{shp_name}"
	vfs_dbf = f"/vsizip/{zip_path.as_posix()}/{dbf_name}"
	output_path.parent.mkdir(parents=True, exist_ok=True)

	# Check if .shp or .dbf exists in the zip
	import zipfile
	with zipfile.ZipFile(zip_path) as zf:
		namelist = set(zf.namelist())
	if shp_name in namelist:
		vfs_path = vfs_shp
		file_type = ".shp (vector)"
	elif dbf_name in namelist:
		vfs_path = vfs_dbf
		file_type = ".dbf (table)"
	else:
		logger.warning(f"No .shp or .dbf found in {zip_path.name}, skipping.")
		return

	cmd = [
		"ogr2ogr",
		"-f", "Parquet",
		str(output_path),
		vfs_path
	]
	logger.info(f"Converting {zip_path.name} ({layer}) to {output_path.name} using {file_type}")
	try:
		subprocess.run(cmd, check=True)
		logger.info(f"Successfully wrote {output_path}")
	except subprocess.CalledProcessError as e:
		logger.error(f"ogr2ogr failed for {zip_path}: {e}")

def convert_all(
	input_dir: Path,
	output_dir: Path,
	year: Optional[str] = None,
	state: Optional[str] = None,
	county: Optional[str] = None,
	layer: Optional[str] = None,
	workers: int = 8,
):
	"""
	Convert all matching TIGER/Line zips to GeoParquet, in parallel.
	"""
	from concurrent.futures import ProcessPoolExecutor, as_completed

	logger.info(f"Starting conversion: input={input_dir}, output={output_dir}, year={year}, state={state}, county={county}, layer={layer}, workers={workers}")
	zips = find_tiger_zips(input_dir)
	tasks = []
	filtered = []
	for zip_path in zips:
		info = parse_tiger_filename(zip_path.stem)
		if not info:
			logger.debug(f"Skipping file (unparseable): {zip_path.name}")
			continue
		if year and info["year"] != year:
			logger.debug(f"Skipping {zip_path.name}: year {info['year']} != {year}")
			continue
		if state and info["state"] != state:
			logger.debug(f"Skipping {zip_path.name}: state {info['state']} != {state}")
			continue
		if county and info["county"] != county:
			logger.debug(f"Skipping {zip_path.name}: county {info['county']} != {county}")
			continue
		if layer and info["layer"].lower() != layer.lower():
			logger.debug(f"Skipping {zip_path.name}: layer {info['layer']} != {layer}")
			continue
		# Organize output as output_dir/YYYY/layer/filename.parquet
		out_name = f"{zip_path.stem}.parquet"
		out_path = output_dir / info["year"] / info["layer"].lower() / out_name
		filtered.append((zip_path, out_path, info["layer"]))

	count = 0
	if workers == 1:
		for zip_path, out_path, lyr in filtered:
			ogr2ogr_shapefile_to_parquet(zip_path, out_path, lyr)
			count += 1
	else:
		with ProcessPoolExecutor(max_workers=workers) as executor:
			futs = [executor.submit(ogr2ogr_shapefile_to_parquet, zip_path, out_path, lyr) for zip_path, out_path, lyr in filtered]
			for fut in as_completed(futs):
				# Optionally, could log per-task completion here
				count += 1
	logger.info(f"Finished conversion. {count} files processed.")
