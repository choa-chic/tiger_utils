"""
tiger_file_structure.py - Utilities for analyzing and migrating TIGER/Line file directory structures

Usage:

Analyze:
	python -m tiger_utils.utils.tiger_file_structure analyze --root tiger_data

Migrate (dry run, default):
	python -m tiger_utils.utils.tiger_file_structure migrate --root tiger_data

Migrate (specify pattern):
	python -m tiger_utils.utils.tiger_file_structure migrate --root tiger_data --pattern year_statecounty

Migrate (actually move files):
	python -m tiger_utils.utils.tiger_file_structure migrate --root tiger_data --no-dry-run

Available migration patterns (use with --pattern):
	year_statecounty   # /YYYY/SSCCC/filename (default, e.g. 2025/13001/tl_2025_13001_EDGES.dbf)
	year_state_type    # /YYYY/SS/TYPE/filename (e.g. 2025/13/EDGES/tl_2025_13001_EDGES.dbf)
	year_type          # /YYYY/TYPE/filename (e.g. 2025/EDGES/tl_2025_13001_EDGES.dbf)
	# Add more patterns below as you implement them, e.g.:
"""

import re
from pathlib import Path
from typing import List, Dict, Callable, Optional
import argparse
import sys

def parse_tiger_filename(filename: str) -> Optional[Dict]:
	"""
	Parse a TIGER/Line filename like tl_YYYY_SSCCC_FEATURE.*
	Returns dict with year, state, county, feature if matched, else None.
	"""
	m = re.match(r"tl_(\d{4})_(\d{2})(\d{3})(?:_(\w+))?", filename)
	if m:
		return {
			"year": m.group(1),
			"state": m.group(2),
			"county": m.group(3),
			"feature": m.group(4) if m.group(4) else None
		}
	return None

def analyze_tiger_structure(root: Path) -> List[Dict]:
	"""
	Recursively analyze the directory structure under root.
	Returns a list of dicts with file path, detected components, and filename validation.
	"""
	results = []
	for path in root.rglob("*"):
		if path.is_file():
			rel = path.relative_to(root)
			parts = rel.parts
			info = {"file": str(rel), "year": None, "state": None, "county": None, "type": None, "valid": False}
			# Try to parse from filename
			parsed = parse_tiger_filename(path.stem)
			if parsed:
				info.update(parsed)
				info["valid"] = True
			# Try to parse from directory structure if not found
			if not info["year"] and len(parts) >= 2 and re.fullmatch(r"\d{4}", parts[0]) and re.fullmatch(r"\d{2}", parts[1]):
				info["year"] = parts[0]
				info["state"] = parts[1]
				if len(parts) >= 3 and re.fullmatch(r"\d{3,5}", parts[2]):
					info["county"] = parts[2]
				if len(parts) >= 4:
					info["type"] = parts[3]
			info["filename_valid"] = bool(parsed)
			results.append(info)
	return results

def migrate_tiger_structure(
	root: Path,
	target_pattern: Callable[[Dict], Path],
	dry_run: bool = True
):
	"""
	Move files to a new directory structure based on target_pattern.
	target_pattern: function that takes file info dict and returns new Path (relative to root)
	dry_run: if True, only print moves; if False, actually move files.
	"""
	files = analyze_tiger_structure(root)
	for info in files:
		src = root / info["file"]
		dst = root / target_pattern(info)
		if src == dst:
			continue
		print(f"{'Would move' if dry_run else 'Moving'}: {src} -> {dst}")
		if not dry_run:
			dst.parent.mkdir(parents=True, exist_ok=True)
			src.rename(dst)

def pattern_year_statecounty(info):
	if info.get("filename_valid") and info.get("year") and info.get("state") and info.get("county"):
		filename = Path(info["file"]).name
		return Path(info["year"]) / (info["state"] + info["county"]) / filename
	return Path(info["file"])

def pattern_year_state_type(info):
	if info.get("filename_valid") and info.get("year") and info.get("state") and info.get("feature"):
		filename = Path(info["file"]).name
		return Path(info["year"]) / info["state"] / info["feature"] / filename
	return Path(info["file"])

def pattern_year_type(info):
	if info.get("filename_valid") and info.get("year") and info.get("feature"):
		filename = Path(info["file"]).name
		return Path(info["year"]) / info["feature"] / filename
	return Path(info["file"])

def main():
	parent_parser = argparse.ArgumentParser(add_help=False)
	parent_parser.add_argument('--root', type=str, default='tiger_data', help='Root directory to analyze (default: tiger_data)')

	parser = argparse.ArgumentParser(
		description="Analyze or migrate TIGER/Line directory structure under a root directory."
	)
	subparsers = parser.add_subparsers(dest='command', required=True)

	# Analyze subcommand
	parser_analyze = subparsers.add_parser('analyze', parents=[parent_parser], help='Analyze and print structure summary')

	# Migrate subcommand
	parser_migrate = subparsers.add_parser('migrate', parents=[parent_parser], help='Migrate files to a new directory structure')
	parser_migrate.add_argument('--pattern', type=str, default='year_type', help='Migration pattern: year_statecounty (default), more patterns can be added')
	parser_migrate.add_argument('--dry-run', action='store_true', default=True, help='Preview moves (default: True)')
	parser_migrate.add_argument('--no-dry-run', dest='dry_run', action='store_false', help='Actually move files (dangerous!)')

	args = parser.parse_args()
	root = Path(args.root)

	if args.command == 'analyze':
		results = analyze_tiger_structure(root)
		for info in results:
			print(info)
		print(f"\nTotal files analyzed: {len(results)}")
	elif args.command == 'migrate':
		pattern_map = {
			'year_statecounty': pattern_year_statecounty,
			'year_state_type': pattern_year_state_type,
			'year_type': pattern_year_type,
			# Add more patterns here as needed
		}
		pattern_func = pattern_map.get(args.pattern)
		if not pattern_func:
			print(f"Unknown pattern: {args.pattern}")
			print(f"Available patterns: {', '.join(pattern_map.keys())}")
			sys.exit(1)
		migrate_tiger_structure(root, pattern_func, dry_run=args.dry_run)

if __name__ == "__main__":
	main()
