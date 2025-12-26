"""
find_duckdb_files.py
Script to find all *.duckdb files in the project tree.
"""
import os
from pathlib import Path

def find_duckdb_files(root_dir: str):
    """
    Recursively find all .duckdb files under root_dir.
    """
    root = Path(root_dir)
    for path in root.rglob("*.duckdb"):
        print(path)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Find all .duckdb files in the project.")
    parser.add_argument(
        "root_dir",
        nargs="?",
        default=os.getcwd(),
        help="Root directory to search (default: current directory)",
    )
    args = parser.parse_args()
    find_duckdb_files(args.root_dir)
