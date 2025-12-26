"""
unzipper.py
Unzips all .zip files in a directory to a specified output directory.
"""
import os
import zipfile
from pathlib import Path

def unzip_all(input_dir: str, output_dir: str, recursive: bool = False, state: str = None, shape_type: str = None) -> None:
    """
    Unzips all .zip files in input_dir to output_dir.
    Supports recursive search and filtering by state FIPS and shape type.
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    # Build pattern
    pattern = "**/*.zip" if recursive else "*.zip"
    zip_files = list(input_path.glob(pattern)) if not recursive else list(input_path.rglob("*.zip"))
    import re
    filtered = []
    for zip_file in zip_files:
        name = zip_file.name
        # Match state FIPS only if it appears after 'tl_YYYY_' and before next '_'
        if state:
            # Accept both 2-digit and 3-digit FIPS (with/without leading zero)
            # e.g., tl_2025_30_ or tl_2025_030_ or tl_2025_30001_
            match = re.search(r"tl_\d{4}_(0?%s)[0-9]{3}_" % re.escape(state), name)
            if not match:
                continue
        if shape_type and shape_type not in name:
            continue
        filtered.append(zip_file)
    for zip_file in filtered:
        try:
            with zipfile.ZipFile(zip_file, 'r') as zf:
                zf.extractall(output_path / zip_file.stem)
                print(f"Unzipped {zip_file} to {output_path / zip_file.stem}")
        except zipfile.BadZipFile:
            print(f"Warning: {zip_file} is not a valid zip file. Skipping.")
        except Exception as e:
            print(f"Warning: Could not unzip {zip_file}: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Unzip all .zip files in a directory (optionally recursive and filtered).")
    parser.add_argument("input_dir", help="Directory containing .zip files")
    parser.add_argument("output_dir", help="Directory to extract files to")
    parser.add_argument("--recursive", action="store_true", help="Recursively search for zip files")
    parser.add_argument("--state", default=None, help="State FIPS code to filter zip files (e.g., 13)")
    parser.add_argument("--type", dest="shape_type", default=None, help="Shape type to filter zip files (e.g., edges, faces)")
    args = parser.parse_args()
    unzip_all(args.input_dir, args.output_dir, recursive=args.recursive, state=args.state, shape_type=args.shape_type)
