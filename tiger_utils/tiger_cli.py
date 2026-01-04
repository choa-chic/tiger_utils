
"""
tiger_cli.py - Command-line interface for TIGER/Line downloads using tiger_utils.download modules
"""

import sys
import argparse

from tiger_utils.commands import cmd_info_types, cmd_info_states, cmd_download
from tiger_utils.utils.logger import get_logger, setup_logger

setup_logger()
logger = get_logger()


def main():
    parser = argparse.ArgumentParser(
        description='TIGER/Line Shapefiles download and data management tool',
        epilog="""
Examples:
  # Show available layers:
  python -m tiger_utils info types

  # Show state FIPS codes:
  python -m tiger_utils info states

  # Discover and populate URLs for 2025 (no download):
  python -m tiger_utils download --discover-only

  # Discover for a specific state (California):
  python -m tiger_utils download --discover-only --states 06

  # Sync database state/status with files on disk:
  python -m tiger_utils download --sync-state

  # Show download status:
  python -m tiger_utils download --show-status

  # Download all data for 2025 (default 50 states):
  python -m tiger_utils download

  # Download for a specific year and state:
  python -m tiger_utils download --year 2023 --states 12
""",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Global arguments
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose (DEBUG) logging output')
    parser.add_argument('--quiet', '-q', action='store_true', help='Enable quiet mode (WARNING and ERROR only)')

    # Create subparsers for commands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # ========== INFO COMMAND ==========
    info_parser = subparsers.add_parser('info', help='Information commands (types, states, etc.)')
    info_subparsers = info_parser.add_subparsers(dest='info_command', help='Info subcommands', required=True)

    # info types
    types_parser = info_subparsers.add_parser('types', help='List available layers')
    types_parser.set_defaults(func=cmd_info_types)

    # info states
    states_parser = info_subparsers.add_parser('states', help='List all state FIPS codes')
    states_parser.set_defaults(func=cmd_info_states)

    # ========== DOWNLOAD COMMAND ==========
    download_parser = subparsers.add_parser('download', help='Download TIGER/Line shapefiles')
    download_parser.set_defaults(func=cmd_download)

    # Download operations
    download_parser.add_argument('--discover-only', action='store_true', help='Only discover and populate URLs in state database, do not download files')
    download_parser.add_argument('--download', action='store_true', help='Download all discovered files that are not yet on disk')
    download_parser.add_argument('--sync-state', action='store_true', help='Synchronize state database with files on disk (mark completed if file exists)')
    download_parser.add_argument('--show-status', action='store_true', help='Show download status for all states/territories and exit')

    # Download parameters
    download_parser.add_argument('--year', type=int, default=2025, help='Year to download (default: 2025)')
    download_parser.add_argument('--output', type=str, default=None, help='Output directory (default: tiger_data/YYYY)')
    download_parser.add_argument('--states', type=str, help='Comma-separated state FIPS codes (e.g., "01,06,48")')
    download_parser.add_argument('--types', type=str, help='Comma-separated layer types (e.g., "EDGES,ADDR")')
    download_parser.add_argument('--progress-file', type=str, default='.tiger_download_state', help='Path to progress file (default: .tiger_download_state, extension added automatically)')
    download_parser.add_argument('--include-territories', action='store_true', help='Include US territories (default: only 50 states)')

    # Download behavior
    download_parser.add_argument('--parallel', type=int, default=8, help='Number of parallel downloads (default: 8)')
    download_parser.add_argument('--timeout', type=int, default=60, help='Download timeout in seconds (default: 60)')
    download_parser.add_argument('--no-use-db', dest='use_db', action='store_false', default=True, help='Use JSON for state tracking instead of DuckDB')

    args = parser.parse_args()

    # Configure logging level (global)
    if args.quiet:
        logger.setLevel(30)
    elif args.verbose:
        logger.setLevel(10)
    else:
        logger.setLevel(20)

    # Check if a command was provided
    if not hasattr(args, 'func'):
        parser.print_help()
        return 0

    # Execute the appropriate command function
    return args.func(args)

if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting.")
        sys.exit(130)