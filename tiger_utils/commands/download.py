"""
Download command - Download TIGER/Line shapefiles from US Census Bureau
"""

import asyncio
from pathlib import Path

from tiger_utils.download.downloader import download_county_data, download_urls, download_data_auto
from tiger_utils.download.progress_manager import DownloadState, DownloadStateDB
from tiger_utils.download.discover import discover_state_files, discover_state_files_multi
from tiger_utils.download.url_patterns import (
    DATASET_TYPES, STATES, COUNTY_LEVEL_TYPES, FIFTY_STATE_FIPS, TERRITORY_FIPS, DEGAUSS_LAYERS, POSTGIS_LAYERS
)
from tiger_utils.utils.logger import get_logger

logger = get_logger()


def create_state_tracker(state_file: Path, use_db: bool = None):
    """
    Create appropriate state tracker (DuckDB or JSON).
    """
    if use_db is False:
        return DownloadState(state_file.with_suffix('.json'))
    try:
        import duckdb
        return DownloadStateDB(state_file.with_suffix('.duckdb'))
    except ImportError:
        logger.warning("DuckDB not available. Falling back to JSON.")
        return DownloadState(state_file.with_suffix('.json'))


def cmd_download(args):
    """Handle 'download' subcommand."""
    # Configure logging level
    if args.quiet:
        logger.setLevel(30)
    elif args.verbose:
        logger.setLevel(10)
    else:
        logger.setLevel(20)

    if args.output is None:
        output_dir = Path(f'tiger_data/{args.year}')
    else:
        output_dir = Path(args.output)
    state_file_base = output_dir / args.progress_file
    output_dir.mkdir(parents=True, exist_ok=True)

    # State tracking backend
    use_db = args.use_db  # True unless --no-use-db is passed
    download_state = create_state_tracker(state_file_base, use_db=use_db)
    
    # Load saved configuration if not explicitly specified
    saved_config = download_state.get_config()
    if saved_config:
        logger.info(f"Found saved configuration from previous run: year={saved_config.get('year')}, "
                   f"states={len(saved_config.get('states', []))} state(s), "
                   f"types={saved_config.get('dataset_types')}")
    
    # Determine year (command line > saved config > default)
    if hasattr(args, 'year') and args.year != 2025:  # 2025 is default
        year = args.year
    elif saved_config and 'year' in saved_config:
        year = saved_config['year']
        logger.info(f"Using saved year: {year}")
    else:
        year = args.year

    # Determine states
    if getattr(args, 'all_states', False):
        state_list = sorted(FIFTY_STATE_FIPS)
        logger.info("--all-states flag set: using all 50 US states.")
    elif args.states:
        state_list = [s.strip().zfill(2) for s in args.states.split(',')]
        invalid = [s for s in state_list if s not in STATES]
        if invalid:
            logger.error(f"Invalid state FIPS codes: {invalid}")
            return 1
        # If any requested FIPS is a territory, allow it, otherwise filter to 50 states unless --include-territories
        if not args.include_territories:
            if not any(s in TERRITORY_FIPS for s in state_list):
                state_list = [s for s in state_list if s in FIFTY_STATE_FIPS]
    elif saved_config and 'states' in saved_config:
        state_list = saved_config['states']
        logger.info(f"Using saved states: {state_list}")
    else:
        # Default: only 50 states unless --include-territories
        if args.include_territories:
            state_list = list(STATES.keys())
        else:
            state_list = sorted(FIFTY_STATE_FIPS)

    # Determine which layers to download
    if args.postgis_all:
        type_list = POSTGIS_LAYERS
    elif args.types:
        type_list = [t.strip().upper() for t in args.types.split(',')]
        invalid = [t for t in type_list if t not in DATASET_TYPES]
        if invalid:
            logger.error(f"Invalid layers: {invalid}")
            return 1
    elif saved_config and 'dataset_types' in saved_config:
        type_list = saved_config['dataset_types']
        logger.info(f"Using saved dataset types: {type_list}")
    else:
        type_list = DEGAUSS_LAYERS

    # Optionally sync state with file system before any other command
    if args.sync_state:
        from tiger_utils.download.progress_manager import sync_state_with_filesystem
        sync_state_with_filesystem(output_dir, download_state, state_list)
        return 0

    # Show status
    if args.show_status:
        states_list = download_state.list_states_requested()
        if not states_list:
            logger.info("No states/territories have been requested for download yet.")
            return 0
        for state_fips in sorted(states_list):
            state_summary = download_state.get_state_summary(state_fips)
            urls = download_state.get_urls_for_state(state_fips)
            progress = download_state.get_download_progress(state_fips)
            state_name = state_summary.get('name', f"State {state_fips}")
            completed = state_summary.get('completed', 0)
            failed = state_summary.get('failed', 0)
            total = completed + failed
            logger.info(f"State: {state_name} (FIPS: {state_fips})")
            logger.info(f"  Completed: {completed}")
            logger.info(f"  Failed:    {failed}")
            logger.info(f"  Total:     {total}")
            if urls['completed']:
                logger.info(f"  Sample Completed URLs: {urls['completed'][:3]}")
            if urls['failed']:
                logger.info(f"  Failed URLs: {urls['failed'][:3]}")
            if progress['pending'] > 0 and progress['pending_urls']:
                logger.info(f"  Sample Pending URLs: {progress['pending_urls'][:3]}")
        return 0

    # Discover-only mode
    if args.discover_only:        # Save configuration for future runs
        download_state.save_config(year, state_list, type_list)
        logger.info(f"Saved configuration: year={year}, states={state_list}, types={type_list}")
        total_discovered = 0
        for state_fips in state_list:
            # If multiple states are provided, use the efficient multi-state function
            if isinstance(state_fips, list):
                discovered = discover_state_files_multi(state_fips, args.year, type_list, args.timeout)
            else:
                discovered = discover_state_files(state_fips, args.year, type_list, args.timeout)
            all_urls = set()
            for urls in discovered.values():
                all_urls.update(urls)
            download_state.set_discovered_urls(state_fips, all_urls)
            total_discovered += len(all_urls)
            logger.info(f"Discovered {len(all_urls)} files for {state_fips}")
        logger.info(f"Total URLs Discovered: {total_discovered}")
        if not args.download:
            return 0

    # Download data for each state
    async def run_all_downloads():        # Save configuration for future runs
        download_state.save_config(year, state_list, type_list)
        logger.info(f"Saved configuration: year={year}, states={state_list}, types={type_list}")
        total_successful = 0
        total_failed = 0
        for state_fips in state_list:
            results = await download_data_auto(
                state_fips, args.year, output_dir, type_list, args.parallel, args.timeout, download_state
            )
            successful = sum(1 for r in results if r[0])
            failed = len(results) - successful
            total_successful += successful
            total_failed += failed
        logger.info(f"Total Successful: {total_successful}")
        logger.info(f"Total Failed:     {total_failed}")
        return 0 if total_failed == 0 else 1

    async def run_discovered_downloads():
        total_successful = 0
        total_failed = 0
        for state_fips in state_list:
            try:
                pending_urls = download_state.get_pending_urls(state_fips)
            except Exception as e:
                logger.error(f"Failed to fetch pending URLs for {state_fips}: {e}")
                continue
            if not pending_urls:
                logger.info(f"No pending discovered files for {state_fips}")
                continue
            
            state_output_dir = output_dir / str(args.year) / state_fips
            results = await download_urls(
                pending_urls, state_output_dir, args.parallel, args.timeout, download_state, state_fips
            )
            successful = sum(1 for r in results if r[0])
            failed = len(results) - successful
            total_successful += successful
            total_failed += failed
        logger.info(f"Total Successful: {total_successful}")
        logger.info(f"Total Failed:     {total_failed}")
        return 0 if total_failed == 0 else 1

    if args.download:
        return asyncio.run(run_discovered_downloads())

    return asyncio.run(run_all_downloads())
