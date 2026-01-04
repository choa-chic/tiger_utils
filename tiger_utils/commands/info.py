"""
Info command - Display information about dataset types and states
"""

from tiger_utils.download.url_patterns import DATASET_TYPES, STATES


def cmd_info_types(args):
    """Handle 'info types' subcommand."""
    print("\nAvailable Layers:")
    print("=" * 70)
    for dtype, desc in DATASET_TYPES.items():
        print(f"  {dtype:15s} - {desc}")
    return 0


def cmd_info_states(args):
    """Handle 'info states' subcommand."""
    print("\nState FIPS Codes:")
    print("=" * 70)
    for fips, name in sorted(STATES.items()):
        print(f"  {fips} - {name}")
    return 0
