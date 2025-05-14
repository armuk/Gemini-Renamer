import argparse
import logging
from pathlib import Path
from . import __version__

def create_parser():
    parser = argparse.ArgumentParser(
        description=f"Advanced media renamer (v{__version__}).",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    parser.add_argument('--config', type=Path, help='Path to TOML config file (overrides default search).')
    parser.add_argument('--profile', type=str, default='default', help='Configuration profile to use.')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default=None, help='Console logging level (overrides config).')
    parser.add_argument('--tmdb-language', type=str, default=None, help='Language for TMDB/TVDB API calls (e.g., "de", overrides config/env).')
    parser.add_argument('--quiet', '-q', action='store_true', default=False, help='Suppress all non-essential console output (progress bars, summaries, info). Errors still shown.')

    subparsers = parser.add_subparsers(dest='command', required=True, help='Action to perform')

    # --- Rename Subparser ---
    parser_rename = subparsers.add_parser('rename', help='Scan and rename files.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_rename.add_argument("directory", type=Path, help="Directory to process.")
    parser_rename.add_argument("--live", action="store_true", default=False, help="Perform live run (Default: dry run).")
    parser_rename.add_argument("-r", "--recursive", action=argparse.BooleanOptionalAction, default=None, help="Process recursively (overrides config).")
    parser_rename.add_argument("--processing-mode", choices=['auto', 'series', 'movie'], default=None, help="Force processing mode (overrides config).")
    parser_rename.add_argument("--use-metadata", action=argparse.BooleanOptionalAction, default=None, help="Enable/disable metadata fetching (overrides config).")
    parser_rename.add_argument("--use-stream-info", action=argparse.BooleanOptionalAction, default=None, help="Enable/disable extracting technical stream info (overrides config).")
    parser_rename.add_argument("--preserve-mtime", action=argparse.BooleanOptionalAction, default=None, help="Enable/disable preserving original file modification time (overrides config).")
    parser_rename.add_argument("--series-format", type=str, default=None, help="Series filename format string (overrides config).")
    parser_rename.add_argument("--movie-format", type=str, default=None, help="Movie filename format string (overrides config).")
    parser_rename.add_argument("--subtitle-format", type=str, default=None, help="Subtitle filename format string (overrides config).")
    parser_rename.add_argument("--extensions", type=str, default=None, help="Comma-separated video+associated extensions (overrides config).")
    parser_rename.add_argument("--on-conflict", choices=['skip', 'overwrite', 'suffix', 'fail'], default=None, help="Action on filename conflict (overrides config).")
    parser_rename.add_argument("--create-folders", action=argparse.BooleanOptionalAction, default=None, help="Enable/disable folder creation (overrides config).")
    parser_rename.add_argument("--folder-format-series", type=str, default=None, help="Folder format string for series (overrides config).")
    parser_rename.add_argument("--folder-format-movie", type=str, default=None, help="Folder format string for movies (overrides config).")
    parser_rename.add_argument("--interactive", "-i", action="store_true", default=False, help="Confirm each batch before live action.")
    parser_rename.add_argument("--enable-undo", action=argparse.BooleanOptionalAction, default=None, help="Enable/disable undo logging (overrides config).")
    parser_rename.add_argument("--undo-integrity-hash-full", action=argparse.BooleanOptionalAction, default=None, help="Calculate full file hash for undo log (SLOW, overrides config).")    
    parser_rename.add_argument("--log-file", type=str, default=None, help="Log file path (overrides config).")
    parser_rename.add_argument("--api-rate-limit-delay", type=float, default=None, help="Delay (sec) between API calls (overrides config).")
    parser_rename.add_argument("--scan-strategy", choices=['memory', 'low_memory'], default=None, help="Scanning strategy (overrides config).")
    parser_rename.add_argument("--scene-tags-in-filename", action=argparse.BooleanOptionalAction, default=None, help="Include scene tags in filename (overrides config).")
    parser_rename.add_argument("--scene-tags-to-preserve", type=str, default=None, help="Comma-separated scene tags to preserve (overrides config).")
    parser_rename.add_argument("--subtitle-encoding-detection", action=argparse.BooleanOptionalAction, default=None, help="Detect subtitle encoding (overrides config).")
    parser_rename.add_argument("--confirm-match-below", type=int, metavar="SCORE", default=None, choices=range(0, 101), help="Interactively confirm metadata match if fuzzy score is below SCORE (0-100).")
    parser_rename.add_argument("--series-source-pref", type=str, default=None, help="Preferred metadata source order for series (comma-separated, e.g., tmdb,tvdb or tvdb,tmdb).")    
    parser_rename.add_argument("--movie-yearless-match-confidence", choices=['high', 'medium', 'low', 'confirm'], default=None, help="Confidence requirement for yearless movie matches (overrides config).")
    parser_rename.add_argument("--unknown-file-handling", choices=['skip', 'guessit_only', 'move_to_unknown'], default=None, help="How to handle files where type cannot be determined (overrides config).")
    parser_rename.add_argument("--unknown-files-dir", type=str, default=None, help="Directory for 'move_to_unknown' handling (relative to target or absolute, overrides config).")

    # --- Direct ID Matching Group ---
    id_group = parser_rename.add_mutually_exclusive_group()
    id_group.add_argument("--tmdb-id", type=int, default=None, help="Force using this TMDB ID for metadata (applies to all files in the run).")
    id_group.add_argument("--tvdb-id", type=int, default=None, help="Force using this TVDB ID for series metadata (applies to all series in the run).")

    # --- Safety Options Group ---
    safety_group = parser_rename.add_mutually_exclusive_group()
    safety_group.add_argument("--backup-dir", type=Path, default=None, help="Backup originals before action.")
    safety_group.add_argument("--stage-dir", type=Path, default=None, help="Move files to staging dir.")
    safety_group.add_argument("--trash", action="store_true", default=False, help="Move originals to trash.")

    # --- Undo Subparser ---
    parser_undo = subparsers.add_parser('undo', help='Revert rename operations or list batches.')
    parser_undo.add_argument("batch_id", type=str, nargs='?', default=None, help="Batch ID of the run to undo/preview (required unless --list is used).")
    parser_undo.add_argument("--list", action="store_true", help="List available batch IDs and their timestamps from the undo log.")
    parser_undo.add_argument("--dry-run", action="store_true", help="Show which files would be reverted for the given batch ID without taking action.")
    parser_undo.add_argument("--enable-undo", action=argparse.BooleanOptionalAction, default=None, help="Enable undo log for revert actions (rarely needed).")
    parser_undo.add_argument("--check-integrity", action=argparse.BooleanOptionalAction, default=None, help="Verify size/mtime before reverting.")
    parser_undo.add_argument("--log-file", type=str, default=None, help="Log file path for undo operation.")

    # --- Config Subparser ---
    parser_config = subparsers.add_parser('config', help='Manage application configuration.')
    config_subparsers = parser_config.add_subparsers(dest='config_command', required=True, help='Configuration action to perform')
    
    parser_config_show = config_subparsers.add_parser('show', help='Show the currently loaded configuration.')
    parser_config_show.add_argument('--profile', type=str, help='Show configuration for a specific profile (merges with default).')
    parser_config_show.add_argument('--raw', action="store_true", help="Show the raw TOML content of the loaded config file without merging or validation.")
    
    parser_config_validate = config_subparsers.add_parser('validate', help='Validate the configuration file against the schema.')

    parser_config_generate = config_subparsers.add_parser('generate', help='Generate a default config.toml file.')
    parser_config_generate.add_argument('--output', type=Path, default=None, help='Optional path to save the generated config.toml. Defaults to the standard location (user config or CWD).')
    parser_config_generate.add_argument('--force', '-f', action='store_true', help='Overwrite the config file if it already exists at the target location.')

    # --- Setup Subparser ---
    parser_setup = subparsers.add_parser('setup', help='Interactively set up API keys and other initial configurations.')
    parser_setup.add_argument("--dotenv-path", type=Path, default=None, help="Specify a custom path for the .env file (default: .env in CWD).")

    return parser

def parse_arguments(argv=None):
    parser = create_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, 'profile') or args.profile is None:
        args.profile = 'default'
    
    if hasattr(args, 'use_stream_info') and args.use_stream_info is not None:
        args.extract_stream_info = args.use_stream_info
    if hasattr(args, 'series_source_pref') and args.series_source_pref is not None:
        args.series_metadata_preference = args.series_source_pref
    
    if hasattr(args, 'quiet') and args.quiet and hasattr(args, 'interactive'):
        if args.interactive:
            # This logging will happen after actual log setup in main
            # logging.getLogger(__name__).warning("Both --quiet and --interactive are set. Quiet mode may suppress some interactive prompts.")
            pass
    return args