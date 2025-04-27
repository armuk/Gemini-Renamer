import argparse
import logging
from pathlib import Path
from . import __version__

def create_parser():
    parser = argparse.ArgumentParser(
        description=f"Advanced media renamer (v{__version__}).",
        formatter_class=argparse.RawTextHelpFormatter
    )
    # Global Args
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    parser.add_argument('--config', type=Path, help='Path to TOML config file (overrides default search).')
    parser.add_argument('--profile', type=str, default='default', help='Configuration profile to use.')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default=None, help='Console logging level (overrides config).')
    parser.add_argument('--tmdb-language', type=str, default=None, help='Language for TMDB/TVDB API calls (e.g., "de", overrides config/env).')

    subparsers = parser.add_subparsers(dest='command', required=True, help='Action to perform')

    # --- Rename Command Parser ---
    parser_rename = subparsers.add_parser('rename', help='Scan and rename files.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # Add all rename-specific arguments (default=None allows config override)
    parser_rename.add_argument("directory", type=Path, help="Directory to process.")
    parser_rename.add_argument("--live", action="store_true", default=False, help="Perform live run (Default: dry run).")
    parser_rename.add_argument("-r", "--recursive", action=argparse.BooleanOptionalAction, default=None, help="Process recursively.")
    parser_rename.add_argument("--processing-mode", choices=['auto', 'series', 'movie'], default=None, help="Force processing mode.")
    parser_rename.add_argument("--use-metadata", action=argparse.BooleanOptionalAction, default=None, help="Enable/disable metadata.")
    parser_rename.add_argument("--series-format", type=str, default=None, help="Series format string.")
    parser_rename.add_argument("--movie-format", type=str, default=None, help="Movie format string.")
    parser_rename.add_argument("--subtitle-format", type=str, default=None, help="Subtitle format string.")
    parser_rename.add_argument("--extensions", type=str, default=None, help="Comma-separated video extensions.")
    parser_rename.add_argument("--on-conflict", choices=['skip', 'overwrite', 'suffix', 'fail'], default=None, help="Action on filename conflict.")
    parser_rename.add_argument("--create-folders", action=argparse.BooleanOptionalAction, default=None, help="Enable/disable folder creation.")
    parser_rename.add_argument("--folder-format-series", type=str, default=None, help="Folder format for series.")
    parser_rename.add_argument("--folder-format-movie", type=str, default=None, help="Folder format for movies.")
    parser_rename.add_argument("--interactive", action="store_true", default=False, help="Confirm each batch before live action.")
    parser_rename.add_argument("--enable-undo", action=argparse.BooleanOptionalAction, default=None, help="Enable/disable undo logging.")
    parser_rename.add_argument("--log-file", type=str, default=None, help="Log file path.") # Keep global and specific? Keep specific here.
    parser_rename.add_argument("--api-rate-limit-delay", type=float, default=None, help="Delay (sec) between API calls.")
    # Scene Tag Args
    parser_rename.add_argument("--scene-tags-in-filename", action=argparse.BooleanOptionalAction, default=None, help="Include scene tags.")
    parser_rename.add_argument("--scene-tags-to-preserve", type=str, default=None, help="Comma-separated scene tags.")
    # Subtitle Encoding Arg
    parser_rename.add_argument("--subtitle-encoding-detection", action=argparse.BooleanOptionalAction, default=None, help="Detect subtitle encoding.")
    # Safety group
    safety_group = parser_rename.add_mutually_exclusive_group()
    safety_group.add_argument("--backup-dir", type=Path, default=None, help="Backup originals before action.")
    safety_group.add_argument("--stage-dir", type=Path, default=None, help="Move files to staging dir.")
    safety_group.add_argument("--trash", action="store_true", default=False, help="Move originals to trash.")


    # --- Undo Command Parser ---
    parser_undo = subparsers.add_parser('undo', help='Revert rename operations.')
    parser_undo.add_argument("batch_id", type=str, help="Batch ID of the run to undo.")
    parser_undo.add_argument("--enable-undo", action=argparse.BooleanOptionalAction, default=None, help="Enable undo log for revert actions.")
    parser_undo.add_argument("--check-integrity", action=argparse.BooleanOptionalAction, default=None, help="Verify size/mtime before reverting.")
    # Allow specifying log file for undo operation too?
    parser_undo.add_argument("--log-file", type=str, default=None, help="Log file path for undo operation.")


    # --- Config Command Parser (Example) ---
    # (Same as before)
    parser_config = subparsers.add_parser('config', help='Manage configuration.')
    # ... config subcommands ...

    return parser

def parse_arguments(argv=None):
    parser = create_parser()
    args = parser.parse_args(argv)
    args.profile = getattr(args, 'profile', 'default') or 'default'
    return args