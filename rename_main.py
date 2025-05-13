#!/usr/bin/env python3
import sys
import logging
import asyncio
# time is now implicitly handled by ui_utils if its Console fallback uses it
from pathlib import Path
from datetime import datetime
from typing import Any, Optional, TYPE_CHECKING, Dict, List # Added List
import json
import pytomlpp
import builtins

# --- MODIFIED RICH IMPORT ---
from rename_app.ui_utils import (
    ConsoleClass, ConfirmClass, TableClass, PanelClass, TextClass,
    RICH_AVAILABLE_UI as RICH_AVAILABLE_MAIN, RichConsoleActual # Import RichConsoleActual for isinstance check
)
# --- END MODIFIED RICH IMPORT ---

from rename_app.cli import parse_arguments
from rename_app.config_manager import (
    ConfigManager, ConfigHelper, interactive_api_setup,
    RootConfigModel, BaseProfileSettings, generate_default_toml_content,
    DEFAULT_CONFIG_FILENAME, PLATFORMDIRS_AVAILABLE, platformdirs
)
from rename_app.log_setup import setup_logging
from rename_app.main_processor import MainProcessor
from rename_app.undo_manager import UndoManager
from rename_app.api_clients import initialize_api_clients
from rename_app.exceptions import RenamerError, UserAbortError, ConfigError as AppConfigError

if TYPE_CHECKING:
    from pydantic import ValidationError

log = logging.getLogger("rename_app")

# Helper to print to stderr, respecting quiet mode for Rich plain text
def print_stderr_message(console_obj: ConsoleClass, message: Any, is_quiet: bool, is_rich_available: bool):
    """
    Prints a message to stderr.
    If Rich is available and not in quiet mode, it attempts to print styled text.
    Otherwise, it prints plain text or uses the fallback console's stderr printing.
    """
    if is_rich_available and isinstance(console_obj, RichConsoleActual):
        if not is_quiet:
            # For Rich, create a temporary console for stderr to print styled message
            try:
                console_stderr_temp = RichConsoleActual(file=sys.stderr, width=console_obj.width) # type: ignore
                console_stderr_temp.print(message)
                return
            except Exception: # Fallback if even temp console fails
                pass # Fall through to builtins.print
        # If quiet or temp Rich console failed, print plain text for Rich objects
        plain_message = message.plain if hasattr(message, 'plain') else str(message)
        builtins.print(plain_message, file=sys.stderr)
    else: # Fallback console handles file kwarg correctly
        console_obj.print(message, file=sys.stderr)


async def main_async(argv=None):
    args = parse_arguments(argv)
    is_quiet = getattr(args, 'quiet', False)
    console = ConsoleClass(quiet=is_quiet)

    config_manager_instance: Optional[ConfigManager] = None
    cfg: Optional[ConfigHelper] = None
    undo_manager_instance: Optional[UndoManager] = None

    try:
        if args.command == 'setup':
            if is_quiet:
                builtins.print("ERROR: Interactive setup cannot be run in quiet mode.", file=sys.stderr)
                sys.exit(1)

            raw_log_level_arg = getattr(args, 'log_level', None)
            effective_raw_log_level = raw_log_level_arg if raw_log_level_arg else 'INFO'
            log_level_val = getattr(logging, effective_raw_log_level.upper(), logging.INFO)
            temp_formatter = logging.Formatter('%(levelname)-8s: %(message)s')
            temp_handler = logging.StreamHandler(sys.stderr)
            temp_handler.setFormatter(temp_formatter)
            temp_handler.setLevel(log_level_val)
            root_logger = logging.getLogger()
            original_root_handlers = root_logger.handlers[:]
            original_root_level = root_logger.level
            root_logger.handlers = [temp_handler]
            root_logger.setLevel(log_level_val)
            log.debug(f"Executing setup command with .env path: {args.dotenv_path}")
            success = interactive_api_setup(dotenv_path_override=args.dotenv_path, quiet_mode=is_quiet)
            root_logger.handlers = original_root_handlers
            root_logger.setLevel(original_root_level)
            sys.exit(0 if success else 1)

        if args.command == 'config' and args.config_command == 'generate':
            if not log.handlers:
                 setup_logging(log_level_console=logging.INFO)
            log.info("Executing 'config generate' command.")
            default_config_content = generate_default_toml_content()

            target_path: Path
            if args.output:
                target_path = args.output.resolve()
                log.debug(f"Generate config: User specified output path: {target_path}")
            else:
                potential_paths = []
                cwd_config = Path.cwd() / DEFAULT_CONFIG_FILENAME
                potential_paths.append(cwd_config)

                if PLATFORMDIRS_AVAILABLE and platformdirs:
                    try:
                        user_dir = Path(platformdirs.user_config_dir("rename_app", "rename_app_author", ensure_exists=False))
                        potential_paths.append(user_dir / DEFAULT_CONFIG_FILENAME)
                    except Exception as e_pdir:
                        log.debug(f"Could not get user config dir via platformdirs for generate: {e_pdir}")
                target_path = potential_paths[0].resolve()
                log.debug(f"Generate config: Using default config path: {target_path}")


            if target_path.exists() and not args.force:
                if is_quiet:
                    builtins.print(f"Config file {target_path} exists. Use --force to overwrite (quiet mode).", file=sys.stderr)
                    sys.exit(1)
                console.print(f"[bold yellow]Warning:[/bold yellow] Config file already exists at [cyan]{target_path}[/cyan].")
                if not ConfirmClass.ask("Overwrite existing file?", default=False):
                    console.print("Config file generation cancelled.")
                    sys.exit(0)
                log.info(f"User confirmed overwrite for existing config file at {target_path}")

            try:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                with open(target_path, "w", encoding="utf-8") as f:
                    f.write(default_config_content)
                console.print(f"[green]✓ Default configuration file generated successfully at: {target_path}[/green]")
                log.info(f"Default config.toml generated at {target_path}")
            except IOError as e:
                builtins.print(f"Error: Could not write configuration file to {target_path}: {e}", file=sys.stderr)
                log.error(f"Failed to write generated config to {target_path}: {e}")
                sys.exit(1)
            sys.exit(0)

        config_manager_instance = ConfigManager(
            config_path_override=getattr(args, 'config', None),
            interactive_fallback=not is_quiet,
            quiet_mode=is_quiet
        )
        cfg = ConfigHelper(config_manager_instance, args)

        log_level_from_args = getattr(args, 'log_level', None)
        log_level_str = cfg('log_level', 'INFO', arg_value=log_level_from_args)
        log_level_val_console = getattr(logging, log_level_str.upper(), logging.INFO)
        log_file_arg = getattr(args, 'log_file', None)
        log_file_path_cfg = cfg('log_file', None, arg_value=log_file_arg)

        setup_logging(
            log_level_console=log_level_val_console,
            log_file=log_file_path_cfg
        )

        log.debug(f"Full logging configured. Parsed args: {args}")
        log.debug(f"Using profile: {args.profile}")
        log.info(f"Effective TMDB/TVDB Language: {cfg('tmdb_language', 'en')}")

        if args.command in ['rename', 'undo']:
            undo_manager_instance = UndoManager(cfg, quiet_mode=is_quiet, console_instance=console)
            if undo_manager_instance.is_enabled:
                undo_manager_instance.prune_old_batches()

        if args.command == 'config':
            if args.config_command == 'show':
                console.print(f"--- Configuration Effective for Profile: '{args.profile}' ---")
                config_file_loc = config_manager_instance.config_path
                if config_file_loc.is_file():
                    console.print(f"Config file loaded: [cyan]{config_file_loc}[/cyan]")
                else:
                    console.print(f"Config file [yellow]{config_file_loc}[/yellow] not found. Using internal defaults and environment variables.")
                if getattr(args, 'raw', False):
                    console.print("\n--- Raw TOML Content ---")
                    raw_content = config_manager_instance.get_raw_toml_content()
                    if raw_content: console.print(raw_content)
                    else: console.print("# No config file loaded or content was empty.")
                else:
                    all_possible_keys = list(BaseProfileSettings.model_fields.keys())
                    effective_settings: Dict[str, Any] = {}
                    for key in all_possible_keys:
                        effective_settings[key] = cfg(key, default_value=None)
                    api_keys_info = {
                        "tmdb_api_key_loaded": bool(cfg.get_api_key('tmdb')),
                        "tvdb_api_key_loaded": bool(cfg.get_api_key('tvdb')),
                        "tmdb_language_env": config_manager_instance._api_keys.get('tmdb_language')
                    }
                    effective_settings["_api_info_"] = api_keys_info
                    try:
                        console.print(json.dumps(effective_settings, indent=2, default=str))
                    except TypeError as e_json:
                        log.error(f"Could not serialize effective_settings to JSON: {e_json}")
                        # Use helper for stderr
                        print_stderr_message(console, "Could not display effective settings due to serialization error. Check logs.", is_quiet, RICH_AVAILABLE_MAIN)
                        print_stderr_message(console, f"Raw effective settings: {effective_settings}", is_quiet, RICH_AVAILABLE_MAIN) # type: ignore
            elif args.config_command == 'validate':
                console.print(f"--- Validating Configuration File: {config_manager_instance.config_path} ---")
                if config_manager_instance.config_path.is_file():
                    try:
                        raw_toml_content = config_manager_instance.config_path.read_text(encoding='utf-8')
                        cfg_dict = pytomlpp.loads(raw_toml_content)
                        RootConfigModel.model_validate(cfg_dict)
                        console.print("[green]Configuration file syntax is valid and conforms to the schema.[/green]")
                        log.info(f"Config file '{config_manager_instance.config_path}' validated successfully by 'config validate' command.")
                    except pytomlpp.DecodeError as e_toml:
                        msg_content = TextClass(f"[bold red]Error:[/bold red] Config file '{config_manager_instance.config_path}' is not valid TOML: {e_toml}", style="bold red")
                        print_stderr_message(console, msg_content, is_quiet, RICH_AVAILABLE_MAIN)
                        log.error(f"Config file TOML validation failed during 'config validate': {e_toml}")
                    except ValidationError as e_val: # type: ignore
                        msg_header = TextClass(f"[bold red]Error:[/bold red] Config file '{config_manager_instance.config_path}' validation failed:", style="bold red")
                        print_stderr_message(console, msg_header, is_quiet, RICH_AVAILABLE_MAIN)
                        for error_item in e_val.errors():
                            loc = " -> ".join(map(str, error_item['loc']))
                            err_detail_msg = TextClass(f"  - Field `[yellow]{loc}[/yellow]`: {error_item['msg']} ([i]type: {error_item['type']}[/i])")
                            print_stderr_message(console, err_detail_msg, is_quiet, RICH_AVAILABLE_MAIN)
                        log.error(f"Config file Pydantic validation failed during 'config validate': {e_val.errors()}")
                    except AppConfigError as e_app_cfg:
                        msg_content = TextClass(f"[bold red]Error:[/bold red] Config file '{config_manager_instance.config_path}' validation issue:\n{str(e_app_cfg)}", style="bold red")
                        print_stderr_message(console, msg_content, is_quiet, RICH_AVAILABLE_MAIN)
                        log.error(f"Config file validation issue during 'config validate': {e_app_cfg}")
                    except Exception as e_unexp_val:
                        msg_content = TextClass(f"An unexpected error occurred during validation: {e_unexp_val}", style="bold red")
                        print_stderr_message(console, msg_content, is_quiet, RICH_AVAILABLE_MAIN)
                        log.exception(f"Unexpected error during 'config validate': {e_unexp_val}")
                else:
                    console.print(f"Config file '[yellow]{config_manager_instance.config_path}[/yellow]' not found. Nothing to validate.")

        elif args.command == 'rename':
            if cfg is None: raise RenamerError("ConfigHelper not initialized for rename command.")
            if undo_manager_instance is None:
                undo_manager_instance = UndoManager(cfg, quiet_mode=is_quiet, console_instance=console)
                if undo_manager_instance.is_enabled:
                    undo_manager_instance.prune_old_batches()

            use_metadata_effective = cfg('use_metadata', False, arg_value=getattr(args, 'use_metadata', None))
            if use_metadata_effective:
                 if not initialize_api_clients(cfg):
                    warning_msg = TextClass("[yellow]Warning: Metadata processing enabled, but failed to initialize API clients (check API keys). Proceeding without metadata.[/yellow]", style="yellow")
                    print_stderr_message(console, warning_msg, is_quiet, RICH_AVAILABLE_MAIN)
                    log.warning("Metadata fetching will be disabled due to API client initialization failure.")
                    args.use_metadata = False

            args.use_metadata = cfg('use_metadata', False, arg_value=getattr(args, 'use_metadata', None))
            args.enable_undo = cfg('enable_undo', False, arg_value=getattr(args, 'enable_undo', None))
            args.live = args.live
            args.trash = getattr(args, 'trash', False)
            args.recursive = cfg('recursive', False, arg_value=getattr(args, 'recursive', None))
            args.create_folders = cfg('create_folders', False, arg_value=getattr(args, 'create_folders', None))
            args.scan_strategy = cfg('scan_strategy', 'memory', arg_value=getattr(args, 'scan_strategy', None))
            args.extract_stream_info = cfg('extract_stream_info', False, arg_value=getattr(args, 'extract_stream_info', None))
            args.unknown_file_handling = cfg('unknown_file_handling', 'skip', arg_value=getattr(args, 'unknown_file_handling', None))
            args.unknown_files_dir = cfg('unknown_files_dir', '_unknown_files_', arg_value=getattr(args, 'unknown_files_dir', None))

            processor = MainProcessor(args, cfg, undo_manager_instance)
            await processor.run_processing()

        elif args.command == 'undo':
            if cfg is None: raise RenamerError("ConfigHelper not initialized for undo command.")
            if undo_manager_instance is None:
                undo_manager_instance = UndoManager(cfg, quiet_mode=is_quiet, console_instance=console)
                if undo_manager_instance.is_enabled:
                    undo_manager_instance.prune_old_batches()

            if args.list:
                if not is_quiet:
                    log.info("Listing undo batches...")
                    batches = undo_manager_instance.list_batches()
                    if not batches: console.print("No undo batches found in the log.")
                    else:
                        console.print("Available Undo Batches:")
                        table = TableClass(show_header=True, header_style="bold magenta")
                        table.add_column("Batch ID", style="cyan", min_width=25)
                        table.add_column("Actions", style="magenta", justify="right")
                        table.add_column("First Action (UTC)", style="green", min_width=20)
                        table.add_column("Last Action (UTC)", style="green", min_width=20)
                        for batch in batches:
                            try: first_ts_dt = datetime.fromisoformat(str(batch['first_timestamp']).replace('Z','+00:00'))
                            except: first_ts_dt = None # type: ignore
                            try: last_ts_dt = datetime.fromisoformat(str(batch['last_timestamp']).replace('Z','+00:00'))
                            except: last_ts_dt = None # type: ignore
                            first_ts_str = first_ts_dt.strftime('%Y-%m-%d %H:%M:%S') if first_ts_dt else str(batch['first_timestamp'])
                            last_ts_str = last_ts_dt.strftime('%Y-%m-%d %H:%M:%S') if last_ts_dt else str(batch['last_timestamp'])
                            table.add_row(str(batch['batch_id']), str(batch['action_count']), first_ts_str, last_ts_str)
                        console.print(table)
                else:
                    log.info("Listing undo batches (quiet mode - output to log only).")
                    batches = undo_manager_instance.list_batches()
                    if not batches: log.info("No undo batches found.")
                    else:
                        for batch in batches: log.info(f"Undo Batch: ID={batch['batch_id']}, Actions={batch['action_count']}")

            elif not args.batch_id:
                 log.error("Batch ID is required for undo/dry-run unless --list is specified.")
                 error_msg_content = TextClass("[bold red]Error:[/bold red] Batch ID is required for undo or dry-run. Use --list to see available batches.", style="bold red")
                 print_stderr_message(console, error_msg_content, is_quiet, RICH_AVAILABLE_MAIN)
                 sys.exit(1)
            else:
                 log.info(f"Performing undo{' (dry run)' if args.dry_run else ''} for batch: {args.batch_id}")
                 undo_manager_instance.perform_undo(args.batch_id, dry_run=args.dry_run)

    except AppConfigError as e_app_cfg_fatal:
        builtins.print(f"FATAL CONFIGURATION ERROR: {e_app_cfg_fatal}", file=sys.stderr)
        if log.handlers: log.critical(f"Config Error: {e_app_cfg_fatal}", exc_info=True)
        sys.exit(2)
    except UserAbortError as e_abort:
        if log.handlers: log.warning(str(e_abort))
        builtins.print(f"\n{e_abort}", file=sys.stderr)
        sys.exit(130)
    except RenamerError as e_rename_app:
        if log.handlers: log.error(f"Application Error: {e_rename_app}", exc_info=True)
        msg_content = TextClass(f"[bold red]ERROR:[/bold red] {e_rename_app}", style="bold red")
        print_stderr_message(console, msg_content, is_quiet, RICH_AVAILABLE_MAIN)
        sys.exit(1)
    except KeyboardInterrupt:
        if log.handlers: log.warning("Operation interrupted by user.")
        builtins.print("\nCancelled by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as e_fatal:
        builtins.print(f"\nFATAL UNEXPECTED ERROR (main_async): {type(e_fatal).__name__}: {e_fatal}", file=sys.stderr)
        builtins.print("Please check the log file for more details if logging was enabled.", file=sys.stderr)
        if log.handlers and logging.getLogger("rename_app").getEffectiveLevel() <= logging.DEBUG:
             log.exception("FATAL UNHANDLED ERROR in main_async")
        elif log.handlers:
             log.critical(f"FATAL UNHANDLED ERROR in main_async: {type(e_fatal).__name__}: {e_fatal}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        builtins.print("\nOperation cancelled by user (main entry).", file=sys.stderr)
        sys.exit(130)
    except Exception as e_top_level:
        # Ensure this gets printed even if logging isn't fully set up
        builtins.print(f"TOP LEVEL UNHANDLED EXCEPTION: {type(e_top_level).__name__}: {e_top_level}", file=sys.stderr)
        # Attempt to log if possible, but don't let logging failure prevent stderr print
        try:
            if log.handlers: # Check if any handlers are configured
                log.critical("TOP LEVEL UNHANDLED EXCEPTION", exc_info=True)
        except Exception:
            pass # Ignore logging errors at this critical stage
        sys.exit(1)