#!/usr/bin/env python3

import sys
import logging
import asyncio
import time # Already present
from pathlib import Path
from datetime import datetime
from typing import Any, Optional, TYPE_CHECKING # Added TYPE_CHECKING
import json
import pytomlpp

# --- RICH IMPORT FOR CONFIRMATION ---
import builtins
try:
    from rich.console import Console as RichConsoleActual 
    from rich.prompt import Confirm as RichConfirm 
    from rich.table import Table as RichTable 
    from rich.text import Text as RichText 
    RICH_AVAILABLE_MAIN = True
except ImportError:
    RICH_AVAILABLE_MAIN = False
    class RichConsoleActual: pass 
    class RichConfirm: pass 
    class RichTable: pass 
    class RichText: pass 

    class Console: 
        def __init__(self, quiet: bool = False, **kwargs: Any):
            self.quiet_mode = quiet
            self.is_interactive: bool = False 
            self.is_jupyter: bool = False
            self._live_display: Optional[Any] = None
        
        def print(self, *args: Any, **kwargs: Any) -> None:
            output_dest = kwargs.pop('file', sys.stdout)
            
            if self.quiet_mode and output_dest != sys.stderr:
                return

            processed_args = []
            for arg in args:
                if hasattr(arg, 'plain') and isinstance(getattr(arg, 'plain'), str):
                    processed_args.append(getattr(arg, 'plain'))
                elif hasattr(arg, 'text') and isinstance(getattr(arg, 'text'), str) and not callable(getattr(arg, 'text')):
                    processed_args.append(getattr(arg, 'text'))
                elif isinstance(arg, str):
                    processed_args.append(arg)
                else:
                    processed_args.append(str(arg))
            
            builtins.print(*processed_args, file=output_dest, **kwargs)
            
        def input(self, *args: Any, **kwargs: Any) -> str: 
            return builtins.input(*args, **kwargs) 

        def get_time(self) -> float:
            return time.monotonic()

        def log(self, *args: Any, **kwargs: Any) -> None:
            if self.quiet_mode:
                return
            # message_parts = [str(arg) for arg in args]
            # builtins.print(f"[LOG_FALLBACK_MAIN] {' '.join(message_parts)}", file=sys.stderr)
            pass

        def set_live(self, live_display: Any, overflow: str = "crop", refresh_per_second: float = 4) -> None:
            self._live_display = live_display
            pass

        def _clear_live(self) -> None:
            self._live_display = None

    class Confirm: 
        @staticmethod
        def ask(prompt_text: str, default: bool = False) -> bool:
            response = builtins.input(f"{prompt_text} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
            if not response: return default
            return response == 'y'
    
    class Table: 
        def __init__(self, *args: Any, **kwargs: Any) -> None: pass
        def add_column(self, *args: Any, **kwargs: Any) -> None: pass
        def add_row(self, *args: Any, **kwargs: Any) -> None: pass
    
    class Text: 
        def __init__(self, text_content: str = "", style: str = ""):
             self.text = text_content; self.style = style
        def __str__(self) -> str: return self.text
        @property
        def plain(self) -> str: return self.text

ConsoleClass = RichConsoleActual if RICH_AVAILABLE_MAIN else Console
ConfirmClass = RichConfirm if RICH_AVAILABLE_MAIN else Confirm
TableClass = RichTable if RICH_AVAILABLE_MAIN else Table
TextClass = RichText if RICH_AVAILABLE_MAIN else Text
# --- END RICH IMPORT ---

from rename_app.cli import parse_arguments
from rename_app.config_manager import (
    ConfigManager, ConfigHelper, interactive_api_setup, 
    RootConfigModel, BaseProfileSettings, generate_default_toml_content, 
    DEFAULT_CONFIG_FILENAME, PLATFORMDIRS_AVAILABLE, platformdirs # Import platformdirs here
)
from rename_app.log_setup import setup_logging
from rename_app.main_processor import MainProcessor
from rename_app.undo_manager import UndoManager
from rename_app.api_clients import initialize_api_clients
from rename_app.exceptions import RenamerError, UserAbortError, ConfigError as AppConfigError

if TYPE_CHECKING: # For Pydantic's ValidationError if not directly imported
    from pydantic import ValidationError

log = logging.getLogger("rename_app") 

async def main_async(argv=None):
    args = parse_arguments(argv)
    console = ConsoleClass(quiet=getattr(args, 'quiet', False)) 

    config_manager_instance: Optional[ConfigManager] = None
    cfg: Optional[ConfigHelper] = None
    undo_manager_instance: Optional[UndoManager] = None

    try:
        if args.command == 'setup':
            if getattr(args, 'quiet', False):
                builtins.print("ERROR: Interactive setup cannot be run in quiet mode.", file=sys.stderr) # Use builtins.print for this critical exit
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
            success = interactive_api_setup(dotenv_path_override=args.dotenv_path, quiet_mode=getattr(args, 'quiet', False)) 
            root_logger.handlers = original_root_handlers
            root_logger.setLevel(original_root_level)
            sys.exit(0 if success else 1)

        if args.command == 'config' and args.config_command == 'generate':
            if not log.handlers: # Ensure basic logging if not already set up
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

                if PLATFORMDIRS_AVAILABLE:
                    try:
                        # PLATFORMDIRS_AVAILABLE is from config_manager import now
                        user_dir = Path(platformdirs.user_config_dir("rename_app", "rename_app_author", ensure_exists=False))
                        potential_paths.append(user_dir / DEFAULT_CONFIG_FILENAME)
                    except Exception:
                        pass 
                
                target_path = potential_paths[0].resolve() 
                log.debug(f"Generate config: Using default config path: {target_path}")


            if target_path.exists() and not args.force:
                if getattr(args, 'quiet', False): 
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
                # Use builtins.print for this critical error if console itself might be problematic
                builtins.print(f"Error: Could not write configuration file to {target_path}: {e}", file=sys.stderr)
                log.error(f"Failed to write generated config to {target_path}: {e}")
                sys.exit(1)
            sys.exit(0) 

        config_manager_instance = ConfigManager(
            config_path_override=getattr(args, 'config', None),
            interactive_fallback=not getattr(args, 'quiet', False), 
            quiet_mode=getattr(args, 'quiet', False) 
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
            undo_manager_instance = UndoManager(cfg, quiet_mode=getattr(args, 'quiet', False))
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
                    effective_settings = {}
                    for key in all_possible_keys:
                        effective_settings[key] = cfg(key, default_value=None) 
                    api_keys_info = {
                        "tmdb_api_key_loaded": bool(cfg.get_api_key('tmdb')),
                        "tvdb_api_key_loaded": bool(cfg.get_api_key('tvdb')),
                        "tmdb_language_env": config_manager_instance._api_keys.get('tmdb_language') 
                    }
                    effective_settings["_api_info_"] = api_keys_info
                    try: console.print(json.dumps(effective_settings, indent=2, default=str))
                    except TypeError as e:
                        log.error(f"Could not serialize effective_settings to JSON: {e}")
                        self.console.print("Could not display effective settings due to serialization error. Check logs.", file=sys.stderr) # type: ignore
                        self.console.print("Raw effective settings:", effective_settings, file=sys.stderr) # type: ignore
            elif args.config_command == 'validate':
                console.print(f"--- Validating Configuration File: {config_manager_instance.config_path} ---") 
                if config_manager_instance.config_path.is_file(): 
                    try:
                        raw_toml_content = config_manager_instance.config_path.read_text(encoding='utf-8') 
                        cfg_dict = pytomlpp.loads(raw_toml_content)
                        RootConfigModel.model_validate(cfg_dict) 
                        console.print("[green]Configuration file syntax is valid and conforms to the schema.[/green]")
                        log.info(f"Config file '{config_manager_instance.config_path}' validated successfully by 'config validate' command.") 
                    except pytomlpp.DecodeError as e:
                        msg_content = f"[bold red]Error:[/bold red] Config file '{config_manager_instance.config_path}' is not valid TOML: {e}"
                        if RICH_AVAILABLE_MAIN and isinstance(console, RichConsoleActual):
                            console_stderr_temp = RichConsoleActual(file=sys.stderr)
                            console_stderr_temp.print(msg_content)
                        else:
                            console.print(msg_content, file=sys.stderr)
                        log.error(f"Config file TOML validation failed during 'config validate': {e}")
                    except ValidationError as e_val: # type: ignore Import pydantic.ValidationError if not already.
                        msg_header = f"[bold red]Error:[/bold red] Config file '{config_manager_instance.config_path}' validation failed:"
                        if RICH_AVAILABLE_MAIN and isinstance(console, RichConsoleActual):
                            console_stderr_temp = RichConsoleActual(file=sys.stderr)
                            console_stderr_temp.print(msg_header)
                            for error_item in e_val.errors(): # Use error_item to avoid clash
                                loc = " -> ".join(map(str, error_item['loc']))
                                console_stderr_temp.print(f"  - Field `[yellow]{loc}[/yellow]`: {error_item['msg']} ([i]type: {error_item['type']}[/i])")
                        else:
                            console.print(msg_header, file=sys.stderr)
                            for error_item in e_val.errors():
                                loc = " -> ".join(map(str, error_item['loc']))
                                console.print(f"  - Field `{loc}`: {error_item['msg']} (type: {error_item['type']})", file=sys.stderr)
                        log.error(f"Config file Pydantic validation failed during 'config validate': {e_val.errors()}")
                    except AppConfigError as e_app_cfg: 
                        msg_content = f"[bold red]Error:[/bold red] Config file '{config_manager_instance.config_path}' validation issue:\n{str(e_app_cfg)}"
                        if RICH_AVAILABLE_MAIN and isinstance(console, RichConsoleActual):
                            console_stderr_temp = RichConsoleActual(file=sys.stderr)
                            console_stderr_temp.print(msg_content)
                        else:
                            console.print(msg_content, file=sys.stderr)
                        log.error(f"Config file validation issue during 'config validate': {e_app_cfg}")
                    except Exception as e_unexp:
                        msg_content = f"An unexpected error occurred during validation: {e_unexp}"
                        if RICH_AVAILABLE_MAIN and isinstance(console, RichConsoleActual):
                            console_stderr_temp = RichConsoleActual(file=sys.stderr)
                            console_stderr_temp.print(msg_content)
                        else:
                            console.print(msg_content, file=sys.stderr)
                        log.exception(f"Unexpected error during 'config validate': {e_unexp}")
                else:
                    console.print(f"Config file '[yellow]{config_manager_instance.config_path}[/yellow]' not found. Nothing to validate.") 
        
        elif args.command == 'rename':
            if cfg is None: raise RenamerError("ConfigHelper not initialized for rename command.") 
            if undo_manager_instance is None: 
                undo_manager_instance = UndoManager(cfg, quiet_mode=getattr(args, 'quiet', False))
                if undo_manager_instance.is_enabled:
                    undo_manager_instance.prune_old_batches()

            use_metadata_effective = cfg('use_metadata', False, arg_value=getattr(args, 'use_metadata', None))
            if use_metadata_effective:
                 if not initialize_api_clients(cfg): 
                    warning_msg = "[yellow]Warning: Metadata processing enabled, but failed to initialize API clients (check API keys). Proceeding without metadata.[/yellow]"
                    # Print to stderr if it's a significant warning impacting functionality
                    if RICH_AVAILABLE_MAIN and isinstance(console, RichConsoleActual):
                        if not getattr(args, 'quiet', False):
                            console_stderr_temp = RichConsoleActual(file=sys.stderr)
                            console_stderr_temp.print(warning_msg)
                    else:
                        console.print(warning_msg, file=sys.stderr)
                    log.warning("Metadata fetching will be disabled due to API client initialization failure.")
                    args.use_metadata = False 
            
            args.use_metadata = cfg('use_metadata', False, arg_value=getattr(args, 'use_metadata', None)) 
            args.enable_undo = cfg('enable_undo', False, arg_value=getattr(args, 'enable_undo', None))
            # ... (rest of args.* assignments unchanged)
            
            processor = MainProcessor(args, cfg, undo_manager_instance) 
            await processor.run_processing()

        elif args.command == 'undo':
            if cfg is None: raise RenamerError("ConfigHelper not initialized for undo command.")
            if undo_manager_instance is None: 
                undo_manager_instance = UndoManager(cfg, quiet_mode=getattr(args, 'quiet', False))
                if undo_manager_instance.is_enabled:
                    undo_manager_instance.prune_old_batches()
            
            if args.list:
                if not getattr(args, 'quiet', False):
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
                            except: first_ts_dt = None
                            try: last_ts_dt = datetime.fromisoformat(str(batch['last_timestamp']).replace('Z','+00:00'))
                            except: last_ts_dt = None
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
                 # Corrected stderr printing
                 error_msg_content = "[bold red]Error:[/bold red] Batch ID is required for undo or dry-run. Use --list to see available batches."
                 if RICH_AVAILABLE_MAIN and isinstance(console, RichConsoleActual):
                     if not getattr(args, 'quiet', False): # Avoid printing to stderr if truly quiet and not a fatal crash
                         console_stderr_temp = RichConsoleActual(file=sys.stderr)
                         console_stderr_temp.print(error_msg_content)
                 else:
                     console.print(error_msg_content, file=sys.stderr)
                 sys.exit(1)
            else:
                 log.info(f"Performing undo{' (dry run)' if args.dry_run else ''} for batch: {args.batch_id}")
                 undo_manager_instance.perform_undo(args.batch_id, dry_run=args.dry_run)

    except AppConfigError as e: 
        # Use builtins.print for truly fatal errors that might occur before console is stable
        builtins.print(f"FATAL CONFIGURATION ERROR: {e}", file=sys.stderr)
        if log.handlers: log.critical(f"Config Error: {e}", exc_info=True)
        sys.exit(2)
    except UserAbortError as e:
        if log.handlers: log.warning(str(e))
        # This is a user action, so print to stderr is fine even with Rich console if not quiet
        if RICH_AVAILABLE_MAIN and isinstance(console, RichConsoleActual):
            if not getattr(args, 'quiet', False):
                console_stderr_temp = RichConsoleActual(file=sys.stderr)
                console_stderr_temp.print(f"\n{e}")
        else:
            console.print(f"\n{e}", file=sys.stderr)
        sys.exit(130)
    except RenamerError as e:
        if log.handlers: log.error(f"Application Error: {e}", exc_info=True)
        msg_content = f"[bold red]ERROR:[/bold red] {e}"
        if RICH_AVAILABLE_MAIN and isinstance(console, RichConsoleActual):
            if not getattr(args, 'quiet', False):
                console_stderr_temp = RichConsoleActual(file=sys.stderr)
                console_stderr_temp.print(msg_content)
        else:
            console.print(msg_content, file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        if log.handlers: log.warning("Operation interrupted by user.")
        # This is a user action, print to stderr is fine
        if RICH_AVAILABLE_MAIN and isinstance(console, RichConsoleActual):
            if not getattr(args, 'quiet', False):
                console_stderr_temp = RichConsoleActual(file=sys.stderr)
                console_stderr_temp.print("\nCancelled by user.")
        else:
            console.print("\nCancelled by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as e_fatal: # Last catch-all
        # Use builtins.print for these truly unexpected fatal errors
        builtins.print(f"\nFATAL UNEXPECTED ERROR (main_async): {type(e_fatal).__name__}: {e_fatal}", file=sys.stderr)
        builtins.print("Please check the log file for more details if logging was enabled.", file=sys.stderr)
        if log.handlers and log.level <= logging.DEBUG:
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
        builtins.print(f"TOP LEVEL UNHANDLED EXCEPTION: {type(e_top_level).__name__}: {e_top_level}", file=sys.stderr)
        sys.exit(1)