#!/usr/bin/env python3

import sys
import logging
import asyncio
from pathlib import Path
from datetime import datetime
import json
import pytomlpp

# --- RICH IMPORT FOR CONFIRMATION ---
import builtins
try:
    from rich.console import Console
    from rich.prompt import Confirm
    RICH_AVAILABLE_MAIN = True
except ImportError:
    RICH_AVAILABLE_MAIN = False
    class Console: # Fallback
        def print(self, *args, **kwargs): builtins.print(*args, **kwargs)
        def input(self, *args, **kwargs) -> str: return builtins.input(*args, **kwargs)
    class Confirm: # Fallback
        @staticmethod
        def ask(prompt_text: str, default: bool = False) -> bool:
            response = builtins.input(f"{prompt_text} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
            if not response: return default
            return response == 'y'
# --- END RICH IMPORT ---


from rename_app.cli import parse_arguments
from rename_app.config_manager import ConfigManager, ConfigHelper, ConfigError, interactive_api_setup, RootConfigModel, BaseProfileSettings, generate_default_toml_content, DEFAULT_CONFIG_FILENAME, Optional
from rename_app.log_setup import setup_logging
from rename_app.main_processor import MainProcessor
from rename_app.undo_manager import UndoManager
from rename_app.api_clients import initialize_api_clients
from rename_app.exceptions import RenamerError, UserAbortError

log = logging.getLogger("rename_app") 

async def main_async(argv=None):
    args = parse_arguments(argv)
    console = Console() 

    config_manager_instance: Optional[ConfigManager] = None
    # Corrected type hint for cfg - it's an instance of ConfigHelper, or None before initialized
    cfg: Optional[ConfigHelper] = None # <--- CORRECTED HERE

    try:
        if args.command == 'setup':
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
            success = interactive_api_setup(dotenv_path_override=args.dotenv_path)
            root_logger.handlers = original_root_handlers
            root_logger.setLevel(original_root_level)
            sys.exit(0 if success else 1)

        # --- Handle `config generate` command FIRST ---
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
                # Determine default path without full ConfigManager init or interactive fallback
                # This re-uses the logic from ConfigManager._resolve_config_path but simplified for this purpose
                # to avoid premature interactive prompts.
                
                # Priority:
                # 1. --config arg if provided (but this is for loading, not generating to, unless used as target)
                #    For generate, if args.output is None, we use standard search.
                # 2. CWD
                # 3. User config dir (if platformdirs available)
                
                # Simplified path resolution for generate default location
                potential_paths = []
                cwd_config = Path.cwd() / DEFAULT_CONFIG_FILENAME
                potential_paths.append(cwd_config) # Prefer CWD for default generation if not specified

                if platformdirs.PLATFORMDIRS_AVAILABLE:
                    try:
                        user_dir = Path(platformdirs.user_config_dir("rename_app", "rename_app_author", ensure_exists=False))
                        potential_paths.append(user_dir / DEFAULT_CONFIG_FILENAME)
                    except Exception:
                        pass # Ignore platformdirs error for this specific path determination

                # Use the first potential path that is "sensible" (e.g., CWD first)
                target_path = potential_paths[0].resolve() # Default to CWD
                # If you prefer user config dir as default over CWD for generation:
                # if user_config_path_obj_for_generate: target_path = user_config_path_obj_for_generate.resolve()
                
                log.debug(f"Generate config: Using default config path: {target_path}")


            if target_path.exists() and not args.force:
                console.print(f"[bold yellow]Warning:[/bold yellow] Config file already exists at [cyan]{target_path}[/cyan].")
                if not Confirm.ask("Overwrite existing file?", default=False):
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
                console.print(f"[bold red]Error:[/bold red] Could not write configuration file to [cyan]{target_path}[/cyan]: {e}")
                log.error(f"Failed to write generated config to {target_path}: {e}")
                sys.exit(1)
            sys.exit(0) # Exit after generating config

        # --- Standard ConfigManager initialization for all other commands ---
        # Now, ConfigManager will be initialized. If config file is still missing (and not 'config generate'),
        # its internal interactive_fallback will trigger.
        config_manager_instance = ConfigManager(config_path_override=getattr(args, 'config', None))
        cfg = ConfigHelper(config_manager_instance, args)

        # Setup full logging based on loaded config and CLI args
        log_level_from_args = getattr(args, 'log_level', None)
        log_level_str = cfg('log_level', 'INFO', arg_value=log_level_from_args)
        log_level_val_console = getattr(logging, log_level_str.upper(), logging.INFO)
        log_file_arg = getattr(args, 'log_file', None)
        log_file_path_cfg = cfg('log_file', None, arg_value=log_file_arg)

        setup_logging(
            log_level_console=log_level_val_console,
            log_file=log_file_path_cfg
        )
        log.debug(f"Full logging configured. Parsed args: {args}") # Re-log with full config
        log.debug(f"Using profile: {args.profile}")
        log.info(f"Effective TMDB/TVDB Language: {cfg('tmdb_language', 'en')}")

        # ... (rest of the command handling for 'config show/validate', 'rename', 'undo')
        if args.command == 'config': # 'show' or 'validate'
            if args.config_command == 'show':
                console.print(f"--- Configuration Effective for Profile: '{args.profile}' ---")
                config_file_loc = config_manager_instance.config_path # type: ignore
                if config_file_loc.is_file():
                    console.print(f"Config file loaded: [cyan]{config_file_loc}[/cyan]")
                else:
                    console.print(f"Config file [yellow]{config_file_loc}[/yellow] not found. Using internal defaults and environment variables.")
                if getattr(args, 'raw', False):
                    console.print("\n--- Raw TOML Content ---")
                    raw_content = config_manager_instance.get_raw_toml_content() # type: ignore
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
                        "tmdb_language_env": config_manager_instance._api_keys.get('tmdb_language') # type: ignore
                    }
                    effective_settings["_api_info_"] = api_keys_info
                    try: console.print(json.dumps(effective_settings, indent=2, default=str))
                    except TypeError as e:
                        log.error(f"Could not serialize effective_settings to JSON: {e}")
                        console.print("Could not display effective settings due to serialization error. Check logs.")
                        console.print("Raw effective settings:", effective_settings)
            elif args.config_command == 'validate':
                console.print(f"--- Validating Configuration File: {config_manager_instance.config_path} ---") # type: ignore
                if config_manager_instance.config_path.is_file(): # type: ignore
                    try:
                        raw_toml_content = config_manager_instance.config_path.read_text(encoding='utf-8') # type: ignore
                        cfg_dict = pytomlpp.loads(raw_toml_content)
                        RootConfigModel.model_validate(cfg_dict) 
                        console.print("[green]Configuration file syntax is valid and conforms to the schema.[/green]")
                        log.info(f"Config file '{config_manager_instance.config_path}' validated successfully by 'config validate' command.") # type: ignore
                    except pytomlpp.DecodeError as e:
                        console.print(f"[bold red]Error:[/bold red] Config file '{config_manager_instance.config_path}' is not valid TOML: {e}") # type: ignore
                        log.error(f"Config file TOML validation failed during 'config validate': {e}")
                    except ValidationError as e: 
                        console.print(f"[bold red]Error:[/bold red] Config file '{config_manager_instance.config_path}' validation failed:") # type: ignore
                        for error in e.errors():
                            loc = " -> ".join(map(str, error['loc']))
                            console.print(f"  - Field `[yellow]{loc}[/yellow]`: {error['msg']} ([i]type: {error['type']}[/i])")
                        log.error(f"Config file Pydantic validation failed during 'config validate': {e.errors()}")
                    except ConfigError as e: 
                        console.print(f"[bold red]Error:[/bold red] Config file '{config_manager_instance.config_path}' validation issue:") # type: ignore
                        console.print(str(e))
                    except Exception as e:
                        console.print(f"An unexpected error occurred during validation: {e}")
                        log.exception(f"Unexpected error during 'config validate': {e}")
                else:
                    console.print(f"Config file '[yellow]{config_manager_instance.config_path}[/yellow]' not found. Nothing to validate.") # type: ignore
        
        elif args.command == 'rename':
            # (Rest of rename logic is the same, ensure cfg is not None)
            if cfg is None: raise RenamerError("ConfigHelper not initialized for rename command.") # Should not happen
            use_metadata_effective = cfg('use_metadata', False, arg_value=getattr(args, 'use_metadata', None))
            if use_metadata_effective:
                 if not initialize_api_clients(cfg): 
                    console.print("[yellow]Warning: Metadata processing enabled, but failed to initialize API clients (check API keys). Proceeding without metadata.[/yellow]")
                    log.warning("Metadata fetching will be disabled due to API client initialization failure.")
                    args.use_metadata = False 
            undo_manager = UndoManager(cfg) 
            if undo_manager.is_enabled: undo_manager.prune_old_batches()
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
            processor = MainProcessor(args, cfg, undo_manager)
            await processor.run_processing()

        elif args.command == 'undo':
            # (Rest of undo logic is the same, ensure cfg is not None)
            if cfg is None: raise RenamerError("ConfigHelper not initialized for undo command.") # Should not happen
            undo_manager = UndoManager(cfg)
            if undo_manager.is_enabled: undo_manager.prune_old_batches()
            if args.list:
                log.info("Listing undo batches...")
                batches = undo_manager.list_batches()
                if not batches: console.print("No undo batches found in the log.")
                else:
                    console.print("Available Undo Batches:")
                    table = Table(show_header=True, header_style="bold magenta")
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
            elif not args.batch_id:
                 log.error("Batch ID is required for undo/dry-run unless --list is specified.")
                 console.print("[bold red]Error:[/bold red] Batch ID is required for undo or dry-run. Use --list to see available batches.")
                 sys.exit(1)
            else:
                 log.info(f"Performing undo{' (dry run)' if args.dry_run else ''} for batch: {args.batch_id}")
                 undo_manager.perform_undo(args.batch_id, dry_run=args.dry_run)

    except ConfigError as e:
        print(f"FATAL CONFIGURATION ERROR: {e}", file=sys.stderr)
        if log.handlers: log.critical(f"Config Error: {e}", exc_info=True)
        sys.exit(2)
    # ... (rest of exception handling unchanged) ...
    except UserAbortError as e:
        if log.handlers: log.warning(str(e))
        console.print(f"\n{e}")
        sys.exit(130) 
    except RenamerError as e:
        if log.handlers: log.error(f"Application Error: {e}", exc_info=True)
        console.print(f"[bold red]ERROR:[/bold red] {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        if log.handlers: log.warning("Operation interrupted by user.")
        console.print("\nCancelled by user.")
        sys.exit(130)
    except Exception as e:
        if log.handlers and log.level <= logging.DEBUG: 
             log.exception("FATAL UNHANDLED ERROR")
        elif log.handlers: 
             log.critical(f"FATAL UNHANDLED ERROR: {type(e).__name__}: {e}")
        console.print(f"\n[bold red]FATAL UNEXPECTED ERROR:[/bold red] {type(e).__name__}: {e}")
        console.print("Please check the log file for more details if logging was enabled.")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user (main entry).")
        sys.exit(130)
    except Exception as e_top:
        print(f"TOP LEVEL UNHANDLED EXCEPTION: {type(e_top).__name__}: {e_top}", file=sys.stderr)
        sys.exit(1)