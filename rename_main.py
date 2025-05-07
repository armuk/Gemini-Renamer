#!/usr/bin/env python3

import sys
import logging
import asyncio
from pathlib import Path
from datetime import datetime
import json
import pytomlpp

from rename_app.cli import parse_arguments
from rename_app.config_manager import ConfigManager, ConfigHelper, ConfigError, interactive_api_setup, RootConfigModel, BaseProfileSettings
from rename_app.log_setup import setup_logging
from rename_app.main_processor import MainProcessor
from rename_app.undo_manager import UndoManager
from rename_app.api_clients import initialize_api_clients
from rename_app.exceptions import RenamerError, UserAbortError

log = logging.getLogger("rename_app")

async def main_async(argv=None):
    args = parse_arguments(argv)

    config_manager_instance = None
    try:
        if args.command == 'setup':
            # ... (setup command logic unchanged) ...
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
            success = interactive_api_setup(dotenv_path_override=args.dotenv_path)
            root_logger.handlers = original_root_handlers
            root_logger.setLevel(original_root_level)
            sys.exit(0 if success else 1)

        config_manager_instance = ConfigManager(getattr(args, 'config', None))
        cfg = ConfigHelper(config_manager_instance, args)

        log_level_from_args = getattr(args, 'log_level', None)
        log_level_str = cfg('log_level', 'INFO', arg_value=log_level_from_args)
        log_file_arg = getattr(args, 'log_file', None)
        log_file_cfg = cfg('log_file', None, arg_value=log_file_arg)
        setup_logging(
            log_level_console=getattr(logging, log_level_str.upper()),
            log_file=log_file_cfg
        )

        log.debug(f"Parsed args: {args}")
        log.debug(f"Using profile: {args.profile}")
        log.info(f"Effective TMDB/TVDB Language: {cfg('tmdb_language', 'en')}")

        if args.command == 'config':
            # ... (config command logic unchanged) ...
            if args.config_command == 'show':
                print(f"--- Configuration Effective for Profile: '{args.profile}' ---")
                print(f"Config file loaded: {config_manager_instance.config_path if config_manager_instance.config_path.is_file() else 'Not found, using defaults.'}")
                if getattr(args, 'raw', False):
                    print("\n--- Raw TOML Content ---")
                    raw_content = config_manager_instance.get_raw_toml_content()
                    if raw_content: print(raw_content)
                    else: print("# No config file loaded or content was empty.")
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
                    try:
                        print(json.dumps(effective_settings, indent=2, default=str))
                    except TypeError as e:
                        log.error(f"Could not serialize effective_settings to JSON: {e}")
                        print("Could not display effective settings due to serialization error. Check logs.")
                        print("Raw effective settings:", effective_settings)
            elif args.config_command == 'validate':
                print(f"--- Validating Configuration File: {config_manager_instance.config_path} ---")
                if config_manager_instance.config_path.is_file():
                    try:
                        raw_toml_content = config_manager_instance.config_path.read_text(encoding='utf-8')
                        cfg_dict = pytomlpp.loads(raw_toml_content)
                        RootConfigModel.model_validate(cfg_dict)
                        print("Configuration file syntax is valid and conforms to the schema.")
                        log.info(f"Config file '{config_manager_instance.config_path}' validated successfully.")
                    except pytomlpp.DecodeError as e:
                        print(f"Error: Config file '{config_manager_instance.config_path}' is not valid TOML: {e}")
                        log.error(f"Config file TOML validation failed: {e}")
                    except ConfigError as e:
                        print(f"Error: Config file '{config_manager_instance.config_path}' validation failed:")
                        print(str(e))
                        log.error(f"Config file Pydantic validation failed: {e}")
                    except Exception as e:
                        print(f"An unexpected error occurred during validation: {e}")
                        log.exception(f"Unexpected error during config validation: {e}")
                else:
                    print(f"Config file '{config_manager_instance.config_path}' not found. Nothing to validate.")
            pass

        elif args.command == 'rename':
            use_metadata_effective = cfg('use_metadata', False, arg_value=getattr(args, 'use_metadata', None))
            if use_metadata_effective:
                 initialize_api_clients(cfg)
            undo_manager = UndoManager(cfg)
            if undo_manager.is_enabled:
                 undo_manager.prune_old_batches()

            args.use_metadata = use_metadata_effective
            args.enable_undo = cfg('enable_undo', False, arg_value=getattr(args, 'enable_undo', None))
            args.dry_run = not args.live
            args.live = args.live
            args.trash = getattr(args, 'trash', False)
            args.recursive = cfg('recursive', False, arg_value=getattr(args, 'recursive', None))
            args.create_folders = cfg('create_folders', False, arg_value=getattr(args, 'create_folders', None))
            args.scan_strategy = cfg('scan_strategy', 'memory', arg_value=getattr(args, 'scan_strategy', None))
            args.extract_stream_info = cfg('extract_stream_info', False, arg_value=getattr(args, 'use_stream_info', None))
            # --- NEW: Pass unknown file handling config to args for MainProcessor ---
            args.unknown_file_handling = cfg('unknown_file_handling', 'skip', arg_value=getattr(args, 'unknown_file_handling', None))
            args.unknown_files_dir = cfg('unknown_files_dir', '_unknown_files_', arg_value=getattr(args, 'unknown_files_dir', None))
            # --- END NEW ---

            processor = MainProcessor(args, cfg, undo_manager)
            await processor.run_processing()

        elif args.command == 'undo':
            # ... (undo command logic unchanged) ...
            undo_manager = UndoManager(cfg)
            if undo_manager.is_enabled:
                 undo_manager.prune_old_batches()
            if args.list:
                log.info("Listing undo batches...")
                batches = undo_manager.list_batches()
                if not batches: print("No undo batches found in the log.")
                else:
                    print("Available Undo Batches:")
                    print("-" * 60); print(f"{'Batch ID':<25} {'Actions':<8} {'First Action':<22} {'Last Action':<22}"); print("-" * 60)
                    for batch in batches:
                         try: first_ts = datetime.fromisoformat(batch['first_timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                         except: first_ts = batch['first_timestamp']
                         try: last_ts = datetime.fromisoformat(batch['last_timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                         except: last_ts = batch['last_timestamp']
                         print(f"{batch['batch_id']:<25} {batch['action_count']:<8} {first_ts:<22} {last_ts:<22}")
                    print("-" * 60)
            elif not args.batch_id:
                 log.error("Batch ID is required for undo/dry-run unless --list is specified.")
                 print("Error: Batch ID is required for undo or dry-run. Use --list to see available batches.")
                 sys.exit(1)
            else:
                 log.info(f"Performing undo{' (dry run)' if args.dry_run else ''} for batch: {args.batch_id}")
                 undo_manager.perform_undo(args.batch_id, dry_run=args.dry_run)

    except ConfigError as e: log.critical(f"Config Error: {e}"); sys.exit(2)
    except UserAbortError as e: log.warning(str(e)); print(f"\n{e}"); sys.exit(130)
    except RenamerError as e: log.error(f"Application Error: {e}"); sys.exit(1)
    except KeyboardInterrupt: log.warning("Operation interrupted."); print("\nCancelled."); sys.exit(130)
    except Exception as e: log.exception("FATAL UNHANDLED ERROR"); print(f"\nFATAL ERROR: {e}", file=sys.stderr); sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.warning("Operation interrupted by user (main)."); print("\nCancelled."); sys.exit(130)
# --- END OF FILE rename_main.py ---