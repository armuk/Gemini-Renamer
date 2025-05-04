#!/usr/bin/env python3

import sys
import logging
import asyncio # Import asyncio
from pathlib import Path

# Allow running directly from project root during development
# sys.path.insert(0, str(Path(__file__).parent.resolve()))

# Import necessary components from the package
from rename_app.cli import parse_arguments
from rename_app.config_manager import ConfigManager, ConfigHelper, ConfigError
from rename_app.log_setup import setup_logging
from rename_app.main_processor import MainProcessor # Import the (now async-capable) processor
from rename_app.undo_manager import UndoManager
from rename_app.api_clients import initialize_api_clients
from rename_app.exceptions import RenamerError, UserAbortError

log = logging.getLogger("rename_app") # Get the app's logger

# Make the main function async
async def main_async(argv=None):
    args = parse_arguments(argv)

    config_manager = None
    try:
        # 1. Load Config (Sync)
        config_manager = ConfigManager(getattr(args, 'config', None))
        cfg = ConfigHelper(config_manager, args) # Create helper

        # 2. Setup Logging (Sync)
        log_level_str = cfg('log_level', 'INFO')
        log_file_arg = getattr(args, 'log_file', None)
        log_file = cfg('log_file', None, arg_value=log_file_arg)
        setup_logging(
            log_level_console=getattr(logging, log_level_str.upper()),
            log_file=log_file
        )

        log.debug(f"Parsed args: {args}")
        log.debug(f"Using profile: {args.profile}")
        log.info(f"Effective TMDB/TVDB Language: {cfg('tmdb_language', 'en')}")

        # 3. Initialize API Clients (Sync - happens before event loop)
        # Determine if metadata is effectively enabled *before* initializing
        use_metadata_effective = cfg('use_metadata', False, arg_value=getattr(args, 'use_metadata', None))
        if args.command == 'rename' and use_metadata_effective:
             initialize_api_clients(cfg)

        # 4. Initialize Undo Manager (Sync)
        undo_manager = UndoManager(cfg)

        # 5. Execute Command
        if args.command == 'rename':
            # Apply final config settings to args needed by MainProcessor
            args.use_metadata = use_metadata_effective
            args.enable_undo = cfg('enable_undo', False, arg_value=getattr(args, 'enable_undo', None))
            args.dry_run = not args.live
            # Apply other necessary final config overrides to args here...

            processor = MainProcessor(args, cfg, undo_manager)
            # Await the async run_processing method
            await processor.run_processing()

        elif args.command == 'undo':
             # Undo remains synchronous for now
            args.undo_check_integrity = cfg('undo_check_integrity', False, arg_value=getattr(args,'check_integrity', None))
            undo_manager.perform_undo(args.batch_id)

        elif args.command == 'config':
            # Config command remains synchronous
            print("Config command not fully implemented yet.")
            pass

    except ConfigError as e: log.critical(f"Config Error: {e}"); sys.exit(2)
    except UserAbortError as e: log.warning(str(e)); print(f"\n{e}"); sys.exit(130)
    except RenamerError as e: log.error(f"Application Error: {e}"); sys.exit(1)
    # KeyboardInterrupt might be caught by asyncio differently, but keep for sync parts
    except KeyboardInterrupt: log.warning("Operation interrupted."); print("\nCancelled."); sys.exit(130)
    except Exception as e: log.exception("FATAL UNHANDLED ERROR"); print(f"\nFATAL ERROR: {e}", file=sys.stderr); sys.exit(1)

# Standard entry point runs the async main function
if __name__ == "__main__":
    # Add asyncio event loop management for Windows if needed (though often automatic now)
    # if sys.platform == "win32":
    #    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.warning("Operation interrupted by user (main)."); print("\nCancelled."); sys.exit(130)

# --- END OF FILE rename_main.py ---