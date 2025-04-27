#!/usr/bin/env python3

import sys
import logging
from pathlib import Path

# Allow running directly from project root during development
# sys.path.insert(0, str(Path(__file__).parent.resolve()))

# Import necessary components from the package
from rename_app.cli import parse_arguments
from rename_app.config_manager import ConfigManager, ConfigHelper, ConfigError
from rename_app.log_setup import setup_logging
from rename_app.main_processor import MainProcessor
from rename_app.undo_manager import UndoManager
from rename_app.api_clients import initialize_api_clients
from rename_app.exceptions import RenamerError, UserAbortError

# Setup root logger initially to catch early errors
logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')
log = logging.getLogger("rename_app") # Get the app's logger


def main(argv=None):
    args = parse_arguments(argv)

    config_manager = None
    try:
        # 1. Load Config
        config_manager = ConfigManager(getattr(args, 'config', None))
        cfg = ConfigHelper(config_manager, args) # Create helper

        # 2. Setup Logging (using final config)
        log_level_str = cfg('log_level', 'INFO') # Uses helper
        log_file_arg = getattr(args, 'log_file', None) # Check specific command arg
        log_file = cfg('log_file', None, arg_value=log_file_arg)
        setup_logging(
            log_level_console=getattr(logging, log_level_str.upper()),
            log_file=log_file
        )

        log.debug(f"Parsed args: {args}")
        log.debug(f"Using profile: {args.profile}")
        # Log effective language etc.
        log.info(f"Effective TMDB/TVDB Language: {cfg('tmdb_language', 'en')}")

        # 3. Initialize API Clients (if needed by command)
        if args.command == 'rename' and cfg('use_metadata', False):
             initialize_api_clients(cfg)

        # 4. Initialize Undo Manager (based on final config)
        undo_manager = UndoManager(cfg)

        # 5. Execute Command
        if args.command == 'rename':
            # Apply final settings to args object needed by MainProcessor
            # This makes MainProcessor less dependent on the raw cfg helper
            args.use_metadata = cfg('use_metadata', False)
            args.enable_undo = cfg('enable_undo', False) # Pass final decision
            args.dry_run = not args.live # Final decision on dry_run
            # Set other args needed by processor based on cfg...
            # (Example: args.recursive = cfg('recursive', False) etc.)

            processor = MainProcessor(args, cfg, undo_manager)
            processor.run_processing()

        elif args.command == 'undo':
            # Pass final config values needed for undo
            args.undo_check_integrity = cfg('undo_check_integrity', False, arg_value=getattr(args,'check_integrity', None))
            # Note: UndoManager uses cfg directly, maybe pass specific args instead?
            undo_manager.perform_undo(args.batch_id)

        elif args.command == 'config':
            # Handle config commands here (sync)
            print("Config command not fully implemented yet.")
            pass

    except ConfigError as e: log.critical(f"Config Error: {e}"); sys.exit(2)
    except UserAbortError as e: log.warning(str(e)); print(f"\n{e}"); sys.exit(130)
    except RenamerError as e: log.error(f"Application Error: {e}"); sys.exit(1)
    except KeyboardInterrupt: log.warning("Operation interrupted."); print("\nCancelled."); sys.exit(130)
    except Exception as e: log.exception("FATAL UNHANDLED ERROR"); print(f"\nFATAL ERROR: {e}", file=sys.stderr); sys.exit(1)


if __name__ == "__main__":
    main()