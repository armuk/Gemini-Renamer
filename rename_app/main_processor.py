# --- START OF FILE main_processor.py ---

# rename_app/main_processor.py

import logging
import uuid
import sys # Import sys for sys.exit
from pathlib import Path
from datetime import datetime, timezone
from .metadata_fetcher import MetadataFetcher
from .renamer_engine import RenamerEngine
from .file_system_ops import perform_file_actions
from .utils import scan_media_files
from .exceptions import UserAbortError, RenamerError # Removed FileExistsError import
from .models import MediaInfo, RenamePlan # Import necessary models
# Import API client getters
from .api_clients import get_tmdb_client, get_tvdb_client

# Import tqdm or rich progress if adapting for progress display
try: from tqdm import tqdm; TQDM_AVAILABLE = True
except ImportError: TQDM_AVAILABLE = False
if not TQDM_AVAILABLE:
    def tqdm(iterable, *args, **kwargs): yield from iterable # Dummy

log = logging.getLogger(__name__)

class MainProcessor:
    def __init__(self, args, cfg_helper, undo_manager):
        self.args = args
        self.cfg = cfg_helper
        self.undo_manager = undo_manager # Store undo manager instance
        # Initialize components (consider dependency injection later)
        self.renamer = RenamerEngine(cfg_helper)
        self.metadata_fetcher = None
        # Initialize fetcher only if configured and needed by the command mode
        # Note: We don't check client availability *here*, only if metadata is enabled.
        # The actual client check happens later in run_processing.
        if self.args.use_metadata: # Directly check the final arg value
             log.info("Metadata fetching enabled by configuration/args.")
             # Assuming API clients were initialized *before* this Processor instance
             # Pass the already initialized (or None) clients to the fetcher
             self.metadata_fetcher = MetadataFetcher(cfg_helper)
        else:
             log.info("Metadata fetching disabled by configuration or command line.")


    def _confirm_live_run(self, potential_actions_count):
        """Handles the pre-scan count and user confirmation for live run."""
        if potential_actions_count == 0:
            log.warning("Pre-scan found no files eligible for action based on current settings.")
            return False # Do not proceed

        print("-" * 30)
        print(f"Pre-scan found {potential_actions_count} potential file actions.")
        print("THIS IS A LIVE RUN.")
        # Print details about backup/stage/trash/undo based on args/config
        if self.args.backup_dir: print(f"Originals will be backed up to: {self.args.backup_dir}")
        elif self.args.stage_dir: print(f"Files will be MOVED to staging: {self.args.stage_dir}")
        elif self.args.trash: print("Originals will be MOVED TO TRASH.")
        else: print("Files will be RENAMED/MOVED IN PLACE.")
        # Use cfg helper directly here for consistency
        if self.cfg('enable_undo', False): print("Undo logging is ENABLED.")
        else: print("Undo logging is DISABLED.")
        print("-" * 30)
        try:
            # Use input() which raises EOFError if input stream closes (e.g., piping)
            confirm = input("Proceed with actions? (y/N): ")
            if confirm.lower() != 'y':
                log.info("User aborted live run.")
                print("Operation cancelled by user.")
                return False # Do not proceed
            log.info("User confirmed live run.")
            return True # Proceed
        except EOFError:
             # Handle case where input cannot be read (e.g., non-interactive session)
             log.error("Cannot confirm live run in non-interactive mode without confirmation.")
             print("\nERROR: Cannot confirm live run. Use --force or run interactively if applicable.")
             # Or perhaps default to NO if non-interactive? For safety, let's abort.
             # Consider adding a --force flag to bypass this check if needed.
             return False


    def run_processing(self):
        """Main synchronous processing loop."""
        target_dir = self.args.directory.resolve()
        if not target_dir.is_dir():
            log.critical(f"Target directory not found or is not a directory: {target_dir}")
            return # Exit if dir not found

        # --- Check API Client Availability ---
        if self.args.use_metadata:
            log.debug("Checking API client availability as metadata is enabled.")
            tmdb_available = get_tmdb_client() is not None
            tvdb_available = get_tvdb_client() is not None

            if not tmdb_available and not tvdb_available:
                log.critical("Metadata processing enabled, but FAILED to initialize BOTH TMDB and TVDB API clients.")
                log.critical("Please check API key configuration (.env file or environment variables) and logs for initialization errors.")
                print("\nCRITICAL ERROR: Metadata fetching is enabled, but API clients could not be initialized.")
                print("Ensure TMDB/TVDB API keys are correctly set in a .env file or environment variables.")
                print("Check the log file for details.")
                return # Stop processing
            elif not tmdb_available:
                log.warning("Metadata processing enabled, but TMDB client is unavailable. Metadata quality may be reduced (relying solely on TVDB).")
            elif not tvdb_available:
                 log.warning("Metadata processing enabled, but TVDB client is unavailable. Metadata quality may be reduced (relying solely on TMDB).")
            else:
                log.debug("Required API clients for metadata fetching appear to be available.")

        # 1. Scan files
        file_batches = scan_media_files(target_dir, self.cfg)
        if not file_batches:
             log.warning("No valid video files/batches found matching criteria in the specified directory.")
             return # Exit if nothing to process
        log.info(f"Found {len(file_batches)} batches to process.")

        # 2. Pre-scan & Confirmation (if live run)
        if not self.args.dry_run:
            log.info("Performing pre-scan for live run confirmation...")
            potential_actions_count = 0
            prescan_iterator = tqdm(file_batches.items(), desc="Pre-scan", unit="batch", disable=True)
            for stem, batch_data in prescan_iterator:
                try:
                    media_info = MediaInfo(original_path=batch_data['video'])
                    media_info.guess_info = self.renamer.parse_filename(media_info.original_path)
                    media_info.file_type = self.renamer._determine_file_type(media_info.guess_info)
                    if self.metadata_fetcher and media_info.file_type != 'unknown':
                        # (Metadata fetching logic - unchanged)
                        if media_info.file_type == 'series':
                            ep_list_guess = media_info.guess_info.get('episode_list', [media_info.guess_info.get('episode')])
                            valid_ep_list = [ep for ep in ep_list_guess if isinstance(ep, int) and ep > 0] if ep_list_guess else []
                            if valid_ep_list:
                                guessed_title_raw = media_info.guess_info.get('title')
                                guessed_title = (guessed_title_raw[0] if isinstance(guessed_title_raw, list) else guessed_title_raw) or 'Unknown Show'
                                season_num = media_info.guess_info.get('season', 0)
                                episodes_tuple = tuple(valid_ep_list)
                                year_guess = media_info.guess_info.get('year')
                                fetched_metadata = self.metadata_fetcher.fetch_series_metadata(guessed_title, season_num, episodes_tuple, year_guess=year_guess)
                                media_info.metadata = fetched_metadata
                        elif media_info.file_type == 'movie':
                             year_guess = media_info.guess_info.get('year')
                             guessed_title_raw = media_info.guess_info.get('title')
                             guessed_title = (guessed_title_raw[0] if isinstance(guessed_title_raw, list) else guessed_title_raw) or 'Unknown Movie'
                             media_info.metadata = self.metadata_fetcher.fetch_movie_metadata(guessed_title, year_guess)

                    plan = self.renamer.plan_rename(batch_data['video'], batch_data['associated'], media_info)
                    if plan.status == 'success':
                         action_count = len(plan.actions) + (1 if plan.created_dir_path else 0)
                         potential_actions_count += action_count
                except Exception as e:
                    log.warning(f"Pre-scan planning error for batch '{stem}': {e}")

            if not self._confirm_live_run(potential_actions_count):
                return

        # 3. Main Processing Loop
        # Use a single consistent run_batch_id for the entire run
        run_batch_id = f"run-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        log.info(f"Starting processing run ID: {run_batch_id}")
        results_summary = {'success': 0, 'skipped': 0, 'error': 0, 'actions': 0}

        process_iterator = tqdm(file_batches.items(), desc="Processing", unit="batch", disable=not TQDM_AVAILABLE or self.args.interactive)
        print("-" * 30) # Separator before detailed output

        for stem, batch_data in process_iterator:
            if not (self.args.interactive and TQDM_AVAILABLE):
                 if batch_data.get('video'):
                      process_iterator.set_postfix_str(batch_data['video'].name, refresh=True)
                 else:
                      process_iterator.set_postfix_str("Invalid Batch Data", refresh=True)

            user_choice = 'y'
            plan = None

            try:
                if not batch_data.get('video'):
                     log.error(f"Skipping batch with missing video data for stem '{stem}'")
                     results_summary['error'] += 1
                     continue

                # a. Gather Info (Parse + Metadata)
                media_info = MediaInfo(original_path=batch_data['video'])
                media_info.guess_info = self.renamer.parse_filename(media_info.original_path)
                media_info.file_type = self.renamer._determine_file_type(media_info.guess_info)
                if self.metadata_fetcher and media_info.file_type != 'unknown':
                    # (Metadata fetching logic - unchanged)
                     if media_info.file_type == 'series':
                         ep_list_guess = media_info.guess_info.get('episode_list', [media_info.guess_info.get('episode')])
                         valid_ep_list = [ep for ep in ep_list_guess if isinstance(ep, int) and ep > 0] if ep_list_guess else []
                         if valid_ep_list:
                              guessed_title_raw = media_info.guess_info.get('title')
                              guessed_title = (guessed_title_raw[0] if isinstance(guessed_title_raw, list) else guessed_title_raw) or 'Unknown Show'
                              year_guess = media_info.guess_info.get('year')
                              media_info.metadata = self.metadata_fetcher.fetch_series_metadata(guessed_title, media_info.guess_info.get('season', 0), tuple(valid_ep_list), year_guess=year_guess)
                     elif media_info.file_type == 'movie':
                         guessed_title_raw = media_info.guess_info.get('title')
                         guessed_title = (guessed_title_raw[0] if isinstance(guessed_title_raw, list) else guessed_title_raw) or 'Unknown Movie'
                         year_guess = media_info.guess_info.get('year')
                         media_info.metadata = self.metadata_fetcher.fetch_movie_metadata(guessed_title, year_guess)

                # b. Plan Rename Actions
                plan = self.renamer.plan_rename(batch_data['video'], batch_data['associated'], media_info)

                # c. Interactive Confirmation (per batch)
                if self.args.interactive and not self.args.dry_run and plan.status == 'success':
                    # (Interactive logic - unchanged)
                    print(f"\n--- Batch: {batch_data['video'].name} ---")
                    if plan.created_dir_path: print(f"  Plan: create_dir '{plan.created_dir_path}'")
                    for action in plan.actions: print(f"  Plan: {action.action_type} '{action.original_path.name}' -> '{action.new_path}'")
                    try:
                         user_choice = input("Apply this batch? [y/N/s/q]: ").lower()
                         if user_choice == 's': plan.status = 'skipped'; plan.message = "Skipped by user (interactive)."
                         elif user_choice == 'q': raise UserAbortError("User quit during interactive mode.")
                         elif user_choice != 'y': plan.status = 'skipped'; plan.message = "Skipped by user (interactive)."
                    except EOFError: raise UserAbortError("User quit (EOF) during interactive mode.")

                # d. Execute Plan (if not skipped/failed)
                action_result = {'success': False, 'message': plan.message, 'actions_taken': 0}
                if plan.status == 'success' and user_choice == 'y': # Check user choice too
                    if self.args.dry_run:
                         # (Dry run simulation - unchanged)
                         action_result['success'] = True
                         dry_run_msgs = []
                         if plan.created_dir_path: dry_run_msgs.append(f"DRY RUN: Would create dir '{plan.created_dir_path}'")
                         dry_run_msgs.extend([f"DRY RUN: Would {a.action_type} '{a.original_path.name}' -> '{a.new_path}'" for a in plan.actions])
                         action_result['message'] = "\n".join(dry_run_msgs)
                         action_result['actions_taken'] = len(plan.actions) + (1 if plan.created_dir_path else 0)
                    else:
                        # --- FIX: Pass run_batch_id AGAIN ---
                        action_result = perform_file_actions(
                            plan=plan,
                            run_batch_id=run_batch_id, # Pass the consistent run ID
                            args_ns=self.args,
                            cfg_helper=self.cfg,
                            undo_manager=self.undo_manager
                        )
                        # --- END FIX ---
                elif plan.status == 'skipped':
                     action_result['success'] = False
                     action_result['message'] = plan.message or f"Skipped batch {stem}."
                else: # Failed planning
                     action_result['success'] = False
                     action_result['message'] = f"ERROR: Planning failed for '{stem}'. Reason: {plan.message}"


                # e. Update Summary & Print Result
                if action_result.get('success', False):
                     results_summary['success'] += 1
                     results_summary['actions'] += action_result.get('actions_taken', 0)
                elif plan and plan.status == 'skipped':
                     results_summary['skipped'] += 1
                else:
                     results_summary['error'] += 1

                if action_result.get('message'):
                     print(action_result['message'])
                elif plan and plan.status != 'success' and plan.message:
                      print(plan.message)
                elif plan is None:
                      print(f"ERROR: Could not process batch for stem '{stem}' - Plan object is None.")
                      results_summary['error'] += 1

                if not self.args.interactive and not self.args.dry_run and action_result.get('success'):
                     print("---")

            # (Exception handling - unchanged)
            except UserAbortError as e:
                log.warning(str(e)); print(f"\n{e}"); break # Stop processing batches
            except FileExistsError as e:
                log.critical(str(e)); print(f"\nSTOPPING: {e}"); results_summary['error'] += 1; break
            except Exception as e:
                results_summary['error'] += 1
                log.exception(f"Critical unhandled error processing batch '{stem}': {e}")
                print(f"CRITICAL ERROR processing batch {stem}. See log.")


        # 4. Final Summary Printout
        # (Summary logic - unchanged)
        print("-" * 30)
        log.info("Processing complete.")
        print("Processing Summary:")
        print(f"  Batches Scanned: {len(file_batches)}")
        print(f"  Batches Successfully Processed: {results_summary['success']}")
        print(f"  Batches Skipped: {results_summary['skipped']}")
        print(f"  Batches with Errors: {results_summary['error']}")
        if not self.args.dry_run: print(f"  Total File Actions Taken: {results_summary['actions']}")
        print("-" * 30)
        if self.args.dry_run and results_summary['success'] > 0:
            print("DRY RUN COMPLETE. To apply changes, run again with --live")
        # Use the consistent run_batch_id for the final message
        if not self.args.dry_run and self.cfg('enable_undo', False) and results_summary['actions'] > 0:
            script_name = Path(sys.argv[0]).name
            print(f"Undo information logged with Run ID: {run_batch_id}") # Show run_batch_id
            print(f"To undo this run: {script_name} undo {run_batch_id}") # Use run_batch_id
        if not self.args.dry_run and self.args.stage_dir and results_summary['actions'] > 0:
             print(f"Renamed files moved to staging: {self.args.stage_dir}")
        if results_summary['error'] > 0:
            print(f"WARNING: {results_summary['error']} errors occurred. Check logs.")
        if results_summary['error'] == 0 and (results_summary['success'] > 0 or results_summary['skipped'] == len(file_batches)):
             print("Operation finished successfully.")

# --- END OF FILE main_processor.py ---