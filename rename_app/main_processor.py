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
        if self.args.use_metadata: # Directly check the final arg value
             log.info("Metadata fetching enabled.")
             # Assuming API clients are initialized separately or via fetcher constructor
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

        # 1. Scan files
        # Pass the resolved target_dir
        file_batches = scan_media_files(target_dir, self.cfg)
        if not file_batches:
             log.warning("No valid video files/batches found matching criteria in the specified directory.")
             return # Exit if nothing to process
        log.info(f"Found {len(file_batches)} batches to process.")

        # 2. Pre-scan & Confirmation (if live run)
        if not self.args.dry_run:
            log.info("Performing pre-scan for live run confirmation...")
            potential_actions_count = 0
            # Disable tqdm for prescan? Might be confusing.
            prescan_iterator = tqdm(file_batches.items(), desc="Pre-scan", unit="batch", disable=True)# not TQDM_AVAILABLE)

            for stem, batch_data in prescan_iterator:
                try:
                    # Simulate planning the batch
                    media_info = MediaInfo(original_path=batch_data['video'])
                    media_info.guess_info = self.renamer.parse_filename(media_info.original_path)
                    media_info.file_type = self.renamer._determine_file_type(media_info.guess_info)

                    # Fetch metadata during pre-scan for accuracy, uses cache if populated
                    if self.metadata_fetcher and media_info.file_type != 'unknown' and self.args.use_metadata:
                        if media_info.file_type == 'series':
                            # Need episode list from guess_info for accurate fetching
                            ep_list_guess = media_info.guess_info.get('episode_list', [media_info.guess_info.get('episode')])
                            # Ensure ep_list_guess contains valid numbers if possible
                            valid_ep_list = [ep for ep in ep_list_guess if isinstance(ep, int) and ep > 0] if ep_list_guess else []
                            if valid_ep_list:
                                # --- FIX: Prepare arguments BEFORE the call ---
                                # 1. Get the raw title value from guessit
                                guessed_title_raw = media_info.guess_info.get('title')

                                # 2. Process it: take first element if list, fallback if None/empty
                                guessed_title = (guessed_title_raw[0] if isinstance(guessed_title_raw, list) else guessed_title_raw) or 'Unknown Show'

                                # 3. Get the season number
                                season_num = media_info.guess_info.get('season', 0)

                                # 4. Convert episode list to tuple
                                episodes_tuple = tuple(valid_ep_list)
                                # --- End Argument Preparation ---

                                # --- FIX: Make ONE call with prepared arguments ---
                                # 5. Call the fetcher function
                                fetched_metadata = self.metadata_fetcher.fetch_series_metadata(
                                    guessed_title,  # Pass the processed string title
                                    season_num,     # Pass the season number
                                    episodes_tuple  # Pass the tuple of episodes
                                )

                                # 6. Assign the result to media_info.metadata
                                media_info.metadata = fetched_metadata
                                # --- End FIX ---

    
                        elif media_info.file_type == 'movie':
                             media_info.metadata = self.metadata_fetcher.fetch_movie_metadata(
                                 media_info.guess_info.get('title','Unknown Movie'),
                                 media_info.guess_info.get('year')
                             )
                    # Now plan based on info gathered
                    plan = self.renamer.plan_rename(batch_data['video'], batch_data['associated'], media_info)
                    if plan.status == 'success':
                         # Count actions more accurately (including potential dir creation)
                         action_count = len(plan.actions) + (1 if plan.created_dir_path else 0)
                         potential_actions_count += action_count
                except Exception as e:
                    log.warning(f"Pre-scan planning error for batch '{stem}': {e}") # Log error but continue pre-scan

            if not self._confirm_live_run(potential_actions_count):
                return # User cancelled or nothing to do


        # 3. Main Processing Loop
        batch_id = f"run-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        log.info(f"Starting processing batch ID: {batch_id}")
        results_summary = {'success': 0, 'skipped': 0, 'error': 0, 'actions': 0}

        process_iterator = tqdm(file_batches.items(), desc="Processing", unit="batch", disable=not TQDM_AVAILABLE or self.args.interactive)
        print("-" * 30) # Separator before detailed output

        for stem, batch_data in process_iterator:
            # Check if interactive mode is active and tqdm is enabled; disable postfix if so
            if not (self.args.interactive and TQDM_AVAILABLE):
                 # Ensure video path exists before accessing name
                 if batch_data.get('video'):
                      process_iterator.set_postfix_str(batch_data['video'].name, refresh=True)
                 else: # Should not happen if scan worked, but safety check
                      process_iterator.set_postfix_str("Invalid Batch Data", refresh=True)


            user_choice = 'y' # Default to yes if not interactive
            plan = None # Initialize plan to None

            try:
                # Ensure video path exists before processing
                if not batch_data.get('video'):
                     log.error(f"Skipping batch with missing video data for stem '{stem}'")
                     results_summary['error'] += 1
                     continue

                # a. Gather Info (Parse + Metadata)
                media_info = MediaInfo(original_path=batch_data['video'])
                media_info.guess_info = self.renamer.parse_filename(media_info.original_path)
                media_info.file_type = self.renamer._determine_file_type(media_info.guess_info)

                if self.metadata_fetcher and media_info.file_type != 'unknown' and self.args.use_metadata:
                    # Fetch metadata (uses internal caching)
                    if media_info.file_type == 'series':
                        ep_list_guess = media_info.guess_info.get('episode_list', [media_info.guess_info.get('episode')])
                        valid_ep_list = [ep for ep in ep_list_guess if isinstance(ep, int) and ep > 0] if ep_list_guess else []
                        if valid_ep_list:
                             # --- FIX: Convert list to tuple for caching ---
                                guessed_title_raw = media_info.guess_info.get('title')
                                guessed_title = (guessed_title_raw[0] if isinstance(guessed_title_raw, list) else guessed_title_raw) or 'Unknown Show'
                                media_info.metadata = self.metadata_fetcher.fetch_series_metadata(
                                guessed_title, # Use the processed title
                                media_info.guess_info.get('season', 0),
                            tuple(valid_ep_list) # Convert to tuple here
                             )
                             # --- End FIX ---
                    elif media_info.file_type == 'movie':
                        # Inside the 'elif media_info.file_type == 'movie':' block
                        guessed_title_raw = media_info.guess_info.get('title')
                        guessed_title = (guessed_title_raw[0] if isinstance(guessed_title_raw, list) else guessed_title_raw) or 'Unknown Movie'
                        media_info.metadata = self.metadata_fetcher.fetch_movie_metadata(
                            guessed_title,
                            media_info.guess_info.get('year')
                         )

                # b. Plan Rename Actions
                plan = self.renamer.plan_rename(batch_data['video'], batch_data['associated'], media_info)

                # c. Interactive Confirmation (per batch)
                if self.args.interactive and not self.args.dry_run and plan.status == 'success':
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
                         # Simulate success for dry run based on plan
                         action_result['success'] = True
                         dry_run_msgs = []
                         if plan.created_dir_path: dry_run_msgs.append(f"DRY RUN: Would create dir '{plan.created_dir_path}'")
                         dry_run_msgs.extend([f"DRY RUN: Would {a.action_type} '{a.original_path.name}' -> '{a.new_path}'" for a in plan.actions])
                         action_result['message'] = "\n".join(dry_run_msgs)
                         action_result['actions_taken'] = len(plan.actions) + (1 if plan.created_dir_path else 0)
                    else:
                        # Pass the whole undo_manager instance, not just one method
                        action_result = perform_file_actions(
                            plan=plan,
                            run_batch_id=batch_id,
                            args_ns=self.args,
                            cfg_helper=self.cfg,
                            undo_manager=self.undo_manager # <-- Pass the instance
                        )
                elif plan.status == 'skipped':
                     action_result['success'] = False # Not a success if skipped
                     action_result['message'] = plan.message or f"Skipped batch {stem}." # Ensure message exists
                else: # Failed planning
                     action_result['success'] = False
                     action_result['message'] = f"ERROR: Planning failed for '{stem}'. Reason: {plan.message}"


                # e. Update Summary & Print Result
                if action_result.get('success', False):
                     results_summary['success'] += 1
                     results_summary['actions'] += action_result.get('actions_taken', 0)
                elif plan and plan.status == 'skipped': # Check plan status if action didn't run
                     results_summary['skipped'] += 1
                else: # Includes planning failures and file op failures
                     results_summary['error'] += 1

                # Print result message (already includes Dry run/Action details)
                # Avoid printing None messages
                if action_result.get('message'):
                     print(action_result['message'])
                # If action didn't run but plan failed/skipped, print plan message
                elif plan and plan.status != 'success' and plan.message:
                      print(plan.message)
                elif plan is None: # Should not happen if scan worked
                      print(f"ERROR: Could not process batch for stem '{stem}' - Plan object is None.")
                      results_summary['error'] += 1


                # Add separator only if not interactive, not dry run, and an action was successful
                if not self.args.interactive and not self.args.dry_run and action_result.get('success'):
                     print("---")


            except UserAbortError as e:
                log.warning(str(e)); print(f"\n{e}"); break # Stop processing batches
            # Catch FileExistsError from file_system_ops if mode is 'fail'
            except FileExistsError as e:
                log.critical(str(e)); print(f"\nSTOPPING: {e}"); results_summary['error'] += 1; break
            except Exception as e:
                results_summary['error'] += 1
                log.exception(f"Critical unhandled error processing batch '{stem}': {e}")
                print(f"CRITICAL ERROR processing batch {stem}. See log.")


        # 4. Final Summary Printout
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
            # Use --live=true syntax consistent with BooleanOptionalAction help text
            print("DRY RUN COMPLETE. To apply changes, run again with --live")
        if not self.args.dry_run and self.cfg('enable_undo', False) and results_summary['actions'] > 0:
            # Use relative script name for user clarity
            script_name = Path(sys.argv[0]).name
            print(f"Undo information logged with Batch ID: {batch_id}")
            print(f"To undo this run: {script_name} undo {batch_id}")
        if not self.args.dry_run and self.args.stage_dir and results_summary['actions'] > 0:
             print(f"Renamed files moved to staging: {self.args.stage_dir}")
        if results_summary['error'] > 0:
            print(f"WARNING: {results_summary['error']} errors occurred. Check logs.")
        # Add a final success message if no errors
        if results_summary['error'] == 0 and (results_summary['success'] > 0 or results_summary['skipped'] == len(file_batches)):
             print("Operation finished successfully.")