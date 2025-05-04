# --- START OF FILE main_processor.py ---

# rename_app/main_processor.py (Async Version - FIX 3 Applied)

import logging
import uuid
import sys
import asyncio # Import asyncio
from pathlib import Path
from datetime import datetime, timezone
from typing import Tuple, Optional # Added for type hints

from .metadata_fetcher import MetadataFetcher # Remains the same import
from .renamer_engine import RenamerEngine
from .file_system_ops import perform_file_actions
from .utils import scan_media_files
from .exceptions import UserAbortError, RenamerError
from .models import MediaInfo, RenamePlan # Import necessary models
# Import API client getters
from .api_clients import get_tmdb_client, get_tvdb_client

try: from tqdm import tqdm; TQDM_AVAILABLE = True
except ImportError: TQDM_AVAILABLE = False

# --- Define AsyncTqdm class unconditionally outside the if/else ---
class AsyncTqdm:
    """Basic dummy async progress indicator."""
    def __init__(self, iterable, *args, **kwargs):
        self.iterable = iterable
        self.desc = kwargs.get("desc", "")
        self.total = kwargs.get("total", None)
        self.unit = kwargs.get("unit", "it")
        self.disable = kwargs.get("disable", False)
        self.count = 0
        if self.total is None:
             try: self.total = len(iterable)
             except TypeError: self.total = '?'

    def __aiter__(self):
        # Ensure iterable is awaitable if needed, but as_completed returns awaitables
        # For simple iterables, just get the standard iterator
        if hasattr(self.iterable, '__aiter__'):
             self.iterator = self.iterable.__aiter__()
        else:
             self.iterator = self.iterable.__iter__()
        self.count = 0 # Reset count when starting iteration
        return self

    async def __anext__(self):
        try:
            # Handle both sync and async iterators
            if hasattr(self.iterator, '__anext__'):
                val = await self.iterator.__anext__()
            else:
                val = next(self.iterator)

            self.count += 1
            if not self.disable and self.total != '?': # Only print if not disabled and total is known
                 # Simple progress print (can be enhanced with rich)
                 # Use carriage return \r to overwrite the line
                 print(f"\r{self.desc}: {self.count}/{self.total} [{self.unit}]... ", end='', flush=True) # Added flush=True
            return val
        except StopIteration:
            # This case is for sync iterators
            if not self.disable:
                if self.total != '?':
                     print(f"\r{self.desc}: Done.                        ", flush=True) # Overwrite with spaces
                else:
                     print(flush=True) # Just a newline if total wasn't known
            raise StopAsyncIteration
        except StopAsyncIteration:
             # This case is for async iterators
            if not self.disable:
                if self.total != '?':
                     print(f"\r{self.desc}: Done.                        ", flush=True) # Overwrite with spaces
                else:
                     print(flush=True) # Just a newline if total wasn't known
            raise StopAsyncIteration


    def set_postfix_str(self, s, refresh=True):
         # Postfix is hard to integrate reliably with the simple \r progress
         # We can just ignore it for the dummy implementation
         pass

    # Add context manager methods for compatibility if used with 'with'
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Clean up the progress line if it was active
        if not self.disable and self.total != '?':
            print(f"\r{self.desc}: Done.                        ", flush=True)
        pass

    # Compatibility for non-async usage (though not recommended)
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): pass
    def update(self, n=1): pass # Dummy update method
    def close(self): pass # Dummy close method


# --- Now assign tqdm_async based on TQDM_AVAILABLE ---
log = logging.getLogger(__name__) # Define logger after imports

if not TQDM_AVAILABLE:
    log.debug("tqdm not found, using basic async progress dummy.")
    tqdm_async = AsyncTqdm # Use the dummy if tqdm isn't installed
else:
    # OPTION 1 (Chosen): Use the dummy anyway for async loops
    # This avoids needing tqdm.asyncio and keeps behavior consistent
    log.debug("tqdm found, but using basic async progress dummy for async iteration.")
    tqdm_async = AsyncTqdm

    # OPTION 2 (Alternative): If you install tqdm[asyncio] you could try this
    # try:
    #     from tqdm.asyncio import tqdm as tqdm_asyncio_real
    #     log.debug("tqdm found, using tqdm.asyncio.")
    #     tqdm_async = tqdm_asyncio_real
    # except ImportError:
    #     log.warning("tqdm installed, but tqdm.asyncio extra not found. Falling back to basic async progress dummy.")
    #     tqdm_async = AsyncTqdm


# --- Helper function to fetch metadata for a single batch ---
async def _fetch_metadata_for_batch(batch_stem, batch_data, processor: "MainProcessor") -> Tuple[str, dict, Optional[MediaInfo]]:
    """Fetches metadata for one batch asynchronously."""
    try:
        if not batch_data.get('video'):
            log.error(f"Skipping metadata fetch for batch with missing video data: stem '{batch_stem}'")
            return batch_stem, batch_data, None # Return None for MediaInfo on error

        media_info = MediaInfo(original_path=batch_data['video'])
        # Run synchronous parts within the async function (they are fast)
        media_info.guess_info = processor.renamer.parse_filename(media_info.original_path)
        media_info.file_type = processor.renamer._determine_file_type(media_info.guess_info)

        if processor.metadata_fetcher and media_info.file_type != 'unknown':
            log.debug(f"Attempting async metadata fetch for '{batch_stem}' ({media_info.file_type})")
            # Await the async fetch methods from the now-async MetadataFetcher
            if media_info.file_type == 'series':
                ep_list_guess = media_info.guess_info.get('episode_list', [media_info.guess_info.get('episode')])
                valid_ep_list = [ep for ep in ep_list_guess if isinstance(ep, int) and ep > 0] if ep_list_guess else []
                if valid_ep_list:
                    guessed_title_raw = media_info.guess_info.get('title')
                    guessed_title = (guessed_title_raw[0] if isinstance(guessed_title_raw, list) else guessed_title_raw) or 'Unknown Show'
                    year_guess = media_info.guess_info.get('year')
                    media_info.metadata = await processor.metadata_fetcher.fetch_series_metadata(
                        guessed_title, media_info.guess_info.get('season', 0), tuple(valid_ep_list), year_guess=year_guess
                    )
            elif media_info.file_type == 'movie':
                guessed_title_raw = media_info.guess_info.get('title')
                guessed_title = (guessed_title_raw[0] if isinstance(guessed_title_raw, list) else guessed_title_raw) or 'Unknown Movie'
                year_guess = media_info.guess_info.get('year')
                media_info.metadata = await processor.metadata_fetcher.fetch_movie_metadata(guessed_title, year_guess)

        return batch_stem, batch_data, media_info # Return stem, original data, and info object

    except Exception as e:
        log.exception(f"Critical error fetching metadata for batch '{batch_stem}': {e}")
        # Return original data but None for MediaInfo to signal the error downstream
        return batch_stem, batch_data, None

class MainProcessor:
    def __init__(self, args, cfg_helper, undo_manager):
        self.args = args
        self.cfg = cfg_helper
        self.undo_manager = undo_manager
        # RenamerEngine uses cfg_helper, which is sync, so init is fine
        self.renamer = RenamerEngine(cfg_helper)
        self.metadata_fetcher = None
        # Check the final effective value of use_metadata
        # Use getattr to safely access args attribute, defaulting to False if not present
        use_metadata_effective = getattr(args, 'use_metadata', False)
        if use_metadata_effective:
             log.info("Metadata fetching enabled by configuration/args.")
             # MetadataFetcher init is sync and uses cfg_helper
             self.metadata_fetcher = MetadataFetcher(cfg_helper)
        else:
             log.info("Metadata fetching disabled by configuration or command line.")

    def _confirm_live_run(self, potential_actions_count):
        """Handles the pre-scan count and user confirmation for live run (remains sync)."""
        if potential_actions_count == 0:
            log.warning("Pre-scan found no files eligible for action based on current settings.")
            return False # Do not proceed

        print("-" * 30)
        print(f"Pre-scan found {potential_actions_count} potential file actions.")
        print("THIS IS A LIVE RUN.")
        if self.args.backup_dir: print(f"Originals will be backed up to: {self.args.backup_dir}")
        elif self.args.stage_dir: print(f"Files will be MOVED to staging: {self.args.stage_dir}")
        # Ensure args has 'trash' attribute (add to mock_args fixture in tests if needed)
        elif getattr(self.args, 'trash', False): print("Originals will be MOVED TO TRASH.")
        else: print("Files will be RENAMED/MOVED IN PLACE.")

        # Use cfg helper directly here for consistency
        if self.cfg('enable_undo', False): print("Undo logging is ENABLED.")
        else: print("Undo logging is DISABLED.")
        print("-" * 30)
        try:
            confirm = input("Proceed with actions? (y/N): ")
            if confirm.lower() != 'y':
                log.info("User aborted live run.")
                print("Operation cancelled by user.")
                return False # Do not proceed
            log.info("User confirmed live run.")
            return True # Proceed
        except EOFError:
             log.error("Cannot confirm live run in non-interactive mode without confirmation.")
             print("\nERROR: Cannot confirm live run. Use --force or run interactively if applicable.")
             return False

    async def run_processing(self):
        """Main asynchronous processing loop."""
        target_dir = self.args.directory.resolve()
        if not target_dir.is_dir():
            log.critical(f"Target directory not found or is not a directory: {target_dir}")
            return

        # --- Check API Client Availability (remains sync check at start) ---
        # Use final effective value for use_metadata check
        use_metadata_effective = getattr(self.args, 'use_metadata', False)
        if use_metadata_effective:
            log.debug("Checking API client availability as metadata is enabled.")
            tmdb_available = get_tmdb_client() is not None
            tvdb_available = get_tvdb_client() is not None

            if not tmdb_available and not tvdb_available:
                log.critical("Metadata processing enabled, but FAILED to initialize BOTH TMDB and TVDB API clients.")
                log.critical("Please check API key configuration (.env file or environment variables) and logs.")
                print("\nCRITICAL ERROR: Metadata fetching is enabled, but API clients could not be initialized.")
                return # Stop processing
            elif not tmdb_available:
                log.warning("Metadata processing enabled, but TMDB client is unavailable. Metadata quality may be reduced.")
            elif not tvdb_available:
                 log.warning("Metadata processing enabled, but TVDB client is unavailable. Metadata quality may be reduced.")
            else:
                log.debug("Required API clients for metadata fetching appear to be available.")

        # 1. Scan files (Get Generator)
        # scan_media_files now returns a generator directly
        batch_generator = scan_media_files(target_dir, self.cfg)

        # Convert generator to list to get count and allow multiple iterations easily.
        try:
            log.info("Collecting batches from scanner...")
            # Use standard tqdm here as this part is synchronous
            file_batches_list = list(tqdm(batch_generator, desc="Collecting Batches", unit="batch", disable=not TQDM_AVAILABLE))
            # Convert back to dictionary
            file_batches = {stem: data for stem, data in file_batches_list}
            batch_count = len(file_batches)
            log.info(f"Collected {batch_count} batches.")
            if batch_count == 0:
                 log.warning("No valid video files/batches found matching criteria.")
                 return
        except Exception as e_scan:
             log.exception(f"Error during batch collection from scanner: {e_scan}")
             return

        # 2. Pre-scan & Confirmation (if live run) - uses file_batches dict
        # Ensure 'live' status is correctly determined from args
        is_live_run = getattr(self.args, 'live', False)
        if is_live_run: # Use the effective live status
            log.info("Performing synchronous pre-scan for live run confirmation...")
            potential_actions_count = 0
            # Use standard tqdm for sync pre-scan
            prescan_iterator = tqdm(file_batches.items(), desc="Pre-scan", unit="batch", total=batch_count, disable=not TQDM_AVAILABLE)
            for stem, batch_data in prescan_iterator:
                 try:
                    # Simulate the planning part synchronously for counting
                    if not batch_data.get('video'): continue
                    media_info = MediaInfo(original_path=batch_data['video'])
                    media_info.guess_info = self.renamer.parse_filename(media_info.original_path) # Sync
                    media_info.file_type = self.renamer._determine_file_type(media_info.guess_info) # Sync
                    # Pre-scan does NOT fetch metadata
                    plan = self.renamer.plan_rename(batch_data['video'], batch_data.get('associated', []), media_info) # Sync plan
                    if plan.status == 'success':
                         action_count = len(plan.actions) + (1 if plan.created_dir_path else 0)
                         potential_actions_count += action_count
                 except Exception as e:
                    log.warning(f"Pre-scan planning error for batch '{stem}': {e}", exc_info=True)

            if not self._confirm_live_run(potential_actions_count):
                return

        # 3. Asynchronous Metadata Fetching Stage - uses file_batches dict
        metadata_results = {} # Store results: {stem: MediaInfo or None}
        fetch_tasks = []
        if use_metadata_effective and self.metadata_fetcher: # Check fetcher exists
             log.info(f"Creating {batch_count} tasks for concurrent metadata fetching...")
             for stem, batch_data in file_batches.items(): # Iterate over dict
                 task = asyncio.create_task(_fetch_metadata_for_batch(stem, batch_data, self), name=f"fetch_{stem}")
                 fetch_tasks.append(task)

             print("Fetching metadata...") # Simple indicator before loop starts
             completed_tasks = []
             # Use the selected async progress bar wrapper
             # Disable progress if interactive mode is on to avoid clutter
             disable_progress = self.args.interactive or not TQDM_AVAILABLE
             try:
                 async for task_result_future in tqdm_async(
                     asyncio.as_completed(fetch_tasks),
                     total=len(fetch_tasks),
                     desc="Fetching Metadata",
                     unit="batch",
                     disable=disable_progress
                 ):
                     # Append the actual result from the completed future
                     completed_tasks.append(await task_result_future)
             except Exception as e_progress:
                  log.error(f"Error during async progress iteration: {e_progress}")
                  # Fallback: gather remaining tasks if progress iteration failed
                  if not completed_tasks: # Only gather if we didn't get any results yet
                       log.warning("Falling back to asyncio.gather due to progress error.")
                       # Gather ensures all tasks are awaited, returns results or exceptions
                       completed_tasks = await asyncio.gather(*fetch_tasks, return_exceptions=True)


             # Process results (no change needed here)
             log.info("Metadata fetching complete. Processing results...")
             for result in completed_tasks:
                 if isinstance(result, Exception):
                     # Log the exception raised by a task
                     log.error(f"Error returned from metadata fetch task: {result}", exc_info=result)
                 elif isinstance(result, tuple) and len(result) == 3:
                     stem, _, media_info_or_none = result
                     metadata_results[stem] = media_info_or_none # Store MediaInfo or None
                     if media_info_or_none is None:
                         log.warning(f"Metadata fetching failed or returned no data for batch '{stem}'.")
                 else:
                     log.error(f"Unexpected result type from metadata fetch task: {type(result)}")
        else:
             log.info("Metadata fetching disabled or fetcher not available, proceeding without online metadata.")
             # Populate results with basic MediaInfo (no metadata)
             for stem, batch_data in file_batches.items():
                 if not batch_data.get('video'):
                      metadata_results[stem] = None
                      continue
                 media_info = MediaInfo(original_path=batch_data['video'])
                 try: # Add try-except for sync parsing during fallback
                     media_info.guess_info = self.renamer.parse_filename(media_info.original_path)
                     media_info.file_type = self.renamer._determine_file_type(media_info.guess_info)
                     metadata_results[stem] = media_info
                 except Exception as e_parse:
                      log.error(f"Error during basic parsing for batch '{stem}' (metadata disabled): {e_parse}")
                      metadata_results[stem] = None # Mark as failed


        # 4. Planning and Execution Stage (Synchronous Loop) - uses file_batches dict
        run_batch_id = f"run-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        log.info(f"Starting planning and execution run ID: {run_batch_id}")
        results_summary = {'success': 0, 'skipped': 0, 'error': 0, 'actions': 0}

        # Use standard tqdm for this synchronous loop over the dict
        process_iterator = tqdm(file_batches.items(), desc="Planning/Executing", unit="batch", total=batch_count, disable=not TQDM_AVAILABLE or self.args.interactive)
        print("-" * 30) # Separator before detailed output

        for stem, batch_data in process_iterator:
            # --- Get the corresponding MediaInfo result from the async stage ---
            media_info = metadata_results.get(stem)

            if not media_info: # Check if metadata fetching failed or basic parsing failed
                log.error(f"Skipping batch '{stem}' due to missing MediaInfo (fetch/parse error).")
                results_summary['error'] += 1
                if TQDM_AVAILABLE and not self.args.interactive: process_iterator.set_postfix_str(f"Error: {stem}", refresh=True)
                continue

            # Update postfix string if tqdm available and not interactive
            if TQDM_AVAILABLE and not self.args.interactive:
                 if batch_data.get('video'):
                      process_iterator.set_postfix_str(batch_data['video'].name, refresh=True)
                 else:
                      process_iterator.set_postfix_str(f"Invalid Batch: {stem}", refresh=True)

            user_choice = 'y'
            plan = None

            try:
                # Ensure we have a valid video path before proceeding
                video_file_path = batch_data.get('video')
                if not video_file_path:
                    log.error(f"Skipping batch '{stem}' due to missing video file path in batch data.")
                    results_summary['error'] += 1
                    if TQDM_AVAILABLE and not self.args.interactive: process_iterator.set_postfix_str(f"Missing video: {stem}", refresh=True)
                    continue

                # a. Plan Rename Actions (using fetched media_info) - Sync
                plan = self.renamer.plan_rename(
                    video_file_path,
                    batch_data.get('associated', []), # Use .get for safety
                    media_info
                )

                # b. Interactive Confirmation (per batch) - Sync
                if self.args.interactive and is_live_run and plan.status == 'success': # Check live status here too
                    print(f"\n--- Batch: {video_file_path.name} ---")
                    if plan.created_dir_path: print(f"  Plan: create_dir '{plan.created_dir_path}'")
                    for action in plan.actions: print(f"  Plan: {action.action_type} '{action.original_path.name}' -> '{action.new_path}'")
                    try:
                         user_choice = input("Apply this batch? [y/N/s/q]: ").lower()
                         if user_choice == 's': plan.status = 'skipped'; plan.message = "Skipped by user (interactive)."
                         elif user_choice == 'q': raise UserAbortError("User quit during interactive mode.")
                         elif user_choice != 'y': plan.status = 'skipped'; plan.message = "Skipped by user (interactive)."
                    except EOFError: raise UserAbortError("User quit (EOF) during interactive mode.")

                # c. Execute Plan (if not skipped/failed) - Sync
                action_result = {'success': False, 'message': plan.message, 'actions_taken': 0}
                if plan.status == 'success' and user_choice == 'y':
                    # Pass the effective live run status to perform_file_actions
                    effective_args = self.args # Make a copy or modify directly if safe
                    effective_args.live = is_live_run # Ensure live status is correct
                    effective_args.dry_run = not is_live_run # Ensure dry_run is inverse

                    action_result = perform_file_actions(
                        plan=plan,
                        run_batch_id=run_batch_id,
                        args_ns=effective_args, # Pass potentially modified args
                        cfg_helper=self.cfg,
                        undo_manager=self.undo_manager
                    )
                elif plan.status == 'skipped':
                     action_result['success'] = False # Skipped is not success
                     action_result['message'] = plan.message or f"Skipped batch {stem}."
                else: # Failed planning
                     action_result['success'] = False
                     action_result['message'] = f"ERROR: Planning failed for '{stem}'. Reason: {plan.message}"


                # d. Update Summary & Print Result
                if action_result.get('success', False):
                     results_summary['success'] += 1
                     # Use actions_taken from result dict (works for dry and live)
                     results_summary['actions'] += action_result.get('actions_taken', 0)
                elif plan and plan.status == 'skipped':
                     results_summary['skipped'] += 1
                else: # Includes planning errors and file op errors
                     results_summary['error'] += 1

                # Print result message from plan/action execution
                if action_result.get('message'):
                     print(action_result['message'])
                elif plan is None: # Should not happen if checks above work, but safety
                      print(f"ERROR: Could not process batch for stem '{stem}' - Plan object is None.")
                      results_summary['error'] += 1 # Count as error


                if not self.args.interactive and is_live_run and action_result.get('success'):
                     print("---") # Separator between successful live actions

            # Exception Handling for the batch loop
            except UserAbortError as e:
                log.warning(str(e)); print(f"\n{e}"); break # Stop processing batches
            except FileExistsError as e: # Raised by file_system_ops on conflict='fail'
                log.critical(str(e)); print(f"\nSTOPPING: {e}"); results_summary['error'] += 1; break
            except Exception as e:
                results_summary['error'] += 1
                log.exception(f"Critical unhandled error processing batch '{stem}': {e}")
                print(f"CRITICAL ERROR processing batch {stem}. See log.")
                # Optionally continue to next batch or break here
                # break


        # 5. Final Summary Printout (sync)
        print("-" * 30)
        log.info("Processing complete.")
        print("Processing Summary:")
        print(f"  Batches Scanned: {batch_count}")
        print(f"  Batches Successfully Processed: {results_summary['success']}")
        print(f"  Batches Skipped: {results_summary['skipped']}")
        print(f"  Batches with Errors: {results_summary['error']}")
        # Base total actions taken on the summary dict, which is updated for both live and dry runs
        total_actions_reported = results_summary['actions']
        if is_live_run:
             print(f"  Total File Actions Taken: {total_actions_reported}")
        else: # Dry Run
             if total_actions_reported > 0:
                 print(f"  Total File Actions Planned: {total_actions_reported}")

        print("-" * 30)
        if not is_live_run and total_actions_reported > 0:
            print("DRY RUN COMPLETE. To apply changes, run again with --live")
        elif not is_live_run:
            print("DRY RUN COMPLETE. No actions were planned.")

        if is_live_run and self.cfg('enable_undo', False) and total_actions_reported > 0:
            script_name = Path(sys.argv[0]).name
            print(f"Undo information logged with Run ID: {run_batch_id}")
            print(f"To undo this run: {script_name} undo {run_batch_id}")
        if is_live_run and self.args.stage_dir and total_actions_reported > 0:
             print(f"Renamed files moved to staging: {self.args.stage_dir}")
        if results_summary['error'] > 0:
            print(f"WARNING: {results_summary['error']} errors occurred. Check logs.")

        # Final status message
        if results_summary['error'] == 0:
            if results_summary['success'] > 0 or results_summary['skipped'] == batch_count:
                print("Operation finished successfully.")
            elif results_summary['skipped'] < batch_count and results_summary['success'] == 0 :
                 print("Operation finished, but some batches were skipped or had no actions planned.")
            else: # Should cover all cases now (e.g., skipped == count)
                 print("Operation finished.")
        else:
             print("Operation finished with errors.")

# --- END OF FILE main_processor.py ---