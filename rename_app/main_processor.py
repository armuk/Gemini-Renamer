# --- START OF FILE main_processor.py ---

import logging
import uuid
import sys
import asyncio
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Tuple, Optional, Dict, Any, cast

# Rich imports and fallbacks
import builtins
try:
    from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TaskID
    from rich.console import Console
    from rich.text import Text
    # --- NEW IMPORTS for Interactive ---
    from rich.prompt import Prompt, Confirm, InvalidResponse
    from rich.panel import Panel
    from rich.table import Table
    # --- END NEW IMPORTS ---
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    TaskID = int
    class Progress:
        def __init__(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def add_task(self, description, total=None, start=True, **fields): return 0
        def update(self, task_id, advance=1, description=None, **fields): pass
        def stop(self): pass
    class Console:
        def __init__(self, *args, **kwargs): pass
        def print(self, *args, **kwargs): builtins.print(*args, **kwargs)
        def input(self, *args, **kwargs) -> str: return builtins.input(*args, **kwargs)
    # --- Minimal Fallbacks ---
    class Prompt:
        @staticmethod
        def ask(*args, **kwargs): return builtins.input(args[0])
    class Confirm:
         @staticmethod
         def ask(*args, **kwargs): return builtins.input(args[0]).lower() == 'y'
    class Panel:
         def __init__(self, content, *args, **kwargs): self.content = content
         def __rich_console__(self, console, options):
            yield str(self.content) # Basic string representation
    class Table:
        def __init__(self, *args, **kwargs): pass
        def add_column(self, *args, **kwargs): pass
        def add_row(self, *args, **kwargs): pass
    class Text:
        def __init__(self, text="", style=""): self.text = text; self.style = style
        def __str__(self): return self.text
        @property
        def plain(self): return self.text # Add plain property for fallback checks
    class InvalidResponse(Exception): pass
    # --- End Fallbacks ---


from .metadata_fetcher import MetadataFetcher
from .renamer_engine import RenamerEngine
from .file_system_ops import perform_file_actions, _handle_conflict, FileOperationError
from .utils import scan_media_files
from .exceptions import UserAbortError, RenamerError, MetadataError
# --- Add RenamePlan ---
from .models import MediaInfo, RenamePlan, MediaMetadata
from .api_clients import get_tmdb_client, get_tvdb_client

log = logging.getLogger(__name__)


# --- REMOVE TQDM Imports ---
# ... (AsyncTqdm class can remain or be removed if progress bars are sufficient) ...


log = logging.getLogger(__name__)


async def _fetch_metadata_for_batch(
    batch_stem: str,
    batch_data: Dict[str, Any],
    processor: "MainProcessor",
    progress: Optional[Progress] = None,
    task_id: Optional[TaskID] = None
) -> Tuple[str, MediaInfo]: # Returns MediaInfo
    # (Function remains the same, ensures it returns MediaInfo)
    video_path_for_media_info = batch_data.get('video')
    if not video_path_for_media_info:
        log.error(f"CRITICAL in _fetch_metadata_for_batch: video_path is None for stem '{batch_stem}'. Using dummy.")
        # Create a dummy MediaInfo to return, marking it as an error state
        error_media_info = MediaInfo(original_path=Path(f"error_dummy_{batch_stem}.file"))
        error_media_info.file_type = 'unknown'
        error_media_info.metadata_error_message = "Missing video path for batch."
        return batch_stem, error_media_info

    media_info = MediaInfo(original_path=video_path_for_media_info)
    media_info.metadata = None # Ensure it's initialized

    try:
        media_info.guess_info = processor.renamer.parse_filename(media_info.original_path)
        original_file_type_from_guessit = processor.renamer._determine_file_type(media_info.guess_info)
        media_info.file_type = original_file_type_from_guessit

        use_metadata_cfg = processor.cfg('use_metadata', False)

        if use_metadata_cfg and processor.metadata_fetcher and media_info.file_type != 'unknown':
            log.debug(f"Attempting async metadata fetch for '{batch_stem}' ({media_info.file_type})")
            year_guess = media_info.guess_info.get('year')

            # Update progress description
            if progress and task_id is not None and progress.tasks:
                if task_id < len(progress.tasks): # Check task_id is still valid before accessing description
                    current_description_obj = progress.tasks[task_id].description
                    base_description = str(current_description_obj).split(" (")[0]
                    new_description_str = f"{base_description} ({batch_stem[:20]}...)"
                    progress.update(task_id, description=new_description_str)

            fetched_api_metadata: Optional[MediaMetadata] = None
            try: # Specific try-block for the fetcher calls
                if media_info.file_type == 'series':
                    ep_list_guess = media_info.guess_info.get('episode_list', [media_info.guess_info.get('episode')])
                    valid_ep_list = [ep for ep in ep_list_guess if isinstance(ep, int) and ep > 0] if ep_list_guess else []
                    guessed_title_raw = media_info.guess_info.get('title')
                    if isinstance(guessed_title_raw, list) and guessed_title_raw: guessed_title = str(guessed_title_raw[0]) if guessed_title_raw[0] else media_info.original_path.stem
                    elif isinstance(guessed_title_raw, str) and guessed_title_raw: guessed_title = guessed_title_raw
                    else: guessed_title = media_info.original_path.stem; log.debug(f"Guessed title empty/invalid for series '{batch_stem}', using stem: '{guessed_title}'")
                    if valid_ep_list:
                        fetched_api_metadata = await processor.metadata_fetcher.fetch_series_metadata(
                            show_title_guess=guessed_title, season_num=media_info.guess_info.get('season', 0),
                            episode_num_list=tuple(valid_ep_list), year_guess=year_guess)
                    else: log.warning(f"No valid episode numbers for series '{batch_stem}'. Skipping series metadata fetch.")

                elif media_info.file_type == 'movie':
                    guessed_title_raw = media_info.guess_info.get('title')
                    if isinstance(guessed_title_raw, list) and guessed_title_raw: guessed_title = str(guessed_title_raw[0]) if guessed_title_raw[0] else media_info.original_path.stem
                    elif isinstance(guessed_title_raw, str) and guessed_title_raw: guessed_title = guessed_title_raw
                    else: guessed_title = media_info.original_path.stem; log.debug(f"Guessed title empty/invalid for movie '{batch_stem}', using stem: '{guessed_title}'")
                    fetched_api_metadata = await processor.metadata_fetcher.fetch_movie_metadata(movie_title_guess=guessed_title, year_guess=year_guess)

                media_info.metadata = fetched_api_metadata # Assign whatever was fetched

            except MetadataError as me: # Catch specific MetadataError from fetcher
                log.error(f"Caught MetadataError for '{batch_stem}': {me}")
                media_info.metadata_error_message = str(me) # Store the user-friendly message
                media_info.metadata = None # Ensure metadata is None
            except Exception as fetch_e: # Catch any other unexpected error during the await
                log.exception(f"Unexpected error during actual metadata API call for '{batch_stem}': {fetch_e}")
                media_info.metadata_error_message = f"Unexpected fetch error: {fetch_e}"
                media_info.metadata = None

            if media_info.metadata is None or not media_info.metadata.source_api:
                 # If metadata_error_message isn't already set by a caught MetadataError, set a generic one.
                if not media_info.metadata_error_message:
                    media_info.metadata_error_message = "Metadata fetch returned no usable API data."
                log.warning(
                    f"Post-fetch: Metadata for '{batch_stem}' (type: {original_file_type_from_guessit}) "
                    f"has an issue. Error: '{media_info.metadata_error_message}'. Metadata object: {media_info.metadata}"
                )

        return batch_stem, media_info # Return the MediaInfo object

    except Exception as e:
        log.exception(f"Critical error in _fetch_metadata_for_batch for batch '{batch_stem}' (outer try): {e}")
        # Ensure media_info is initialized even if guessit fails
        if not hasattr(media_info, 'guess_info') or not media_info.guess_info:
             media_info.guess_info = {}
        media_info.file_type = 'unknown'
        media_info.metadata = None
        media_info.metadata_error_message = f"Processing error in _fetch_metadata_for_batch: {e}"
        return batch_stem, media_info
    finally:
        if progress and task_id is not None:
            if progress.tasks and task_id < len(progress.tasks):
                 progress.update(task_id, advance=1)


class MainProcessor:
    def __init__(self, args, cfg_helper, undo_manager):
        self.args = args
        self.cfg = cfg_helper
        self.undo_manager = undo_manager
        self.renamer = RenamerEngine(cfg_helper) # Instantiates RenamerEngine
        self.metadata_fetcher = None             # Initialized based on args/cfg below

        use_metadata_effective = getattr(args, 'use_metadata', False)
        if use_metadata_effective:
             log.info("Metadata fetching enabled by configuration/args for MainProcessor.")
             # Ensure MetadataFetcher is instantiated only if needed
             if get_tmdb_client() or get_tvdb_client(): # Check if clients are actually available
                 self.metadata_fetcher = MetadataFetcher(cfg_helper)
             else:
                 log.warning("Metadata enabled but no API clients initialized. Disabling fetcher.")
                 args.use_metadata = False # Override arg if clients failed
        else:
             log.info("Metadata fetching disabled by configuration or command line for MainProcessor.")

        self.console = Console() # Use rich console

    # --- NEW HELPER: Display plan details ---
    def _display_plan_for_confirmation(self, plan: RenamePlan, media_info: MediaInfo):
        """Prints the proposed plan using rich formatting."""
        if not plan or plan.status != 'success':
            self.console.print(f"[yellow]No valid rename plan generated for {media_info.original_path.name}.[/yellow]")
            if plan and plan.message:
                self.console.print(f"[yellow]Reason: {plan.message}[/yellow]")
            return

        panel_content = []
        panel_content.append(f"[bold]File:[/bold] {media_info.original_path.name}")
        if media_info.metadata and media_info.metadata.source_api:
            source_info = f"[i]via {media_info.metadata.source_api.upper()}"
            score = getattr(media_info.metadata, 'match_confidence', None)
            if isinstance(score, float):
                score_color = "green" if score >= 85 else "yellow" if score >= self.cfg('tmdb_match_fuzzy_cutoff', 70) else "red"
                source_info += f" (Score: [{score_color}]{score:.1f}%[/])"
            source_info += "[/i]"
            panel_content.append(f"[bold]Type:[/bold] {media_info.file_type.capitalize()} {source_info}")

            if media_info.metadata.is_series:
                title = media_info.metadata.show_title or "[missing]"
                year = f"({media_info.metadata.show_year})" if media_info.metadata.show_year else ""
                # --- CORRECTED ACCESS ---
                ep_list = media_info.metadata.episode_list # Get list from metadata object
                ep_num = ep_list[0] if ep_list else 0      # Safely get first episode number
                ep_title = media_info.metadata.episode_titles.get(ep_num, "[missing]") # Use ep_num
                season_num = media_info.metadata.season if media_info.metadata.season is not None else 0 # Use metadata season
                panel_content.append(f"[bold]Match:[/bold] {title} {year} - S{season_num:02d}E{ep_num:02d} - {ep_title}")
                # --- END CORRECTION ---
            elif media_info.metadata.is_movie:
                # ... (movie display logic - likely correct) ...
                title = media_info.metadata.movie_title or "[missing]"
                year = f"({media_info.metadata.movie_year})" if media_info.metadata.movie_year else ""
                panel_content.append(f"[bold]Match:[/bold] {title} {year}")
        else:
             panel_content.append(f"[bold]Type:[/bold] {media_info.file_type.capitalize()} ([i]via Guessit[/i])")
             panel_content.append(f"[bold]Guess:[/bold] {media_info.guess_info.get('title', media_info.original_path.stem)}")

        panel_content.append("\n[bold cyan]Proposed Actions:[/bold cyan]")

        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column("Original")
        table.add_column("Arrow", justify="center")
        table.add_column("New")

        if plan.created_dir_path:
             # Ensure path separators are displayed correctly (might not be needed depending on OS)
             dir_path_str = str(plan.created_dir_path).replace("\\", "/")
             table.add_row("[dim]-[/dim]", "[dim]->[/dim]", f"[green]{dir_path_str}[/green] [i](Create Dir)[/i]")
        for action_item in plan.actions:
             action_style = "blue" if action_item.action_type == 'move' else "default"
             # Ensure path separators are displayed correctly
             new_path_str = str(action_item.new_path).replace("\\", "/")
             table.add_row(f"{action_item.original_path.name}", f"[{action_style}]->[/]", f"[{action_style}]{new_path_str}[/]")

        panel_content.append(table)
        self.console.print(Panel("\n".join(str(c) for c in panel_content), title="[yellow]Confirm Batch Action", border_style="yellow"))

    # --- NEW HELPER: Re-fetch metadata with manual ID ---
    async def _refetch_with_manual_id(self, media_info: MediaInfo, api_source: str, manual_id: int) -> Optional[MediaMetadata]:
        """
        Attempts to fetch metadata using a manually provided ID.
        Returns updated MediaMetadata or None on failure.
        """
        if not self.metadata_fetcher:
            self.console.print("[red]Error: Metadata fetcher not initialized.[/red]")
            return None

        log.info(f"Attempting re-fetch for '{media_info.original_path.name}' using {api_source.upper()} ID: {manual_id}")
        new_metadata: Optional[MediaMetadata] = None
        current_lang = self.cfg('tmdb_language', 'en')

        try:
            if api_source == 'tmdb':
                if media_info.file_type == 'movie':
                    # Directly call internal method - requires understanding its return format
                    # We might need to adjust _do_fetch_tmdb_movie to accept an ID
                    # Or add a new method like fetch_movie_by_id
                    # For now, let's *simulate* by re-searching title (less ideal)
                    # A better approach would be fetcher.fetch_movie_by_id(manual_id) if implemented
                    self.console.print(f"[yellow]Re-fetching TMDB movie details for ID {manual_id}...[/yellow]")
                    # Placeholder: Ideally call a direct ID fetch method
                    # movie_data, ids, score = await self.metadata_fetcher._do_fetch_tmdb_movie_by_id(manual_id, current_lang) # Hypothetical
                    # Let's try calling the search and hope it finds the exact ID first
                    title_guess = media_info.guess_info.get('title', media_info.original_path.stem)
                    year_guess = media_info.guess_info.get('year')
                    new_metadata = await self.metadata_fetcher.fetch_movie_metadata(title_guess, year_guess) # Re-fetch normally for now
                    if not new_metadata or new_metadata.ids.get('tmdb_id') != manual_id:
                        log.warning(f"Re-fetch for TMDB ID {manual_id} didn't return the expected movie.")
                        # Here you would ideally handle the direct ID fetch result
                        new_metadata = None # Mark as failed if direct ID call was intended
                elif media_info.file_type == 'series':
                    self.console.print(f"[yellow]Re-fetching TMDB series details for ID {manual_id}...[/yellow]")
                    # Placeholder: Ideally call a direct ID fetch method
                    title_guess = media_info.guess_info.get('title', media_info.original_path.stem)
                    season_guess = media_info.guess_info.get('season', 0)
                    ep_list_guess = media_info.guess_info.get('episode_list', [media_info.guess_info.get('episode')])
                    valid_ep_list = tuple(ep for ep in ep_list_guess if isinstance(ep, int) and ep > 0) if ep_list_guess else tuple()
                    year_guess = media_info.guess_info.get('year')
                    new_metadata = await self.metadata_fetcher.fetch_series_metadata(title_guess, season_guess, valid_ep_list, year_guess)
                    if not new_metadata or new_metadata.ids.get('tmdb_id') != manual_id:
                        log.warning(f"Re-fetch for TMDB ID {manual_id} didn't return the expected series.")
                        new_metadata = None

            elif api_source == 'tvdb':
                 if media_info.file_type == 'series':
                    self.console.print(f"[yellow]Re-fetching TVDB series details for ID {manual_id}...[/yellow]")
                    # TVDB fetcher might already support ID directly, or need internal call
                    # show_data, ep_map, ids, score = await self.metadata_fetcher._do_fetch_tvdb_series(..., tvdb_id_arg=manual_id) # Example
                    # Let's re-fetch normally for now
                    title_guess = media_info.guess_info.get('title', media_info.original_path.stem)
                    season_guess = media_info.guess_info.get('season', 0)
                    ep_list_guess = media_info.guess_info.get('episode_list', [media_info.guess_info.get('episode')])
                    valid_ep_list = tuple(ep for ep in ep_list_guess if isinstance(ep, int) and ep > 0) if ep_list_guess else tuple()
                    year_guess = media_info.guess_info.get('year')
                    new_metadata = await self.metadata_fetcher.fetch_series_metadata(title_guess, season_guess, valid_ep_list, year_guess) # Fetches both, check if TVDB ID matches
                    if not new_metadata or new_metadata.ids.get('tvdb_id') != manual_id:
                         log.warning(f"Re-fetch using TVDB ID {manual_id} didn't result in metadata with that ID.")
                         new_metadata = None # Mark as failed if TVDB was specifically requested and didn't match
                 else:
                     self.console.print("[red]TVDB ID only applicable for Series.[/red]")
                     return None
            else:
                self.console.print(f"[red]Unsupported API source: {api_source}[/red]")
                return None

            if new_metadata and new_metadata.source_api:
                 self.console.print(f"[green]Successfully re-fetched metadata from {new_metadata.source_api.upper()}.[/green]")
                 return new_metadata
            else:
                 self.console.print(f"[red]Failed to fetch valid metadata using {api_source.upper()} ID {manual_id}.[/red]")
                 return None

        except Exception as e:
            log.exception(f"Error during manual ID re-fetch ({api_source} ID {manual_id}): {e}")
            self.console.print(f"[red]Error during re-fetch: {e}[/red]")
            return None


    def _confirm_live_run(self, potential_actions_count):
        # (Unchanged - uses Rich Confirm)
        if potential_actions_count == 0:
            log.warning("Pre-scan found no files eligible for action based on current settings.")
            self.console.print("[yellow]Pre-scan found no files eligible for action based on current settings.[/yellow]")
            return False
        self.console.print("-" * 30)
        self.console.print(f"Pre-scan found {potential_actions_count} potential file actions.")
        self.console.print("[bold red]THIS IS A LIVE RUN.[/bold red]")
        if self.args.backup_dir: self.console.print(f"Originals will be backed up to: {self.args.backup_dir}")
        elif self.args.stage_dir: self.console.print(f"Files will be MOVED to staging: {self.args.stage_dir}")
        elif getattr(self.args, 'trash', False): self.console.print("Originals will be MOVED TO TRASH.")
        else: self.console.print("Files will be RENAMED/MOVED IN PLACE.")
        if self.cfg('enable_undo', False): self.console.print("Undo logging is [green]ENABLED[/green].")
        else: self.console.print("Undo logging is [yellow]DISABLED[/yellow].")
        self.console.print("-" * 30)
        try:
            # Use rich Confirm for y/n
            if Confirm.ask("Proceed with actions?", default=False):
                log.info("User confirmed live run."); return True
            else:
                log.info("User aborted live run."); self.console.print("Operation cancelled by user."); return False
        except EOFError:
             log.error("Cannot confirm live run in non-interactive mode without confirmation (EOF).")
             self.console.print("\n[bold red]ERROR: Cannot confirm live run. Run interactively or use a flag to force if available (not implemented).[/bold red]")
             return False
        except Exception as e:
            log.error(f"Error during live run confirmation: {e}", exc_info=True)
            self.console.print(f"\n[bold red]ERROR: Could not get confirmation: {e}[/bold red]")
            return False

    def _handle_move_to_unknown(self, batch_stem: str, batch_data: Dict[str, Any], run_batch_id: str) -> Dict[str, Any]:
        # (Unchanged)
        results = {'success': True, 'message': "", 'actions_taken': 0}; action_messages = []
        unknown_dir_str = self.args.unknown_files_dir
        if not unknown_dir_str:
            msg = f"ERROR: Unknown files directory not configured for batch '{batch_stem}'. Skipping move."
            log.error(msg); results['success'] = False; results['message'] = msg; return results
        base_target_dir = self.args.directory.resolve(); unknown_target_dir = Path(unknown_dir_str)
        if not unknown_target_dir.is_absolute(): unknown_target_dir = base_target_dir / unknown_dir_str
        unknown_target_dir = unknown_target_dir.resolve()
        log.info(f"Handling unknown batch '{batch_stem}': Moving files to '{unknown_target_dir}'")
        is_live_run = getattr(self.args, 'live', False)
        if not is_live_run:
            if not unknown_target_dir.exists(): action_messages.append(f"DRY RUN: Would create directory '{unknown_target_dir}'")
            all_files_in_batch = [batch_data.get('video')] + batch_data.get('associated', []); files_to_log_dry_run = [f for f in all_files_in_batch if f and isinstance(f, Path) and f.exists()]
            if not files_to_log_dry_run and unknown_target_dir.exists(): action_messages.append(f"DRY RUN: No files to move for '{batch_stem}' to existing '{unknown_target_dir}'.")
            elif not files_to_log_dry_run and not unknown_target_dir.exists(): action_messages.append(f"DRY RUN: No files to move for '{batch_stem}', but would create '{unknown_target_dir}'.")
            for file_path in files_to_log_dry_run:
                 sim_dest_path = unknown_target_dir / file_path.name; conflict_msg = ""
                 if sim_dest_path.exists():
                     temp_conflict_mode = self.cfg('on_conflict', 'skip')
                     if temp_conflict_mode == 'skip': conflict_msg = f" (WARNING: Target '{sim_dest_path.name}' exists - would be SKIPPED)"
                     elif temp_conflict_mode == 'fail': conflict_msg = f" (ERROR: Target '{sim_dest_path.name}' exists - would FAIL)"
                     elif temp_conflict_mode == 'overwrite': conflict_msg = f" (WARNING: Target '{sim_dest_path.name}' exists - would be OVERWRITTEN)"
                     elif temp_conflict_mode == 'suffix': conflict_msg = f" (WARNING: Target '{sim_dest_path.name}' exists - would be SUFFIXED)"
                 action_messages.append(f"DRY RUN: Would move '{file_path.name}' to '{unknown_target_dir}'{conflict_msg}")
            results['message'] = "\n".join(action_messages) if action_messages else f"DRY RUN: No actions planned for unknown batch '{batch_stem}'."; return results
        try:
            if not unknown_target_dir.exists():
                log.info(f"Creating unknown files directory: {unknown_target_dir}"); unknown_target_dir.mkdir(parents=True, exist_ok=True)
                if self.undo_manager.is_enabled: self.undo_manager.log_action(batch_id=run_batch_id, original_path=unknown_target_dir, new_path=unknown_target_dir, item_type='dir', status='created_dir')
                action_messages.append(f"CREATED DIR (unknowns): '{unknown_target_dir}'")
        except OSError as e: msg = f"ERROR: Could not create directory '{unknown_target_dir}': {e}"; log.error(msg, exc_info=True); results['success'] = False; results['message'] = msg; return results
        conflict_mode = self.cfg('on_conflict', 'skip'); files_to_move = [batch_data.get('video')] + batch_data.get('associated', [])
        for original_file_path in files_to_move:
            if not original_file_path or not isinstance(original_file_path, Path): log.warning(f"Skipping move of invalid file entry: {original_file_path}"); continue
            if not original_file_path.exists(): log.warning(f"Skipping move of non-existent file: {original_file_path}"); continue
            target_file_path_in_unknown_dir = unknown_target_dir / original_file_path.name
            try:
                final_target_path = _handle_conflict(original_file_path, target_file_path_in_unknown_dir, conflict_mode)
                if self.undo_manager.is_enabled: self.undo_manager.log_action(batch_id=run_batch_id, original_path=original_file_path, new_path=final_target_path, item_type='file', status='moved')
                log.debug(f"Moving '{original_file_path.name}' to '{final_target_path}'"); shutil.move(str(original_file_path), str(final_target_path))
                action_messages.append(f"MOVED (unknown): '{original_file_path.name}' to '{final_target_path}'"); results['actions_taken'] += 1
            except FileExistsError as e_fe: msg = f"ERROR (move unknown): {e_fe} - File '{original_file_path.name}' not moved."; log.error(msg); action_messages.append(msg); results['success'] = False
            except FileOperationError as e_foe: msg = f"SKIPPED (move unknown): {e_foe} - File '{original_file_path.name}' not moved."; log.warning(msg); action_messages.append(msg)
            except OSError as e_os: msg = f"ERROR (move unknown): Failed to move '{original_file_path.name}': {e_os}"; log.error(msg, exc_info=True); action_messages.append(msg); results['success'] = False
            except Exception as e_generic: msg = f"ERROR (move unknown): Unexpected error for '{original_file_path.name}': {e_generic}"; log.exception(msg); action_messages.append(msg); results['success'] = False
        if results['actions_taken'] == 0 and any("ERROR" in m for m in action_messages): results['success'] = False
        elif results['actions_taken'] < len([f for f in files_to_move if f and f.exists()]) and any("ERROR" in m for m in action_messages): results['success'] = False
        results['message'] = "\n".join(action_messages)
        return results


    async def run_processing(self):
        # ... (initial setup, scanning, pre-scan, initial parse, metadata fetch remain the same) ...
        target_dir = self.args.directory.resolve()
        if not target_dir.is_dir():
            log.critical(f"Target directory not found or is not a directory: {target_dir}")
            self.console.print(f"[bold red]Error: Target directory not found: {target_dir}[/]")
            return

        use_metadata_effective = getattr(self.args, 'use_metadata', False)
        if use_metadata_effective:
            log.debug("Checking API client availability as metadata is enabled.")
            tmdb_available = get_tmdb_client() is not None
            tvdb_available = get_tvdb_client() is not None
            if not tmdb_available and not tvdb_available:
                log.critical("Metadata processing enabled, but FAILED to initialize BOTH TMDB and TVDB API clients.")
                self.console.print("\n[bold red]CRITICAL ERROR: Metadata fetching enabled, but API clients could not be initialized.[/bold red]")
                return
            elif not tmdb_available: log.warning("Metadata enabled, but TMDB client is unavailable. Metadata quality may be reduced.")
            elif not tvdb_available: log.warning("Metadata enabled, but TVDB client is unavailable. Metadata quality may be reduced.")
            else: log.debug("Required API clients for metadata fetching appear to be available.")

        log.info("Collecting batches from scanner...")
        batch_generator = scan_media_files(target_dir, self.cfg)
        file_batches_list = []
        if batch_generator:
            file_batches_list = list(batch_generator)

        disable_rich_progress = self.args.interactive or not RICH_AVAILABLE

        file_batches = {stem: data for stem, data in file_batches_list}
        batch_count = len(file_batches)
        log.info(f"Collected {batch_count} batches.")
        if batch_count == 0:
             log.warning("No valid video files/batches found matching criteria.")
             self.console.print("[yellow]No valid video files/batches found matching criteria.[/yellow]")
             return

        is_live_run = getattr(self.args, 'live', False)
        if is_live_run:
            log.info("Performing synchronous pre-scan for live run confirmation...")
            potential_actions_count = 0
            # Use Rich progress bar for pre-scan
            with Progress(TextColumn("[progress.description]{task.description} {task.fields[item_name]}"), BarColumn(), TextColumn("({task.completed} of {task.total})"), TimeElapsedColumn(), console=self.console, disable=disable_rich_progress) as progress:
                prescan_task = progress.add_task("Pre-scan", total=batch_count, item_name="")
                for stem, batch_data in file_batches.items():
                    # Use shorter name for progress bar update
                    item_name_short = Path(batch_data.get('video', stem)).name[:30]
                    if len(item_name_short) < len(Path(batch_data.get('video', stem)).name):
                        item_name_short += "..."
                    progress.update(prescan_task, advance=1, item_name=item_name_short)

                    # Pre-scan logic (remains the same)
                    try:
                        if not batch_data.get('video'): continue
                        media_info_prescan = MediaInfo(original_path=batch_data['video'])
                        media_info_prescan.guess_info = self.renamer.parse_filename(media_info_prescan.original_path)
                        media_info_prescan.file_type = self.renamer._determine_file_type(media_info_prescan.guess_info)
                        if media_info_prescan.file_type == 'unknown':
                            unknown_handling_mode_prescan = self.cfg('unknown_file_handling', 'skip', arg_value=getattr(self.args, 'unknown_file_handling', None))
                            if unknown_handling_mode_prescan == 'move_to_unknown': potential_actions_count += 1 + len(batch_data.get('associated', []))
                            elif unknown_handling_mode_prescan == 'guessit_only':
                                # Temporarily set metadata to None for planning
                                media_info_prescan.metadata = None
                                plan = self.renamer.plan_rename(batch_data['video'], batch_data.get('associated', []), media_info_prescan)
                                if plan.status == 'success': potential_actions_count += len(plan.actions) + (1 if plan.created_dir_path else 0)
                        else:
                            # Plan without API metadata for pre-scan estimate
                            media_info_prescan.metadata = None
                            plan = self.renamer.plan_rename(batch_data['video'], batch_data.get('associated', []), media_info_prescan)
                            if plan.status == 'success': potential_actions_count += len(plan.actions) + (1 if plan.created_dir_path else 0)
                    except Exception as e: log.warning(f"Pre-scan planning error for batch '{stem}': {e}", exc_info=True)
            if not self._confirm_live_run(potential_actions_count): return

        initial_media_infos: Dict[str, Optional[MediaInfo]] = {} # Allow None for failed initial parse
        log.info("Performing initial file parsing...")
        with Progress(TextColumn("[progress.description]{task.description}"), BarColumn(), TextColumn("({task.completed} of {task.total})"), TimeElapsedColumn(), console=self.console, disable=disable_rich_progress) as progress:
            parse_task = progress.add_task("Parsing Filenames", total=batch_count)
            for stem, batch_data in file_batches.items():
                if not batch_data.get('video'): initial_media_infos[stem] = None; progress.update(parse_task, advance=1); continue
                media_info_obj = MediaInfo(original_path=batch_data['video'])
                try:
                    media_info_obj.guess_info = self.renamer.parse_filename(media_info_obj.original_path)
                    media_info_obj.file_type = self.renamer._determine_file_type(media_info_obj.guess_info)
                    initial_media_infos[stem] = media_info_obj
                except Exception as e_parse: log.error(f"Error parsing '{stem}': {e_parse}"); initial_media_infos[stem] = None
                progress.update(parse_task, advance=1)

        # Metadata Fetching (remains the same async logic, updating initial_media_infos)
        fetch_tasks = []
        if use_metadata_effective and self.metadata_fetcher:
            stems_to_fetch = [stem for stem, info in initial_media_infos.items() if info and info.file_type != 'unknown']
            log.info(f"Creating {len(stems_to_fetch)} tasks for concurrent metadata fetching...")

            if stems_to_fetch:
                with Progress(TextColumn("[progress.description]{task.description} {task.fields[item_name]}"), BarColumn(), TextColumn("({task.completed} of {task.total})"), TimeElapsedColumn(), console=self.console, disable=disable_rich_progress) as progress_bar:
                    metadata_overall_task = progress_bar.add_task("Fetching Metadata", total=len(stems_to_fetch), item_name="")
                    for stem in stems_to_fetch:
                        batch_data = file_batches[stem]
                        task = asyncio.create_task(
                            _fetch_metadata_for_batch(stem, batch_data, self, progress_bar, metadata_overall_task),
                            name=f"fetch_{stem}"
                        )
                        fetch_tasks.append(task)

                    completed_fetch_results_tuples = []
                    if fetch_tasks:
                        # Use asyncio.gather to get results in order potentially (or keep as_completed)
                        # completed_fetch_results_tuples = await asyncio.gather(*fetch_tasks) # If order matters
                        for f_task in asyncio.as_completed(fetch_tasks): # If order doesn't matter
                            completed_fetch_results_tuples.append(await f_task)

                        if metadata_overall_task < len(progress_bar.tasks) and not progress_bar.tasks[metadata_overall_task].finished : # Check task_id is valid
                             progress_bar.update(metadata_overall_task, completed=len(stems_to_fetch))

                for result_item in completed_fetch_results_tuples:
                    if isinstance(result_item, tuple) and len(result_item) == 2:
                        stem_from_task, updated_media_info_obj = result_item # updated_media_info_obj is a MediaInfo object
                        if updated_media_info_obj:
                            initial_media_infos[stem_from_task] = updated_media_info_obj # Replace the original MediaInfo
                        else:
                            log.error(f"Async task for {stem_from_task} returned None for MediaInfo object")
            else: log.info("No batches required metadata fetching (all unknown or invalid initial parse).")
        else: log.info("Metadata fetching disabled or fetcher not available.")

        # --- Main Processing Loop ---
        run_batch_id = f"run-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        log.info(f"Starting planning and execution run ID: {run_batch_id}")
        results_summary = {'success': 0, 'skipped': 0, 'error': 0, 'actions': 0, 'moved_unknown': 0}

        self.console.print("-" * 30)

        # Use Rich progress bar for processing
        with Progress(TextColumn("[progress.description]{task.description} {task.fields[item_name]}"), BarColumn(), TextColumn("({task.completed} of {task.total})"), TimeElapsedColumn(), console=self.console, disable=disable_rich_progress) as progress_bar:
            main_processing_task = progress_bar.add_task("Planning/Executing", total=batch_count, item_name="")

            for stem, batch_data in file_batches.items():
                 # Update progress bar with shortened name
                item_name_short = Path(batch_data.get('video', stem)).name[:30]
                if len(item_name_short) < len(Path(batch_data.get('video', stem)).name):
                    item_name_short += "..."
                progress_bar.update(main_processing_task, advance=1, item_name=item_name_short)

                media_info = initial_media_infos.get(stem)
                if not media_info:
                    log.error(f"Skipping batch '{stem}' (initial parse error or no video).")
                    results_summary['error'] += 1; continue

                # --- Log Match Confidence ---
                match_conf_score = getattr(media_info.metadata, 'match_confidence', None)
                log_score_str = f"{match_conf_score:.1f}%" if isinstance(match_conf_score, float) else "N/A"
                log.debug(
                    f"Processing batch '{stem}'. Type: {media_info.file_type}, "
                    f"Metadata Source: {getattr(media_info.metadata, 'source_api', 'N/A')}, "
                    # Add score to log
                    f"Match Score: {log_score_str}, "
                    f"Metadata Error: {media_info.metadata_error_message}"
                )
                # --- End Log Match Confidence ---


                if media_info.metadata_error_message:
                    self.console.print(f"[bold red]API Error for '{media_info.original_path.name}': {media_info.metadata_error_message}[/bold red]")

                apply_unknown_logic = False
                unknown_handling_mode = self.args.unknown_file_handling
                plan: Optional[RenamePlan] = None
                action_result: Dict[str, Any] = {'success': False, 'message': '', 'actions_taken': 0}
                video_file_path = cast(Path, batch_data.get('video'))

                try:
                    # --- Determine if unknown logic applies ---
                    if media_info.file_type == 'unknown':
                        apply_unknown_logic = True
                        log.info(f"Batch '{stem}' is type 'unknown' by guessit. Applying mode: '{unknown_handling_mode}'")
                    elif use_metadata_effective and (media_info.metadata is None or not media_info.metadata.source_api):
                        if not media_info.metadata_error_message:
                            log.warning(f"Batch '{stem}' (type: {media_info.file_type}) has missing/failed metadata. Applying unknown_file_handling: '{unknown_handling_mode}'.")

                        if unknown_handling_mode in ['skip', 'move_to_unknown']:
                            apply_unknown_logic = True

                    # --- Apply unknown logic if needed ---
                    if apply_unknown_logic:
                        if unknown_handling_mode == 'skip':
                            skip_msg = media_info.metadata_error_message or f"Skipped (unknown/meta-fail): {video_file_path.name}"
                            plan = RenamePlan(batch_id=f"unknown_skip_{stem}", video_file=video_file_path, status='skipped', message=skip_msg)
                            action_result['message'] = plan.message
                        elif unknown_handling_mode == 'move_to_unknown':
                            action_result = self._handle_move_to_unknown(stem, batch_data, run_batch_id)
                            if action_result.get('success', False) and action_result.get('actions_taken', 0) > 0: results_summary['moved_unknown'] += action_result.get('actions_taken',0)
                            elif not action_result.get('success', False): results_summary['error'] += 1
                            else: results_summary['skipped'] +=1
                            if action_result.get('message') and (disable_rich_progress or not self.args.interactive):
                                self.console.print(action_result['message'])
                            continue # To next batch

                    # --- Regular planning if not handled by unknown logic ---
                    if not plan:
                         plan = self.renamer.plan_rename(video_file_path, batch_data.get('associated', []), media_info)

                    # --- START ENHANCED INTERACTIVE LOOP ---
                    user_choice = 'y' # Default to yes if not interactive or no valid plan
                    current_plan_to_action = plan # Keep track of the plan to potentially execute

                    if self.args.interactive and is_live_run and plan and plan.status in ['success', 'conflict_unresolved']: # Also ask for unresolved conflicts
                         # --- Get threshold ---
                         confirm_threshold = self.cfg('confirm_match_below', default_value=None, arg_value=getattr(self.args, 'confirm_match_below', None))

                         while True: # Loop for interaction until a decision (y/n/s/q) is made
                             self._display_plan_for_confirmation(current_plan_to_action, media_info)

                             # --- Check and display confidence warning ---
                             show_confidence_warning = False
                             current_score = getattr(media_info.metadata, 'match_confidence', None)
                             if confirm_threshold is not None and isinstance(current_score, float) and current_score < confirm_threshold:
                                  self.console.print(f"[bold yellow]Warning: Match confidence ({current_score:.1f}%) is below threshold ({confirm_threshold}%).[/bold yellow]")
                                  show_confidence_warning = True
                             # --- End confidence check ---

                             choices = ["y", "n", "s", "q", "g", "m"]
                             prompt_text = "[bold]Apply? ([Y]es/[N]o/[S]kip, [G]uessit Only, [M]anual ID, [Q]uit)"
                             if show_confidence_warning:
                                 prompt_text += " - Low confidence match![/bold]"
                             else:
                                 prompt_text += ":[/bold]"

                             try:
                                choice = Prompt.ask(prompt_text, choices=choices, default="y", show_default=False).lower()

                                if choice == 'y': # Accept current plan
                                    user_choice = 'y'
                                    if current_plan_to_action.status == 'conflict_unresolved':
                                        self.console.print("[yellow]Warning: Plan has unresolved conflicts. Proceeding may fail based on config.[/yellow]")
                                    break
                                elif choice in ('n', 's'): # Skip
                                    user_choice = 's'
                                    current_plan_to_action.status = 'skipped' # Mark plan as skipped
                                    current_plan_to_action.message = "Skipped by user (interactive)."
                                    self.console.print("[yellow]Batch skipped by user.[/yellow]")
                                    break
                                elif choice == 'q': # Quit
                                    raise UserAbortError("User quit during interactive mode.")
                                elif choice == 'g': # Guessit Only
                                    self.console.print("[cyan]Re-planning using Guessit data only...[/cyan]")
                                    # Create a temporary copy or modify carefully
                                    temp_media_info = MediaInfo(original_path=media_info.original_path, guess_info=media_info.guess_info, file_type=media_info.file_type)
                                    temp_media_info.metadata = None # Key change for Guessit Only
                                    current_plan_to_action = self.renamer.plan_rename(video_file_path, batch_data.get('associated', []), temp_media_info)
                                    # Reset confidence score as metadata is now ignored
                                    media_info.metadata = None # Update the main object for display consistency
                                    media_info.metadata_error_message = "Using Guessit Only by user choice."
                                    # Continue loop to show the new plan
                                elif choice == 'm': # Manual ID
                                    # ... (Manual ID logic as implemented previously) ...
                                    api_choice = Prompt.ask("Enter API source ([T]MDB/[V]TVDB):", choices=["t", "v"], default="t").lower()
                                    api_source = "tmdb" if api_choice == "t" else "tvdb"
                                    while True:
                                        try:
                                            id_str = Prompt.ask(f"Enter {api_source.upper()} ID:")
                                            manual_id = int(id_str)
                                            break
                                        except (ValueError, InvalidResponse): self.console.print("[red]Invalid ID. Please enter a number.[/red]")
                                        except EOFError: raise UserAbortError("User quit (EOF) during interactive mode.")

                                    refetched_metadata = await self._refetch_with_manual_id(media_info, api_source, manual_id)
                                    if refetched_metadata:
                                        self.console.print("[cyan]Re-planning using manually fetched metadata...[/cyan]")
                                        media_info.metadata = refetched_metadata
                                        # Confidence score is now set within the refetched_metadata
                                        media_info.metadata_error_message = None # Clear previous errors
                                        current_plan_to_action = self.renamer.plan_rename(video_file_path, batch_data.get('associated', []), media_info)
                                    else:
                                        self.console.print("[yellow]Manual fetch failed. Displaying previous plan again.[/yellow]")
                                    # Continue loop

                             except EOFError: raise UserAbortError("User quit (EOF) during interactive mode.")
                             except InvalidResponse: self.console.print("[red]Invalid choice.[/red]")
                    # --- END Enhanced Interactive Loop ---


                    # --- Execute Action ---
                    # Uses current_plan_to_action which might have been modified by interaction
                    if current_plan_to_action and current_plan_to_action.status == 'success' and user_choice == 'y':
                        effective_args = self.args; effective_args.live = is_live_run; effective_args.dry_run = not is_live_run
                        action_result = perform_file_actions(plan=current_plan_to_action, run_batch_id=run_batch_id, args_ns=effective_args, cfg_helper=self.cfg, undo_manager=self.undo_manager)
                    elif current_plan_to_action and current_plan_to_action.status == 'skipped':
                         action_result['success'] = False; action_result['message'] = current_plan_to_action.message or f"Skipped batch {stem}."
                    elif current_plan_to_action: # Plan exists but status is not success/skipped (e.g., failed, conflict_unresolved and user said yes anyway)
                         action_result['success'] = False; action_result['message'] = f"ERROR: Planning failed for '{stem}'. Reason: {current_plan_to_action.message}"
                    elif not action_result.get('message'): # Handles cases where plan was never generated (e.g., initial unknown logic path without message)
                        action_result['message'] = f"No action for batch {stem} (plan not generated)."

                    # Update summary based on the FINAL action result
                    if action_result.get('success', False): results_summary['success'] += 1; results_summary['actions'] += action_result.get('actions_taken', 0)
                    elif current_plan_to_action and current_plan_to_action.status == 'skipped': results_summary['skipped'] += 1
                    else: results_summary['error'] += 1

                    # Print result message (unless interactive already showed it via panel)
                    if action_result.get('message'):
                        # Only print detailed results if not interactive OR if an error occurred
                        if not self.args.interactive or not action_result.get('success'):
                           use_rule = not self.args.interactive and is_live_run and action_result.get('success', False) and action_result.get('actions_taken',0) > 0
                           if use_rule: self.console.rule()
                           # Use Text to potentially handle formatting/styles if needed later
                           self.console.print(Text(action_result['message']))
                           if use_rule: self.console.rule()


                except UserAbortError as e: log.warning(str(e)); self.consocsle.print(f"\n{e}"); break
                except FileExistsError as e: log.critical(str(e)); self.console.print(f"\n[bold red]STOPPING: {e}[/bold red]"); results_summary['error'] += 1; break
                except Exception as e: results_summary['error'] += 1; log.exception(f"Critical unhandled error processing batch '{stem}': {e}"); self.console.print(f"[bold red]CRITICAL ERROR processing batch {stem}. See log.[/bold red]")

        # ... (Final Summary Printout remains the same) ...
        self.console.print("-" * 30)
        log.info("Processing complete.")
        self.console.print("Processing Summary:")
        self.console.print(f"  Batches Scanned: {batch_count}")
        self.console.print(f"  Batches Successfully Processed: {results_summary['success']}")
        self.console.print(f"  Batches Skipped: {results_summary['skipped']}")
        if results_summary['moved_unknown'] > 0:
            self.console.print(f"  Files Moved to Unknown Dir: {results_summary['moved_unknown']}")
        self.console.print(f"  Batches with Errors: {results_summary['error']}")
        total_actions_reported = results_summary['actions'] + results_summary['moved_unknown']
        if is_live_run: self.console.print(f"  Total File Actions Taken: {total_actions_reported}")
        else:
             if total_actions_reported > 0: self.console.print(f"  Total File Actions Planned: {total_actions_reported}")
        self.console.print("-" * 30)
        if not is_live_run and total_actions_reported > 0: self.console.print("[yellow]DRY RUN COMPLETE. To apply changes, run again with --live[/yellow]")
        elif not is_live_run: self.console.print("DRY RUN COMPLETE. No actions were planned.")
        if is_live_run and self.cfg('enable_undo', False) and total_actions_reported > 0:
            script_name = Path(sys.argv[0]).name
            self.console.print(f"Undo information logged with Run ID: [bold cyan]{run_batch_id}[/bold cyan]")
            self.console.print(f"To undo this run: {script_name} undo {run_batch_id}")
        if is_live_run and self.args.stage_dir and results_summary['actions'] > 0 : self.console.print(f"Renamed files moved to staging: {self.args.stage_dir}")
        if results_summary['error'] > 0: self.console.print(f"[bold yellow]WARNING: {results_summary['error']} errors occurred. Check logs.[/bold yellow]")

        if results_summary['error'] == 0:
            if results_summary['success'] > 0 or results_summary['skipped'] == batch_count or results_summary['moved_unknown'] > 0 : self.console.print("[green]Operation finished successfully.[/green]")
            elif results_summary['skipped'] < batch_count and results_summary['success'] == 0 and results_summary['moved_unknown'] == 0 : self.console.print("Operation finished, but some batches were skipped or had no actions planned.")
            else: self.console.print("Operation finished.")
        else: self.console.print("[bold red]Operation finished with errors.[/bold red]")


# --- END OF FILE main_processor.py ---