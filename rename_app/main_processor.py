# rename_app/main_processor.py
import logging
import uuid
import builtins
import sys
# import time # Not directly used
import asyncio
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Tuple, Optional, Dict, Any, cast, List, Deque

from collections import deque

from .metadata_fetcher import MetadataFetcher, DIRECT_ID_MATCH_SCORE
from .renamer_engine import RenamerEngine
from .file_system_ops import perform_file_actions, _handle_conflict, FileOperationError
from .utils import scan_media_files
from .exceptions import UserAbortError, RenamerError, MetadataError
from .models import MediaInfo, RenamePlan, MediaMetadata
from .api_clients import get_tmdb_client, get_tvdb_client
from .enums import ProcessingStatus
from .undo_manager import UndoManager
from .config_manager import ConfigHelper
from .ui_utils import (
    ConsoleClass, TextClass, PanelClass, TableClass, ProgressClass,
    BarColumnClass, ProgressTextColumnClass, TimeElapsedColumnClass,
    MofNCompleteColumnClass, TaskIDClass, ConfirmClass, PromptClass,
    InvalidResponseClass, RICH_AVAILABLE_UI as RICH_AVAILABLE, RichConsoleActual,
    RichConfirm, RichPrompt # Import Rich versions for type checking in helper
)

log = logging.getLogger(__name__)

DEFAULT_PROGRESS_COLUMNS_DEF = (
    (ProgressTextColumnClass("[progress.description]{task.description}") if RICH_AVAILABLE else None),
    (BarColumnClass() if RICH_AVAILABLE else None),
    (MofNCompleteColumnClass() if RICH_AVAILABLE else None),
    (TimeElapsedColumnClass() if RICH_AVAILABLE else None),
    (ProgressTextColumnClass("[cyan]{task.fields[item_name]}") if RICH_AVAILABLE else None),
)
DEFAULT_PROGRESS_COLUMNS = tuple(col for col in DEFAULT_PROGRESS_COLUMNS_DEF if col is not None)


async def _fetch_metadata_for_batch(
    processor: "MainProcessor", 
    batch_stem: str,
    batch_data: Dict[str, Any],
    progress: Optional[ProgressClass] = None,
    task_id: Optional[TaskIDClass] = None
) -> Tuple[str, MediaInfo]:
    video_path_for_media_info = batch_data.get('video')
    if not video_path_for_media_info:
        log.error(f"CRITICAL in _fetch_metadata_for_batch: video_path is None for stem '{batch_stem}'. Using dummy.")
        error_media_info = MediaInfo(original_path=Path(f"error_dummy_{batch_stem}.file"))
        error_media_info.file_type = 'unknown'
        error_media_info.metadata_error_message = f"[{ProcessingStatus.MISSING_VIDEO_FILE_IN_BATCH}] Missing video path for batch."
        return batch_stem, error_media_info

    media_info = MediaInfo(original_path=video_path_for_media_info)
    media_info.metadata = None
    item_name_short = media_info.original_path.name[:30] + ("..." if len(media_info.original_path.name) > 30 else "")

    if progress and task_id is not None and hasattr(progress, 'tasks') and progress.tasks:
        task_obj = None
        if isinstance(progress.tasks, list):
            safe_task_id = int(task_id) if isinstance(task_id, (int, float)) or (isinstance(task_id, str) and task_id.isdigit()) else -1
            if 0 <= safe_task_id < len(progress.tasks): task_obj = progress.tasks[safe_task_id]
        elif isinstance(progress.tasks, dict): 
            task_obj = progress.tasks.get(task_id)

        if task_obj and not task_obj.finished:
            try:
                progress.update(task_id, item_name=f"fetching: {item_name_short}")
            except Exception as e_prog_update:
                log.error(f"Error updating progress bar item name in fetch: {e_prog_update}")
    try: # Outer try for the whole fetch process
        media_info.guess_info = processor.renamer.parse_filename(media_info.original_path)
        original_file_type_from_guessit = processor.renamer._determine_file_type(media_info.guess_info)
        media_info.file_type = original_file_type_from_guessit

        forced_tmdb_id: Optional[int] = getattr(processor.args, 'tmdb_id', None)
        forced_tvdb_id: Optional[int] = getattr(processor.args, 'tvdb_id', None)
        should_fetch_metadata = getattr(processor.args, 'use_metadata', False) or forced_tmdb_id or forced_tvdb_id

        if should_fetch_metadata and processor.metadata_fetcher and \
           (media_info.file_type != 'unknown' or forced_tmdb_id or forced_tvdb_id):
            
            if forced_tmdb_id or forced_tvdb_id:
                log.info(f"Metadata fetch for '{batch_stem}' will be forced by CLI ID (TMDB: {forced_tmdb_id}, TVDB: {forced_tvdb_id}).")
            else:
                log.debug(f"Attempting async metadata search for '{batch_stem}' ({media_info.file_type})")
            
            year_guess = media_info.guess_info.get('year')
            fetched_api_metadata: Optional[MediaMetadata] = None
            try:
                effective_file_type = media_info.file_type
                if effective_file_type == 'unknown':
                    if forced_tvdb_id: effective_file_type = 'series'
                    elif forced_tmdb_id: effective_file_type = 'movie' 

                if effective_file_type == 'series':
                    raw_episode_data: Any = None; valid_ep_list: List[int] = []
                    if isinstance(media_info.guess_info.get('episode_list'), list): raw_episode_data = media_info.guess_info['episode_list']
                    elif 'episode' in media_info.guess_info: raw_episode_data = media_info.guess_info['episode']
                    elif 'episode_number' in media_info.guess_info: raw_episode_data = media_info.guess_info['episode_number']
                    
                    if raw_episode_data is not None:
                        ep_data_list = raw_episode_data if isinstance(raw_episode_data, list) else [raw_episode_data]
                        for ep in ep_data_list:
                            try: # This is the try that was missing its except
                                ep_int = int(str(ep))
                                if ep_int > 0: valid_ep_list.append(ep_int)
                            except (ValueError, TypeError): # ADDED THIS EXCEPT BLOCK
                                log.warning(f"Could not parse episode number '{ep}' from guessit data for '{batch_stem}'.")
                    valid_ep_list = sorted(list(set(valid_ep_list)))
                    log.debug(f"Final valid episode list for API call for '{batch_stem}': {valid_ep_list}")
                    guessed_title_raw = media_info.guess_info.get('title')
                    guessed_title = str(guessed_title_raw[0] if isinstance(guessed_title_raw, list) and guessed_title_raw else guessed_title_raw if isinstance(guessed_title_raw, str) and guessed_title_raw else media_info.original_path.stem)
                    if not guessed_title_raw or (isinstance(guessed_title_raw, list) and not guessed_title_raw[0]): log.debug(f"Guessed title empty for series '{batch_stem}', using stem: '{guessed_title}'")

                    if valid_ep_list or forced_tmdb_id or forced_tvdb_id: 
                        fetched_api_metadata = await processor.metadata_fetcher.fetch_series_metadata(
                            show_title_guess=guessed_title, season_num=media_info.guess_info.get('season', 0),
                            episode_num_list=tuple(valid_ep_list), year_guess=year_guess,
                            force_tmdb_id=forced_tmdb_id, force_tvdb_id=forced_tvdb_id
                        )
                    else: log.warning(f"No valid episode numbers and no forced ID for series '{batch_stem}'. Skipping series metadata fetch.")
                
                elif effective_file_type == 'movie':
                    guessed_title_raw = media_info.guess_info.get('title')
                    guessed_title = str(guessed_title_raw[0] if isinstance(guessed_title_raw, list) and guessed_title_raw else guessed_title_raw if isinstance(guessed_title_raw, str) and guessed_title_raw else media_info.original_path.stem)
                    if not guessed_title_raw or (isinstance(guessed_title_raw, list) and not guessed_title_raw[0]): log.debug(f"Guessed title empty for movie '{batch_stem}', using stem: '{guessed_title}'")
                    
                    fetched_api_metadata = await processor.metadata_fetcher.fetch_movie_metadata(
                        movie_title_guess=guessed_title, year_guess=year_guess, force_tmdb_id=forced_tmdb_id
                    )
                
                if effective_file_type == 'unknown' and fetched_api_metadata is None and (forced_tmdb_id or forced_tvdb_id):
                    log.warning(f"Forced ID lookup for '{batch_stem}' (type unknown) did not yield metadata. File type might be incorrect for the given ID type.")
                    media_info.metadata_error_message = f"[{ProcessingStatus.METADATA_NO_MATCH}] Forced ID did not match expected media type or was not found."
                media_info.metadata = fetched_api_metadata
            except MetadataError as me:
                log.error(f"Caught MetadataError for '{batch_stem}': {me}")
                media_info.metadata_error_message = str(me); media_info.metadata = None
            except Exception as fetch_e:
                log.exception(f"Unexpected error during metadata API call for '{batch_stem}': {fetch_e}")
                media_info.metadata_error_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Unexpected fetch error: {fetch_e}"; media_info.metadata = None

            if media_info.metadata is None or not media_info.metadata.source_api:
                if not media_info.metadata_error_message:
                    media_info.metadata_error_message = f"[{ProcessingStatus.METADATA_NO_MATCH}] Metadata fetch returned no usable API data."
        elif not should_fetch_metadata: 
            log.debug(f"Metadata fetching disabled and no CLI ID provided for '{batch_stem}'. Skipping metadata phase.")
            
        return batch_stem, media_info
    # ... (Outer try's except and finally blocks) ...
    except Exception as e: # Outer catch for errors in guessit parsing etc.
        log.exception(f"Critical error in _fetch_metadata_for_batch for '{batch_stem}' (outside API call try): {e}")
        if not hasattr(media_info, 'guess_info') or not media_info.guess_info: media_info.guess_info = {}
        media_info.file_type = 'unknown'; media_info.metadata = None
        media_info.metadata_error_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Processing error in _fetch_metadata_for_batch: {e}"
        return batch_stem, media_info
    finally: # Progress bar update
        if progress and task_id is not None and hasattr(progress, 'tasks') and progress.tasks:
            # ... (progress update logic)
            task_obj = None
            if isinstance(progress.tasks, list):
                safe_task_id = int(task_id) if isinstance(task_id, (int, float)) or (isinstance(task_id, str) and task_id.isdigit()) else -1
                if 0 <= safe_task_id < len(progress.tasks): task_obj = progress.tasks[safe_task_id]
            elif isinstance(progress.tasks, dict): task_obj = progress.tasks.get(task_id)
            if task_obj and not task_obj.finished:
                try: progress.update(task_id, advance=1, item_name="")
                except Exception as e_prog_final: log.error(f"Error finalizing progress bar item name in fetch: {e_prog_final}")

class MainProcessor:
    def __init__(self, args, cfg_helper: ConfigHelper, undo_manager: UndoManager):
        self.args = args
        self.cfg = cfg_helper
        self.undo_manager = undo_manager
        self.renamer = RenamerEngine(cfg_helper)
        self.metadata_fetcher: Optional[MetadataFetcher] = None
        
        self.console = ConsoleClass(quiet=getattr(args, 'quiet', False))
               
        cli_use_metadata_arg = getattr(args, 'use_metadata', None)
        cli_forced_id = getattr(args, 'tmdb_id', None) is not None or \
                        getattr(args, 'tvdb_id', None) is not None
        
        use_metadata_effective = self.cfg('use_metadata', False, arg_value=cli_use_metadata_arg) or cli_forced_id

        if use_metadata_effective:
             log.info("Metadata fetching will be attempted (enabled by config/args or CLI ID).")
             if get_tmdb_client() or get_tvdb_client():
                 self.metadata_fetcher = MetadataFetcher(cfg_helper, console=self.console)
                 if cli_forced_id and (cli_use_metadata_arg is None or not cli_use_metadata_arg):
                     self.args.use_metadata = True 
                     log.debug("args.use_metadata set to True due to CLI forced ID.")
                 elif cli_use_metadata_arg is not None: 
                     self.args.use_metadata = cli_use_metadata_arg 
                 else: 
                     self.args.use_metadata = True # Default to True if enabled by config and no CLI override
                 
             else: 
                 log.warning("Metadata processing desired, but FAILED to initialize API clients. Disabling metadata fetcher.")
                 self.args.use_metadata = False 
        else: 
            log.info("Metadata fetching disabled (not enabled in config/args and no CLI ID provided).")
            self.args.use_metadata = False 

    def _display_plan_for_confirmation(self, plan: RenamePlan, media_info: MediaInfo):
        if not plan or plan.status != 'success':
            self.console.print(f"[yellow]No valid rename plan generated for {media_info.original_path.name}.[/yellow]")
            if plan and plan.message:
                self.console.print(f"[yellow]Reason: {plan.message}[/yellow]")
            return

        panel_content: List[Any] = []
        panel_content.append(f"[bold]File:[/bold] {media_info.original_path.name}")

        if media_info.metadata and media_info.metadata.source_api:
            source_info = f"[i]via {media_info.metadata.source_api.upper()}"
            score = getattr(media_info.metadata, 'match_confidence', None)
            if isinstance(score, float) and score != -1.0 and score != DIRECT_ID_MATCH_SCORE:
                score_color = "green" if score >= 85 else "yellow" if score >= self.cfg('tmdb_match_fuzzy_cutoff', 70) else "red"
                source_info += f" (Score: [{score_color}]{score:.1f}%[/])"
            elif score == DIRECT_ID_MATCH_SCORE:
                source_info += f" ([bold green]Direct ID Match[/])"

            source_info += "[/i]"
            panel_content.append(f"[bold]Type:[/bold] {media_info.file_type.capitalize()} {source_info}")

            if media_info.metadata.is_series:
                title = media_info.metadata.show_title or "[missing]"
                year = f"({media_info.metadata.show_year})" if media_info.metadata.show_year else ""
                ep_list = media_info.metadata.episode_list
                ep_num = ep_list[0] if ep_list else 0
                ep_title_raw = media_info.metadata.episode_titles.get(ep_num, "[missing episode title]")
                ep_title = ep_title_raw if ep_title_raw else "[missing episode title]"
                season_num = media_info.metadata.season if media_info.metadata.season is not None else 0
                panel_content.append(f"[bold]Match:[/bold] {title} {year} - S{season_num:02d}E{ep_num:02d} - {ep_title}")
            elif media_info.metadata.is_movie:
                title = media_info.metadata.movie_title or "[missing]"
                year = f"({media_info.metadata.movie_year})" if media_info.metadata.movie_year else ""
                panel_content.append(f"[bold]Match:[/bold] {title} {year}")
        else:
             panel_content.append(f"[bold]Type:[/bold] {media_info.file_type.capitalize()} ([i]via Guessit[/i])")
             panel_content.append(f"[bold]Guess:[/bold] {media_info.guess_info.get('title', media_info.original_path.stem)}")

        panel_content.append("\n[bold cyan]Proposed Actions:[/bold cyan]")
        table = TableClass(show_header=False, box=None, padding=(0, 1))
        table.add_column("Original")
        table.add_column("Arrow", justify="center")
        table.add_column("New")

        if plan.created_dir_path:
             dir_path_str = str(plan.created_dir_path).replace("\\", "/")
             table.add_row("[dim]-[/dim]", "[dim]->[/dim]", f"[green]{dir_path_str}[/green] [i](Create Dir)[/i]")

        for action_item in plan.actions:
             action_style = "blue" if action_item.action_type == 'move' else "default"
             new_path_str = str(action_item.new_path).replace("\\", "/")
             table.add_row(f"{action_item.original_path.name}", f"[{action_style}]->[/]", f"[{action_style}]{new_path_str}[/]")
        panel_content.append(table)
        self.console.print(PanelClass("\n".join(str(c) for c in panel_content), title="[yellow]Confirm Batch Action", border_style="yellow"))

    async def _refetch_with_manual_id(self, media_info: MediaInfo, api_source: str, manual_id: int, is_interactive_refetch: bool = False) -> Optional[MediaMetadata]:
        if not self.metadata_fetcher:
            self.console.print("[red]Error: Metadata fetcher not initialized.[/red]", file=sys.stderr)
            return None
        
        log_prefix = "[Interactive Refetch]" if is_interactive_refetch else "[Refetch]"
        log.info(f"{log_prefix} Attempting for '{media_info.original_path.name}' using {api_source.upper()} ID: {manual_id}")
        new_metadata: Optional[MediaMetadata] = None

        ep_list_for_refetch = tuple()
        if media_info.file_type == 'series' and media_info.guess_info:
            raw_ep_data = media_info.guess_info.get('episode_list', media_info.guess_info.get('episode'))
            if raw_ep_data is not None:
                ep_data_list = raw_ep_data if isinstance(raw_ep_data, list) else [raw_ep_data]
                valid_ep_nums = []
                for ep in ep_data_list:
                    try: valid_ep_nums.append(int(str(ep)))
                    except (ValueError, TypeError): pass
                ep_list_for_refetch = tuple(sorted(set(n for n in valid_ep_nums if n > 0)))
        try:
            dummy_title_for_id_fetch = f"{api_source.upper()}_ID_{manual_id}"

            if api_source == 'tmdb':
                if media_info.file_type == 'movie':
                    self.console.print(f"[yellow]{log_prefix} Re-fetching TMDB movie details for ID {manual_id}...[/yellow]")
                    new_metadata = await self.metadata_fetcher.fetch_movie_metadata(
                        movie_title_guess=dummy_title_for_id_fetch, year_guess=None, force_tmdb_id=manual_id
                    ) 
                elif media_info.file_type == 'series':
                    self.console.print(f"[yellow]{log_prefix} Re-fetching TMDB series details for ID {manual_id}...[/yellow]")
                    new_metadata = await self.metadata_fetcher.fetch_series_metadata(
                        show_title_guess=dummy_title_for_id_fetch, 
                        season_num=media_info.guess_info.get('season', 0) if media_info.guess_info else 0, 
                        episode_num_list=ep_list_for_refetch, year_guess=None, force_tmdb_id=manual_id
                    )
            elif api_source == 'tvdb':
                 if media_info.file_type == 'series':
                    self.console.print(f"[yellow]{log_prefix} Re-fetching TVDB series details for ID {manual_id}...[/yellow]")
                    new_metadata = await self.metadata_fetcher.fetch_series_metadata(
                        show_title_guess=dummy_title_for_id_fetch, 
                        season_num=media_info.guess_info.get('season', 0) if media_info.guess_info else 0,
                        episode_num_list=ep_list_for_refetch, year_guess=None, force_tvdb_id=manual_id
                    )
                 else: self.console.print(f"[red]{log_prefix} TVDB ID only applicable for Series.[/red]", file=sys.stderr); return None
            else: self.console.print(f"[red]{log_prefix} Unsupported API source: {api_source}[/red]", file=sys.stderr); return None

            if new_metadata and new_metadata.source_api:
                id_key_check = f"{api_source}_id" 
                if new_metadata.ids.get(id_key_check) == manual_id:
                    self.console.print(f"[green]{log_prefix} Successfully re-fetched metadata from {new_metadata.source_api.upper()} for ID {manual_id}.[/green]")
                    return new_metadata
                else:
                    log.warning(f"{log_prefix} Re-fetch for {api_source.upper()} ID {manual_id} returned metadata for a different ID: {new_metadata.ids.get(id_key_check)}. Discarding.")
                    self.console.print(f"[red]{log_prefix} Failed: Re-fetched metadata ID mismatch for {api_source.upper()} ID {manual_id}.[/red]")
                    return None
            else:
                 self.console.print(f"[red]{log_prefix} Failed to fetch valid metadata using {api_source.upper()} ID {manual_id}.[/red]")
                 return None
        except Exception as e:
            log.exception(f"{log_prefix} Error during manual ID re-fetch ({api_source} ID {manual_id}): {e}")
            self.console.print(f"[red]{log_prefix} Error during re-fetch: {e}[/red]", file=sys.stderr)
            return None

    def _confirm_live_run(self, potential_actions_count: int) -> bool:
        if getattr(self.args, 'quiet', False):
            log.info("Quiet mode: Live run confirmation automatically affirmative.")
            return True 

        if potential_actions_count == 0:
            log.warning("Pre-scan found no files eligible for action. Live run will not proceed.")
            self.console.print("[yellow]Pre-scan found no files eligible for action. Live run will not proceed.[/yellow]")
            return False
        
        self.console.print("-" * 30)
        self.console.print(f"Pre-scan found {potential_actions_count} potential file actions.")
        self.console.print("[bold red]THIS IS A LIVE RUN.[/bold red]")
        if hasattr(self.args, 'backup_dir') and self.args.backup_dir: self.console.print(f"Originals will be backed up to: {self.args.backup_dir}")
        elif hasattr(self.args, 'stage_dir') and self.args.stage_dir: self.console.print(f"Files will be MOVED to staging: {self.args.stage_dir}")
        elif hasattr(self.args, 'trash') and self.args.trash: self.console.print("Originals will be MOVED TO TRASH.")
        else: self.console.print("Files will be RENAMED/MOVED IN PLACE.")

        undo_enabled_effective = self.cfg('enable_undo', False, arg_value=getattr(self.args, 'enable_undo', None))
        if undo_enabled_effective: self.console.print("Undo logging is [green]ENABLED[/green].")
        else: self.console.print("Undo logging is [yellow]DISABLED[/yellow].")
        self.console.print("-" * 30)
        
        try:
            if ConfirmClass.ask("Proceed with actions?", default=False): 
                log.info("User confirmed live run.")
                return True
            else:
                log.info("User aborted live run.")
                self.console.print("Operation cancelled by user.")
                return False
        except (EOFError, KeyboardInterrupt) as e:
             log.warning(f"Live run confirmation aborted by user ({type(e).__name__}).")
             self.console.print("\nOperation cancelled by user.", file=sys.stderr)
             return False
        except InvalidResponseClass: 
            log.warning("Invalid response during live run confirmation.")
            self.console.print("\nInvalid response. Operation cancelled.", file=sys.stderr)
            return False
        except Exception as e:
            log.error(f"Error during live run confirmation: {e}", exc_info=True)
            self.console.print(f"\n[bold red]ERROR: Could not get confirmation: {e}[/bold red]", file=sys.stderr)
            return False

    def _handle_move_to_unknown(self, batch_stem: str, batch_data: Dict[str, Any], run_batch_id: str) -> Dict[str, Any]:
        results: Dict[str, Any] = {'move_success': False, 'message': "", 'actions_taken': 0, 'fs_errors': 0}
        action_messages: List[str] = []
        unknown_dir_str_from_args = getattr(self.args, 'unknown_files_dir', None)
        unknown_dir_str = self.cfg('unknown_files_dir', '_unknown_files_', arg_value=unknown_dir_str_from_args)
        
        base_message_prefix = f"Batch '{batch_stem}': "

        if not unknown_dir_str:
            msg = f"[{ProcessingStatus.CONFIG_MISSING_FORMAT_STRING}] {base_message_prefix}Unknown files directory not configured. Skipping move."
            log.error(msg)
            results['message'] = msg; results['fs_errors'] += 1
            return results

        base_target_dir = self.args.directory.resolve()
        unknown_target_dir_path = Path(unknown_dir_str)
        if not unknown_target_dir_path.is_absolute():
            unknown_target_dir_path = base_target_dir / unknown_target_dir_path
        unknown_target_dir = unknown_target_dir_path.resolve()

        log.info(f"Handling unknown/failed batch '{batch_stem}': Moving files to '{unknown_target_dir}'")
        is_live_run = getattr(self.args, 'live', False)

        if not is_live_run:
            dry_run_actions_count = 0
            if not unknown_target_dir.exists():
                action_messages.append(f"DRY RUN: [{ProcessingStatus.SUCCESS}] Would create directory '{unknown_target_dir}'")
                dry_run_actions_count += 1
            
            all_files_in_batch: List[Optional[Path]] = [batch_data.get('video')] + batch_data.get('associated', [])
            files_to_log_dry_run = [f for f in all_files_in_batch if f and isinstance(f, Path) and f.exists()]

            if not files_to_log_dry_run and unknown_target_dir.exists():
                action_messages.append(f"DRY RUN: [{ProcessingStatus.SKIPPED}] No files to move for '{batch_stem}' to existing '{unknown_target_dir}'.")
            
            for file_path in files_to_log_dry_run:
                sim_dest_path = unknown_target_dir / file_path.name
                try:
                    _handle_conflict(file_path, sim_dest_path, self.cfg('on_conflict', 'skip'))
                    action_messages.append(f"DRY RUN: [{ProcessingStatus.SUCCESS}] Would move '{file_path.name}' to '{unknown_target_dir}'")
                    dry_run_actions_count += 1
                except FileOperationError as e_foe: 
                    action_messages.append(f"DRY RUN: [{ProcessingStatus.PLAN_TARGET_EXISTS_SKIP_MODE}] Would attempt move '{file_path.name}' to '{unknown_target_dir}' (WARNING: Target exists - would be SKIPPED: {e_foe})")
                except FileExistsError as e_fe: 
                    action_messages.append(f"DRY RUN: [{ProcessingStatus.PLAN_TARGET_EXISTS_FAIL_MODE}] Would attempt move '{file_path.name}' to '{unknown_target_dir}' (ERROR: Target exists - would FAIL: {e_fe})")
            
            results['message'] = "\n".join(action_messages) if action_messages else f"DRY RUN: [{ProcessingStatus.SKIPPED}] No actions planned for unknown batch '{batch_stem}'."
            results['move_success'] = True; results['actions_taken'] = dry_run_actions_count
            return results

        try:
            if not unknown_target_dir.exists():
                log.info(f"Creating unknown files directory: {unknown_target_dir}")
                unknown_target_dir.mkdir(parents=True, exist_ok=True)
                if self.undo_manager.is_enabled:
                    self.undo_manager.log_action(batch_id=run_batch_id, original_path=unknown_target_dir, new_path=unknown_target_dir, item_type='dir', status='created_dir')
                action_messages.append(f"[{ProcessingStatus.SUCCESS}] CREATED DIR (unknowns): '{unknown_target_dir}'")
                results['actions_taken'] +=1 
        except OSError as e:
            msg = f"[{ProcessingStatus.FILE_OPERATION_ERROR}] {base_message_prefix}Could not create directory '{unknown_target_dir}': {e}"
            log.error(msg, exc_info=True); results['message'] = msg; results['fs_errors'] += 1
            return results

        conflict_mode = self.cfg('on_conflict', 'skip')
        files_to_move_live: List[Optional[Path]] = [batch_data.get('video')] + batch_data.get('associated', [])
        files_moved_successfully = 0; files_to_move_count = 0

        for original_file_path_live in files_to_move_live:
            if not original_file_path_live or not isinstance(original_file_path_live, Path): continue
            if not original_file_path_live.exists():
                log.warning(f"Skipping move of non-existent file: {original_file_path_live}"); continue
            
            files_to_move_count += 1
            target_file_path_in_unknown_dir = unknown_target_dir / original_file_path_live.name
            
            try:
                final_target_path = _handle_conflict(original_file_path_live, target_file_path_in_unknown_dir, conflict_mode)
                if self.undo_manager.is_enabled:
                    self.undo_manager.log_action(batch_id=run_batch_id, original_path=original_file_path_live, new_path=final_target_path, item_type='file', status='moved')
                
                log.debug(f"Moving '{original_file_path_live.name}' to '{final_target_path}' for unknown handling.")
                shutil.move(str(original_file_path_live), str(final_target_path))
                action_messages.append(f"[{ProcessingStatus.SUCCESS}] MOVED (unknown): '{original_file_path_live.name}' to '{final_target_path}'")
                results['actions_taken'] += 1; files_moved_successfully += 1
            except FileExistsError as e_fe: 
                msg = f"[{ProcessingStatus.PLAN_TARGET_EXISTS_FAIL_MODE}] ERROR (move unknown): {e_fe} - File '{original_file_path_live.name}' not moved."
                log.error(msg); action_messages.append(msg); results['fs_errors'] += 1
            except FileOperationError as e_foe: 
                msg = f"[{ProcessingStatus.PLAN_TARGET_EXISTS_SKIP_MODE}] SKIPPED (move unknown): {e_foe} - File '{original_file_path_live.name}' not moved."
                log.warning(msg); action_messages.append(msg)
            except OSError as e_os:
                msg = f"[{ProcessingStatus.FILE_OPERATION_ERROR}] ERROR (move unknown): Failed to move '{original_file_path_live.name}': {e_os}"
                log.error(msg, exc_info=True); action_messages.append(msg); results['fs_errors'] += 1
            except Exception as e_generic:
                msg = f"[{ProcessingStatus.INTERNAL_ERROR}] ERROR (move unknown): Unexpected error for '{original_file_path_live.name}': {e_generic}"
                log.exception(msg); action_messages.append(msg); results['fs_errors'] += 1
        
        results['move_success'] = (files_to_move_count > 0 and files_moved_successfully == files_to_move_count) and (results['fs_errors'] == 0)
        if not action_messages: action_messages.append(f"[{ProcessingStatus.SKIPPED}] {base_message_prefix}No files moved to unknown.")
        results['message'] = "\n".join(action_messages)
        return results
    
    def _perform_prescan(self, file_batches: Dict[str, Dict[str, Any]], batch_count: int, initial_media_infos_for_prescan: Dict[str, Optional[MediaInfo]]) -> int:
        log.info("Performing synchronous pre-scan for live run confirmation...")
        potential_actions_count = 0
        disable_rich_progress = getattr(self.args, 'quiet', False) or getattr(self.args, 'interactive', False) or not RICH_AVAILABLE
        
        with ProgressClass(*DEFAULT_PROGRESS_COLUMNS, console=self.console, disable=disable_rich_progress) as progress:
            prescan_task: TaskIDClass = progress.add_task("Pre-scan", total=batch_count, item_name="")
            for stem, batch_data in file_batches.items():
                video_path_obj = batch_data.get('video')
                item_name_short = Path(video_path_obj if video_path_obj else stem).name[:30] + \
                                  ("..." if len(Path(video_path_obj if video_path_obj else stem).name) > 30 else "")
                progress.update(prescan_task, advance=1, item_name=item_name_short)
                
                try:
                    if not video_path_obj: continue
                    
                    video_path = cast(Path, video_path_obj)
                    media_info_prescan = initial_media_infos_for_prescan.get(stem) 
                    if not media_info_prescan: 
                        log.warning(f"Pre-scan: MediaInfo for '{stem}' missing, re-parsing for pre-scan count.")
                        media_info_prescan = MediaInfo(original_path=video_path)
                        media_info_prescan.guess_info = self.renamer.parse_filename(media_info_prescan.original_path)
                        media_info_prescan.file_type = self.renamer._determine_file_type(media_info_prescan.guess_info)
                    
                    associated_paths_prescan = batch_data.get('associated', [])
                    if not isinstance(associated_paths_prescan, list): associated_paths_prescan = []

                    use_metadata_effective = getattr(self.args, 'use_metadata', False) 
                                             
                    metadata_failed_or_rejected_for_prescan = use_metadata_effective and \
                                                              (media_info_prescan.metadata is None or bool(media_info_prescan.metadata_error_message))

                    if media_info_prescan.file_type == 'unknown' or metadata_failed_or_rejected_for_prescan:
                        unknown_handling_mode_prescan = self.cfg('unknown_file_handling', 'skip', arg_value=getattr(self.args, 'unknown_file_handling', None))
                        if unknown_handling_mode_prescan == 'move_to_unknown':
                            potential_actions_count += 1 
                            if video_path.exists(): potential_actions_count +=1
                            potential_actions_count += sum(1 for p in associated_paths_prescan if p and p.exists())
                        elif unknown_handling_mode_prescan == 'guessit_only':
                            temp_mi_guessit_only = MediaInfo(original_path=video_path, guess_info=media_info_prescan.guess_info, file_type=media_info_prescan.file_type, metadata=None)
                            plan = self.renamer.plan_rename(video_path, associated_paths_prescan, temp_mi_guessit_only)
                            if plan.status == 'success':
                                potential_actions_count += len(plan.actions) + (1 if plan.created_dir_path else 0)
                    else: 
                        plan = self.renamer.plan_rename(video_path, associated_paths_prescan, media_info_prescan)
                        if plan.status == 'success':
                            potential_actions_count += len(plan.actions) + (1 if plan.created_dir_path else 0)
                except Exception as e:
                    log.warning(f"Pre-scan planning error for batch '{stem}': {e}", exc_info=True)
        return potential_actions_count

    def _perform_initial_parsing(self, file_batches: Dict[str, Dict[str, Any]], batch_count: int) -> Dict[str, Optional[MediaInfo]]:
        initial_media_infos: Dict[str, Optional[MediaInfo]] = {}
        log.info("Phase 1: Performing initial file parsing...")
        disable_rich_progress = getattr(self.args, 'quiet', False) or getattr(self.args, 'interactive', False) or not RICH_AVAILABLE
        
        with ProgressClass(*DEFAULT_PROGRESS_COLUMNS, console=self.console, disable=disable_rich_progress) as progress:
            parse_task: TaskIDClass = progress.add_task("Parsing Filenames", total=batch_count, item_name="")
            for stem, batch_data in file_batches.items():
                video_path_obj = batch_data.get('video')
                item_name_short = Path(video_path_obj if video_path_obj else stem).name[:30] + \
                                  ("..." if len(Path(video_path_obj if video_path_obj else stem).name) > 30 else "")
                progress.update(parse_task, advance=1, item_name=item_name_short)
                
                if not video_path_obj:
                    initial_media_infos[stem] = None
                    continue
                
                media_info_obj = MediaInfo(original_path=cast(Path, video_path_obj))
                try:
                    media_info_obj.guess_info = self.renamer.parse_filename(media_info_obj.original_path)
                    media_info_obj.file_type = self.renamer._determine_file_type(media_info_obj.guess_info)
                    initial_media_infos[stem] = media_info_obj
                except Exception as e_parse:
                    log.error(f"Error parsing '{stem}': {e_parse}", exc_info=True)
                    initial_media_infos[stem] = None 
        return initial_media_infos

    async def _fetch_all_metadata( self, file_batches: Dict[str, Dict[str, Any]], initial_media_infos: Dict[str, Optional[MediaInfo]] ) -> Dict[str, Optional[MediaInfo]]:
        use_metadata_effective = getattr(self.args, 'use_metadata', False)

        if not (use_metadata_effective and self.metadata_fetcher): 
            log.info("Metadata fetching disabled or fetcher not available. Skipping metadata phase.")
            if use_metadata_effective and not self.metadata_fetcher:
                 self.console.print("[yellow]Warning: Metadata fetching was enabled (or ID forced) but API clients are not available. Proceeding with filename parsing data only.[/yellow]")
            elif not use_metadata_effective:
                 self.console.print("[yellow]Metadata fetching is disabled. Proceeding with filename parsing data only.[/yellow]")
            return initial_media_infos
       
        stems_to_fetch = [ stem for stem, info in initial_media_infos.items() 
                           if info and (info.file_type != 'unknown' or \
                                        getattr(self.args, 'tmdb_id', None) is not None or \
                                        getattr(self.args, 'tvdb_id', None) is not None) ]
        
        log.info(f"Phase 2: Creating {len(stems_to_fetch)} tasks for concurrent metadata fetching...")
        if not stems_to_fetch:
            log.info("No batches required metadata fetching.")
            return initial_media_infos

        fetch_tasks: List[asyncio.Task[Tuple[str, MediaInfo]]] = []
        disable_rich_progress = getattr(self.args, 'quiet', False) or getattr(self.args, 'interactive', False) or not RICH_AVAILABLE
        
        with ProgressClass(*DEFAULT_PROGRESS_COLUMNS, console=self.console, disable=disable_rich_progress) as progress_bar:
            metadata_overall_task: TaskIDClass = progress_bar.add_task("Fetching Metadata", total=len(stems_to_fetch), item_name="")
            for stem in stems_to_fetch:
                batch_data = file_batches[stem]
                task = asyncio.create_task(
                    _fetch_metadata_for_batch(self, stem, batch_data, progress_bar, metadata_overall_task), 
                    name=f"fetch_{stem}"
                )
                fetch_tasks.append(task)
            
            completed_fetch_results_tuples: List[Tuple[str, MediaInfo]] = []
            try:
                for f_task_completed in asyncio.as_completed(fetch_tasks):
                    completed_fetch_results_tuples.append(await f_task_completed)
            except Exception as e_async_task_collection:
                log.error(f"Error collecting results from async metadata tasks: {e_async_task_collection}")

            if hasattr(progress_bar, 'tasks') and progress_bar.tasks: 
                task_obj = None
                if isinstance(progress_bar.tasks, list):
                    safe_task_id = int(metadata_overall_task) if isinstance(metadata_overall_task, (int, float)) or (isinstance(metadata_overall_task, str) and metadata_overall_task.isdigit()) else -1
                    if 0 <= safe_task_id < len(progress_bar.tasks): task_obj = progress_bar.tasks[safe_task_id]
                elif isinstance(progress_bar.tasks, dict):
                    task_obj = progress_bar.tasks.get(metadata_overall_task)

                if task_obj and not task_obj.finished: 
                    progress_bar.update(metadata_overall_task, completed=len(stems_to_fetch), item_name="") 
        
        for result_item in completed_fetch_results_tuples:
            if isinstance(result_item, tuple) and len(result_item) == 2:
                stem_from_task, updated_media_info_obj = result_item
                if updated_media_info_obj:
                    initial_media_infos[stem_from_task] = updated_media_info_obj
                else: 
                    log.error(f"Async task for {stem_from_task} returned None for MediaInfo object")
                    original_path_fallback = file_batches.get(stem_from_task, {}).get('video', Path(f"error_dummy_{stem_from_task}.file"))
                    mi_fallback = MediaInfo(original_path=cast(Path, original_path_fallback))
                    mi_fallback.metadata_error_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Async task returned invalid data"
                    mi_fallback.file_type = 'unknown'
                    initial_media_infos[stem_from_task] = mi_fallback
        return initial_media_infos
    
    async def _get_user_confirmation_in_executor(self, prompt_text: str, default_val: bool, choices_list: Optional[List[str]] = None, is_confirm_type: bool = True) -> Any:
        loop = asyncio.get_running_loop()
        def do_prompt_sync():
            prompt_target_console: Optional[ConsoleClass] = None
            is_main_progress_likely_active = not getattr(self.args, 'interactive', False) and \
                                             not getattr(self.args, 'quiet', False)
            
            current_prompt_ui_class = ConfirmClass if is_confirm_type else PromptClass
            is_rich_prompt_class_in_use = False
            if RICH_AVAILABLE:
                if is_confirm_type and current_prompt_ui_class is RichConfirm: 
                    is_rich_prompt_class_in_use = True
                elif not is_confirm_type and current_prompt_ui_class is RichPrompt: 
                    is_rich_prompt_class_in_use = True

            if is_rich_prompt_class_in_use and is_main_progress_likely_active:
                try:
                    width_arg = getattr(self.console, 'width', None) 
                    prompt_target_console = RichConsoleActual(
                        file=sys.stderr, force_terminal=sys.stderr.isatty(), width=width_arg
                    ) 
                    log.debug(f"Using dedicated Rich stderr console for prompt: '{prompt_text}'")
                except Exception as e_stderr_console:
                    log.warning(f"Could not create dedicated Rich stderr console for prompt, falling back: {e_stderr_console}")
                    prompt_target_console = None 
            
            effective_console_arg = {'console': prompt_target_console} if prompt_target_console else {}

            try:
                if is_confirm_type:
                    return ConfirmClass.ask(prompt_text, default=default_val, **effective_console_arg)
                else:
                    return PromptClass.ask(prompt_text, choices=choices_list, **effective_console_arg) 
            except Exception as e_prompt:
                log.error(f"Error during sync prompt execution with {type(current_prompt_ui_class).__name__}: {e_prompt}. Falling back to basic input.", exc_info=True)
                prompt_suffix = ""
                if is_confirm_type: prompt_suffix = f" [{'Y/n' if default_val else 'y/N'}]"
                elif choices_list: prompt_suffix = f" (choices: {', '.join(choices_list)})"
                
                builtins.print(f"{prompt_text}{prompt_suffix}: ", end="", file=sys.stderr); sys.stderr.flush()
                response_str = sys.stdin.readline().strip() 
                
                if choices_list: 
                    if response_str in choices_list: return response_str
                    return response_str if response_str else (choices_list[0] if choices_list and response_str == "" else "")
                
                response_lower = response_str.lower()
                if not response_str: return default_val 
                return response_lower == 'y' or response_lower == 'yes'
        return await loop.run_in_executor(None, do_prompt_sync)

    async def _process_single_batch_confirmations(
        self,
        stem: str,
        media_info: MediaInfo,
    ) -> Tuple[bool, bool]: 
        user_quit_flag = False
        metadata_rejected_flag = False
        is_live_run = getattr(self.args, 'live', False)

        if (media_info.file_type == 'movie' and
            media_info.metadata and media_info.metadata.movie_title and
            media_info.metadata.match_confidence == -1.0 and 
            self.cfg('movie_yearless_match_confidence', 'medium') == 'confirm'):

            can_prompt_yearless = not getattr(self.args, 'quiet', False) and \
                                  (self.args.interactive or not is_live_run or ConfirmClass is not None)

            if can_prompt_yearless:
                self.console.print(PanelClass(
                    TextClass.assemble(
                        TextClass("A movie match was found without using a year from the filename.\n\n", style="yellow"),
                        TextClass("Original Filename: ", style="bold"), TextClass(f"{media_info.original_path.name}\n"),
                        TextClass("Guessed Title:     ", style="bold"), TextClass(f"{media_info.guess_info.get('title', media_info.original_path.stem)}\n"),
                        TextClass("API Found Title:   ", style="bold"), TextClass(f"{media_info.metadata.movie_title}\n", style="cyan"),
                        TextClass("API Found Year:    ", style="bold"), TextClass(f"{media_info.metadata.movie_year or 'N/A'}\n", style="cyan"),
                        TextClass("\nPlease confirm if this is the correct match.", style="yellow")
                    ), title="[bold yellow]Yearless Movie Match Confirmation[/bold yellow]", border_style="yellow", expand=False
                ))
                confirm_prompt_text = "Is this the correct movie match?"
                try:
                    user_confirmed = await self._get_user_confirmation_in_executor(confirm_prompt_text, default_val=False, is_confirm_type=True)
                    if user_confirmed:
                        self.console.print("[green]✓ Yearless match confirmed by user.[/green]")
                        log.info(f"User confirmed yearless match for '{media_info.original_path.name}'.")
                        if media_info.metadata: media_info.metadata.match_confidence = None
                    else:
                        self.console.print("[yellow]✗ Yearless match rejected by user (or default 'No' taken).[/yellow]")
                        log.info(f"User REJECTED yearless match for '{media_info.original_path.name}' (or default 'No' taken).")
                        if media_info.metadata:
                            media_info.metadata_error_message = f"[{ProcessingStatus.USER_INTERACTIVE_SKIP}] User rejected yearless match confirmation."
                            media_info.metadata = None; metadata_rejected_flag = True
                except (EOFError, KeyboardInterrupt):
                    self.console.print("[yellow]✗ Yearless match confirmation aborted.[/yellow]")
                    log.warning(f"Yearless match confirmation aborted for '{media_info.original_path.name}'.")
                    if media_info.metadata:
                        media_info.metadata_error_message = f"[{ProcessingStatus.USER_ABORTED_OPERATION}] Confirmation aborted."; media_info.metadata = None; metadata_rejected_flag = True
                    user_quit_flag = True
                except Exception as e_confirm:
                    self.console.print(f"[red]Error during yearless confirmation prompt: {e_confirm}[/red]")
                    log.error(f"Error during yearless confirm prompt: {e_confirm}", exc_info=True)
                    if media_info.metadata:
                        media_info.metadata_error_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Error during yearless confirmation."; media_info.metadata = None; metadata_rejected_flag = True
                self.console.print("-" * 30)
            else: 
                log.warning(f"Yearless match for '{media_info.original_path.name}' requires confirmation but cannot prompt. Rejecting.")
                if media_info.metadata:
                    media_info.metadata_error_message = f"[{ProcessingStatus.METADATA_NO_MATCH}] Yearless match rejected (non-interactive confirm)."; media_info.metadata = None; metadata_rejected_flag = True
        
        if media_info.metadata and media_info.metadata.match_confidence == -1.0: 
            media_info.metadata.match_confidence = None
        
        if user_quit_flag or metadata_rejected_flag: return user_quit_flag, metadata_rejected_flag

        confirm_match_below_threshold = self.cfg('confirm_match_below')
        if (media_info.metadata and
            media_info.metadata.match_confidence is not None and 
            confirm_match_below_threshold is not None and
            media_info.metadata.match_confidence < confirm_match_below_threshold):
            
            can_prompt_low_score = not getattr(self.args, 'quiet', False) and \
                                   (self.args.interactive or not is_live_run or ConfirmClass is not None)

            if can_prompt_low_score:
                self.console.print(PanelClass(
                     TextClass.assemble(
                        TextClass(f"The match confidence score for this item is {media_info.metadata.match_confidence:.1f}%, "
                                  f"which is below your threshold of {confirm_match_below_threshold}%.\n\n", style="yellow"),
                        TextClass("Original Filename: ", style="bold"), TextClass(f"{media_info.original_path.name}\n"),
                        TextClass("API Found:         ", style="bold"), 
                        TextClass(f"{media_info.metadata.movie_title or media_info.metadata.show_title} "
                                  f"({media_info.metadata.movie_year or media_info.metadata.show_year or 'N/A'})\n", style="cyan"),
                        TextClass("\nPlease confirm if this is the correct match.", style="yellow")
                    ), title="[bold yellow]Low Confidence Match Confirmation[/bold yellow]", border_style="yellow", expand=False
                ))
                confirm_prompt_text = "Is this low-confidence match correct?"
                try:
                    user_confirmed = await self._get_user_confirmation_in_executor(confirm_prompt_text, default_val=False, is_confirm_type=True)
                    if user_confirmed:
                        self.console.print("[green]✓ Low-confidence match accepted by user.[/green]")
                        log.info(f"User accepted low-confidence match for '{media_info.original_path.name}'.")
                    else:
                        self.console.print("[yellow]✗ Low-confidence match rejected by user.[/yellow]")
                        log.info(f"User REJECTED low-confidence match for '{media_info.original_path.name}'.")
                        media_info.metadata_error_message = f"[{ProcessingStatus.USER_INTERACTIVE_SKIP}] User rejected low-confidence match."
                        media_info.metadata = None; metadata_rejected_flag = True
                except (EOFError, KeyboardInterrupt):
                    self.console.print("[yellow]✗ Low-confidence match confirmation aborted.[/yellow]")
                    log.warning(f"Low-confidence match confirmation aborted for {media_info.original_path.name}.")
                    media_info.metadata_error_message = f"[{ProcessingStatus.USER_ABORTED_OPERATION}] Confirmation aborted."
                    media_info.metadata = None; metadata_rejected_flag = True; user_quit_flag = True
                except Exception as e_confirm_low:
                    self.console.print(f"[red]Error during low-confidence confirmation: {e_confirm_low}[/red]")
                    log.error(f"Error during low-confidence confirm: {e_confirm_low}", exc_info=True)
                    media_info.metadata_error_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Error during low-confidence confirmation."
                    media_info.metadata = None; metadata_rejected_flag = True
                self.console.print("-" * 30)
            else: 
                log.warning(f"Low-confidence match for '{media_info.original_path.name}' requires conf but cannot prompt. Rejecting.")
                media_info.metadata_error_message = f"[{ProcessingStatus.METADATA_NO_MATCH}] Low-confidence match rejected (non-interactive)."
                media_info.metadata = None; metadata_rejected_flag = True
                
        return user_quit_flag, metadata_rejected_flag

    async def _process_single_batch(
        self,
        stem: str,
        batch_data: Dict[str, Any],
        media_info: MediaInfo, 
        run_batch_id: str,
        is_live_run: bool
    ) -> Tuple[Dict[str, Any], bool, bool]:
        action_result: Dict[str, Any] = {'success': True, 'message': '', 'actions_taken': 0}
        user_quit_flag = False 
        plan: Optional[RenamePlan] = None
        final_batch_processing_error_occurred = False 
        video_file_path = cast(Path, batch_data.get('video'))
        use_metadata_effectively_on = getattr(self.args, 'use_metadata', False) 
        
        specific_metadata_issue_message: Optional[str] = None
        if media_info.metadata_error_message:
            if "FORCED_TMDB_ID_NOT_FOUND::" in media_info.metadata_error_message:
                try: specific_metadata_issue_message = f"Provided TMDB ID '{media_info.metadata_error_message.split('::')[1]}' was not found."
                except: specific_metadata_issue_message = media_info.metadata_error_message 
            elif "FORCED_TVDB_ID_NOT_FOUND::" in media_info.metadata_error_message:
                try: specific_metadata_issue_message = f"Provided TVDB ID '{media_info.metadata_error_message.split('::')[1]}' was not found."
                except: specific_metadata_issue_message = media_info.metadata_error_message 
            else: 
                specific_metadata_issue_message = media_info.metadata_error_message
            
            if specific_metadata_issue_message != media_info.metadata_error_message: 
                log.info(f"Refined metadata error for batch '{stem}' to: {specific_metadata_issue_message}")
            # Update media_info with the refined message for consistency downstream
            media_info.metadata_error_message = specific_metadata_issue_message
        
        metadata_failed_or_rejected = use_metadata_effectively_on and \
                                      (bool(media_info.metadata_error_message) or media_info.metadata is None)
        
        proceed_with_normal_planning: bool = True 

        if metadata_failed_or_rejected:
            is_user_skip_or_abort = media_info.metadata_error_message and \
                                   (ProcessingStatus.USER_INTERACTIVE_SKIP.name in media_info.metadata_error_message or \
                                    ProcessingStatus.USER_ABORTED_OPERATION.name in media_info.metadata_error_message)
            
            panel_error_message_content = media_info.metadata_error_message or \
                                           f"[{ProcessingStatus.METADATA_FETCH_API_ERROR}] Metadata error for '{video_file_path.name}' (unknown details)."

            if not is_user_skip_or_abort: 
                if not getattr(self.args, 'quiet', False) and not self.args.interactive: 
                    self.console.print(PanelClass(
                        f"[bold red]API/Metadata Error:[/bold red] {panel_error_message_content}", 
                        title=f"[yellow]'{media_info.original_path.name}'[/yellow]", 
                        border_style="red"
                    ))
        
        unknown_handling_mode = self.cfg('unknown_file_handling', 'skip', arg_value=getattr(self.args, 'unknown_file_handling', None))

        if media_info.file_type == 'unknown' or metadata_failed_or_rejected:
            if media_info.file_type == 'unknown': handling_reason = "unknown file type"
            elif media_info.metadata_error_message and (ProcessingStatus.USER_INTERACTIVE_SKIP.name in media_info.metadata_error_message or ProcessingStatus.USER_ABORTED_OPERATION.name in media_info.metadata_error_message):
                handling_reason = "metadata match rejected/aborted by user"
            else: handling_reason = "metadata fetch failed"

            log.info(f"Batch '{stem}' (type: {media_info.file_type}) handled via '{unknown_handling_mode}' due to: {handling_reason}.")
            
            message_for_this_outcome = media_info.metadata_error_message or \
                                       f"[{ProcessingStatus.METADATA_NO_MATCH}] No metadata due to {handling_reason}."

            if unknown_handling_mode == 'skip':
                action_result['message'] = message_for_this_outcome
                action_result['success'] = True; final_batch_processing_error_occurred = False 
                proceed_with_normal_planning = False 
            elif unknown_handling_mode == 'move_to_unknown':
                move_result = self._handle_move_to_unknown(stem, batch_data, run_batch_id)
                reason_prefix = f"{message_for_this_outcome}. "
                action_result['message'] = reason_prefix + move_result.get('message', "Move to unknown attempted.")
                action_result['actions_taken'] = move_result.get('actions_taken',0)
                action_result['success'] = move_result.get('move_success', False)
                final_batch_processing_error_occurred = not action_result['success'] 
                proceed_with_normal_planning = False 
            elif unknown_handling_mode == 'guessit_only':
                log.debug(f"Proceeding with guessit_only planning for '{stem}' due to {handling_reason}.")
                media_info.metadata = None; media_info.metadata_error_message = None 
                proceed_with_normal_planning = True; final_batch_processing_error_occurred = False 
                action_result['message'] = f"Using Guessit-only for '{stem}' due to: {handling_reason}." 
            else: 
                action_result['message'] = message_for_this_outcome
                final_batch_processing_error_occurred = True
                proceed_with_normal_planning = False 
            
            if not proceed_with_normal_planning:
                 return action_result, final_batch_processing_error_occurred, user_quit_flag
        
        is_skip_or_correct_batch_plan: bool = False
        try:
            if not proceed_with_normal_planning:
                action_result['message'] = media_info.metadata_error_message or f"[{ProcessingStatus.INTERNAL_ERROR}] Logic error for {stem}."
                final_batch_processing_error_occurred = True
                return action_result, final_batch_processing_error_occurred, user_quit_flag

            plan = self.renamer.plan_rename(video_file_path, batch_data.get('associated', []), media_info)
            user_choice_for_action = 'y' 
            current_plan_for_interaction = plan 

            is_interactive_prompt_allowed = self.args.interactive and not getattr(self.args, 'quiet', False)
            
            if is_interactive_prompt_allowed and is_live_run: 
                initial_plan_prompt_message = f"Initial plan for '{stem}'"
                if current_plan_for_interaction:
                    initial_plan_prompt_message += f" (Status: {current_plan_for_interaction.status}, Message: {current_plan_for_interaction.message or 'N/A'})"
                else: initial_plan_prompt_message += " (No plan generated)"
                log.debug(initial_plan_prompt_message)

                while True: 
                    if current_plan_for_interaction and current_plan_for_interaction.status in ['success', 'conflict_unresolved']:
                        self._display_plan_for_confirmation(current_plan_for_interaction, media_info) 
                    elif current_plan_for_interaction: 
                        self.console.print(f"[yellow]Current plan for '{stem}': {current_plan_for_interaction.message or current_plan_for_interaction.status}[/yellow]")
                    else: self.console.print(f"[red]No valid plan generated for '{stem}' to confirm or act upon.[/red]")

                    try:
                        choice_prompt_text = "Action for this batch? ([y]es to apply current plan, [n]o/[s]kip, [q]uit"
                        available_choices_list = ["y", "n", "s", "q"]
                        if getattr(self.args, 'use_metadata', False) and self.metadata_fetcher:
                             choice_prompt_text += ", [g]uessit only, [m]anual search"
                             available_choices_list.extend(["g", "m"])
                        choice_prompt_text += ")"
                        
                        choice_obj = await self._get_user_confirmation_in_executor(choice_prompt_text, default_val=False, choices_list=available_choices_list, is_confirm_type=False)
                        choice = str(choice_obj).lower() 
                        
                        if choice == 'y':
                            if current_plan_for_interaction and current_plan_for_interaction.status == 'success':
                                user_choice_for_action = 'y'; break
                            else: self.console.print("[yellow]No valid plan to apply. Choose another option or skip.[/yellow]"); continue 
                        elif choice in ('n', 's'): user_choice_for_action = 's'; break
                        elif choice == 'q': user_quit_flag = True; raise UserAbortError(f"[{ProcessingStatus.USER_ABORTED_OPERATION}] User quit.")
                        elif choice == 'g' and 'g' in available_choices_list:
                            self.console.print("[cyan]Re-planning using Guessit data only...[/cyan]")
                            media_info.metadata = None; media_info.metadata_error_message = None 
                            current_plan_for_interaction = self.renamer.plan_rename(media_info.original_path, batch_data.get('associated', []), media_info)
                        elif choice == 'm' and 'm' in available_choices_list:
                            self.console.print("[cyan]Manual metadata search/selection...[/cyan]")
                            api_src_choices = ['tmdb']
                            if media_info.file_type == 'series': api_src_choices.append('tvdb') 
                            api_source_prompt_text = "Search with [t]mdb" 
                            if 'tvdb' in api_src_choices: api_source_prompt_text += " or t[v]db"; api_source_prompt_text += "?"
                            api_choice_map = {'t': 'tmdb'}; 
                            if 'tvdb' in api_src_choices: api_choice_map['v'] = 'tvdb'
                            api_choice_key_obj = await self._get_user_confirmation_in_executor(api_source_prompt_text, default_val=False, choices_list=list(api_choice_map.keys()), is_confirm_type=False)
                            api_source_to_search = api_choice_map.get(str(api_choice_key_obj).lower())
                            if not api_source_to_search: self.console.print("[yellow]Invalid API source. Returning to options.[/yellow]"); continue
                            guessed_title_for_search = media_info.guess_info.get('title', media_info.original_path.stem)
                            self.console.print(f"Searching {api_source_to_search.upper()} for: \"{guessed_title_for_search}\"...")
                            search_results: List[Dict[str, Any]] = []
                            if self.metadata_fetcher:
                                if api_source_to_search == 'tmdb':
                                    if media_info.file_type == 'movie': search_results = await self.metadata_fetcher.search_tmdb_movies_interactive(guessed_title_for_search)
                                    elif media_info.file_type == 'series': search_results = await self.metadata_fetcher.search_tmdb_series_interactive(guessed_title_for_search)
                                elif api_source_to_search == 'tvdb' and media_info.file_type == 'series': search_results = await self.metadata_fetcher.search_tvdb_series_interactive(guessed_title_for_search)
                            if not search_results: self.console.print(f"[yellow]No results found on {api_source_to_search.upper()} for \"{guessed_title_for_search}\".[/yellow]"); continue
                            self.console.print("Search Results:"); result_choices_map: Dict[str, int] = {}; display_choices_for_prompt: List[str] = []
                            for i, res in enumerate(search_results[:7], 1): 
                                choice_key = str(i); display_choices_for_prompt.append(choice_key); result_id = res.get('id')
                                if result_id is None: continue; result_choices_map[choice_key] = int(result_id)
                                self.console.print(f"  [cyan]{choice_key}[/cyan]: {res.get('text', 'N/A')} [dim](ID: {result_id})[/dim]")
                            display_choices_for_prompt.append("0"); self.console.print("  [cyan]0[/cyan]: None of these / Skip")
                            selected_choice_key_obj = await self._get_user_confirmation_in_executor("Select correct match (number) or 0 to skip:", default_val=False, choices_list=display_choices_for_prompt, is_confirm_type=False)
                            selected_choice_key = str(selected_choice_key_obj)
                            if selected_choice_key == "0" or not selected_choice_key: self.console.print("[yellow]Skipping manual selection.[/yellow]"); continue
                            if selected_choice_key not in result_choices_map: self.console.print("[red]Invalid selection number.[/red]"); continue
                            selected_id = result_choices_map[selected_choice_key]
                            new_metadata = await self._refetch_with_manual_id(media_info, api_source_to_search, selected_id, is_interactive_refetch=True)
                            if new_metadata:
                                media_info.metadata = new_metadata; media_info.metadata_error_message = None 
                                quit_after_refetch_confirm, rejected_after_refetch_confirm = await self._process_single_batch_confirmations(stem, media_info)
                                if quit_after_refetch_confirm: user_quit_flag = True; break 
                                if rejected_after_refetch_confirm: self.console.print("[yellow]Metadata from manual selection was subsequently rejected by confirmation rules.[/yellow]")
                                current_plan_for_interaction = self.renamer.plan_rename(media_info.original_path, batch_data.get('associated', []), media_info)
                            else: self.console.print(f"[red]Manual ID selection ({api_source_to_search.upper()} ID {selected_id}) failed to fetch details. Keeping previous state.[/red]")
                        else: self.console.print("[red]Invalid choice. Please try again.[/red]")
                    except (EOFError, KeyboardInterrupt) as e_int_abort: user_quit_flag = True; raise UserAbortError(f"[{ProcessingStatus.USER_ABORTED_OPERATION}] User quit ({type(e_int_abort).__name__})") from e_int_abort
                    except InvalidResponseClass: self.console.print("[red]Invalid choice.[/red]") 
                    except Exception as e_prompt_loop: 
                        log.error(f"Error in interactive prompt loop: {e_prompt_loop}", exc_info=True)
                        self.console.print(f"[red]An error occurred: {e_prompt_loop}. Skipping batch.[/red]")
                        user_choice_for_action = 's'; break 
            
            if user_quit_flag: 
                action_result['message'] = media_info.metadata_error_message or action_result.get('message') or f"[{ProcessingStatus.USER_ABORTED_OPERATION}] User quit."
                return action_result, True, True

            final_plan_to_execute = current_plan_for_interaction             

            if user_choice_for_action == 's': 
                action_result['success'] = True 
                action_result['message'] = media_info.metadata_error_message if metadata_failed_or_rejected and media_info.metadata_error_message else \
                                           f"[{ProcessingStatus.USER_INTERACTIVE_SKIP}] User skipped batch '{stem}'."
                log.info(action_result['message']); final_batch_processing_error_occurred = False
                is_skip_or_correct_batch_plan = True 
            elif final_plan_to_execute and final_plan_to_execute.status == 'success':
                action_result = perform_file_actions( plan=final_plan_to_execute, args_ns=self.args, cfg_helper=self.cfg, undo_manager=self.undo_manager, run_batch_id=run_batch_id, media_info=media_info, quiet_mode=getattr(self.args, 'quiet', False) )
                if media_info.metadata_error_message and action_result.get('success') and unknown_handling_mode == 'guessit_only' and metadata_failed_or_rejected:
                    action_result['message'] = f"(Original issue: '{media_info.metadata_error_message}') -> {action_result.get('message', 'Actions performed via Guessit.')}"
                final_batch_processing_error_occurred = not action_result.get('success', False)
            elif final_plan_to_execute and final_plan_to_execute.message: 
                final_msg = final_plan_to_execute.message
                if media_info.metadata_error_message and metadata_failed_or_rejected : final_msg = f"({media_info.metadata_error_message}) -> Plan status: {final_msg}"
                action_result['message'] = final_msg
                is_skip_or_correct_batch_plan = ( final_plan_to_execute.status == 'skipped' or ProcessingStatus.PATH_ALREADY_CORRECT.name in final_plan_to_execute.message or ProcessingStatus.PLAN_TARGET_EXISTS_SKIP_MODE.name in final_plan_to_execute.message )
                if is_skip_or_correct_batch_plan: action_result['success'] = True; final_batch_processing_error_occurred = False
                else: action_result['success'] = False; final_batch_processing_error_occurred = True
            elif not final_plan_to_execute: 
                 action_result['message'] = media_info.metadata_error_message or f"[{ProcessingStatus.INTERNAL_ERROR}] No plan generated for '{stem}'."
                 final_batch_processing_error_occurred = True
            else: 
                action_result['message'] = media_info.metadata_error_message or f"[{ProcessingStatus.SKIPPED}] Undetermined outcome for '{stem}'."
                final_batch_processing_error_occurred = True 
        except UserAbortError as e_abort:
            log.warning(str(e_abort)); self.console.print(f"\n{e_abort}", file=sys.stderr)
            action_result['message'] = str(e_abort); final_batch_processing_error_occurred = True; user_quit_flag = True
        except FileExistsError as e_fe: 
            log.critical(str(e_fe)); self.console.print(f"\n[bold red]STOPPING: {e_fe}[/bold red]", file=sys.stderr)
            action_result['message'] = f"[{ProcessingStatus.PLAN_TARGET_EXISTS_FAIL_MODE}] {e_fe}"; final_batch_processing_error_occurred = True; user_quit_flag = True
        except RenamerError as e_rename: 
            log.error(f"RenamerError processing batch '{stem}': {e_rename}", exc_info=False)
            action_result['message'] = str(e_rename); final_batch_processing_error_occurred = True
        except Exception as e_crit: 
            log.exception(f"Critical unhandled error processing batch '{stem}': {e_crit}")
            action_result['message'] = f"[{ProcessingStatus.INTERNAL_ERROR}] Critical error processing batch '{stem}'. Details: {type(e_crit).__name__}: {str(e_crit).splitlines()[0]}"
            from rename_main import print_stderr_message 
            print_stderr_message(self.console, TextClass(f"[bold red]CRITICAL ERROR processing batch {stem}. See log.[/bold red]", style="bold red"), getattr(self.args,'quiet',False), RICH_AVAILABLE)
            final_batch_processing_error_occurred = True
        
        return action_result, final_batch_processing_error_occurred, user_quit_flag

    async def run_processing(self):
        # ... (This method remains structurally the same as the last full version provided)
        target_dir = self.args.directory.resolve()
        if not target_dir.is_dir():
            msg = f"[{ProcessingStatus.INTERNAL_ERROR}] Target directory not found or is not a directory: {target_dir}"
            log.critical(msg)
            from rename_main import print_stderr_message 
            print_stderr_message(self.console, TextClass(f"[bold red]Error: {msg}[/]", style="bold red"), getattr(self.args, 'quiet', False), RICH_AVAILABLE)
            return

        use_metadata_globally = getattr(self.args, 'use_metadata', False)
                                 
        if use_metadata_globally and not self.metadata_fetcher:
            msg = f"[{ProcessingStatus.METADATA_CLIENT_UNAVAILABLE}] Metadata processing enabled, but FAILED to initialize API clients."
            log.critical(msg)
            from rename_main import print_stderr_message 
            print_stderr_message(self.console, TextClass(f"\n[bold red]CRITICAL ERROR: {msg}[/]", style="bold red"), getattr(self.args, 'quiet', False), RICH_AVAILABLE)
            return
        
        log.info("Phase 1: Collecting and Parsing Batches...")
        file_batches = {stem: data for stem, data in scan_media_files(target_dir, self.cfg)}
        batch_count = len(file_batches)
        log.info(f"Collected {batch_count} batches.")

        if batch_count == 0:
             log.warning(f"[{ProcessingStatus.SKIPPED}] No valid video files/batches found matching criteria.")
             self.console.print(TextClass(f"[yellow][{ProcessingStatus.SKIPPED}] No valid video files/batches found.[/yellow]", style="yellow"))
             return

        initial_media_infos = self._perform_initial_parsing(file_batches, batch_count)
        
        initial_media_infos = await self._fetch_all_metadata(file_batches, initial_media_infos)
        
        log.info("Phase 2.5: Handling Metadata Confirmations...")
        user_quit_during_meta_confirm = False
        items_for_meta_confirmation_phase: Deque[Tuple[str, MediaInfo]] = deque()

        if use_metadata_globally and not getattr(self.args, 'quiet', False) : 
            for stem, mi in initial_media_infos.items():
                if mi and mi.metadata: 
                    is_yearless_confirm_needed = (
                        mi.file_type == 'movie' and
                        mi.metadata.match_confidence == -1.0 and 
                        self.cfg('movie_yearless_match_confidence', 'medium') == 'confirm'
                    )
                    confirm_match_below_threshold = self.cfg('confirm_match_below')
                    is_low_score_confirm_needed = (
                        mi.metadata.match_confidence is not None and mi.metadata.match_confidence != -1.0 and
                        confirm_match_below_threshold is not None and
                        mi.metadata.match_confidence < confirm_match_below_threshold
                    )
                    if is_yearless_confirm_needed or is_low_score_confirm_needed:
                        items_for_meta_confirmation_phase.append((stem, mi))
            
            if items_for_meta_confirmation_phase:
                self.console.print("\n--- Metadata Confirmation Phase ---")
                for stem_mc, media_info_mc in list(items_for_meta_confirmation_phase): 
                    self.console.rule(f"Metadata review for: [cyan]{media_info_mc.original_path.name}[/cyan]", style="dim")
                    quit_flag, _ = await self._process_single_batch_confirmations(stem_mc, media_info_mc)
                    if quit_flag:
                        user_quit_during_meta_confirm = True; break
                self.console.print("--- End Metadata Confirmation Phase ---\n")

        if user_quit_during_meta_confirm:
            self.console.print("[yellow]Operation aborted by user during metadata confirmation.[/yellow]")
            return

        is_live_run = getattr(self.args, 'live', False)
        if is_live_run:
            log.info("Phase 3: Performing pre-scan for live run final confirmation...")
            potential_actions_count = self._perform_prescan(file_batches, batch_count, initial_media_infos)
            if not self._confirm_live_run(potential_actions_count):
                return
        
        run_batch_id = f"run-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        log.info(f"Phase 4: Starting planning and execution run ID: {run_batch_id}")

        results_summary = {
            'success_renames_moves': 0, 'skipped_correct_or_conflict': 0, 'error_batches': 0,
            'actions_taken': 0, 'moved_unknown_files': 0, 
            'user_skipped_batches': 0, 'config_skipped_batches': 0
        }
        self.console.print("-" * 30) 
        total_planned_actions_accumulator_for_dry_run = 0
        
        disable_final_progress = getattr(self.args, 'quiet', False) or getattr(self.args, 'interactive', False) or not RICH_AVAILABLE
        with ProgressClass(*DEFAULT_PROGRESS_COLUMNS, console=self.console, disable=disable_final_progress) as final_progress_bar:
            main_processing_task: TaskIDClass = final_progress_bar.add_task("Planning/Executing", total=batch_count, item_name="")

            for stem, batch_data in file_batches.items():
                item_name_short = Path(batch_data.get('video', stem)).name[:30] + "..."
                final_progress_bar.update(main_processing_task, advance=1, item_name=f"Processing: {item_name_short}")

                media_info = initial_media_infos.get(stem)
                if not media_info: 
                    log.error(f"[{ProcessingStatus.INTERNAL_ERROR}] CRITICAL: Skipping batch '{stem}' due to missing MediaInfo object before final processing.")
                    results_summary['error_batches'] += 1
                    continue
                
                log_base_info = f"Final Processing Batch '{stem}': Type='{media_info.file_type}', API='{getattr(media_info.metadata, 'source_api', 'N/A')}', Score='{getattr(media_info.metadata, 'match_confidence', 'N/A')}'"
                if media_info.metadata_error_message: 
                    log_base_info += f", MetaError='{media_info.metadata_error_message}'"
                log.debug(log_base_info)

                action_result, final_batch_had_error_flag, user_quit_processing = await self._process_single_batch(
                    stem, batch_data, media_info, run_batch_id, is_live_run
                )
                
                batch_msg_from_action = action_result.get('message', f"[{ProcessingStatus.INTERNAL_ERROR}] No message from batch processing for '{stem}'.")
                primary_reason_for_log_and_console = batch_msg_from_action

                if final_batch_had_error_flag and not action_result.get('success'):
                    # If metadata_error_message is present and more specific than the batch_msg_from_action (unless batch_msg is an internal error)
                    if media_info.metadata_error_message and \
                       ProcessingStatus.INTERNAL_ERROR.name not in batch_msg_from_action and \
                       not (ProcessingStatus.USER_INTERACTIVE_SKIP.name in media_info.metadata_error_message or \
                            ProcessingStatus.USER_ABORTED_OPERATION.name in media_info.metadata_error_message):
                        # Construct user-facing version of metadata error if it was an internal signal
                        refined_metadata_error = media_info.metadata_error_message
                        if "FORCED_TMDB_ID_NOT_FOUND::" in media_info.metadata_error_message:
                            try: refined_metadata_error = f"Provided TMDB ID '{media_info.metadata_error_message.split('::')[1]}' was not found."
                            except: pass
                        elif "FORCED_TVDB_ID_NOT_FOUND::" in media_info.metadata_error_message:
                            try: refined_metadata_error = f"Provided TVDB ID '{media_info.metadata_error_message.split('::')[1]}' was not found."
                            except: pass
                        primary_reason_for_log_and_console = f"{refined_metadata_error} (Handling also failed: {batch_msg_from_action})"
                    # else, batch_msg_from_action is already the primary reason

                if action_result.get('success', False) and not final_batch_had_error_flag:
                    if f"[{ProcessingStatus.SUCCESS.name}] MOVED (UNKNOWN)" in batch_msg_from_action.upper():
                        log.info(f"MOVED_TO_UNKNOWN: Batch '{stem}'. Actions: {action_result.get('actions_taken',0)}. Message: {primary_reason_for_log_and_console}")
                        results_summary['moved_unknown_files'] += action_result.get('actions_taken', 0)
                    elif ProcessingStatus.PATH_ALREADY_CORRECT.name in batch_msg_from_action or \
                         ProcessingStatus.PLAN_TARGET_EXISTS_SKIP_MODE.name in batch_msg_from_action:
                        log.info(f"SKIPPED (Benign): Batch '{stem}'. Reason: {primary_reason_for_log_and_console}")
                        results_summary['skipped_correct_or_conflict'] += 1
                    elif ProcessingStatus.USER_INTERACTIVE_SKIP.name in batch_msg_from_action: 
                        log.info(f"SKIPPED (User Batch Plan/Meta): Batch '{stem}'. Reason: {primary_reason_for_log_and_console}")
                        results_summary['user_skipped_batches'] += 1
                    elif ProcessingStatus.UNKNOWN_HANDLING_CONFIG_SKIP.name in batch_msg_from_action:
                        log.info(f"SKIPPED (Config): Batch '{stem}'. Reason: {primary_reason_for_log_and_console}")
                        results_summary['config_skipped_batches'] += 1
                    else: 
                        log.info(f"SUCCESS: Batch '{stem}'. Actions: {action_result.get('actions_taken',0)}. Message: {primary_reason_for_log_and_console}")
                        results_summary['success_renames_moves'] += 1
                else: 
                    log.error(f"FAILED_PROCESSING: Batch '{stem}'. Final Reason: {primary_reason_for_log_and_console}")
                    # If batch_msg_from_action was different and not an internal error, log it as detail
                    if batch_msg_from_action != primary_reason_for_log_and_console and \
                       ProcessingStatus.INTERNAL_ERROR.name not in primary_reason_for_log_and_console and \
                       batch_msg_from_action: # Ensure it's not empty
                        log.info(f"  Detail/Action Outcome for Failed Batch '{stem}': {batch_msg_from_action}")
                    results_summary['error_batches'] += 1
                
                if is_live_run:
                    results_summary['actions_taken'] += action_result.get('actions_taken', 0)
                else: 
                    total_planned_actions_accumulator_for_dry_run += action_result.get('actions_taken', 0)

                should_print_to_console = bool(primary_reason_for_log_and_console) # Use the determined primary message
                if not self.args.interactive and ProcessingStatus.PATH_ALREADY_CORRECT.name in primary_reason_for_log_and_console and not final_batch_had_error_flag:
                    should_print_to_console = False
                
                if should_print_to_console:
                    use_rule = not self.args.interactive and is_live_run and action_result.get('success') and \
                               action_result.get('actions_taken',0) > 0 and \
                               not (f"[{ProcessingStatus.SUCCESS.name}] MOVED (UNKNOWN)" in primary_reason_for_log_and_console.upper())

                    if use_rule: self.console.print("-" * 70) 
                    
                    style_for_text = "default"; print_to_stderr_flag = False
                    if final_batch_had_error_flag and not action_result.get('success'):
                        style_for_text = "red"; print_to_stderr_flag = True 
                    elif not action_result.get('success') or \
                         any(f"[{status.name}]" in primary_reason_for_log_and_console for status in [
                             ProcessingStatus.PLAN_TARGET_EXISTS_SKIP_MODE, ProcessingStatus.UNKNOWN_HANDLING_CONFIG_SKIP,
                             ProcessingStatus.USER_INTERACTIVE_SKIP, ProcessingStatus.METADATA_NO_MATCH,
                         ]):
                        style_for_text = "yellow"
                    elif action_result.get('success') and (action_result.get('actions_taken',0) > 0 or ProcessingStatus.PATH_ALREADY_CORRECT.name in primary_reason_for_log_and_console):
                         style_for_text = "green" 

                    message_renderable = TextClass(primary_reason_for_log_and_console, style=style_for_text)
                    
                    if print_to_stderr_flag:
                        from rename_main import print_stderr_message
                        print_stderr_message(self.console, message_renderable, self.args.quiet, RICH_AVAILABLE)
                    else: 
                        self.console.print(message_renderable)
                    if use_rule: self.console.print("-" * 70) 

                if user_quit_processing: break 

        self.console.print("-" * 30) 
        log.info("Processing complete.")
        self.console.print("Processing Summary:")
        self.console.print(f"  Batches Scanned: {batch_count}")
        self.console.print(f"  Successfully Renamed/Moved: {results_summary['success_renames_moves']}")
        if results_summary['moved_unknown_files'] > 0 :
            self.console.print(f"  Files Moved to Unknown Dir: {results_summary['moved_unknown_files']}")
        
        total_skipped = results_summary['skipped_correct_or_conflict'] + \
                        results_summary['user_skipped_batches'] + \
                        results_summary['config_skipped_batches']
        if total_skipped > 0:
            self.console.print(f"  Batches Skipped (various reasons): {total_skipped}")
            if results_summary['skipped_correct_or_conflict'] > 0:
                 self.console.print(f"    - Path correct or target exists (skip mode): {results_summary['skipped_correct_or_conflict']}")
            if results_summary['user_skipped_batches'] > 0: 
                 self.console.print(f"    - User interactive skip (batch plan or metadata): {results_summary['user_skipped_batches']}")
            if results_summary['config_skipped_batches'] > 0:
                 self.console.print(f"    - Configured to skip (unknown/metadata fail): {results_summary['config_skipped_batches']}")
        
        initial_meta_errors_count = sum(1 for mi in initial_media_infos.values() if mi and mi.metadata_error_message and not \
                                        (ProcessingStatus.USER_INTERACTIVE_SKIP.name in mi.metadata_error_message or \
                                         ProcessingStatus.USER_ABORTED_OPERATION.name in mi.metadata_error_message))
        if initial_meta_errors_count > 0:
            self.console.print(f"  Initial Metadata Fetch Issues (Batches): {initial_meta_errors_count}")
        
        if results_summary['error_batches'] > 0 : 
            error_summary_msg_content = f"Batches with Processing Errors: {results_summary['error_batches']}"
            from rename_main import print_stderr_message
            print_stderr_message(self.console, TextClass(error_summary_msg_content, style="bold red"), getattr(self.args, 'quiet', False), RICH_AVAILABLE)
        elif results_summary['error_batches'] == 0 and not (results_summary['success_renames_moves'] > 0 or results_summary['moved_unknown_files'] > 0 or total_skipped > 0) and batch_count > 0 :
             self.console.print(f"  Batches with Errors: 0 (but no successful actions or skips recorded - check logic)")
        elif results_summary['error_batches'] == 0 :
            self.console.print(f"  Batches with Processing Errors: 0")

        if is_live_run:
            self.console.print(f"  Total File System Actions Logged (files+dirs): {results_summary['actions_taken']}")
        else:
            self.console.print(f"  Total File Actions Planned (Dry Run): {total_planned_actions_accumulator_for_dry_run}")
        self.console.print("-" * 30)

        if not is_live_run:
             if total_planned_actions_accumulator_for_dry_run > 0:
                 self.console.print("[yellow]DRY RUN COMPLETE. To apply changes, run again with --live[/yellow]")
             else:
                 self.console.print("DRY RUN COMPLETE. No actions were planned.")
        
        undo_enabled_final_check = self.cfg('enable_undo', False, arg_value=getattr(self.args, 'enable_undo', None))
        if is_live_run and undo_enabled_final_check and results_summary['actions_taken'] > 0:
            script_name = Path(sys.argv[0]).name
            self.console.print(f"Undo information logged with Run ID: [bold cyan]{run_batch_id}[/bold cyan]")
            self.console.print(f"To undo this run: {script_name} undo {run_batch_id}")
        
        if is_live_run and hasattr(self.args, 'stage_dir') and self.args.stage_dir and results_summary['actions_taken'] > 0 :
            self.console.print(f"Renamed files moved to staging: {self.args.stage_dir}")
                
        if results_summary['error_batches'] > 0:
            problem_msg_content = f"Operation finished with {results_summary['error_batches']} batches encountering processing errors. Check logs."
            from rename_main import print_stderr_message
            print_stderr_message(self.console, TextClass(problem_msg_content, style="bold red"), getattr(self.args, 'quiet', False), RICH_AVAILABLE)

        elif results_summary['success_renames_moves'] == 0 and results_summary['moved_unknown_files'] == 0 and total_skipped == batch_count and batch_count > 0:
             self.console.print("Operation finished. All batches were skipped (e.g. already correct, or by config/user choice).")
        elif results_summary['success_renames_moves'] > 0 or results_summary['moved_unknown_files'] > 0 :
             self.console.print("[green]Operation finished successfully.[/green]")
             if total_skipped > 0:
                 self.console.print(f"[yellow] ({total_skipped} batches were skipped for various reasons).[/yellow]")
        elif batch_count > 0: 
             self.console.print("Operation finished. (No explicit success, errors, or all skips recorded - check logs for details).")