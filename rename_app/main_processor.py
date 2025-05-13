# rename_app/main_processor.py
import logging
import uuid
import builtins
import sys
import time
import asyncio
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Tuple, Optional, Dict, Any, cast, List

from .metadata_fetcher import MetadataFetcher # Keep this
from .renamer_engine import RenamerEngine
from .file_system_ops import perform_file_actions, _handle_conflict, FileOperationError
from .utils import scan_media_files
from .exceptions import UserAbortError, RenamerError, MetadataError
from .models import MediaInfo, RenamePlan, MediaMetadata
from .api_clients import get_tmdb_client, get_tvdb_client
from .enums import ProcessingStatus
from .undo_manager import UndoManager
from .config_manager import ConfigHelper
from .ui_utils import (ConsoleClass, TextClass, PanelClass, TableClass, ProgressClass, BarColumnClass, ProgressTextColumnClass, TimeElapsedColumnClass, MofNCompleteColumnClass, TaskIDClass, ConfirmClass, PromptClass, InvalidResponseClass, RICH_AVAILABLE_UI as RICH_AVAILABLE, RichConsoleActual)

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
    batch_stem: str,
    batch_data: Dict[str, Any],
    processor: "MainProcessor",
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
        if isinstance(progress.tasks, list) and task_id < len(progress.tasks):
            task_obj = progress.tasks[task_id]
        elif isinstance(progress.tasks, dict) and task_id in progress.tasks:
            task_obj = progress.tasks[task_id]

        if task_obj and not task_obj.finished:
            try:
                progress.update(task_id, item_name=f"fetching: {item_name_short}")
            except Exception as e_prog_update:
                log.error(f"Error updating progress bar item name in fetch: {e_prog_update}")
    try:
        media_info.guess_info = processor.renamer.parse_filename(media_info.original_path)
        original_file_type_from_guessit = processor.renamer._determine_file_type(media_info.guess_info)
        media_info.file_type = original_file_type_from_guessit

        use_metadata_cfg = processor.cfg('use_metadata', False)
        if use_metadata_cfg and processor.metadata_fetcher and media_info.file_type != 'unknown':
            log.debug(f"Attempting async metadata fetch for '{batch_stem}' ({media_info.file_type})")
            year_guess = media_info.guess_info.get('year')
            fetched_api_metadata: Optional[MediaMetadata] = None
            try:
                if media_info.file_type == 'series':
                    raw_episode_data: Any = None
                    valid_ep_list: List[int] = []
                    if isinstance(media_info.guess_info.get('episode_list'), list):
                        raw_episode_data = media_info.guess_info['episode_list']
                    elif 'episode' in media_info.guess_info:
                        raw_episode_data = media_info.guess_info['episode']
                    elif 'episode_number' in media_info.guess_info:
                        raw_episode_data = media_info.guess_info['episode_number']

                    if raw_episode_data is not None:
                        ep_data_list = raw_episode_data if isinstance(raw_episode_data, list) else [raw_episode_data]
                        for ep in ep_data_list:
                            try:
                                ep_int = int(str(ep))
                                if ep_int > 0:
                                    valid_ep_list.append(ep_int)
                            except (ValueError, TypeError):
                                log.warning(f"Could not parse episode '{ep}' from guessit for '{batch_stem}'.")
                    valid_ep_list = sorted(list(set(valid_ep_list)))
                    log.debug(f"Final valid episode list for API call for '{batch_stem}': {valid_ep_list}")

                    guessed_title_raw = media_info.guess_info.get('title')
                    guessed_title: str
                    if isinstance(guessed_title_raw, list) and guessed_title_raw:
                        guessed_title = str(guessed_title_raw[0]) if guessed_title_raw[0] else media_info.original_path.stem
                    elif isinstance(guessed_title_raw, str) and guessed_title_raw:
                        guessed_title = guessed_title_raw
                    else:
                        guessed_title = media_info.original_path.stem
                        log.debug(f"Guessed title empty for series '{batch_stem}', using stem: '{guessed_title}'")

                    if valid_ep_list:
                        fetched_api_metadata = await processor.metadata_fetcher.fetch_series_metadata(
                            show_title_guess=guessed_title,
                            season_num=media_info.guess_info.get('season', 0),
                            episode_num_list=tuple(valid_ep_list),
                            year_guess=year_guess
                        )
                    else:
                        log.warning(f"No valid episode numbers for series '{batch_stem}'. Skipping series metadata fetch.")
                elif media_info.file_type == 'movie':
                    guessed_title_raw = media_info.guess_info.get('title')
                    guessed_title: str
                    if isinstance(guessed_title_raw, list) and guessed_title_raw:
                        guessed_title = str(guessed_title_raw[0]) if guessed_title_raw[0] else media_info.original_path.stem
                    elif isinstance(guessed_title_raw, str) and guessed_title_raw:
                        guessed_title = guessed_title_raw
                    else:
                        guessed_title = media_info.original_path.stem
                        log.debug(f"Guessed title empty for movie '{batch_stem}', using stem: '{guessed_title}'")
                    fetched_api_metadata = await processor.metadata_fetcher.fetch_movie_metadata(
                        movie_title_guess=guessed_title, year_guess=year_guess # year_guess can be None
                    )
                media_info.metadata = fetched_api_metadata
            except MetadataError as me:
                log.error(f"Caught MetadataError for '{batch_stem}': {me}")
                media_info.metadata_error_message = str(me)
                media_info.metadata = None
            except Exception as fetch_e:
                log.exception(f"Unexpected error during metadata API call for '{batch_stem}': {fetch_e}")
                media_info.metadata_error_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Unexpected fetch error: {fetch_e}"
                media_info.metadata = None

            if media_info.metadata is None or not media_info.metadata.source_api:
                if not media_info.metadata_error_message: # If not already set by a specific error
                    media_info.metadata_error_message = f"[{ProcessingStatus.METADATA_NO_MATCH}] Metadata fetch returned no usable API data."
        return batch_stem, media_info
    except Exception as e:
        log.exception(f"Critical error in _fetch_metadata_for_batch for '{batch_stem}': {e}")
        if not hasattr(media_info, 'guess_info') or not media_info.guess_info: # Ensure guess_info exists
            media_info.guess_info = {} # Initialize if missing
        media_info.file_type = 'unknown'
        media_info.metadata = None
        media_info.metadata_error_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Processing error in _fetch_metadata_for_batch: {e}"
        return batch_stem, media_info
    finally:
        if progress and task_id is not None and hasattr(progress, 'tasks') and progress.tasks:
            task_obj = None
            if isinstance(progress.tasks, list) and task_id < len(progress.tasks):
                task_obj = progress.tasks[task_id]
            elif isinstance(progress.tasks, dict) and task_id in progress.tasks:
                task_obj = progress.tasks[task_id]

            if task_obj and not task_obj.finished:
                try:
                    progress.update(task_id, advance=1, item_name="")
                except Exception as e_prog_final:
                     log.error(f"Error finalizing progress bar item name in fetch: {e_prog_final}")

class MainProcessor:
    def __init__(self, args, cfg_helper: ConfigHelper, undo_manager: UndoManager):
        self.args = args
        self.cfg = cfg_helper
        self.undo_manager = undo_manager
        self.renamer = RenamerEngine(cfg_helper)
        self.metadata_fetcher: Optional[MetadataFetcher] = None
        
        self.console = ConsoleClass(quiet=getattr(args, 'quiet', False))
               
        use_metadata_effective = getattr(args, 'use_metadata', False)
        if use_metadata_effective:
             log.info("Metadata fetching enabled for MainProcessor.")
             if get_tmdb_client() or get_tvdb_client():
                 self.metadata_fetcher = MetadataFetcher(cfg_helper, console=self.console)
             else:
                 log.warning("Metadata enabled but no API clients initialized. Disabling fetcher in MainProcessor.")
                 setattr(self.args, 'use_metadata', False)
        else:
            log.info("Metadata fetching disabled for MainProcessor.")

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
            if isinstance(score, float):
                score_color = "green" if score >= 85 else "yellow" if score >= self.cfg('tmdb_match_fuzzy_cutoff', 70) else "red"
                source_info += f" (Score: [{score_color}]{score:.1f}%[/])"
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


    async def _refetch_with_manual_id(self, media_info: MediaInfo, api_source: str, manual_id: int) -> Optional[MediaMetadata]:
        if not self.metadata_fetcher:
            self.console.print("[red]Error: Metadata fetcher not initialized.[/red]", file=sys.stderr)
            return None
        
        log.info(f"Attempting re-fetch for '{media_info.original_path.name}' using {api_source.upper()} ID: {manual_id}")
        new_metadata: Optional[MediaMetadata] = None

        try:
            if api_source == 'tmdb':
                # For manual ID, we don't need guess_info, we directly fetch by ID.
                # However, fetch_movie_metadata and fetch_series_metadata expect title/season etc.
                # We need to modify them or add new methods in MetadataFetcher for direct ID lookup.
                # For now, let's assume MetadataFetcher will be enhanced to handle direct ID lookups.
                # This part needs more thought on how MetadataFetcher's API is designed for direct ID lookups.
                # Let's simulate this for now and assume fetch_..._metadata can handle an ID override
                # if it's a new feature or adjust what we pass.
                # TODO: Enhance MetadataFetcher for direct ID lookups.
                # For now, we'll pass minimal info and hope the ID takes precedence internally.
                if media_info.file_type == 'movie':
                    self.console.print(f"[yellow]Re-fetching TMDB movie details for ID {manual_id}...[/yellow]")
                    # We would ideally call a method like:
                    # new_metadata = await self.metadata_fetcher.fetch_movie_by_tmdb_id(manual_id)
                    # For now, reusing existing, which might not be ideal for pure ID lookup.
                    new_metadata = await self.metadata_fetcher.fetch_movie_metadata(
                        movie_title_guess=f"TMDB_ID_{manual_id}", # Dummy title
                        year_guess=None # year_guess is optional
                    ) 
                    if not new_metadata or new_metadata.ids.get('tmdb_id') != manual_id:
                        log.warning(f"Re-fetch for TMDB Movie ID {manual_id} didn't return the expected movie or ID mismatch.")
                        new_metadata = None
                elif media_info.file_type == 'series':
                    self.console.print(f"[yellow]Re-fetching TMDB series details for ID {manual_id}...[/yellow]")
                    new_metadata = await self.metadata_fetcher.fetch_series_metadata(
                        show_title_guess=f"TMDB_ID_{manual_id}", # Dummy title
                        season_num=media_info.guess_info.get('season', 0), # Keep season/ep for context
                        episode_num_list=tuple(media_info.guess_info.get('episode_list', [])),
                        year_guess=None
                    )
                    if not new_metadata or new_metadata.ids.get('tmdb_id') != manual_id:
                        log.warning(f"Re-fetch for TMDB Series ID {manual_id} didn't return the expected series or ID mismatch.")
                        new_metadata = None
            elif api_source == 'tvdb':
                 if media_info.file_type == 'series':
                    self.console.print(f"[yellow]Re-fetching TVDB series details for ID {manual_id}...[/yellow]")
                    # Similar to TMDB, ideal would be fetch_series_by_tvdb_id(manual_id)
                    new_metadata = await self.metadata_fetcher.fetch_series_metadata(
                        show_title_guess=f"TVDB_ID_{manual_id}", # Dummy title
                        season_num=media_info.guess_info.get('season', 0),
                        episode_num_list=tuple(media_info.guess_info.get('episode_list', [])),
                        year_guess=None # TVDB fetch might use this ID if passed correctly
                    )
                    if not new_metadata or new_metadata.ids.get('tvdb_id') != manual_id:
                         log.warning(f"Re-fetch using TVDB Series ID {manual_id} didn't result in metadata with that ID.")
                         new_metadata = None
                 else:
                    self.console.print("[red]TVDB ID only applicable for Series.[/red]", file=sys.stderr)
                    return None
            else:
                self.console.print(f"[red]Unsupported API source: {api_source}[/red]", file=sys.stderr)
                return None

            if new_metadata and new_metadata.source_api:
                 self.console.print(f"[green]Successfully re-fetched metadata from {new_metadata.source_api.upper()}.[/green]")
                 return new_metadata
            else:
                 self.console.print(f"[red]Failed to fetch valid metadata using {api_source.upper()} ID {manual_id}.[/red]")
                 return None
        except Exception as e:
            log.exception(f"Error during manual ID re-fetch ({api_source} ID {manual_id}): {e}")
            self.console.print(f"[red]Error during re-fetch: {e}[/red]", file=sys.stderr)
            return None

    def _confirm_live_run(self, potential_actions_count: int) -> bool:
        if getattr(self.args, 'quiet', False):
            log.info("Quiet mode: Skipping live run confirmation. Live run will NOT proceed by default.")
            return False

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
            if ConfirmClass.ask("Proceed with actions?", default=False): # Use ConfirmClass
                log.info("User confirmed live run.")
                return True
            else:
                log.info("User aborted live run.")
                self.console.print("Operation cancelled by user.")
                return False
        except EOFError:
             log.error("Cannot confirm live run in non-interactive mode without confirmation (EOF).")
             self.console.print("\n[bold red]ERROR: Cannot confirm. Run interactively or use force flag (not implemented).[/bold red]", file=sys.stderr)
             return False
        except KeyboardInterrupt:
            log.warning("Live run confirmation aborted by user (KeyboardInterrupt).")
            self.console.print("\nOperation cancelled by user.", file=sys.stderr)
            return False
        except InvalidResponseClass: # Catch specific Rich error if ConfirmClass is RichConfirm
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
        unknown_dir_str = self.args.unknown_files_dir
        
        base_message_prefix = f"Batch '{batch_stem}': "

        if not unknown_dir_str:
            msg = f"[{ProcessingStatus.CONFIG_MISSING_FORMAT_STRING}] {base_message_prefix}Unknown files directory not configured. Skipping move."
            log.error(msg)
            results['message'] = msg
            results['fs_errors'] += 1
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
            results['move_success'] = True
            results['actions_taken'] = dry_run_actions_count
            return results

        try:
            if not unknown_target_dir.exists():
                log.info(f"Creating unknown files directory: {unknown_target_dir}")
                unknown_target_dir.mkdir(parents=True, exist_ok=True)
                if self.undo_manager.is_enabled:
                    self.undo_manager.log_action(batch_id=run_batch_id, original_path=unknown_target_dir, new_path=unknown_target_dir, item_type='dir', status='created_dir')
                action_messages.append(f"[{ProcessingStatus.SUCCESS}] CREATED DIR (unknowns): '{unknown_target_dir}'")
        except OSError as e:
            msg = f"[{ProcessingStatus.FILE_OPERATION_ERROR}] {base_message_prefix}Could not create directory '{unknown_target_dir}': {e}"
            log.error(msg, exc_info=True)
            results['message'] = msg
            results['fs_errors'] += 1
            return results

        conflict_mode = self.cfg('on_conflict', 'skip')
        files_to_move_live: List[Optional[Path]] = [batch_data.get('video')] + batch_data.get('associated', [])
        files_moved_successfully = 0
        files_to_move_count = 0

        for original_file_path_live in files_to_move_live:
            if not original_file_path_live or not isinstance(original_file_path_live, Path):
                continue
            if not original_file_path_live.exists():
                log.warning(f"Skipping move of non-existent file: {original_file_path_live}")
                continue
            
            files_to_move_count += 1
            target_file_path_in_unknown_dir = unknown_target_dir / original_file_path_live.name
            
            try:
                final_target_path = _handle_conflict(original_file_path_live, target_file_path_in_unknown_dir, conflict_mode)
                if self.undo_manager.is_enabled:
                    self.undo_manager.log_action(batch_id=run_batch_id, original_path=original_file_path_live, new_path=final_target_path, item_type='file', status='moved')
                
                log.debug(f"Moving '{original_file_path_live.name}' to '{final_target_path}' for unknown handling.")
                shutil.move(str(original_file_path_live), str(final_target_path))
                action_messages.append(f"[{ProcessingStatus.SUCCESS}] MOVED (unknown): '{original_file_path_live.name}' to '{final_target_path}'")
                results['actions_taken'] += 1
                files_moved_successfully += 1
            except FileExistsError as e_fe:
                msg = f"[{ProcessingStatus.PLAN_TARGET_EXISTS_FAIL_MODE}] ERROR (move unknown): {e_fe} - File '{original_file_path_live.name}' not moved."
                log.error(msg)
                action_messages.append(msg)
                results['fs_errors'] += 1
            except FileOperationError as e_foe:
                msg = f"[{ProcessingStatus.PLAN_TARGET_EXISTS_SKIP_MODE}] SKIPPED (move unknown): {e_foe} - File '{original_file_path_live.name}' not moved."
                log.warning(msg)
                action_messages.append(msg)
            except OSError as e_os:
                msg = f"[{ProcessingStatus.FILE_OPERATION_ERROR}] ERROR (move unknown): Failed to move '{original_file_path_live.name}': {e_os}"
                log.error(msg, exc_info=True)
                action_messages.append(msg)
                results['fs_errors'] += 1
            except Exception as e_generic:
                msg = f"[{ProcessingStatus.INTERNAL_ERROR}] ERROR (move unknown): Unexpected error for '{original_file_path_live.name}': {e_generic}"
                log.exception(msg)
                action_messages.append(msg)
                results['fs_errors'] += 1
        
        results['move_success'] = (files_to_move_count > 0 and files_moved_successfully == files_to_move_count) and (results['fs_errors'] == 0)
        if not action_messages:
            action_messages.append(f"[{ProcessingStatus.SKIPPED}] {base_message_prefix}No files moved to unknown.")
        results['message'] = "\n".join(action_messages)
        return results
    
    def _perform_prescan(self, file_batches: Dict[str, Dict[str, Any]], batch_count: int) -> int:
        log.info("Performing synchronous pre-scan for live run confirmation...")
        potential_actions_count = 0
        disable_rich_progress = getattr(self.args, 'quiet', False) or self.args.interactive or not RICH_AVAILABLE
        
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
                    media_info_prescan = MediaInfo(original_path=video_path)
                    media_info_prescan.guess_info = self.renamer.parse_filename(media_info_prescan.original_path)
                    media_info_prescan.file_type = self.renamer._determine_file_type(media_info_prescan.guess_info)
                    media_info_prescan.metadata = None
                    
                    associated_paths_prescan = batch_data.get('associated', [])
                    if not isinstance(associated_paths_prescan, list): associated_paths_prescan = []


                    if media_info_prescan.file_type == 'unknown':
                        unknown_handling_mode_prescan = self.cfg('unknown_file_handling', 'skip', arg_value=getattr(self.args, 'unknown_file_handling', None))
                        if unknown_handling_mode_prescan == 'move_to_unknown':
                            potential_actions_count += 1 + len(associated_paths_prescan)
                        elif unknown_handling_mode_prescan == 'guessit_only':
                            plan = self.renamer.plan_rename(video_path, associated_paths_prescan, media_info_prescan)
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
        log.info("Performing initial file parsing...")
        disable_rich_progress = getattr(self.args, 'quiet', False) or self.args.interactive or not RICH_AVAILABLE
        
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
                    log.error(f"Error parsing '{stem}': {e_parse}")
                    initial_media_infos[stem] = None
        return initial_media_infos

    async def _fetch_all_metadata( self, file_batches: Dict[str, Dict[str, Any]], initial_media_infos: Dict[str, Optional[MediaInfo]] ) -> Dict[str, Optional[MediaInfo]]:
        use_metadata_effective = getattr(self.args, 'use_metadata', False)
        if not (use_metadata_effective and self.metadata_fetcher):
            log.info("Metadata fetching disabled or fetcher not available. Skipping metadata phase.")
            return initial_media_infos

        stems_to_fetch = [ stem for stem, info in initial_media_infos.items() if info and info.file_type != 'unknown' ]
        log.info(f"Creating {len(stems_to_fetch)} tasks for concurrent metadata fetching...")
        if not stems_to_fetch:
            log.info("No batches required metadata fetching.")
            return initial_media_infos

        fetch_tasks: List[asyncio.Task[Tuple[str, MediaInfo]]] = []
        disable_rich_progress = getattr(self.args, 'quiet', False) or self.args.interactive or not RICH_AVAILABLE
        
        with ProgressClass(*DEFAULT_PROGRESS_COLUMNS, console=self.console, disable=disable_rich_progress) as progress_bar:
            metadata_overall_task: TaskIDClass = progress_bar.add_task("Fetching Metadata", total=len(stems_to_fetch), item_name="")
            for stem in stems_to_fetch:
                batch_data = file_batches[stem]
                # Pass ProgressClass instance to _fetch_metadata_for_batch
                task = asyncio.create_task(
                    _fetch_metadata_for_batch(stem, batch_data, self, progress_bar, metadata_overall_task),
                    name=f"fetch_{stem}"
                )
                fetch_tasks.append(task)
            
            completed_fetch_results_tuples: List[Tuple[str, MediaInfo]] = []
            try:
                for f_task_completed in asyncio.as_completed(fetch_tasks):
                    completed_fetch_results_tuples.append(await f_task_completed)
            except Exception as e_async_task_collection:
                log.error(f"Error collecting results from async metadata tasks: {e_async_task_collection}")

            # Ensure progress bar completes if not already
            if hasattr(progress_bar, 'tasks') and progress_bar.tasks: # type: ignore
                task_obj = None
                if isinstance(progress_bar.tasks, list) and metadata_overall_task < len(progress_bar.tasks): # type: ignore
                    task_obj = progress_bar.tasks[metadata_overall_task] # type: ignore
                elif isinstance(progress_bar.tasks, dict) and metadata_overall_task in progress_bar.tasks: # type: ignore
                    task_obj = progress_bar.tasks[metadata_overall_task] # type: ignore

                if task_obj and not task_obj.finished: # type: ignore
                    progress_bar.update(metadata_overall_task, completed=len(stems_to_fetch), item_name="") # type: ignore
        
        for result_item in completed_fetch_results_tuples:
            if isinstance(result_item, tuple) and len(result_item) == 2:
                stem_from_task, updated_media_info_obj = result_item
                if updated_media_info_obj:
                    initial_media_infos[stem_from_task] = updated_media_info_obj
                else:
                    log.error(f"Async task for {stem_from_task} returned None for MediaInfo object")
                    if initial_media_infos.get(stem_from_task):
                        existing_info = initial_media_infos[stem_from_task]
                        if existing_info:
                           existing_info.metadata_error_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Async task returned None"
                    else:
                        log.warning(f"Stem {stem_from_task} not found in initial_media_infos after async failure.")
        return initial_media_infos
    
    async def _process_single_batch(
        self,
        stem: str,
        batch_data: Dict[str, Any],
        media_info: MediaInfo, # This is the MediaInfo object after initial fetch
        run_batch_id: str,
        is_live_run: bool
    ) -> Tuple[Dict[str, Any], bool, bool]:

        action_result: Dict[str, Any] = {'success': False, 'message': '', 'actions_taken': 0}
        user_quit_flag = False
        plan: Optional[RenamePlan] = None
        final_batch_processing_error_occurred = False
        video_file_path = cast(Path, batch_data.get('video'))
        use_metadata_effective = getattr(self.args, 'use_metadata', False)
        
        # --- Yearless Movie Confirmation (if needed) ---
        if (media_info.file_type == 'movie' and
            media_info.metadata and
            media_info.metadata.match_confidence == -1.0 and # Signal for yearless confirm
            self.cfg('movie_yearless_match_confidence', 'medium') == 'confirm'):

            if not getattr(self.args, 'quiet', False) and (self.args.interactive or not is_live_run or ConfirmClass is not None): # Check if we can prompt
                confirm_prompt = (
                    f"Yearless match for '{media_info.original_path.name}': "
                    f"Found API result '{media_info.metadata.movie_title}' ({media_info.metadata.movie_year or 'N/A'}). "
                    f"Confirm this match?"
                )
                try:
                    if ConfirmClass.ask(confirm_prompt, default=True):
                        log.info(f"User confirmed yearless match for '{media_info.original_path.name}'.")
                        media_info.metadata.match_confidence = None # Remove the -1.0 signal
                    else:
                        log.info(f"User REJECTED yearless match for '{media_info.original_path.name}'.")
                        media_info.metadata_error_message = f"[{ProcessingStatus.USER_INTERACTIVE_SKIP}] User rejected yearless match confirmation."
                        media_info.metadata = None # Discard the metadata
                except (EOFError, KeyboardInterrupt): # Handle cases where prompt might be interrupted
                    log.warning(f"Yearless match confirmation aborted for '{media_info.original_path.name}'.")
                    media_info.metadata_error_message = f"[{ProcessingStatus.USER_ABORTED_OPERATION}] Confirmation aborted for yearless match."
                    media_info.metadata = None
                    user_quit_flag = True # Potentially treat as a desire to quit all
                except Exception as e_confirm: # Catch other rich prompt errors
                    log.error(f"Error during yearless confirm prompt: {e_confirm}")
                    media_info.metadata_error_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Error during yearless confirmation prompt."
                    media_info.metadata = None

            else: # Cannot prompt (quiet mode or non-interactive dry run without ConfirmClass)
                log.warning(f"Yearless match for '{media_info.original_path.name}' requires confirmation but cannot prompt. Rejecting match.")
                media_info.metadata_error_message = f"[{ProcessingStatus.METADATA_NO_MATCH}] Yearless match rejected (confirmation required, non-interactive)."
                media_info.metadata = None
        
        # If metadata was rejected, ensure match_confidence is None if it was -1.0
        if media_info.metadata and media_info.metadata.match_confidence == -1.0:
            media_info.metadata.match_confidence = None


        # This flag is only about the *initial* metadata fetch attempt or subsequent rejection
        metadata_failed_or_rejected = use_metadata_effective and (bool(media_info.metadata_error_message) or media_info.metadata is None)
        
        proceed_with_normal_planning = False
        current_batch_status_message = f"Processing batch '{stem}'"

        if metadata_failed_or_rejected:
            current_batch_status_message = media_info.metadata_error_message or \
                                       f"[{ProcessingStatus.METADATA_FETCH_API_ERROR}] Metadata error for '{video_file_path.name}' (unknown details)."
            if not getattr(self.args, 'quiet', False) and not self.args.interactive:
                 self.console.print(PanelClass(f"[bold red]API/Metadata Error:[/bold red] {current_batch_status_message}", title=f"[yellow]'{media_info.original_path.name}'[/yellow]", border_style="red"))

        unknown_handling_mode = self.cfg('unknown_file_handling', 'skip', arg_value=getattr(self.args, 'unknown_file_handling', None))

        if media_info.file_type == 'unknown' or metadata_failed_or_rejected:
            handling_reason = "unknown file type" if media_info.file_type == 'unknown' else "metadata fetch/confirmation failed"
            log.info(f"Batch '{stem}' (type: {media_info.file_type}) handled via '{unknown_handling_mode}' due to: {handling_reason}.")

            if unknown_handling_mode == 'skip':
                action_result['message'] = current_batch_status_message if metadata_failed_or_rejected else \
                                           f"[{ProcessingStatus.UNKNOWN_HANDLING_CONFIG_SKIP}] Skipped ({handling_reason}): {video_file_path.name}"
                action_result['success'] = True 
                final_batch_processing_error_occurred = False 
            elif unknown_handling_mode == 'move_to_unknown':
                move_result = self._handle_move_to_unknown(stem, batch_data, run_batch_id)
                action_result.update(move_result) 
                action_result['success'] = move_result.get('move_success', False)
                final_batch_processing_error_occurred = not action_result['success'] 
            elif unknown_handling_mode == 'guessit_only':
                log.debug(f"Proceeding with guessit_only planning for '{stem}' due to {handling_reason}.")
                media_info.metadata = None 
                media_info.metadata_error_message = None 
                proceed_with_normal_planning = True
                final_batch_processing_error_occurred = False 
            else: 
                action_result['message'] = f"[{ProcessingStatus.INTERNAL_ERROR}] Invalid unknown_handling_mode '{unknown_handling_mode}' for '{stem}'"
                final_batch_processing_error_occurred = True
            
            if not proceed_with_normal_planning:
                 return action_result, final_batch_processing_error_occurred, user_quit_flag
        else: 
            proceed_with_normal_planning = True
            final_batch_processing_error_occurred = False 

        is_skip_or_correct: bool = False

        try:
            if not proceed_with_normal_planning:
                action_result['message'] = f"[{ProcessingStatus.INTERNAL_ERROR}] Logic error: Should not attempt normal plan if not proceeding for {stem}."
                final_batch_processing_error_occurred = True
                return action_result, final_batch_processing_error_occurred, user_quit_flag

            plan = self.renamer.plan_rename(video_file_path, batch_data.get('associated', []), media_info)
            user_choice_for_action = 'y' 
            current_plan_for_interaction = plan 

            is_interactive_prompt_allowed = self.args.interactive and not getattr(self.args, 'quiet', False)
            
            # Check confirm_match_below
            confirm_due_to_low_score = False
            if (media_info.metadata and 
                media_info.metadata.match_confidence is not None and 
                media_info.metadata.match_confidence != -1.0 and # -1.0 was for yearless internal signal
                self.cfg('confirm_match_below') is not None and 
                media_info.metadata.match_confidence < self.cfg('confirm_match_below')):
                confirm_due_to_low_score = True
                log.info(f"Match score {media_info.metadata.match_confidence:.1f} for '{stem}' is below threshold {self.cfg('confirm_match_below')}. Will require confirmation if interactive.")


            if is_interactive_prompt_allowed and is_live_run and current_plan_for_interaction and \
               (current_plan_for_interaction.status in ['success', 'conflict_unresolved'] or confirm_due_to_low_score):
                
                if confirm_due_to_low_score and not (current_plan_for_interaction.status in ['success', 'conflict_unresolved']):
                     self.console.print(f"[yellow]Low confidence match for '{media_info.original_path.name}' (Score: {media_info.metadata.match_confidence:.1f if media_info.metadata else 'N/A'}). Plan status: {current_plan_for_interaction.status}[/yellow]")
                
                while True: # Interactive loop
                    if not current_plan_for_interaction: 
                        self.console.print("[red]Error: No plan to display in interactive mode.[/red]", file=sys.stderr)
                        user_choice_for_action = 's'; break
                    self._display_plan_for_confirmation(current_plan_for_interaction, media_info)
                    try:
                        choice_prompt = "Apply? ([y]es/[n]o/[s]kip, [q]uit"
                        if media_info.file_type != 'unknown': # Only offer g/m if type is known
                             choice_prompt += ", [g]uessit only, [m]anual ID"
                        choice_prompt += ")"
                        
                        available_choices = ["y", "n", "s", "q"]
                        if media_info.file_type != 'unknown':
                            available_choices.extend(["g", "m"])

                        choice = PromptClass.ask(choice_prompt, choices=available_choices).lower()
                        
                        if choice == 'y': user_choice_for_action = 'y'; break
                        elif choice in ('n', 's'): user_choice_for_action = 's'; break
                        elif choice == 'q': user_quit_flag = True; raise UserAbortError(f"[{ProcessingStatus.USER_ABORTED_OPERATION}] User quit during interactive mode.")
                        elif choice == 'g' and 'g' in available_choices:
                            self.console.print("[cyan]Re-planning using Guessit data only...[/cyan]")
                            media_info.metadata = None; media_info.metadata_error_message = None
                            current_plan_for_interaction = self.renamer.plan_rename(media_info.original_path, batch_data.get('associated', []), media_info)
                            # Re-evaluate if this new plan needs confirmation (e.g., if it creates conflicts)
                            if not (current_plan_for_interaction and current_plan_for_interaction.status in ['success', 'conflict_unresolved']):
                                # If guessit_only plan is not immediately actionable, maybe just break and let outer logic handle it
                                self.console.print(f"[yellow]Guessit-only plan resulted in status: {current_plan_for_interaction.status if current_plan_for_interaction else 'None'}. Message: {current_plan_for_interaction.message if current_plan_for_interaction else 'N/A'}[/yellow]")
                                if not current_plan_for_interaction or current_plan_for_interaction.status != 'success':
                                    user_choice_for_action = 's' # Effectively skip if guessit plan fails
                                break 
                            # Loop back to display new plan
                        elif choice == 'm' and 'm' in available_choices:
                            api_source_choice = PromptClass.ask("Enter API source ([t]mdb or t[v]db)", choices=['t', 'v']).lower()
                            api_source = 'tmdb' if api_source_choice == 't' else 'tvdb'
                            manual_id_str = PromptClass.ask(f"Enter {api_source.upper()} ID")
                            try:
                                manual_id = int(manual_id_str)
                                new_metadata = await self._refetch_with_manual_id(media_info, api_source, manual_id)
                                if new_metadata:
                                    media_info.metadata = new_metadata
                                    media_info.metadata_error_message = None # Clear previous error
                                    current_plan_for_interaction = self.renamer.plan_rename(media_info.original_path, batch_data.get('associated', []), media_info)
                                else:
                                    self.console.print(f"[red]Manual ID fetch for {api_source.upper()} ID {manual_id} failed. Keeping previous state.[/red]")
                                    # Loop back to display original plan / offer choices again
                            except ValueError:
                                self.console.print("[red]Invalid ID format. Must be an integer.[/red]")
                        else: # Should not happen if choices are enforced by PromptClass.ask
                             self.console.print("[red]Invalid choice. Please try again.[/red]")

                    except (EOFError, KeyboardInterrupt) as e_int_abort:
                        user_quit_flag = True; raise UserAbortError(f"[{ProcessingStatus.USER_ABORTED_OPERATION}] User quit ({type(e_int_abort).__name__}) during interactive mode.")
                    except InvalidResponseClass: self.console.print("[red]Invalid choice.[/red]")
            
            if user_quit_flag: 
                action_result['message'] = action_result.get('message') or f"[{ProcessingStatus.USER_ABORTED_OPERATION}] User quit."
                return action_result, True, True

            final_plan_to_execute = current_plan_for_interaction 

            if user_choice_for_action == 's': 
                action_result['success'] = True 
                action_result['message'] = f"[{ProcessingStatus.USER_INTERACTIVE_SKIP}] User skipped batch '{stem}'."
                log.info(action_result['message'])
                final_batch_processing_error_occurred = False
                is_skip_or_correct = True 
            elif final_plan_to_execute and final_plan_to_execute.status == 'success':
                action_result = perform_file_actions(
                    plan=final_plan_to_execute, args_ns=self.args, cfg_helper=self.cfg,
                    undo_manager=self.undo_manager, run_batch_id=run_batch_id, media_info=media_info,
                    quiet_mode=getattr(self.args, 'quiet', False)
                )
                final_batch_processing_error_occurred = not action_result.get('success', False)
            elif final_plan_to_execute and final_plan_to_execute.message: 
                action_result['message'] = final_plan_to_execute.message
                is_skip_or_correct = ( 
                    final_plan_to_execute.status == 'skipped' or
                    ProcessingStatus.PATH_ALREADY_CORRECT.name in final_plan_to_execute.message or
                    ProcessingStatus.PLAN_TARGET_EXISTS_SKIP_MODE.name in final_plan_to_execute.message
                )
                if is_skip_or_correct:
                    action_result['success'] = True 
                    final_batch_processing_error_occurred = False
                else: 
                    action_result['success'] = False
                    final_batch_processing_error_occurred = True
            elif not final_plan_to_execute: 
                 action_result['message'] = f"[{ProcessingStatus.INTERNAL_ERROR}] No plan generated or selected for batch '{stem}'."
                 final_batch_processing_error_occurred = True
            else: 
                action_result['message'] = f"[{ProcessingStatus.SKIPPED}] No specific action or message determined for batch '{stem}'."
                final_batch_processing_error_occurred = True 

        except UserAbortError as e_abort:
            log.warning(str(e_abort))
            self.console.print(f"\n{e_abort}", file=sys.stderr)
            action_result['message'] = str(e_abort); final_batch_processing_error_occurred = True; user_quit_flag = True
        except FileExistsError as e_fe: 
            log.critical(str(e_fe))
            self.console.print(f"\n[bold red]STOPPING: {e_fe}[/bold red]", file=sys.stderr)
            action_result['message'] = f"[{ProcessingStatus.PLAN_TARGET_EXISTS_FAIL_MODE}] {e_fe}"; final_batch_processing_error_occurred = True; user_quit_flag = True
        except RenamerError as e_rename: 
            log.error(f"RenamerError processing batch '{stem}': {e_rename}", exc_info=False)
            action_result['message'] = str(e_rename); final_batch_processing_error_occurred = True
        except Exception as e_crit: 
            log.exception(f"Critical unhandled error processing batch '{stem}': {e_crit}")
            action_result['message'] = f"[{ProcessingStatus.INTERNAL_ERROR}] Critical error processing batch '{stem}'. Details: {type(e_crit).__name__}: {str(e_crit).splitlines()[0]}"
            error_msg_content = f"[bold red]CRITICAL ERROR processing batch {stem}. See log.[/bold red]"
            if RICH_AVAILABLE and isinstance(self.console, RichConsoleActual):
                if not getattr(self.args, 'quiet', False):
                    plain_text_for_stderr = TextClass(error_msg_content).plain 
                    builtins.print(plain_text_for_stderr, file=sys.stderr)
            else: 
                self.console.print(TextClass(error_msg_content), file=sys.stderr)
            final_batch_processing_error_occurred = True
        
        return action_result, final_batch_processing_error_occurred, user_quit_flag

    async def run_processing(self):
        target_dir = self.args.directory.resolve()
        if not target_dir.is_dir():
            # ... (error handling for invalid target_dir) ...
            msg = f"[{ProcessingStatus.INTERNAL_ERROR}] Target directory not found or is not a directory: {target_dir}"
            log.critical(msg)
            error_renderable = TextClass(f"[bold red]Error: {msg}[/]", style="bold red")
            if RICH_AVAILABLE and isinstance(self.console, RichConsoleActual) and not getattr(self.args, 'quiet', False):
                builtins.print(error_renderable.plain, file=sys.stderr)
            else:
                self.console.print(error_renderable, file=sys.stderr)
            return

        use_metadata_effective = getattr(self.args, 'use_metadata', False)
        if use_metadata_effective and not self.metadata_fetcher:
            # ... (error handling for unavailable metadata_fetcher) ...
            msg = f"[{ProcessingStatus.METADATA_CLIENT_UNAVAILABLE}] Metadata processing enabled, but FAILED to initialize API clients."
            log.critical(msg)
            error_renderable = TextClass(f"\n[bold red]CRITICAL ERROR: {msg}[/]", style="bold red")
            if RICH_AVAILABLE and isinstance(self.console, RichConsoleActual) and not getattr(self.args, 'quiet', False):
                builtins.print(error_renderable.plain, file=sys.stderr)
            else:
                self.console.print(error_renderable, file=sys.stderr)
            return
        
        log.info("Collecting batches from scanner...")
        file_batches = {stem: data for stem, data in scan_media_files(target_dir, self.cfg)}
        batch_count = len(file_batches)
        log.info(f"Collected {batch_count} batches.")
        if batch_count == 0:
             log.warning(f"[{ProcessingStatus.SKIPPED}] No valid video files/batches found matching criteria.")
             self.console.print(TextClass(f"[yellow][{ProcessingStatus.SKIPPED}] No valid video files/batches found.[/yellow]", style="yellow"))
             return

        is_live_run = getattr(self.args, 'live', False)
        if is_live_run:
            potential_actions_count = self._perform_prescan(file_batches, batch_count)
            if not self._confirm_live_run(potential_actions_count):
                return
        
        initial_media_infos = self._perform_initial_parsing(file_batches, batch_count)
        initial_media_infos = await self._fetch_all_metadata(file_batches, initial_media_infos)
        
        run_batch_id = f"run-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        log.info(f"Starting planning and execution run ID: {run_batch_id}")

        results_summary = {
            'success_renames_moves': 0, 'skipped_correct_or_conflict': 0, 'error_batches': 0,
            'actions_taken': 0, 'moved_unknown_files': 0, 'meta_error_batches': 0,
            'user_skipped_batches': 0, 'config_skipped_batches': 0
        }
        self.console.print("-" * 30)
        
        total_planned_actions_accumulator_for_dry_run = 0
        disable_rich_progress = getattr(self.args, 'quiet', False) or self.args.interactive or not RICH_AVAILABLE

        with ProgressClass(*DEFAULT_PROGRESS_COLUMNS, console=self.console, disable=disable_rich_progress) as progress_bar:
            main_processing_task: TaskIDClass = progress_bar.add_task("Planning/Executing", total=batch_count, item_name="")

            for stem, batch_data in file_batches.items():
                video_path_obj = batch_data.get('video')
                item_name_short = Path(video_path_obj if video_path_obj else stem).name[:30] + \
                                  ("..." if len(Path(video_path_obj if video_path_obj else stem).name) > 30 else "")
                progress_bar.update(main_processing_task, advance=1, item_name=f"Processing: {item_name_short}")

                media_info = initial_media_infos.get(stem)
                if not media_info:
                    log.error(f"[{ProcessingStatus.INTERNAL_ERROR}] CRITICAL: Skipping batch '{stem}' due to missing MediaInfo object before individual processing.")
                    results_summary['error_batches'] += 1
                    continue

                log_base_info = f"Batch '{stem}': Type='{media_info.file_type}', API='{getattr(media_info.metadata, 'source_api', 'N/A')}', Score='{getattr(media_info.metadata, 'match_confidence', 'N/A')}'"
                if media_info.metadata_error_message: # Log if there was an initial metadata error
                    log_base_info += f", InitialMetaError='{media_info.metadata_error_message}'"
                    results_summary['meta_error_batches'] +=1 # Count all initial metadata issues
                log.debug(log_base_info)

                action_result, final_batch_had_error_flag, user_quit_processing = await self._process_single_batch(
                    stem, batch_data, media_info, run_batch_id, is_live_run
                )
                
                batch_msg_from_action = action_result.get('message', f"[{ProcessingStatus.INTERNAL_ERROR}] No message from batch processing for '{stem}'.")
                
                # If the batch processing itself indicated an error, use that message.
                # Otherwise, use the specific message from the action (which could be a success, skip, or handled error).
                primary_reason_for_log_and_console = batch_msg_from_action
                if final_batch_had_error_flag and not action_result.get('success'):
                    # If the batch handling failed, and there was an initial metadata error, prioritize the initial error for context.
                    if media_info.metadata_error_message and ProcessingStatus.INTERNAL_ERROR.name not in batch_msg_from_action:
                         primary_reason_for_log_and_console = media_info.metadata_error_message + " (Handling also failed: " + batch_msg_from_action + ")"
                    else: # Use the direct processing error message
                         primary_reason_for_log_and_console = batch_msg_from_action


                # Categorize based on the outcome of _process_single_batch
                if action_result.get('success', False) and not final_batch_had_error_flag:
                    if f"[{ProcessingStatus.SUCCESS.name}] MOVED (UNKNOWN)" in batch_msg_from_action.upper():
                        log.info(f"MOVED_TO_UNKNOWN: Batch '{stem}'. Actions: {action_result.get('actions_taken',0)}. Message: {batch_msg_from_action}")
                        results_summary['moved_unknown_files'] += action_result.get('actions_taken', 0)
                    elif ProcessingStatus.PATH_ALREADY_CORRECT.name in batch_msg_from_action or \
                         ProcessingStatus.PLAN_TARGET_EXISTS_SKIP_MODE.name in batch_msg_from_action:
                        log.info(f"SKIPPED (Benign): Batch '{stem}'. Reason: {batch_msg_from_action}")
                        results_summary['skipped_correct_or_conflict'] += 1
                    elif ProcessingStatus.USER_INTERACTIVE_SKIP.name in batch_msg_from_action:
                        log.info(f"SKIPPED (User): Batch '{stem}'. Reason: {batch_msg_from_action}")
                        results_summary['user_skipped_batches'] += 1
                    elif ProcessingStatus.UNKNOWN_HANDLING_CONFIG_SKIP.name in batch_msg_from_action:
                        log.info(f"SKIPPED (Config): Batch '{stem}'. Reason: {batch_msg_from_action}")
                        results_summary['config_skipped_batches'] += 1
                    else: # True successful rename/move
                        log.info(f"SUCCESS: Batch '{stem}'. Actions: {action_result.get('actions_taken',0)}. Message: {batch_msg_from_action}")
                        results_summary['success_renames_moves'] += 1
                else: # final_batch_had_error_flag is True or success is False without a specific skip
                    log.error(f"FAILED_PROCESSING: Batch '{stem}'. Final Reason: {primary_reason_for_log_and_console}")
                    if batch_msg_from_action != primary_reason_for_log_and_console and batch_msg_from_action and ProcessingStatus.INTERNAL_ERROR.name not in primary_reason_for_log_and_console:
                        log.info(f"  Detail/Action Outcome for Failed Batch '{stem}': {batch_msg_from_action}")
                    results_summary['error_batches'] += 1
                
                if is_live_run:
                    results_summary['actions_taken'] += action_result.get('actions_taken', 0)
                else: # Dry run
                    total_planned_actions_accumulator_for_dry_run += action_result.get('actions_taken', 0)


                # Console printing logic (can be refined based on the new categorization)
                should_print_to_console = bool(batch_msg_from_action)
                # Avoid printing for "path already correct" if not interactive and not an error
                if not self.args.interactive and ProcessingStatus.PATH_ALREADY_CORRECT.name in batch_msg_from_action and not final_batch_had_error_flag:
                    should_print_to_console = False
                
                if should_print_to_console:
                    use_rule = not self.args.interactive and is_live_run and action_result.get('success') and \
                               action_result.get('actions_taken',0) > 0 and \
                               not (f"[{ProcessingStatus.SUCCESS.name}] MOVED (UNKNOWN)" in batch_msg_from_action.upper())

                    if use_rule: self.console.print("-" * 70) 
                    
                    console_message_to_print = primary_reason_for_log_and_console
                    
                    style_for_text = "default" 
                    print_to_stderr_flag = False
                    
                    if final_batch_had_error_flag and not action_result.get('success'):
                        style_for_text = "red"; print_to_stderr_flag = True 
                    elif not action_result.get('success') or \
                         any(f"[{status.name}]" in console_message_to_print for status in [
                             ProcessingStatus.PLAN_TARGET_EXISTS_SKIP_MODE, 
                             ProcessingStatus.UNKNOWN_HANDLING_CONFIG_SKIP,
                             ProcessingStatus.USER_INTERACTIVE_SKIP,
                         ]):
                        style_for_text = "yellow"
                    elif action_result.get('success') and (action_result.get('actions_taken',0) > 0 or ProcessingStatus.PATH_ALREADY_CORRECT.name in batch_msg_from_action):
                         style_for_text = "green" # Explicitly green for success and path correct

                    message_renderable = TextClass(console_message_to_print, style=style_for_text)
                    
                    # ... (stderr printing logic as before) ...
                    if print_to_stderr_flag:
                        if RICH_AVAILABLE and isinstance(self.console, RichConsoleActual) and not getattr(self.args, 'quiet', False):
                            console_stderr_temp = RichConsoleActual(file=sys.stderr, width=self.console.width if hasattr(self.console, 'width') else None) # type: ignore
                            console_stderr_temp.print(message_renderable)
                        else:
                            self.console.print(message_renderable, file=sys.stderr)
                    else: 
                        self.console.print(message_renderable)

                    if use_rule: self.console.print("-" * 70) 

                if user_quit_processing:
                    break 

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
                 self.console.print(f"    - User interactive skip: {results_summary['user_skipped_batches']}")
            if results_summary['config_skipped_batches'] > 0:
                 self.console.print(f"    - Configured to skip (unknown/metadata fail): {results_summary['config_skipped_batches']}")

        # Report initial metadata issues separately from final batch processing errors
        if results_summary['meta_error_batches'] > 0 : 
            self.console.print(f"  Initial Metadata Fetch Issues (Batches): {results_summary['meta_error_batches']}")
        
        # Final 'error_batches' should now only reflect actual processing/file op failures
        if results_summary['error_batches'] > 0 : 
            error_summary_msg_content = f"Batches with Processing Errors: {results_summary['error_batches']}"
            # ... (stderr print for error_summary_msg_content) ...
            error_renderable = TextClass(error_summary_msg_content, style="bold red")
            if RICH_AVAILABLE and isinstance(self.console, RichConsoleActual) and not getattr(self.args, 'quiet', False):
                console_stderr_temp = RichConsoleActual(file=sys.stderr, width=self.console.width if hasattr(self.console, 'width') else None) # type: ignore
                console_stderr_temp.print(error_renderable)
            else:
                self.console.print(error_renderable, file=sys.stderr)
        elif results_summary['error_batches'] == 0 and not (results_summary['success_renames_moves'] > 0 or results_summary['moved_unknown_files'] > 0 or total_skipped > 0) :
             # This case should ideally not be hit if batch_count > 0
             self.console.print(f"  Batches with Errors: 0 (but no successful actions or skips recorded - check logic)")
        elif results_summary['error_batches'] == 0:
            self.console.print(f"  Batches with Processing Errors: 0")


        if is_live_run:
            self.console.print(f"  Total File System Actions Logged (files+dirs): {results_summary['actions_taken']}")
        else:
            self.console.print(f"  Total File Actions Planned (Dry Run): {total_planned_actions_accumulator_for_dry_run}")
        self.console.print("-" * 30)

        # ... (rest of the summary messages for dry run, undo, staging, and final status) ...
        if not is_live_run:
             if total_planned_actions_accumulator_for_dry_run > 0:
                 self.console.print("[yellow]DRY RUN COMPLETE. To apply changes, run again with --live[/yellow]")
             else:
                 self.console.print("DRY RUN COMPLETE. No actions were planned.")
        
        if is_live_run and self.cfg('enable_undo', False) and results_summary['actions_taken'] > 0:
            script_name = Path(sys.argv[0]).name
            self.console.print(f"Undo information logged with Run ID: [bold cyan]{run_batch_id}[/bold cyan]")
            self.console.print(f"To undo this run: {script_name} undo {run_batch_id}")
        
        if is_live_run and self.args.stage_dir and results_summary['actions_taken'] > 0 : # Check if any actions were taken for stage dir message
            self.console.print(f"Renamed files moved to staging: {self.args.stage_dir}")
                
        if results_summary['error_batches'] > 0:
            problem_msg_content = f"Operation finished with {results_summary['error_batches']} batches encountering processing errors. Check logs."
            # ... (stderr print for problem_msg_content) ...
            problem_renderable = TextClass(problem_msg_content, style="bold red")
            if RICH_AVAILABLE and isinstance(self.console, RichConsoleActual) and not getattr(self.args, 'quiet', False):
                console_stderr_temp = RichConsoleActual(file=sys.stderr, width=self.console.width if hasattr(self.console, 'width') else None) # type: ignore
                console_stderr_temp.print(problem_renderable)
            else:
                self.console.print(problem_renderable, file=sys.stderr)

        elif results_summary['success_renames_moves'] == 0 and results_summary['moved_unknown_files'] == 0 and total_skipped == batch_count:
             self.console.print("Operation finished. All batches were skipped (e.g. already correct, or by config/user choice).")
        elif results_summary['success_renames_moves'] > 0 or results_summary['moved_unknown_files'] > 0 :
             self.console.print("[green]Operation finished successfully.[/green]")
             if total_skipped > 0:
                 self.console.print(f"[yellow] ({total_skipped} batches were skipped for various reasons).[/yellow]")
        else: # No successes, no moves, not all skipped, but no errors - unusual
             self.console.print("Operation finished. (No explicit success, errors, or all skips recorded - check logs for details).")