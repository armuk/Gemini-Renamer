import logging
import uuid
import sys
import asyncio
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Tuple, Optional, Dict, Any, cast, List

# Rich imports and fallbacks (ensure these are complete as in your original file)
import builtins
try:
    from rich.progress import (
        Progress, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn, TaskID
    )
    from rich.console import Console
    from rich.text import Text
    from rich.prompt import Prompt, Confirm, InvalidResponse
    from rich.panel import Panel
    from rich.table import Table
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False; TaskID = int #type: ignore
    class Progress: #type: ignore
        def __init__(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def add_task(self, description, total=None, start=True, **fields): return 0
        def update(self, task_id, advance=1, description=None, **fields): pass
        def stop(self): pass
    class Console: #type: ignore
        def __init__(self, *args, **kwargs): pass
        def print(self, *args, **kwargs): builtins.print(*args, **kwargs)
        def input(self, *args, **kwargs) -> str: return builtins.input(*args, **kwargs) #type: ignore
    class Prompt: #type: ignore
        @staticmethod
        def ask(*args, **kwargs): return builtins.input(args[0]) #type: ignore
    class Confirm: #type: ignore
         @staticmethod
         def ask(*args, **kwargs): return builtins.input(args[0]).lower() == 'y' #type: ignore
    class Panel: #type: ignore
         def __init__(self, content, *args, **kwargs): self.content = content
         def __rich_console__(self, console, options): yield str(self.content) 
    class Table: #type: ignore
        def __init__(self, *args, **kwargs): pass
        def add_column(self, *args, **kwargs): pass
        def add_row(self, *args, **kwargs): pass
    class Text: #type: ignore
        def __init__(self, text="", style=""): self.text = text; self.style = style
        def __str__(self): return self.text
        @property
        def plain(self): return self.text 
    class InvalidResponse(Exception): pass #type: ignore
    class BarColumn: pass #type: ignore
    class TextColumn: pass #type: ignore
    class TimeElapsedColumn: pass #type: ignore
    class MofNCompleteColumn: pass #type: ignore


from .metadata_fetcher import MetadataFetcher
from .renamer_engine import RenamerEngine
from .file_system_ops import perform_file_actions, _handle_conflict, FileOperationError
from .utils import scan_media_files
from .exceptions import UserAbortError, RenamerError, MetadataError
from .models import MediaInfo, RenamePlan, MediaMetadata
from .api_clients import get_tmdb_client, get_tvdb_client
from .enums import ProcessingStatus

log = logging.getLogger(__name__)

DEFAULT_PROGRESS_COLUMNS = (
    TextColumn("[progress.description]{task.description}"),
    BarColumn(),
    MofNCompleteColumn(),
    TimeElapsedColumn(),
    TextColumn("[cyan]{task.fields[item_name]}"),
)

async def _fetch_metadata_for_batch(
    batch_stem: str,
    batch_data: Dict[str, Any],
    processor: "MainProcessor",
    progress: Optional[Progress] = None,
    task_id: Optional[TaskID] = None
) -> Tuple[str, MediaInfo]:
    # (Function unchanged)
    video_path_for_media_info = batch_data.get('video')
    if not video_path_for_media_info:
        log.error(f"CRITICAL in _fetch_metadata_for_batch: video_path is None for stem '{batch_stem}'. Using dummy.")
        error_media_info = MediaInfo(original_path=Path(f"error_dummy_{batch_stem}.file"))
        error_media_info.file_type = 'unknown'
        error_media_info.metadata_error_message = f"[{ProcessingStatus.MISSING_VIDEO_FILE_IN_BATCH}] Missing video path for batch."
        return batch_stem, error_media_info
    media_info = MediaInfo(original_path=video_path_for_media_info); media_info.metadata = None
    item_name_short = media_info.original_path.name[:30] + ("..." if len(media_info.original_path.name) > 30 else "")
    if progress and task_id is not None and progress.tasks:
        if task_id < len(progress.tasks) and not progress.tasks[task_id].finished: #type: ignore
            try: progress.update(task_id, item_name=f"fetching: {item_name_short}")
            except Exception as e_prog_update: log.error(f"Error updating progress bar item name in fetch: {e_prog_update}")
    try:
        media_info.guess_info = processor.renamer.parse_filename(media_info.original_path)
        original_file_type_from_guessit = processor.renamer._determine_file_type(media_info.guess_info)
        media_info.file_type = original_file_type_from_guessit
        use_metadata_cfg = processor.cfg('use_metadata', False)
        if use_metadata_cfg and processor.metadata_fetcher and media_info.file_type != 'unknown':
            log.debug(f"Attempting async metadata fetch for '{batch_stem}' ({media_info.file_type})")
            year_guess = media_info.guess_info.get('year'); fetched_api_metadata: Optional[MediaMetadata] = None
            try:
                if media_info.file_type == 'series':
                    raw_episode_data = None; valid_ep_list = []
                    if isinstance(media_info.guess_info.get('episode_list'), list): raw_episode_data = media_info.guess_info['episode_list']
                    elif 'episode' in media_info.guess_info: raw_episode_data = media_info.guess_info['episode']
                    elif 'episode_number' in media_info.guess_info: raw_episode_data = media_info.guess_info['episode_number']
                    if raw_episode_data is not None:
                        if not isinstance(raw_episode_data, list): raw_episode_data = [raw_episode_data]
                        for ep in raw_episode_data:
                            try:
                                ep_int = int(str(ep))
                                if ep_int > 0: valid_ep_list.append(ep_int)
                            except (ValueError, TypeError): log.warning(f"Could not parse episode '{ep}' from guessit for '{batch_stem}'.")
                    valid_ep_list = sorted(list(set(valid_ep_list)))
                    log.debug(f"Final valid episode list for API call for '{batch_stem}': {valid_ep_list}")
                    guessed_title_raw = media_info.guess_info.get('title')
                    if isinstance(guessed_title_raw, list) and guessed_title_raw: guessed_title = str(guessed_title_raw[0]) if guessed_title_raw[0] else media_info.original_path.stem
                    elif isinstance(guessed_title_raw, str) and guessed_title_raw: guessed_title = guessed_title_raw
                    else: guessed_title = media_info.original_path.stem; log.debug(f"Guessed title empty for series '{batch_stem}', using stem: '{guessed_title}'")
                    if valid_ep_list:
                        fetched_api_metadata = await processor.metadata_fetcher.fetch_series_metadata(
                            show_title_guess=guessed_title, season_num=media_info.guess_info.get('season', 0),
                            episode_num_list=tuple(valid_ep_list), year_guess=year_guess )
                    else: log.warning(f"No valid episode numbers for series '{batch_stem}'. Skipping series metadata fetch.")
                elif media_info.file_type == 'movie':
                    guessed_title_raw = media_info.guess_info.get('title')
                    if isinstance(guessed_title_raw, list) and guessed_title_raw: guessed_title = str(guessed_title_raw[0]) if guessed_title_raw[0] else media_info.original_path.stem
                    elif isinstance(guessed_title_raw, str) and guessed_title_raw: guessed_title = guessed_title_raw
                    else: guessed_title = media_info.original_path.stem; log.debug(f"Guessed title empty for movie '{batch_stem}', using stem: '{guessed_title}'")
                    fetched_api_metadata = await processor.metadata_fetcher.fetch_movie_metadata(movie_title_guess=guessed_title, year_guess=year_guess)
                media_info.metadata = fetched_api_metadata
            except MetadataError as me:
                log.error(f"Caught MetadataError for '{batch_stem}': {me}")
                media_info.metadata_error_message = str(me) # Already contains ProcessingStatus
                media_info.metadata = None
            except Exception as fetch_e:
                log.exception(f"Unexpected error during metadata API call for '{batch_stem}': {fetch_e}")
                media_info.metadata_error_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Unexpected fetch error: {fetch_e}"
                media_info.metadata = None
            if media_info.metadata is None or not media_info.metadata.source_api:
                if not media_info.metadata_error_message: 
                    media_info.metadata_error_message = f"[{ProcessingStatus.METADATA_NO_MATCH}] Metadata fetch returned no usable API data."
        return batch_stem, media_info
    except Exception as e:
        log.exception(f"Critical error in _fetch_metadata_for_batch for '{batch_stem}': {e}")
        if not hasattr(media_info, 'guess_info') or not media_info.guess_info: media_info.guess_info = {}
        media_info.file_type = 'unknown'; media_info.metadata = None
        media_info.metadata_error_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Processing error in _fetch_metadata_for_batch: {e}"
        return batch_stem, media_info
    finally:
        if progress and task_id is not None:
            if progress.tasks and task_id < len(progress.tasks) and not progress.tasks[task_id].finished: #type: ignore
                 progress.update(task_id, advance=1, item_name="")


class MainProcessor:
    def __init__(self, args, cfg_helper, undo_manager):
        self.args = args; self.cfg = cfg_helper; self.undo_manager = undo_manager
        self.renamer = RenamerEngine(cfg_helper); self.metadata_fetcher = None
        self.console = Console()
        use_metadata_effective = getattr(args, 'use_metadata', False) 
        if use_metadata_effective:
             log.info("Metadata fetching enabled for MainProcessor.")
             if get_tmdb_client() or get_tvdb_client(): self.metadata_fetcher = MetadataFetcher(cfg_helper)
             else:
                 log.warning("Metadata enabled but no API clients initialized. Disabling fetcher.")
                 setattr(self.args, 'use_metadata', False) 
        else: log.info("Metadata fetching disabled for MainProcessor.")

    def _display_plan_for_confirmation(self, plan: RenamePlan, media_info: MediaInfo):
        # (Function unchanged)
        if not plan or plan.status != 'success':
            self.console.print(f"[yellow]No valid rename plan generated for {media_info.original_path.name}.[/yellow]")
            if plan and plan.message: self.console.print(f"[yellow]Reason: {plan.message}[/yellow]")
            return
        panel_content = []; panel_content.append(f"[bold]File:[/bold] {media_info.original_path.name}")
        if media_info.metadata and media_info.metadata.source_api:
            source_info = f"[i]via {media_info.metadata.source_api.upper()}"
            score = getattr(media_info.metadata, 'match_confidence', None)
            if isinstance(score, float):
                score_color = "green" if score >= 85 else "yellow" if score >= self.cfg('tmdb_match_fuzzy_cutoff', 70) else "red"
                source_info += f" (Score: [{score_color}]{score:.1f}%[/])"
            source_info += "[/i]"; panel_content.append(f"[bold]Type:[/bold] {media_info.file_type.capitalize()} {source_info}")
            if media_info.metadata.is_series:
                title = media_info.metadata.show_title or "[missing]"
                year = f"({media_info.metadata.show_year})" if media_info.metadata.show_year else ""
                ep_list = media_info.metadata.episode_list; ep_num = ep_list[0] if ep_list else 0
                ep_title = media_info.metadata.episode_titles.get(ep_num, "[missing]")
                season_num = media_info.metadata.season if media_info.metadata.season is not None else 0
                panel_content.append(f"[bold]Match:[/bold] {title} {year} - S{season_num:02d}E{ep_num:02d} - {ep_title}")
            elif media_info.metadata.is_movie:
                title = media_info.metadata.movie_title or "[missing]"
                year = f"({media_info.metadata.movie_year})" if media_info.metadata.movie_year else ""
                panel_content.append(f"[bold]Match:[/bold] {title} {year}")
        else:
             panel_content.append(f"[bold]Type:[/bold] {media_info.file_type.capitalize()} ([i]via Guessit[/i])")
             panel_content.append(f"[bold]Guess:[/bold] {media_info.guess_info.get('title', media_info.original_path.stem)}")
        panel_content.append("\n[bold cyan]Proposed Actions:[/bold cyan]"); table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column("Original"); table.add_column("Arrow", justify="center"); table.add_column("New")
        if plan.created_dir_path:
             dir_path_str = str(plan.created_dir_path).replace("\\", "/")
             table.add_row("[dim]-[/dim]", "[dim]->[/dim]", f"[green]{dir_path_str}[/green] [i](Create Dir)[/i]")
        for action_item in plan.actions:
             action_style = "blue" if action_item.action_type == 'move' else "default"
             new_path_str = str(action_item.new_path).replace("\\", "/")
             table.add_row(f"{action_item.original_path.name}", f"[{action_style}]->[/]", f"[{action_style}]{new_path_str}[/]")
        panel_content.append(table)
        self.console.print(Panel("\n".join(str(c) for c in panel_content), title="[yellow]Confirm Batch Action", border_style="yellow"))

    async def _refetch_with_manual_id(self, media_info: MediaInfo, api_source: str, manual_id: int) -> Optional[MediaMetadata]:
        # (Function unchanged)
        if not self.metadata_fetcher: self.console.print("[red]Error: Metadata fetcher not initialized.[/red]"); return None
        log.info(f"Attempting re-fetch for '{media_info.original_path.name}' using {api_source.upper()} ID: {manual_id}")
        new_metadata: Optional[MediaMetadata] = None; current_lang = self.cfg('tmdb_language', 'en')
        try:
            if api_source == 'tmdb':
                if media_info.file_type == 'movie':
                    self.console.print(f"[yellow]Re-fetching TMDB movie details for ID {manual_id}...[/yellow]")
                    title_guess = media_info.guess_info.get('title', media_info.original_path.stem); year_guess = media_info.guess_info.get('year')
                    new_metadata = await self.metadata_fetcher.fetch_movie_metadata(title_guess, year_guess) 
                    if not new_metadata or new_metadata.ids.get('tmdb_id') != manual_id:
                        log.warning(f"Re-fetch for TMDB ID {manual_id} didn't return the expected movie."); new_metadata = None
                elif media_info.file_type == 'series':
                    self.console.print(f"[yellow]Re-fetching TMDB series details for ID {manual_id}...[/yellow]")
                    title_guess = media_info.guess_info.get('title', media_info.original_path.stem); season_guess = media_info.guess_info.get('season', 0)
                    ep_list_guess = media_info.guess_info.get('episode_list', [media_info.guess_info.get('episode')])
                    valid_ep_list = tuple(ep for ep in ep_list_guess if isinstance(ep, int) and ep > 0) if ep_list_guess else tuple()
                    year_guess = media_info.guess_info.get('year')
                    new_metadata = await self.metadata_fetcher.fetch_series_metadata(title_guess, season_guess, valid_ep_list, year_guess)
                    if not new_metadata or new_metadata.ids.get('tmdb_id') != manual_id:
                        log.warning(f"Re-fetch for TMDB ID {manual_id} didn't return the expected series."); new_metadata = None
            elif api_source == 'tvdb':
                 if media_info.file_type == 'series':
                    self.console.print(f"[yellow]Re-fetching TVDB series details for ID {manual_id}...[/yellow]")
                    title_guess = media_info.guess_info.get('title', media_info.original_path.stem); season_guess = media_info.guess_info.get('season', 0)
                    ep_list_guess = media_info.guess_info.get('episode_list', [media_info.guess_info.get('episode')])
                    valid_ep_list = tuple(ep for ep in ep_list_guess if isinstance(ep, int) and ep > 0) if ep_list_guess else tuple()
                    year_guess = media_info.guess_info.get('year')
                    new_metadata = await self.metadata_fetcher.fetch_series_metadata(title_guess, season_guess, valid_ep_list, year_guess) 
                    if not new_metadata or new_metadata.ids.get('tvdb_id') != manual_id:
                         log.warning(f"Re-fetch using TVDB ID {manual_id} didn't result in metadata with that ID."); new_metadata = None
                 else: self.console.print("[red]TVDB ID only applicable for Series.[/red]"); return None
            else: self.console.print(f"[red]Unsupported API source: {api_source}[/red]"); return None
            if new_metadata and new_metadata.source_api:
                 self.console.print(f"[green]Successfully re-fetched metadata from {new_metadata.source_api.upper()}.[/green]"); return new_metadata
            else: self.console.print(f"[red]Failed to fetch valid metadata using {api_source.upper()} ID {manual_id}.[/red]"); return None
        except Exception as e:
            log.exception(f"Error during manual ID re-fetch ({api_source} ID {manual_id}): {e}"); self.console.print(f"[red]Error during re-fetch: {e}[/red]"); return None

    def _confirm_live_run(self, potential_actions_count):
        # (Function unchanged)
        if potential_actions_count == 0:
            log.warning("Pre-scan found no files eligible for action based on current settings.")
            self.console.print("[yellow]Pre-scan found no files eligible for action based on current settings.[/yellow]"); return False
        self.console.print("-" * 30); self.console.print(f"Pre-scan found {potential_actions_count} potential file actions.")
        self.console.print("[bold red]THIS IS A LIVE RUN.[/bold red]")
        if self.args.backup_dir: self.console.print(f"Originals will be backed up to: {self.args.backup_dir}")
        elif self.args.stage_dir: self.console.print(f"Files will be MOVED to staging: {self.args.stage_dir}")
        elif getattr(self.args, 'trash', False): self.console.print("Originals will be MOVED TO TRASH.")
        else: self.console.print("Files will be RENAMED/MOVED IN PLACE.")
        if self.cfg('enable_undo', False): self.console.print("Undo logging is [green]ENABLED[/green].")
        else: self.console.print("Undo logging is [yellow]DISABLED[/yellow].")
        self.console.print("-" * 30)
        try:
            if Confirm.ask("Proceed with actions?", default=False): log.info("User confirmed live run."); return True
            else: log.info("User aborted live run."); self.console.print("Operation cancelled by user."); return False
        except EOFError:
             log.error("Cannot confirm live run in non-interactive mode without confirmation (EOF).")
             self.console.print("\n[bold red]ERROR: Cannot confirm. Run interactively or use force flag (not implemented).[/bold red]"); return False
        except Exception as e:
            log.error(f"Error during live run confirmation: {e}", exc_info=True)
            self.console.print(f"\n[bold red]ERROR: Could not get confirmation: {e}[/bold red]"); return False

    # --- MODIFIED: _handle_move_to_unknown ---
    def _handle_move_to_unknown(self, batch_stem: str, batch_data: Dict[str, Any], run_batch_id: str) -> Dict[str, Any]:
        results = {'move_success': False, 'message': "", 'actions_taken': 0, 'fs_errors': 0}
        action_messages: List[str] = []
        unknown_dir_str = self.args.unknown_files_dir
        
        base_message_prefix = f"Batch '{batch_stem}': "

        if not unknown_dir_str:
            msg = f"[{ProcessingStatus.CONFIG_MISSING_FORMAT_STRING}] {base_message_prefix}Unknown files directory not configured. Skipping move."
            log.error(msg)
            results['message'] = msg; results['fs_errors'] += 1
            return results

        base_target_dir = self.args.directory.resolve()
        unknown_target_dir = Path(unknown_dir_str)
        if not unknown_target_dir.is_absolute(): unknown_target_dir = base_target_dir / unknown_dir_str
        unknown_target_dir = unknown_target_dir.resolve()

        log.info(f"Handling unknown/failed batch '{batch_stem}': Moving files to '{unknown_target_dir}'")
        is_live_run = getattr(self.args, 'live', False)

        if not is_live_run:
            dry_run_actions_count = 0
            if not unknown_target_dir.exists():
                action_messages.append(f"DRY RUN: [{ProcessingStatus.SUCCESS}] Would create directory '{unknown_target_dir}'")
                dry_run_actions_count += 1
            all_files_in_batch = [batch_data.get('video')] + batch_data.get('associated', [])
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
        except OSError as e:
            msg = f"[{ProcessingStatus.FILE_OPERATION_ERROR}] {base_message_prefix}Could not create directory '{unknown_target_dir}': {e}"
            log.error(msg, exc_info=True); results['message'] = msg; results['fs_errors'] += 1
            return results

        conflict_mode = self.cfg('on_conflict', 'skip')
        files_to_move = [batch_data.get('video')] + batch_data.get('associated', [])
        files_moved_successfully = 0; files_to_move_count = 0

        for original_file_path in files_to_move:
            if not original_file_path or not isinstance(original_file_path, Path): continue
            if not original_file_path.exists(): log.warning(f"Skipping move of non-existent file: {original_file_path}"); continue
            files_to_move_count += 1
            target_file_path_in_unknown_dir = unknown_target_dir / original_file_path.name
            try:
                final_target_path = _handle_conflict(original_file_path, target_file_path_in_unknown_dir, conflict_mode)
                if self.undo_manager.is_enabled:
                    self.undo_manager.log_action(batch_id=run_batch_id, original_path=original_file_path, new_path=final_target_path, item_type='file', status='moved') # 'moved' is appropriate here
                log.debug(f"Moving '{original_file_path.name}' to '{final_target_path}' for unknown handling.")
                shutil.move(str(original_file_path), str(final_target_path))
                action_messages.append(f"[{ProcessingStatus.SUCCESS}] MOVED (unknown): '{original_file_path.name}' to '{final_target_path}'")
                results['actions_taken'] += 1; files_moved_successfully += 1
            except FileExistsError as e_fe: 
                msg = f"[{ProcessingStatus.PLAN_TARGET_EXISTS_FAIL_MODE}] ERROR (move unknown): {e_fe} - File '{original_file_path.name}' not moved."
                log.error(msg); action_messages.append(msg); results['fs_errors'] += 1
            except FileOperationError as e_foe: 
                msg = f"[{ProcessingStatus.PLAN_TARGET_EXISTS_SKIP_MODE}] SKIPPED (move unknown): {e_foe} - File '{original_file_path.name}' not moved."
                log.warning(msg); action_messages.append(msg)
            except OSError as e_os:
                msg = f"[{ProcessingStatus.FILE_OPERATION_ERROR}] ERROR (move unknown): Failed to move '{original_file_path.name}': {e_os}"
                log.error(msg, exc_info=True); action_messages.append(msg); results['fs_errors'] += 1
            except Exception as e_generic:
                msg = f"[{ProcessingStatus.INTERNAL_ERROR}] ERROR (move unknown): Unexpected error for '{original_file_path.name}': {e_generic}"
                log.exception(msg); action_messages.append(msg); results['fs_errors'] += 1
        
        results['move_success'] = (files_to_move_count > 0 and files_moved_successfully == files_to_move_count) and (results['fs_errors'] == 0)
        if not action_messages: action_messages.append(f"[{ProcessingStatus.SKIPPED}] {base_message_prefix}No files moved to unknown.")
        results['message'] = "\n".join(action_messages)
        return results
    
    def _perform_prescan(self, file_batches: Dict[str, Dict[str, Any]], batch_count: int) -> int:
        # (Function unchanged)
        log.info("Performing synchronous pre-scan for live run confirmation...")
        potential_actions_count = 0; disable_rich_progress = self.args.interactive or not RICH_AVAILABLE
        with Progress(*DEFAULT_PROGRESS_COLUMNS, console=self.console, disable=disable_rich_progress) as progress:
            prescan_task = progress.add_task("Pre-scan", total=batch_count, item_name="")
            for stem, batch_data in file_batches.items():
                item_name_short = Path(batch_data.get('video', stem)).name[:30] + ("..." if len(Path(batch_data.get('video', stem)).name) > 30 else "")
                progress.update(prescan_task, advance=1, item_name=item_name_short)
                try:
                    if not batch_data.get('video'): continue
                    video_path = cast(Path, batch_data['video'])
                    media_info_prescan = MediaInfo(original_path=video_path)
                    media_info_prescan.guess_info = self.renamer.parse_filename(media_info_prescan.original_path)
                    media_info_prescan.file_type = self.renamer._determine_file_type(media_info_prescan.guess_info)
                    media_info_prescan.metadata = None 
                    if media_info_prescan.file_type == 'unknown':
                        unknown_handling_mode_prescan = self.cfg('unknown_file_handling', 'skip', arg_value=getattr(self.args, 'unknown_file_handling', None))
                        if unknown_handling_mode_prescan == 'move_to_unknown': potential_actions_count += 1 + len(batch_data.get('associated', []))
                        elif unknown_handling_mode_prescan == 'guessit_only':
                            plan = self.renamer.plan_rename(video_path, batch_data.get('associated', []), media_info_prescan)
                            if plan.status == 'success': potential_actions_count += len(plan.actions) + (1 if plan.created_dir_path else 0)
                    else:
                        plan = self.renamer.plan_rename(video_path, batch_data.get('associated', []), media_info_prescan)
                        if plan.status == 'success': potential_actions_count += len(plan.actions) + (1 if plan.created_dir_path else 0)
                except Exception as e: log.warning(f"Pre-scan planning error for batch '{stem}': {e}", exc_info=True)
        return potential_actions_count

    def _perform_initial_parsing(self, file_batches: Dict[str, Dict[str, Any]], batch_count: int) -> Dict[str, Optional[MediaInfo]]:
        # (Function unchanged)
        initial_media_infos: Dict[str, Optional[MediaInfo]] = {}; log.info("Performing initial file parsing...")
        disable_rich_progress = self.args.interactive or not RICH_AVAILABLE
        with Progress(*DEFAULT_PROGRESS_COLUMNS, console=self.console, disable=disable_rich_progress) as progress:
            parse_task = progress.add_task("Parsing Filenames", total=batch_count, item_name="")
            for stem, batch_data in file_batches.items():
                item_name_short = Path(batch_data.get('video', stem)).name[:30] + ("..." if len(Path(batch_data.get('video', stem)).name) > 30 else "")
                progress.update(parse_task, advance=1, item_name=item_name_short)
                video_path = batch_data.get('video')
                if not video_path: initial_media_infos[stem] = None; continue
                media_info_obj = MediaInfo(original_path=cast(Path, video_path))
                try:
                    media_info_obj.guess_info = self.renamer.parse_filename(media_info_obj.original_path)
                    media_info_obj.file_type = self.renamer._determine_file_type(media_info_obj.guess_info)
                    initial_media_infos[stem] = media_info_obj
                except Exception as e_parse: log.error(f"Error parsing '{stem}': {e_parse}"); initial_media_infos[stem] = None
        return initial_media_infos

    async def _fetch_all_metadata( self, file_batches: Dict[str, Dict[str, Any]], initial_media_infos: Dict[str, Optional[MediaInfo]] ) -> Dict[str, Optional[MediaInfo]]:
        # (Function unchanged)
        use_metadata_effective = getattr(self.args, 'use_metadata', False)
        if not (use_metadata_effective and self.metadata_fetcher):
            log.info("Metadata fetching disabled or fetcher not available. Skipping metadata phase.")
            return initial_media_infos
        stems_to_fetch = [ stem for stem, info in initial_media_infos.items() if info and info.file_type != 'unknown' ]
        log.info(f"Creating {len(stems_to_fetch)} tasks for concurrent metadata fetching...")
        if not stems_to_fetch: log.info("No batches required metadata fetching."); return initial_media_infos
        fetch_tasks: List[asyncio.Task] = []; disable_rich_progress = self.args.interactive or not RICH_AVAILABLE
        with Progress(*DEFAULT_PROGRESS_COLUMNS, console=self.console, disable=disable_rich_progress) as progress_bar:
            metadata_overall_task = progress_bar.add_task("Fetching Metadata", total=len(stems_to_fetch), item_name="")
            for stem in stems_to_fetch:
                batch_data = file_batches[stem]
                task = asyncio.create_task( _fetch_metadata_for_batch(stem, batch_data, self, progress_bar, metadata_overall_task), name=f"fetch_{stem}" )
                fetch_tasks.append(task)
            completed_fetch_results_tuples: List[Tuple[str, MediaInfo]] = []
            for f_task in asyncio.as_completed(fetch_tasks):
                try: completed_fetch_results_tuples.append(await f_task)
                except Exception as e_async_task: log.error(f"Async metadata task failed: {e_async_task}")
            if progress_bar.tasks and metadata_overall_task < len(progress_bar.tasks) and \
               not progress_bar.tasks[metadata_overall_task].finished: #type: ignore
                progress_bar.update(metadata_overall_task, completed=len(stems_to_fetch), item_name="") #type: ignore
        for result_item in completed_fetch_results_tuples:
            if isinstance(result_item, tuple) and len(result_item) == 2:
                stem_from_task, updated_media_info_obj = result_item
                if updated_media_info_obj: initial_media_infos[stem_from_task] = updated_media_info_obj
                else:
                    log.error(f"Async task for {stem_from_task} returned None for MediaInfo object")
                    if initial_media_infos.get(stem_from_task): initial_media_infos[stem_from_task].metadata_error_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Async task returned None" #type: ignore
                    else: log.warning(f"Stem {stem_from_task} not found after async failure.")
        return initial_media_infos
    
    # --- MODIFIED: _process_single_batch ---
    async def _process_single_batch(
        self,
        stem: str,
        batch_data: Dict[str, Any],
        media_info: MediaInfo,
        run_batch_id: str,
        is_live_run: bool
    ) -> Tuple[Dict[str, Any], bool, bool]:
        action_result: Dict[str, Any] = {'success': False, 'message': '', 'actions_taken': 0}
        batch_had_error = False; user_quit = False
        plan: Optional[RenamePlan] = None
        video_file_path = cast(Path, batch_data.get('video'))
        use_metadata_effective = getattr(self.args, 'use_metadata', False)
        metadata_failed = use_metadata_effective and bool(media_info.metadata_error_message)
        proceed_with_normal_planning = False
        
        current_status_message = f"Processing batch '{stem}'" 

        # --- Get unknown_handling_mode from config ---
        unknown_handling_mode = self.cfg('unknown_file_handling', 'skip', arg_value=getattr(self.args, 'unknown_file_handling', None))
        # --- End get ---

        if metadata_failed:
            batch_had_error = True
            current_status_message = media_info.metadata_error_message or f"[{ProcessingStatus.METADATA_FETCH_API_ERROR}] Metadata error for '{video_file_path.name}' (unknown details)."
            if not self.args.interactive:
                 self.console.print(Panel(f"[bold red]API Error:[/bold red] {current_status_message}", title=f"[yellow]'{media_info.original_path.name}'[/yellow]", border_style="red"))

        # unknown_handling_mode is now defined above
        if media_info.file_type == 'unknown' or metadata_failed:
            handling_reason = "unknown type" if media_info.file_type == 'unknown' else "metadata fetch failed"
            log.info(f"Batch '{stem}' (type: {media_info.file_type}) handled via '{unknown_handling_mode}' due to: {handling_reason}.")

            if unknown_handling_mode == 'skip':
                skip_msg = current_status_message if metadata_failed else \
                           f"[{ProcessingStatus.UNKNOWN_HANDLING_CONFIG_SKIP}] Skipped ({handling_reason}): {video_file_path.name}"
                plan = RenamePlan(batch_id=f"skip_{stem}", video_file=video_file_path, status='skipped', message=skip_msg)
                action_result['message'] = plan.message
                if media_info.file_type == 'unknown' and not metadata_failed:
                    batch_had_error = False 
            elif unknown_handling_mode == 'move_to_unknown':
                move_result = self._handle_move_to_unknown(stem, batch_data, run_batch_id)
                action_result['actions_taken'] = move_result.get('actions_taken', 0)
                action_result['message'] = move_result.get('message', '')
                if not move_result.get('move_success', False): batch_had_error = True
            elif unknown_handling_mode == 'guessit_only':
                log.debug(f"Proceeding with guessit_only planning for '{stem}' due to {handling_reason}.")
                media_info.metadata = None 
                proceed_with_normal_planning = True
                batch_had_error = False 
            else: 
                log.error(f"Invalid unknown_handling_mode '{unknown_handling_mode}'. Skipping.")
                skip_msg = f"[{ProcessingStatus.INTERNAL_ERROR}] Skipped (invalid unknown_handling_mode '{unknown_handling_mode}'): {video_file_path.name}"
                plan = RenamePlan(batch_id=f"error_{stem}", video_file=video_file_path, status='skipped', message=skip_msg)
                action_result['message'] = plan.message; batch_had_error = True
            
            if not proceed_with_normal_planning:
                 return action_result, batch_had_error, user_quit
        else: 
            proceed_with_normal_planning = True

        try:
            if proceed_with_normal_planning: 
                plan = self.renamer.plan_rename(video_file_path, batch_data.get('associated', []), media_info)
            
            user_choice = 'y' 
            current_plan_to_action = plan

            if self.args.interactive and is_live_run and current_plan_to_action and \
               current_plan_to_action.status in ['success', 'conflict_unresolved']: 
                confirm_threshold = self.cfg('confirm_match_below', default_value=None, arg_value=getattr(self.args, 'confirm_match_below', None))
                while True:
                    self._display_plan_for_confirmation(current_plan_to_action, media_info) #type: ignore
                    show_confidence_warning = False 
                    try:
                        choice = Prompt.ask("Apply? ([y]es/[n]o/[s]kip, [q]uit, [g]uessit only, [m]anual ID)", choices=["y", "n", "s", "q", "g", "m"]).lower() #type: ignore
                        if choice == 'y': user_choice = 'y'; break
                        elif choice in ('n', 's'): user_choice = 's'; break
                        elif choice == 'q': user_quit = True; raise UserAbortError(f"[{ProcessingStatus.USER_ABORTED_OPERATION}] User quit during interactive mode.")
                        elif choice == 'g':
                            self.console.print("[cyan]Re-planning using Guessit data only...[/cyan]")
                            media_info.metadata = None 
                            current_plan_to_action = self.renamer.plan_rename(media_info.original_path, batch_data.get('associated', []), media_info)
                        elif choice == 'm': 
                            api_pref = self.cfg('series_metadata_preference', ['tmdb','tvdb']) if media_info.file_type == 'series' else ['tmdb']
                            api_to_try = Prompt.ask("Enter API Source for Manual ID", choices=api_pref, default=api_pref[0]).lower() #type: ignore
                            try:
                                manual_id_str = Prompt.ask(f"Enter {api_to_try.upper()} ID") #type: ignore
                                if not manual_id_str.isdigit(): raise ValueError("ID must be numeric.")
                                new_meta = await self._refetch_with_manual_id(media_info, api_to_try, int(manual_id_str))
                                if new_meta:
                                    media_info.metadata = new_meta
                                    media_info.metadata_error_message = None 
                                    current_plan_to_action = self.renamer.plan_rename(media_info.original_path, batch_data.get('associated', []), media_info)
                                else: self.console.print(f"[red]Manual ID fetch failed. Original plan stands.[/red]")
                            except (ValueError, InvalidResponse) as e_mid: self.console.print(f"[red]Invalid ID: {e_mid}[/red]") #type: ignore
                    except EOFError: user_quit = True; raise UserAbortError(f"[{ProcessingStatus.USER_ABORTED_OPERATION}] User quit (EOF) during interactive mode.")
                    except InvalidResponse: self.console.print("[red]Invalid choice.[/red]") #type: ignore

            if user_quit: return action_result, True, user_quit 

            if current_plan_to_action and current_plan_to_action.status == 'success' and user_choice == 'y':
                action_result = perform_file_actions(
                    plan=current_plan_to_action, args_ns=self.args, cfg_helper=self.cfg,
                    undo_manager=self.undo_manager, run_batch_id=run_batch_id, media_info=media_info )
            elif user_choice == 's': 
                action_result['success'] = False 
                action_result['message'] = f"[{ProcessingStatus.USER_INTERACTIVE_SKIP}] User skipped batch '{stem}'."
                log.info(action_result['message'])
            elif current_plan_to_action and current_plan_to_action.message:
                action_result['success'] = False
                action_result['message'] = current_plan_to_action.message
                if current_plan_to_action.status not in ['skipped', 'success']: batch_had_error = True
            elif not current_plan_to_action: 
                 action_result['success'] = False
                 action_result['message'] = f"[{ProcessingStatus.INTERNAL_ERROR}] No plan generated for batch '{stem}'."
                 batch_had_error = True
            if not action_result.get('message'):
                 action_result['message'] = f"[{ProcessingStatus.SKIPPED}] No action performed for batch '{stem}'."

        except UserAbortError as e: 
            log.warning(str(e)); self.console.print(f"\n{e}")
            action_result['message'] = str(e) 
            batch_had_error = True; user_quit = True 
        except FileExistsError as e: 
            log.critical(str(e)); self.console.print(f"\n[bold red]STOPPING: {e}[/bold red]")
            action_result['message'] = f"[{ProcessingStatus.PLAN_TARGET_EXISTS_FAIL_MODE}] {e}"
            batch_had_error = True; user_quit = True 
        except RenamerError as e: 
            log.error(f"RenamerError processing batch '{stem}': {e}", exc_info=False)
            action_result['message'] = str(e) 
            batch_had_error = True
        except Exception as e:
            log.exception(f"Critical unhandled error processing batch '{stem}': {e}")
            action_result['message'] = f"[{ProcessingStatus.INTERNAL_ERROR}] Critical error processing batch '{stem}'. Details: {e}"
            self.console.print(f"[bold red]CRITICAL ERROR processing batch {stem}. See log.[/bold red]")
            batch_had_error = True
        
        return action_result, batch_had_error, user_quit

    # --- MODIFIED: run_processing ---
    async def run_processing(self):
        # (Function logic largely unchanged, but logging and result interpretation will be more standardized)
        target_dir = self.args.directory.resolve()
        if not target_dir.is_dir():
            msg = f"[{ProcessingStatus.INTERNAL_ERROR}] Target directory not found or is not a directory: {target_dir}"
            log.critical(msg); self.console.print(f"[bold red]Error: {msg}[/]"); return
        use_metadata_effective = getattr(self.args, 'use_metadata', False)
        if use_metadata_effective and not self.metadata_fetcher:
            msg = f"[{ProcessingStatus.METADATA_CLIENT_UNAVAILABLE}] Metadata processing enabled, but FAILED to initialize API clients."
            log.critical(msg); self.console.print(f"\n[bold red]CRITICAL ERROR: {msg}[/]"); return
        log.info("Collecting batches from scanner...")
        file_batches = {stem: data for stem, data in scan_media_files(target_dir, self.cfg)}
        batch_count = len(file_batches)
        log.info(f"Collected {batch_count} batches.")
        if batch_count == 0:
             log.warning(f"[{ProcessingStatus.SKIPPED}] No valid video files/batches found matching criteria.")
             self.console.print(f"[yellow][{ProcessingStatus.SKIPPED}] No valid video files/batches found.[/yellow]"); return
        is_live_run = getattr(self.args, 'live', False)
        if is_live_run:
            potential_actions_count = self._perform_prescan(file_batches, batch_count)
            if not self._confirm_live_run(potential_actions_count): return
        initial_media_infos = self._perform_initial_parsing(file_batches, batch_count)
        initial_media_infos = await self._fetch_all_metadata(file_batches, initial_media_infos)
        run_batch_id = f"run-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        log.info(f"Starting planning and execution run ID: {run_batch_id}")
        results_summary = {'success_batches': 0, 'skipped_batches': 0, 'error_batches': 0, 'actions_taken': 0, 'moved_unknown_files': 0, 'meta_error_batches': 0}
        self.console.print("-" * 30)
        total_planned_actions_accumulator_for_dry_run = 0
        disable_rich_progress = self.args.interactive or not RICH_AVAILABLE
        with Progress(*DEFAULT_PROGRESS_COLUMNS, console=self.console, disable=disable_rich_progress) as progress_bar:
            main_processing_task = progress_bar.add_task("Planning/Executing", total=batch_count, item_name="")
            for stem, batch_data in file_batches.items():
                item_name_short = Path(batch_data.get('video', stem)).name[:30] + ("..." if len(Path(batch_data.get('video', stem)).name) > 30 else "")
                progress_bar.update(main_processing_task, advance=1, item_name=f"Processing: {item_name_short}")
                media_info = initial_media_infos.get(stem)
                if not media_info:
                    log.error(f"[{ProcessingStatus.INTERNAL_ERROR}] Skipping batch '{stem}' (missing MediaInfo after metadata fetch).")
                    results_summary['error_batches'] += 1; continue
                log_base = f"Batch '{stem}': Type='{media_info.file_type}', API='{getattr(media_info.metadata, 'source_api', 'N/A')}', Score='{getattr(media_info.metadata, 'match_confidence', 'N/A')}'"
                if media_info.metadata_error_message: log_base += f", MetaError='{media_info.metadata_error_message}'"
                log.debug(log_base)
                action_result, batch_had_error, user_quit = await self._process_single_batch(
                    stem, batch_data, media_info, run_batch_id, is_live_run
                )
                batch_msg = action_result.get('message', f"[{ProcessingStatus.INTERNAL_ERROR}] No message from batch processing.")
                is_success_op = action_result.get('success', False) and not batch_had_error
                # Check for specific skip reasons more reliably
                is_skip_op = any(f"[{status.name}]" in batch_msg for status in [
                    ProcessingStatus.SKIPPED, ProcessingStatus.PATH_ALREADY_CORRECT, 
                    ProcessingStatus.PLAN_TARGET_EXISTS_SKIP_MODE, ProcessingStatus.USER_INTERACTIVE_SKIP,
                    ProcessingStatus.UNKNOWN_HANDLING_CONFIG_SKIP
                ]) and not batch_had_error # Ensure it's not an error that looks like a skip

                if is_success_op:
                    log.info(f"SUCCESS: Batch '{stem}'. Actions: {action_result.get('actions_taken',0)}. Message: {batch_msg}")
                    results_summary['success_batches'] += 1
                elif is_skip_op:
                    log.info(f"SKIPPED: Batch '{stem}'. Reason: {batch_msg}")
                    results_summary['skipped_batches'] += 1
                else: # Error or Unresolved Conflict that is not a simple skip
                    log.error(f"FAILED: Batch '{stem}'. Reason: {batch_msg}")
                    results_summary['error_batches'] += 1
                
                # Consolidate metadata error count
                if media_info.metadata_error_message and not is_success_op:
                    # Count as a meta_error_batch if there was a metadata error message
                    # AND the batch didn't succeed (it could be skipped or failed due to this or other reasons)
                    # Avoid double-counting if unknown_handling_mode='guessit_only' and that plan also fails
                    if not (self.cfg('unknown_file_handling') == 'guessit_only' and f"[{ProcessingStatus.UNKNOWN_HANDLING_GUESSIT_PLAN_FAILED}]" in batch_msg):
                         results_summary['meta_error_batches'] +=1

                if not is_live_run: total_planned_actions_accumulator_for_dry_run += action_result.get('actions_taken', 0)
                else: 
                    results_summary['actions_taken'] += action_result.get('actions_taken', 0)
                    if f"[{ProcessingStatus.SUCCESS}] MOVED (unknown)" in batch_msg.upper(): # More specific check
                        results_summary['moved_unknown_files'] += action_result.get('actions_taken',0)
                if action_result.get('message') and not (self.args.interactive and is_success_op):
                    use_rule = not self.args.interactive and is_live_run and is_success_op and action_result.get('actions_taken',0) > 0
                    if use_rule: self.console.rule()
                    style = "red" if batch_had_error or "ERROR" in batch_msg or f"[{ProcessingStatus.FAILED.name}]" in batch_msg else \
                            "yellow" if is_skip_op else "default"
                    self.console.print(Text(batch_msg, style=style))
                    if use_rule: self.console.rule()
                if user_quit: break 
        self.console.print("-" * 30); log.info("Processing complete.")
        self.console.print("Processing Summary:")
        self.console.print(f"  Batches Scanned: {batch_count}")
        self.console.print(f"  Batches Successfully Processed: {results_summary['success_batches']}")
        self.console.print(f"  Batches Skipped (various reasons): {results_summary['skipped_batches']}")
        if results_summary['moved_unknown_files'] > 0 : self.console.print(f"  Files Moved to Unknown Dir: {results_summary['moved_unknown_files']}")
        if results_summary['meta_error_batches'] > 0 : self.console.print(f"  Metadata Related Issues (Batches): {results_summary['meta_error_batches']}")
        
        # Ensure error_batches only counts non-meta-error-specific failures if meta_error_batches is already reported
        actual_other_errors = results_summary['error_batches']
        if results_summary['meta_error_batches'] > 0 and results_summary['error_batches'] <= results_summary['meta_error_batches']:
            # If all errors were already counted as meta_errors, don't show "Other Processing Errors: 0"
            # Or if error_batches somehow became less (which it shouldn't)
            pass
        elif actual_other_errors > 0:
             self.console.print(f"  [bold red]Other Processing Errors (Batches):[/bold red] {actual_other_errors}")
        elif results_summary['meta_error_batches'] == 0 and actual_other_errors == 0: # Only if truly no errors
             self.console.print(f"  Batches with Errors: 0")
        
        if is_live_run: self.console.print(f"  Total File Actions Taken (files+dirs): {results_summary['actions_taken']}")
        else: self.console.print(f"  Total File Actions Planned: {total_planned_actions_accumulator_for_dry_run}")
        self.console.print("-" * 30)
        if not is_live_run:
             if total_planned_actions_accumulator_for_dry_run > 0: self.console.print("[yellow]DRY RUN COMPLETE. To apply changes, run again with --live[/yellow]")
             else: self.console.print("DRY RUN COMPLETE. No actions were planned.")
        if is_live_run and self.cfg('enable_undo', False) and results_summary['actions_taken'] > 0:
            script_name = Path(sys.argv[0]).name
            self.console.print(f"Undo information logged with Run ID: [bold cyan]{run_batch_id}[/bold cyan]")
            self.console.print(f"To undo this run: {script_name} undo {run_batch_id}")
        if is_live_run and self.args.stage_dir and results_summary['actions_taken'] > 0 : self.console.print(f"Renamed files moved to staging: {self.args.stage_dir}")
        
        total_problem_batches_final = results_summary['error_batches'] 
        if total_problem_batches_final > 0: self.console.print(f"[bold red]Operation finished with {total_problem_batches_final} problematic batches. Check logs.[/bold red]")
        elif results_summary['moved_unknown_files'] > 0 and (results_summary['success_batches'] + results_summary['skipped_batches'] < batch_count) :
             self.console.print(f"[yellow]Operation finished. Some files were moved to the unknown folder.[/yellow]")
        elif results_summary['success_batches'] + results_summary['skipped_batches'] == batch_count:
             if results_summary['success_batches'] > 0 : self.console.print("[green]Operation finished successfully.[/green]")
             else: self.console.print("Operation finished. All batches were skipped.")
        else: self.console.print("Operation finished.")