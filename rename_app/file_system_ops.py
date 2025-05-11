# rename_app/file_system_ops.py
import logging
import shutil
import uuid
from pathlib import Path
import argparse
import sys
import os
import builtins # For safety
import time # Though not directly used, often good to have with os/sys
from typing import Dict, Set, Optional, Any, List, Tuple

from rename_app.ui_utils import (
    ConsoleClass, TableClass, TextClass, # TextClass is imported
    RICH_AVAILABLE_UI as RICH_AVAILABLE, 
    RichConsoleActual, 
    RichText # Import RichText for isinstance
)

from .models import RenamePlan, RenameAction, MediaInfo, MediaMetadata
from .exceptions import FileOperationError, RenamerError
from .undo_manager import UndoManager
from .config_manager import ConfigHelper

try: import send2trash; SEND2TRASH_AVAILABLE = True
except ImportError: SEND2TRASH_AVAILABLE = False

log = logging.getLogger(__name__)
TEMP_SUFFIX_PREFIX = ".renametmp_"
WINDOWS_PATH_LENGTH_WARNING_THRESHOLD = 240

def _compare_and_format(
    field_name: str,
    guess_value: Optional[Any],
    final_value: Optional[Any],
    is_numeric: bool = False
) -> Optional[TextClass]:
    g_val_str = str(guess_value) if guess_value is not None else ""
    f_val_str = str(final_value) if final_value is not None else ""

    if is_numeric:
        try:
            g_num = int(g_val_str) if g_val_str else None
            f_num = int(f_val_str) if f_val_str else None
            if g_num == f_num: return None
        except (ValueError, TypeError):
            pass

    if g_val_str != f_val_str:
        if RICH_AVAILABLE: # RICH_AVAILABLE is from ui_utils
            # TextClass is RichText when RICH_AVAILABLE is True
            return TextClass.assemble( # type: ignore
                TextClass(f"{field_name}: ", style="dim blue"),
                TextClass(f"'{g_val_str or '<unset>'}'", style="dim red"),
                TextClass(" -> ", style="dim blue"),
                TextClass(f"'{f_val_str or '<unset>'}'", style="dim green")
            )
        else: # TextClass is the fallback Text
            return TextClass(f"{field_name}: '{g_val_str or '<unset>'}' -> '{f_val_str or '<unset>'}'")
    return None

def _handle_conflict(original_path: Path, target_path: Path, conflict_mode: str) -> Path:
    if not target_path.exists() and not target_path.is_symlink():
        return target_path
    
    log.warning(f"Conflict detected: Target '{target_path}' exists.")
    if conflict_mode == 'skip':
        raise FileOperationError(f"Target '{target_path.name}' exists (mode: skip).")
    if conflict_mode == 'fail':
        raise FileExistsError(f"Target '{target_path.name}' exists (mode: fail). Stopping.")
    if conflict_mode == 'overwrite':
        log.warning(f"Overwrite mode: Target '{target_path.name}' will be overwritten if needed during Phase 2.")
        return target_path # The actual overwrite happens in the move/rename operation
    if conflict_mode == 'suffix':
        counter = 1
        original_stem = target_path.stem
        original_ext = target_path.suffix
        suffixed_path = target_path

        while suffixed_path.exists() or suffixed_path.is_symlink():
            new_stem = f"{original_stem}_{counter}"
            if len(str(target_path.parent / (new_stem + original_ext))) > (WINDOWS_PATH_LENGTH_WARNING_THRESHOLD + 10): # Heuristic
                raise FileOperationError(f"Suffix failed: generated name likely too long for '{original_stem}' after {counter} attempts.")
            suffixed_path = target_path.with_name(f"{new_stem}{original_ext}")
            counter += 1
            if counter > 100:
                raise FileOperationError(f"Suffix failed: >100 attempts for '{original_stem}'")
        log.info(f"Conflict resolved: Using suffixed name '{suffixed_path.name}' for original '{original_path.name}'.")
        return suffixed_path
    
    raise RenamerError(f"Internal Error: Unknown conflict mode '{conflict_mode}'")


# rename_app/file_system_ops.py

# ... (imports and other functions as previously corrected) ...

def _display_dry_run_plan(
    plan: RenamePlan,
    cfg_helper: ConfigHelper,
    media_info: Optional[MediaInfo] = None,
    quiet_mode: bool = False
) -> Tuple[bool, str, int]:
    console = ConsoleClass(quiet=quiet_mode)
    log.info(f"--- DRY RUN Display for Plan ID: {plan.batch_id} ---")
    dry_run_actions_display_data: List[Dict[str, TextClass]] = []

    original_paths_in_plan_dry: Set[Path] = {a.original_path.resolve() for a in plan.actions}
    current_targets_dry: Set[Path] = set()
    dry_run_conflict_error = False
    conflict_mode = cfg_helper('on_conflict', 'skip')
    should_preserve_mtime = cfg_helper('preserve_mtime', False)

    original_guess: Dict[str, Any] = {}
    final_metadata: Optional[MediaMetadata] = None
    final_file_type: str = 'unknown'
    if media_info:
         original_guess = media_info.guess_info if media_info.guess_info else {}
         final_metadata = media_info.metadata
         final_file_type = media_info.file_type

    if plan.created_dir_path:
         status_text_dir = TextClass("OK", style="green")
         action_text_dir = TextClass("Create Dir", style="bold green")
         new_path_text_dir_val = TextClass(str(plan.created_dir_path), style="green")
         if plan.created_dir_path.exists():
             status_text_dir = TextClass("Exists", style="yellow")
             action_text_dir = TextClass("Create Dir", style="bold yellow")
             new_path_text_dir_val = TextClass(str(plan.created_dir_path), style="yellow")
         
         dry_run_actions_display_data.append({
             "original": TextClass("-", style="dim"), 
             "arrow": TextClass("->", style="dim"),
             "new": new_path_text_dir_val,
             "action": action_text_dir,
             "status": status_text_dir, 
             "reason": TextClass("")
         })

    for action in plan.actions:
        original_text = TextClass(str(action.original_path.name))
        # Use a consistent variable name for the TextClass object representing the new path for display
        current_new_path_text_obj: TextClass = TextClass(str(action.new_path)) # Initialize with original planned new path

        action_text_display = TextClass(action.action_type.capitalize(), style="blue")
        status_text = TextClass("OK", style="green")
        
        simulated_target_path_for_logic: Optional[Path] = action.new_path.resolve()

        target_exists_externally = (simulated_target_path_for_logic.exists() and
                                    simulated_target_path_for_logic not in original_paths_in_plan_dry) or \
                                   simulated_target_path_for_logic in current_targets_dry

        if sys.platform == 'win32' and len(str(simulated_target_path_for_logic)) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
             status_text = TextClass(f"Long Path (> {WINDOWS_PATH_LENGTH_WARNING_THRESHOLD})", style="bold yellow")
        
        if target_exists_externally:
            try:
                effective_conflict_mode_for_sim = conflict_mode
                if conflict_mode == 'fail': 
                    effective_conflict_mode_for_sim = 'skip'
                
                resolved_target_dry_sim = _handle_conflict(action.original_path, simulated_target_path_for_logic, effective_conflict_mode_for_sim)
                
                if resolved_target_dry_sim != simulated_target_path_for_logic: 
                    status_text = TextClass(f"Conflict: Suffix -> '{resolved_target_dry_sim.name}'", style="yellow")
                    current_new_path_text_obj = TextClass(str(resolved_target_dry_sim), style="yellow") # Update this
                    simulated_target_path_for_logic = resolved_target_dry_sim
                elif conflict_mode == 'overwrite':
                    status_text = TextClass("Conflict: Overwrite", style="bold yellow")
                    current_new_path_text_obj = TextClass(str(action.new_path), style="yellow") # Update this
            except FileOperationError: 
                status_text = TextClass("Conflict: Skip", style="bold yellow")
                action_text_display = TextClass("Skip", style="bold yellow")
                current_new_path_text_obj = TextClass(str(action.new_path), style="dim yellow") # Update this
                simulated_target_path_for_logic = None
            except FileExistsError: 
                status_text = TextClass("Conflict: Would Fail", style="bold red")
                action_text_display = TextClass("Fail", style="bold red")
                current_new_path_text_obj = TextClass(str(action.new_path), style="dim red") # Update this
                dry_run_conflict_error = True
                simulated_target_path_for_logic = None
            except Exception as e_dry_other_conflict:
                status_text = TextClass(f"Error in Conflict Sim ({type(e_dry_other_conflict).__name__})", style="bold red")
                action_text_display = TextClass("Fail", style="bold red")
                current_new_path_text_obj = TextClass(str(action.new_path), style="dim red") # Update this
                dry_run_conflict_error = True
                simulated_target_path_for_logic = None

        if simulated_target_path_for_logic:
            if simulated_target_path_for_logic in current_targets_dry:
                status_text = TextClass("Conflict: Target Collision", style="bold red")
                action_text_display = TextClass("Fail", style="bold red")
                dry_run_conflict_error = True
            else:
                current_targets_dry.add(simulated_target_path_for_logic)
        
        reason_details_list: List[Optional[TextClass]] = []
        # ... (populate reason_details_list - this logic seemed okay) ...
        if media_info and action.original_path.resolve() == media_info.original_path.resolve():
            g_title = original_guess.get('title', ''); g_year = original_guess.get('year'); g_season = original_guess.get('season'); g_ep_raw = original_guess.get('episode'); g_ep = g_ep_raw[0] if isinstance(g_ep_raw, list) and g_ep_raw else g_ep_raw
            f_title, f_year, f_season, f_ep = None, None, None, None
            if final_metadata:
                if final_file_type == 'movie': f_title, f_year = final_metadata.movie_title, final_metadata.movie_year
                elif final_file_type == 'series': f_title, f_year, f_season = final_metadata.show_title, final_metadata.show_year, final_metadata.season; f_ep = final_metadata.episode_list[0] if final_metadata.episode_list else None
            f_title = f_title if f_title is not None else g_title; f_year = f_year if f_year is not None else g_year
            f_season = f_season if f_season is not None else g_season; f_ep = f_ep if f_ep is not None else g_ep
            reason_details_list.extend(filter(None, [_compare_and_format("Title", g_title, f_title), _compare_and_format("Year", g_year, f_year, is_numeric=True)]))
            if final_file_type == 'series': reason_details_list.extend(filter(None, [_compare_and_format("Season", g_season, f_season, is_numeric=True), _compare_and_format("Episode", g_ep, f_ep, is_numeric=True)]))
            if action.new_path.parent.resolve() != action.original_path.parent.resolve(): reason_details_list.append(TextClass("Folder Change", style="dim blue"))
        elif not media_info: reason_details_list.append(TextClass("Reason N/A (internal error)", style="yellow"))
        else: reason_details_list.append(TextClass("(matches video)", style="dim"))
                
        valid_reason_details = [r for r in reason_details_list if r is not None]
        reason_text_combined: TextClass
        if RICH_AVAILABLE and valid_reason_details and all(isinstance(r, RichText) for r in valid_reason_details):
            assembled_parts: List[Any] = []
            for i, reason_part in enumerate(valid_reason_details):
                if i > 0: assembled_parts.append("\n")
                assembled_parts.append(reason_part)
            reason_text_combined = TextClass.assemble(*assembled_parts) # type: ignore
        else:
            reason_text_combined = TextClass(" | ".join(str(r) for r in valid_reason_details))

        preserve_mtime_info = TextClass(" (mtime preserved)", style="italic dim") if should_preserve_mtime and action.action_type in ['rename', 'move'] else TextClass("")
        
        new_path_for_display_col: TextClass
        # Use the consistently named current_new_path_text_obj here
        if RICH_AVAILABLE and hasattr(TextClass, 'assemble') and isinstance(current_new_path_text_obj, RichText) and isinstance(preserve_mtime_info, RichText): # type: ignore
            new_path_for_display_col = TextClass.assemble(current_new_path_text_obj, preserve_mtime_info) # type: ignore
        else:
            new_path_for_display_col = TextClass(f"{str(current_new_path_text_obj)}{str(preserve_mtime_info)}")

        action_text_plain_check = action_text_display.plain if hasattr(action_text_display, 'plain') else str(action_text_display)
        arrow_style = "red" if action_text_plain_check == "Fail" else "dim"

        item_data_for_table = {
            "original": original_text,
            "arrow": TextClass("->", style=arrow_style),
            "new": new_path_for_display_col, # This is the key
            "action": action_text_display,
            "status": status_text,
            "reason": reason_text_combined
        }
        dry_run_actions_display_data.append(item_data_for_table) # type: ignore

    # ... (rest of the function for table printing and returning values)
    # Ensure table.add_row uses item_dict_for_row["new"], etc.
    # This part was already correct.
    message_for_caller: str
    if dry_run_actions_display_data:
        table = TableClass(title=f"Dry Run Plan - Batch ID (approx): {plan.batch_id[:15]}", show_header=True, header_style="bold magenta")
        column_names = ["Original Name", " ", "New Path / Name", "Action", "Status / Conflict", "Reason / Changes"]
        column_styles_justify = [
            {"style": "dim cyan", "no_wrap": False, "min_width": 20},
            {"justify": "center", "width": 2},
            {"style": "cyan", "no_wrap": False, "min_width": 30},
            {"justify": "center"},
            {"justify": "left", "min_width": 15},
            {"justify": "left", "min_width": 20}
        ]
        for i, name in enumerate(column_names):
            table.add_column(name, **column_styles_justify[i]) # type: ignore

        for item_dict_for_row in dry_run_actions_display_data:
            table.add_row( # type: ignore
                item_dict_for_row["original"], 
                item_dict_for_row["arrow"], 
                item_dict_for_row["new"], 
                item_dict_for_row["action"], 
                item_dict_for_row["status"], 
                item_dict_for_row["reason"]
            )
        console.print(table)
        message_for_caller = f"Dry Run plan displayed ({len(dry_run_actions_display_data)} potential actions)."
        if quiet_mode: 
            log.info("Dry Run Table generated (output suppressed by quiet mode).")
    else:
        message_for_caller = "DRY RUN: No actions planned."
        console.print(message_for_caller)

    planned_count = len([a for a in dry_run_actions_display_data if hasattr(a.get("action"), 'plain') and a.get("action").plain not in ["Skip", "Fail"]]) # type: ignore
    return dry_run_conflict_error, message_for_caller, planned_count


def _prepare_live_actions(
    plan: RenamePlan,
    cfg_helper: ConfigHelper,
    action_messages: List[str]
) -> Tuple[Optional[Path], Dict[Path, Path], Dict[Path, float], bool]:
    conflict_mode = cfg_helper('on_conflict', 'skip')
    should_preserve_mtime = cfg_helper('preserve_mtime', False)
    
    created_dir_path: Optional[Path] = None
    resolved_target_map: Dict[Path, Path] = {}
    original_mtimes: Dict[Path, float] = {}
    preparation_success = True

    log.debug("Phase 0: Resolving final paths, checking conflicts, getting mtimes...")
    
    current_final_target_paths_in_plan: Set[Path] = set()
    original_paths_being_moved_or_renamed: Set[Path] = {a.original_path.resolve() for a in plan.actions}

    try:
        if plan.created_dir_path:
            if not plan.created_dir_path.exists():
                log.info(f"Creating planned folder: {plan.created_dir_path}")
                plan.created_dir_path.mkdir(parents=True, exist_ok=True)
                created_dir_path = plan.created_dir_path
            elif plan.created_dir_path.is_dir():
                log.debug(f"Planned target directory already exists: {plan.created_dir_path}")
            else:
                raise FileOperationError(f"Cannot create planned directory: Path '{plan.created_dir_path}' exists and is not a directory.")

        for action in plan.actions:
            original_p_resolved = action.original_path.resolve()
            
            if not action.original_path.exists():
                log.warning(f"Phase 0 Skip: Original file '{action.original_path.name}' not found.")
                continue 

            intended_final_path = action.new_path.resolve()
            final_target_for_this_action = intended_final_path

            if should_preserve_mtime and action.action_type in ['rename', 'move'] :
                try:
                    original_mtimes[original_p_resolved] = action.original_path.stat().st_mtime
                    log.debug(f"  Stored original mtime for '{action.original_path.name}'")
                except OSError as stat_err:
                    log.warning(f"Could not get mtime for '{action.original_path.name}': {stat_err}. Cannot preserve.")
            
            is_external_conflict = (final_target_for_this_action.exists() and
                                    final_target_for_this_action not in original_paths_being_moved_or_renamed)
            is_internal_pre_conflict = final_target_for_this_action in current_final_target_paths_in_plan

            if is_external_conflict or is_internal_pre_conflict:
                if is_external_conflict: log.debug(f"  External conflict detected for '{action.original_path.name}' -> '{final_target_for_this_action.name}'")
                if is_internal_pre_conflict: log.debug(f"  Internal pre-conflict detected for '{action.original_path.name}' -> '{final_target_for_this_action.name}'")
                                
                final_target_for_this_action = _handle_conflict(
                    action.original_path, 
                    final_target_for_this_action, 
                    conflict_mode
                )
            
            if sys.platform == 'win32' and len(str(final_target_for_this_action)) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
                log.warning(f"Potential long path issue on Windows for target: '{final_target_for_this_action}'")

            if final_target_for_this_action in current_final_target_paths_in_plan:
                raise FileOperationError(f"Internal Error: Multiple files resolve to target '{final_target_for_this_action.name}' even after conflict resolution attempt.")
            
            resolved_target_map[original_p_resolved] = final_target_for_this_action
            current_final_target_paths_in_plan.add(final_target_for_this_action)
            log.debug(f"  Phase 0 Resolved: '{action.original_path.name}' -> '{final_target_for_this_action.name}'")

    except (FileOperationError, FileExistsError) as e_prep: 
        log.error(f"Phase 0: Conflict check or directory creation failed: {e_prep}")
        action_messages.append(f"ERROR (Preparation): {e_prep}")
        preparation_success = False
        if isinstance(e_prep, FileExistsError) and conflict_mode == 'fail':
            raise 
    except Exception as e_unexp_prep:
        log.exception(f"Phase 0: Unexpected error: {e_unexp_prep}")
        action_messages.append(f"ERROR (Preparation - Unexpected): {e_unexp_prep}")
        preparation_success = False
        
    return created_dir_path, resolved_target_map, original_mtimes, preparation_success

def _perform_backup_action(
    plan: RenamePlan,
    backup_dir_path: Path,
    action_messages: List[str]
) -> None:
    if not backup_dir_path:
        raise FileOperationError("Backup directory not specified or invalid.")
    
    backup_dir_path.mkdir(parents=True, exist_ok=True)
    backed_up_count = 0
    log.info(f"Starting backup phase to {backup_dir_path}...")

    for action in plan.actions:
        original_p = action.original_path
        if not original_p.exists():
            log.warning(f"Cannot backup non-existent file: '{original_p.name}'. Skipping backup.")
            continue
        
        backup_target = backup_dir_path / original_p.name
        
        counter = 0
        final_backup_target = backup_target
        while final_backup_target.exists():
            counter += 1
            final_backup_target = backup_dir_path / f"{backup_target.stem}_{counter}{backup_target.suffix}"
            if counter > 100: 
                log.error(f"Could not find unique backup name for '{original_p.name}' in '{backup_dir_path}' after 100 attempts. Skipping backup.")
                final_backup_target = None; break 
        if not final_backup_target: continue

        if sys.platform == 'win32' and len(str(final_backup_target.resolve())) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
            log.warning(f"Potential long path issue on Windows for backup target: '{final_backup_target.resolve()}'.")
        
        try:
            shutil.copy2(str(original_p), str(final_backup_target))
            log.debug(f"Backed up '{original_p.name}' to '{final_backup_target.name}'")
            backed_up_count += 1
        except Exception as e_backup:
            log.error(f"Failed to backup '{original_p.name}' to '{final_backup_target}': {e_backup}")
            action_messages.append(f"ERROR (Backup): Failed for '{original_p.name}': {e_backup}")

    if backed_up_count > 0:
        action_messages.append(f"Backed up {backed_up_count} files to '{backup_dir_path}'.")


def _perform_trash_action(
    plan: RenamePlan,
    run_batch_id: str,
    undo_manager: UndoManager,
    action_messages: List[str]
) -> int:
    if not SEND2TRASH_AVAILABLE:
        raise FileOperationError("'send2trash' library not installed or available. Cannot move files to trash.")
    
    log.info("Starting trash phase...")
    trashed_count = 0
    for action in plan.actions:
        original_p = action.original_path
        final_p_intended_for_log = action.new_path

        if not original_p.exists():
            log.warning(f"Cannot trash non-existent file: '{original_p.name}'. Skipping.")
            continue
        
        try:
            if undo_manager.is_enabled:
                undo_manager.log_action(
                    batch_id=run_batch_id,
                    original_path=original_p,
                    new_path=final_p_intended_for_log,
                    item_type='file',
                    status='trashed'
                )
            send2trash.send2trash(str(original_p))
            action_messages.append(f"TRASHED: '{original_p.name}' (intended new name: '{final_p_intended_for_log.name}')")
            trashed_count += 1
        except Exception as e_trash:
            log.error(f"Failed to move '{original_p.name}' to trash: {e_trash}")
            action_messages.append(f"ERROR (Trash): Failed for '{original_p.name}': {e_trash}")
            if undo_manager.is_enabled:
                undo_manager.update_action_status(run_batch_id, str(original_p), 'failed_pending')
    return trashed_count


def _perform_stage_action(
    plan: RenamePlan,
    stage_dir_path: Path,
    run_batch_id: str,
    undo_manager: UndoManager,
    resolved_target_map: Dict[Path, Path],
    original_mtimes: Dict[Path, float],
    should_preserve_mtime: bool,
    action_messages: List[str]
) -> int:
    if not stage_dir_path:
        raise FileOperationError("Staging directory not specified or invalid.")
    
    stage_dir_path.mkdir(parents=True, exist_ok=True)
    log.info(f"Starting staging phase to {stage_dir_path}...")
    staged_count = 0

    for action in plan.actions:
        original_p = action.original_path
        original_p_resolved = original_p.resolve()
        
        final_staged_path = resolved_target_map.get(original_p_resolved)

        if not final_staged_path:
            log.warning(f"Stage Skip: Could not find resolved target path for '{original_p.name}' in resolved_target_map.")
            continue
        if not original_p.exists():
            log.warning(f"Cannot stage non-existent file: '{original_p.name}'. Skipping stage.")
            continue
        
        final_staged_path.parent.mkdir(parents=True, exist_ok=True)

        if sys.platform == 'win32' and len(str(final_staged_path.resolve())) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
            log.warning(f"Potential long path issue on Windows for staged target: '{final_staged_path.resolve()}'.")
        
        try:
            if undo_manager.is_enabled:
                undo_manager.log_action(
                    batch_id=run_batch_id,
                    original_path=original_p,
                    new_path=final_staged_path,
                    item_type='file',
                    status='moved'
                )
            shutil.move(str(original_p), str(final_staged_path))
            action_messages.append(f"MOVED to stage: '{original_p.name}' -> '{final_staged_path}'")
            staged_count += 1

            if should_preserve_mtime:
                original_mtime_val = original_mtimes.get(original_p_resolved)
                if original_mtime_val is not None:
                    try:
                        log.debug(f"  -> Preserving mtime ({original_mtime_val:.2f}) for staged file '{final_staged_path.name}'")
                        os.utime(str(final_staged_path), (original_mtime_val, original_mtime_val))
                    except OSError as utime_err:
                        log.warning(f"  -> Failed to preserve mtime for staged file '{final_staged_path.name}': {utime_err}")
                else:
                    log.debug(f"  -> Could not preserve mtime for staged file '{final_staged_path.name}': Original mtime not found.")
        except Exception as e_stage:
            log.error(f"Failed to stage '{original_p.name}' to '{final_staged_path}': {e_stage}")
            action_messages.append(f"ERROR (Stage): Failed for '{original_p.name}': {e_stage}")
            if undo_manager.is_enabled:
                undo_manager.update_action_status(run_batch_id, str(original_p), 'failed_pending')
    return staged_count


def _perform_transactional_rename_move(
    plan: RenamePlan,
    run_batch_id: str,
    undo_manager: UndoManager,
    resolved_target_map: Dict[Path, Path],
    original_mtimes: Dict[Path, float],
    should_preserve_mtime: bool,
    conflict_mode_for_phase2: str,
    action_messages: List[str]
) -> Tuple[int, bool]:
    original_to_temp_map: Dict[Path, Path] = {}
    temp_to_final_map: Dict[Path, Path] = {}
    phase1_ok = True
    actions_taken_count = 0

    log.debug(f"Starting Phase 1: Move to temporary paths for run {run_batch_id}")
    for action in plan.actions:
        orig_p_resolved = action.original_path.resolve()
        final_p_intended = resolved_target_map.get(orig_p_resolved)

        if not final_p_intended:
            log.error(f"P1 Skip: Missing resolved final path for '{action.original_path.name}'. This implies an issue in _prepare_live_actions or map.")
            continue
        if not action.original_path.exists():
            log.warning(f"P1 Skip: Original file '{action.original_path.name}' missing before move to temp.")
            continue

        try:
            temp_file_uuid = uuid.uuid4().hex[:8]
            temp_path = final_p_intended.parent / f"{final_p_intended.stem}{TEMP_SUFFIX_PREFIX}{temp_file_uuid}{final_p_intended.suffix}"
            
            while temp_path.exists() or temp_path.is_symlink():
                temp_file_uuid = uuid.uuid4().hex[:8]
                temp_path = final_p_intended.parent / f"{final_p_intended.stem}{TEMP_SUFFIX_PREFIX}{temp_file_uuid}{final_p_intended.suffix}"

            if undo_manager.is_enabled:
                undo_manager.log_action(
                    batch_id=run_batch_id,
                    original_path=action.original_path,
                    new_path=final_p_intended,
                    item_type='file',
                    status='pending_final'
                )

            log.debug(f"  P1 Moving '{action.original_path}' -> Temp '{temp_path}' (Final Target Dir: {final_p_intended.parent})")
            shutil.move(str(action.original_path), str(temp_path))

            original_to_temp_map[orig_p_resolved] = temp_path
            temp_to_final_map[temp_path] = final_p_intended
            log.debug(f"  P1 Success: '{action.original_path.name}' moved to temp '{temp_path.name}'.")

        except Exception as e_p1:
            msg = f"P1 Error moving '{action.original_path.name}' to temp: {e_p1}"
            log.error(msg, exc_info=True)
            action_messages.append(f"ERROR: {msg}")
            if undo_manager.is_enabled:
                 undo_manager.update_action_status(run_batch_id, str(action.original_path), 'failed_pending')
            phase1_ok = False
            break

    if not phase1_ok:
        log.warning(f"Rolling back Phase 1 for run {run_batch_id} due to error...")
        rollback_success_count = 0; rollback_fail_count = 0
        
        for orig_p_res_rb, temp_p_rb in original_to_temp_map.items():
            original_path_for_rollback = Path(orig_p_res_rb)
            log.debug(f"  Attempting P1 rollback: Temp '{temp_p_rb}' -> Original '{original_path_for_rollback}'")
            try:
                if temp_p_rb.exists():
                    original_path_for_rollback.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(temp_p_rb), str(original_path_for_rollback))
                    log.info(f"  P1 Rollback successful: '{temp_p_rb.name}' -> '{original_path_for_rollback.name}'")
                    if undo_manager.is_enabled:
                        undo_manager.update_action_status(run_batch_id, str(original_path_for_rollback), 'failed_pending')
                    rollback_success_count += 1
                else:
                    log.warning(f"  P1 Rollback skipped for '{original_path_for_rollback.name}', temp file '{temp_p_rb}' not found (vanished or already handled).")
                    if undo_manager.is_enabled:
                        undo_manager.update_action_status(run_batch_id, str(original_path_for_rollback), 'failed_pending')
            except Exception as e_rb_p1:
                log.error(f"  P1 Rollback Error moving '{temp_p_rb.name}' to '{original_path_for_rollback.name}': {e_rb_p1}")
                action_messages.append(f"CRITICAL: P1 Rollback failed for '{temp_p_rb.name}'. File may be at '{temp_p_rb}'. Original was '{original_path_for_rollback}'.")
                rollback_fail_count += 1
        
        created_dir_for_rollback = plan.created_dir_path
        if created_dir_for_rollback and created_dir_for_rollback.is_dir():
            try:
                if not any(created_dir_for_rollback.iterdir()):
                    log.debug(f"  Attempting P1 rollback: Removing created directory '{created_dir_for_rollback}'")
                    created_dir_for_rollback.rmdir()
                    log.info(f"  P1 Rollback successful: Removed directory '{created_dir_for_rollback}'")
                    if undo_manager.is_enabled:
                         undo_manager.update_action_status(run_batch_id, str(created_dir_for_rollback), 'reverted')
                else:
                    log.warning(f"  P1 Rollback: Directory '{created_dir_for_rollback}' not empty or not created by this run, not removed.")
            except OSError as e_rdir_p1:
                log.error(f"  P1 Rollback could not remove created dir '{created_dir_for_rollback}': {e_rdir_p1}")
                action_messages.append(f"CRITICAL: P1 Rollback failed to remove directory '{created_dir_for_rollback}'.")

        action_messages.append(f"Phase 1 Rollback Summary: {rollback_success_count} files restored, {rollback_fail_count} file restore failures.")
        return 0, True

    log.debug(f"Starting Phase 2: Rename temporary paths to final for run {run_batch_id}")
    phase2_errors_occurred = False
    for temp_path, final_path_target in temp_to_final_map.items():
        original_path_for_log_str: Optional[str] = None
        original_path_resolved_for_mtime: Optional[Path] = None

        for orig_res_path_obj_key, mapped_temp_path_val in original_to_temp_map.items():
            if mapped_temp_path_val.resolve() == temp_path.resolve():
                original_path_for_log_str = str(orig_res_path_obj_key)
                original_path_resolved_for_mtime = orig_res_path_obj_key
                break
        
        if not original_path_for_log_str:
            log.error(f"P2 CRITICAL: Could not find original path mapping for temp file '{temp_path}'. Skipping.")
            action_messages.append(f"ERROR: P2 Internal error for temp file '{temp_path.name}'.")
            phase2_errors_occurred = True; continue

        if not temp_path.exists():
            log.error(f"P2 Error: Temp file '{temp_path}' not found! Cannot complete rename for original '{original_path_for_log_str}'.")
            action_messages.append(f"ERROR: P2 Temp file '{temp_path.name}' missing for original '{Path(original_path_for_log_str).name}'.")
            if undo_manager.is_enabled:
                undo_manager.update_action_status(run_batch_id, original_path_for_log_str, 'failed_pending')
            phase2_errors_occurred = True; continue

        try:
            if final_path_target.exists() or final_path_target.is_symlink():
                if conflict_mode_for_phase2 == 'overwrite':
                    log.warning(f"  P2 Overwriting existing target (as per conflict_mode='overwrite'): '{final_path_target}'")
                    try:
                        if final_path_target.is_dir(): shutil.rmtree(str(final_path_target))
                        else: final_path_target.unlink(missing_ok=True)
                    except OSError as e_del_target:
                        log.error(f"  P2 Failed to delete existing target '{final_path_target}' for overwrite: {e_del_target}")
                        action_messages.append(f"ERROR: P2 Failed to overwrite '{final_path_target.name}' for original '{Path(original_path_for_log_str).name}'.")
                        if undo_manager.is_enabled: undo_manager.update_action_status(run_batch_id, original_path_for_log_str, 'failed_pending')
                        phase2_errors_occurred = True; continue
                else:
                    log.error(f"  P2 Error: Target '{final_path_target}' exists unexpectedly! Conflict mode '{conflict_mode_for_phase2}'. Original: '{Path(original_path_for_log_str).name}'.")
                    action_messages.append(f"ERROR: P2 Target '{final_path_target.name}' exists unexpectedly for original '{Path(original_path_for_log_str).name}'.")
                    if undo_manager.is_enabled: undo_manager.update_action_status(run_batch_id, original_path_for_log_str, 'failed_pending')
                    phase2_errors_occurred = True; continue
            
            rename_move_successful_p2 = False
            try:
                log.debug(f"  P2 Attempting (os.rename): '{temp_path.name}' -> '{final_path_target.name}'")
                os.rename(str(temp_path), str(final_path_target))
                rename_move_successful_p2 = True
            except OSError as e_os_rename_p2:
                log.warning(f"  P2 os.rename failed ('{e_os_rename_p2}'), attempting shutil.move for '{temp_path.name}' -> '{final_path_target.name}'...")
                try:
                    shutil.move(str(temp_path), str(final_path_target))
                    rename_move_successful_p2 = True
                except Exception as e_shutil_move_p2:
                    log.error(f"  P2 shutil.move also failed for '{temp_path.name}': {e_shutil_move_p2}")
            except Exception as e_generic_rename_p2:
                log.error(f"  P2 Unexpected error during os.rename for '{temp_path.name}': {e_generic_rename_p2}")

            if rename_move_successful_p2:
                actions_taken_count += 1
                log.info(f"  P2 Successfully renamed/moved original '{Path(original_path_for_log_str).name}' (from temp) to '{final_path_target}'")

                if should_preserve_mtime and original_path_resolved_for_mtime:
                    original_mtime_val = original_mtimes.get(original_path_resolved_for_mtime)
                    if original_mtime_val is not None:
                        try:
                            log.debug(f"    P2 Preserving mtime ({original_mtime_val:.2f}) for '{final_path_target.name}'")
                            os.utime(str(final_path_target), (original_mtime_val, original_mtime_val))
                        except OSError as utime_err_p2:
                            log.warning(f"    P2 Failed to preserve mtime for '{final_path_target.name}': {utime_err_p2}")
                    else:
                        log.debug(f"    P2 Could not preserve mtime for '{final_path_target.name}': Original mtime not found for key {original_path_resolved_for_mtime}.")
                
                if undo_manager.is_enabled:
                    final_op_status = 'moved' if final_path_target.parent.resolve() != Path(original_path_for_log_str).parent.resolve() else 'renamed'
                    if not undo_manager.update_action_status(run_batch_id, original_path_for_log_str, final_op_status):
                        log.error(f"  P2 Success, but FAILED to update undo log status for '{original_path_for_log_str}' to '{final_op_status}'")
                        action_messages.append(f"ACTION LOG UPDATE FAILED? '{Path(original_path_for_log_str).name}' -> '{final_path_target}'")
                    else:
                        action_messages.append(f"{final_op_status.upper()}D: '{Path(original_path_for_log_str).name}' -> '{final_path_target}'")
            else:
                msg_p2_fail = f"P2 All rename/move attempts FAILED for original '{Path(original_path_for_log_str).name}' (from temp '{temp_path.name}') to '{final_path_target.name}'."
                log.error(msg_p2_fail)
                action_messages.append(f"ERROR: {msg_p2_fail} File remains at '{temp_path}'.")
                if undo_manager.is_enabled:
                    undo_manager.update_action_status(run_batch_id, original_path_for_log_str, 'failed_pending')
                phase2_errors_occurred = True
        
        except Exception as e_outer_loop_p2:
            msg_p2_outer_fail = f"P2 Outer Error processing temp '{temp_path.name}' to '{final_path_target.name}' (Original: '{Path(original_path_for_log_str).name}'): {e_outer_loop_p2}"
            log.critical(msg_p2_outer_fail, exc_info=True)
            action_messages.append(f"ERROR: {msg_p2_outer_fail}. File may remain at '{temp_path}'.")
            if undo_manager.is_enabled and original_path_for_log_str:
                undo_manager.update_action_status(run_batch_id, original_path_for_log_str, 'failed_pending')
            phase2_errors_occurred = True

    if phase2_errors_occurred:
        log.error(f"Phase 2 completed with errors for run {run_batch_id}. Some files may be in temporary state or not fully processed.")
    else:
        log.debug(f"Phase 2 completed successfully for run {run_batch_id}.")
        
    return actions_taken_count, phase2_errors_occurred


def perform_file_actions(
    plan: RenamePlan,
    args_ns: argparse.Namespace,
    cfg_helper: ConfigHelper,
    undo_manager: UndoManager,
    run_batch_id: str,
    media_info: Optional[MediaInfo] = None,
    quiet_mode: bool = False
) -> Dict[str, Any]:
    results: Dict[str, Any] = {'success': True, 'message': "", 'actions_taken': 0}
    action_messages: List[str] = []

    if not getattr(args_ns, 'live', False):
        conflict_error_dry_run, msg, planned_actions_count = _display_dry_run_plan(
            plan, cfg_helper, media_info, quiet_mode
        )
        results['success'] = not conflict_error_dry_run
        results['message'] = msg
        results['actions_taken'] = planned_actions_count
        return results

    log.info(f"--- LIVE RUN for Run ID: {run_batch_id} (Plan ID: {plan.batch_id}) ---")
    
    primary_action_type = 'rename'
    backup_dir_path: Optional[Path] = None
    stage_dir_path: Optional[Path] = None

    if hasattr(args_ns, 'backup_dir') and args_ns.backup_dir:
        primary_action_type = 'backup'
        backup_dir_path = Path(args_ns.backup_dir).resolve()
    elif hasattr(args_ns, 'stage_dir') and args_ns.stage_dir:
        primary_action_type = 'stage'
        stage_dir_path = Path(args_ns.stage_dir).resolve()
    elif hasattr(args_ns, 'trash') and args_ns.trash:
        primary_action_type = 'trash'

    created_dir_this_plan: Optional[Path] = None
    try:
        created_dir_this_plan, resolved_target_map, original_mtimes, prep_ok = _prepare_live_actions(
            plan, cfg_helper, action_messages
        )
        if not prep_ok:
            results['success'] = False
            results['message'] = "\n".join(action_messages) if action_messages else "Preparation phase failed."
            return results

        if created_dir_this_plan:
            if undo_manager.is_enabled:
                undo_manager.log_action(
                    batch_id=run_batch_id,
                    original_path=created_dir_this_plan,
                    new_path=created_dir_this_plan,
                    item_type='dir',
                    status='created_dir'
                )
            action_messages.append(f"CREATED DIR: '{created_dir_this_plan}'")
            results['actions_taken'] += 1

        actions_performed_count = 0
        if primary_action_type == 'backup' and backup_dir_path:
            _perform_backup_action(plan, backup_dir_path, action_messages)
            actions_performed_count, phase2_errors_rename = _perform_transactional_rename_move(
                plan, run_batch_id, undo_manager, resolved_target_map, original_mtimes,
                cfg_helper('preserve_mtime', False), cfg_helper('on_conflict', 'skip'), action_messages
            )
            if phase2_errors_rename: results['success'] = False
        elif primary_action_type == 'trash':
            actions_performed_count = _perform_trash_action(plan, run_batch_id, undo_manager, action_messages)
        elif primary_action_type == 'stage' and stage_dir_path:
            actions_performed_count = _perform_stage_action(
                plan, stage_dir_path, run_batch_id, undo_manager,
                resolved_target_map, original_mtimes,
                cfg_helper('preserve_mtime', False), action_messages
            )
        elif primary_action_type == 'rename':
            actions_performed_count, phase2_errors_std_rename = _perform_transactional_rename_move(
                plan, run_batch_id, undo_manager, resolved_target_map, original_mtimes,
                cfg_helper('preserve_mtime', False), cfg_helper('on_conflict', 'skip'), action_messages
            )
            if phase2_errors_std_rename: results['success'] = False
        else:
            raise RenamerError(f"Internal Error: Unknown live action type '{primary_action_type}'")
        
        results['actions_taken'] += actions_performed_count

    except FileExistsError as e_fe_outer:
        log.critical(f"Stopping due to FileExistsError (conflict_mode='fail'): {e_fe_outer}")
        action_messages.append(f"STOPPED (File Exists): {e_fe_outer}")
        results['success'] = False
    except FileOperationError as e_foe_outer:
        log.error(f"File operation error during live run for plan {plan.batch_id}: {e_foe_outer}", exc_info=True)
        action_messages.append(f"ERROR (File Operation): {e_foe_outer}")
        results['success'] = False
    except Exception as e_unhandled_outer:
        log.exception(f"Unhandled error during file actions for run {run_batch_id}, plan {plan.batch_id}: {e_unhandled_outer}")
        results['success'] = False
        action_messages.append(f"CRITICAL UNHANDLED ERROR: {e_unhandled_outer}")

    if results['success'] and primary_action_type == 'backup' and actions_performed_count > 0 :
         backup_msg_idx = -1
         for i, msg_item in enumerate(action_messages):
             if msg_item.startswith("Backed up"): backup_msg_idx = i; break
         
         rename_summary_after_backup = f"Renamed/Moved {actions_performed_count} files after backup."
         if backup_msg_idx != -1 and backup_msg_idx + 1 <= len(action_messages):
             action_messages.insert(backup_msg_idx + 1, rename_summary_after_backup)
         elif backup_msg_idx != -1 : 
             action_messages.append(rename_summary_after_backup)
         else: 
             action_messages.insert(0, rename_summary_after_backup)

    results['message'] = "\n".join(action_messages) if action_messages else "Live run completed, no specific messages recorded."
    if results['success'] and results['actions_taken'] == 0 and not created_dir_this_plan :
        if not any(err_kw in m.upper() for m in action_messages for err_kw in ["ERROR", "CRITICAL", "STOPPED", "FAILED"]):
            results['message'] = "No file operations were performed (files may already be correct or skipped by configuration)."

    return results