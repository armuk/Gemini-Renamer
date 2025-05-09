import logging
import shutil
import uuid
from pathlib import Path
import argparse 
import sys
import os
import time
from typing import Dict, Callable, Set, Optional, Any, List, Tuple

# RICH Imports and Fallbacks
# ... (Keep these as they are) ...
import builtins
try:
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    class Console:
        def print(self, *args, **kwargs): builtins.print(*args, **kwargs)
        def input(self, *args, **kwargs) -> str: return builtins.input(*args, **kwargs)
    class Table:
        def __init__(self, *args, **kwargs): pass
        def add_column(self, *args, **kwargs): pass
        def add_row(self, *args, **kwargs): pass
    class Text:
        def __init__(self, text="", style=""): self.text = text; self.style = style
        def __str__(self): return self.text
        @property
        def plain(self): return self.text
# --- End RICH Imports ---


from .models import RenamePlan, RenameAction, MediaInfo, MediaMetadata
from .exceptions import FileOperationError, RenamerError
from .undo_manager import UndoManager

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
) -> Optional[Text]:
    # (Function unchanged)
    g_val = str(guess_value) if guess_value is not None else ""
    f_val = str(final_value) if final_value is not None else ""
    if is_numeric:
        try:
            g_num = int(g_val) if g_val else None
            f_num = int(f_val) if f_val else None
            if g_num == f_num: return None
        except (ValueError, TypeError): pass
    if g_val != f_val:
        return Text.assemble(
            Text(f"{field_name}: ", style="dim blue"),
            Text(f"'{g_val or '<unset>'}'", style="dim red"),
            Text(" -> ", style="dim blue"),
            Text(f"'{f_val or '<unset>'}'", style="dim green")
        )
    return None

def _handle_conflict(original_path: Path, target_path: Path, conflict_mode: str) -> Path:
    # (Function unchanged)
    if not target_path.exists() and not target_path.is_symlink(): return target_path
    log.warning(f"Conflict detected: Target '{target_path}' exists.")
    if conflict_mode == 'skip': raise FileOperationError(f"Target '{target_path.name}' exists (mode: skip).")
    if conflict_mode == 'fail': raise FileExistsError(f"Target '{target_path.name}' exists (mode: fail). Stopping.")
    if conflict_mode == 'overwrite': log.warning(f"Overwrite mode: Target '{target_path.name}' will be overwritten later."); return target_path
    if conflict_mode == 'suffix':
        counter = 1; original_stem = target_path.stem; original_ext = target_path.suffix;
        suffixed_path = target_path
        while suffixed_path.exists() or suffixed_path.is_symlink():
            new_stem = f"{original_stem}_{counter}"
            if len(new_stem) > 240: raise FileOperationError(f"Suffix failed: name too long for '{original_stem}'")
            suffixed_path = suffixed_path.with_name(f"{new_stem}{original_ext}")
            counter += 1;
            if counter > 100: raise FileOperationError(f"Suffix failed: >100 attempts for '{original_stem}'")
        log.info(f"Conflict resolved: Using suffixed name '{suffixed_path.name}' for '{original_path.name}'.")
        return suffixed_path
    raise RenamerError(f"Internal Error: Unknown conflict mode '{conflict_mode}'")


# --- HELPER: Dry Run Display ---
def _display_dry_run_plan(
    plan: RenamePlan,
    cfg_helper,
    media_info: Optional[MediaInfo] = None
) -> Tuple[bool, str, int]:
    # (Function unchanged)
    console = Console()
    log.info(f"--- DRY RUN Display for Plan ID: {plan.batch_id} ---")
    dry_run_actions_display_data = []
    original_paths_in_plan_dry = {a.original_path.resolve() for a in plan.actions}
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
         status_text_dir = Text("OK", style="green")
         action_text_dir = Text("Create Dir", style="bold green")
         new_path_text_dir = Text(str(plan.created_dir_path), style="green")
         if plan.created_dir_path.exists():
             status_text_dir = Text("Exists", style="yellow")
             action_text_dir = Text("Create Dir", style="bold yellow")
             new_path_text_dir = Text(str(plan.created_dir_path), style="yellow")
         dry_run_actions_display_data.append({
             "original": Text("-", style="dim"), "arrow": Text("->", style="dim"),
             "new": new_path_text_dir, "action": action_text_dir,
             "status": status_text_dir, "reason": Text("")
         })

    for action in plan.actions:
        simulated_final_target = action.new_path.resolve()
        target_exists_externally = (simulated_final_target.exists() and simulated_final_target not in original_paths_in_plan_dry) or simulated_final_target in current_targets_dry
        status_text = Text("OK", style="green")
        action_text = Text(action.action_type.capitalize(), style="blue")
        new_path_text = Text(str(action.new_path))
        reason_details: List[Text] = []

        if sys.platform == 'win32' and len(str(simulated_final_target)) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
             status_text = Text(f"Long Path (> {WINDOWS_PATH_LENGTH_WARNING_THRESHOLD})", style="bold yellow")
        if target_exists_externally:
            try:
                temp_conflict_mode_sim = conflict_mode if conflict_mode != 'fail' else 'skip'
                resolved_target_dry_sim = _handle_conflict(action.original_path, simulated_final_target, temp_conflict_mode_sim)
                if resolved_target_dry_sim != simulated_final_target:
                    status_text = Text(f"Conflict: Suffix -> '{resolved_target_dry_sim.name}'", style="yellow")
                    new_path_text = Text(str(resolved_target_dry_sim), style="yellow")
                elif conflict_mode == 'skip': status_text = Text("Conflict: Skip", style="bold yellow"); action_text = Text("Skip", style="bold yellow"); new_path_text = Text(str(action.new_path), style="dim yellow")
                elif conflict_mode == 'overwrite': status_text = Text("Conflict: Overwrite", style="bold yellow"); new_path_text = Text(str(action.new_path), style="yellow")
                elif conflict_mode == 'fail': status_text = Text("Conflict: Would Fail", style="bold red"); action_text = Text("Fail", style="bold red"); new_path_text = Text(str(action.new_path), style="dim red"); dry_run_conflict_error = True
                simulated_final_target = resolved_target_dry_sim
            except FileOperationError as e_dry_skip: status_text = Text(f"Conflict: Skip ({e_dry_skip})", style="bold yellow"); action_text = Text("Skip", style="bold yellow"); new_path_text = Text(str(action.new_path), style="dim yellow"); simulated_final_target = None
            except Exception as e_dry: status_text = Text(f"Error ({e_dry})", style="bold red"); action_text = Text("Fail", style="bold red"); simulated_final_target = None; dry_run_conflict_error = True
        if simulated_final_target:
            if simulated_final_target in current_targets_dry: status_text = Text("Conflict: Target Collision", style="bold red"); action_text = Text("Fail", style="bold red"); new_path_text = Text(str(action.new_path), style="dim red"); dry_run_conflict_error=True
            else: current_targets_dry.add(simulated_final_target)

        if media_info and action.original_path.resolve() == media_info.original_path.resolve():
            g_title = original_guess.get('title', ''); g_year = original_guess.get('year'); g_season = original_guess.get('season'); g_ep_raw = original_guess.get('episode'); g_ep = g_ep_raw[0] if isinstance(g_ep_raw, list) and g_ep_raw else g_ep_raw
            f_title, f_year, f_season, f_ep = None, None, None, None
            if final_metadata:
                if final_file_type == 'movie': f_title = final_metadata.movie_title; f_year = final_metadata.movie_year
                elif final_file_type == 'series': f_title = final_metadata.show_title; f_year = final_metadata.show_year; f_season = final_metadata.season; f_ep = final_metadata.episode_list[0] if final_metadata.episode_list else None
            f_title = f_title if f_title is not None else g_title; f_year = f_year if f_year is not None else g_year; f_season = f_season if f_season is not None else g_season; f_ep = f_ep if f_ep is not None else g_ep
            reason_details.extend(filter(None, [
                _compare_and_format("Title", g_title, f_title),
                _compare_and_format("Year", g_year, f_year, is_numeric=True)
            ]))
            if final_file_type == 'series': reason_details.extend(filter(None, [_compare_and_format("Season", g_season, f_season, is_numeric=True), _compare_and_format("Episode", g_ep, f_ep, is_numeric=True)]))
            if action.new_path.parent.resolve() != action.original_path.parent.resolve(): reason_details.append(Text("Folder Change", style="dim blue"))
        elif not media_info: reason_details.append(Text("Reason N/A (internal error)", style="yellow"))
        else: reason_details.append(Text("(matches video)", style="dim"))
        reason_text = Text("\n").join(filter(None, reason_details))
        preserve_mtime_info = Text(" (mtime preserved)", style="italic dim") if should_preserve_mtime and action.action_type in ['rename', 'move'] else Text("")
        dry_run_actions_display_data.append({"original": Text(str(action.original_path.name)), "arrow": Text("->", style="dim" if action_text.plain != "Fail" else "red"), "new": Text.assemble(new_path_text, preserve_mtime_info), "action": action_text, "status": status_text, "reason": reason_text})

    if dry_run_actions_display_data:
        table = Table(title=f"Dry Run Plan - Batch ID (approx): {plan.batch_id[:15]}", show_header=True, header_style="bold magenta")
        table.add_column("Original Name", style="dim cyan", no_wrap=False, min_width=20); table.add_column(" ", justify="center", width=2); table.add_column("New Path / Name", style="cyan", no_wrap=False, min_width=30); table.add_column("Action", justify="center"); table.add_column("Status / Conflict", justify="left", min_width=15); table.add_column("Reason / Changes", justify="left", min_width=20)
        for item in dry_run_actions_display_data: table.add_row(item["original"], item["arrow"], item["new"], item["action"], item["status"], item["reason"])
        console.print(table)
        message = f"Dry Run plan displayed above ({len(dry_run_actions_display_data)} potential actions)."
    else: message = "DRY RUN: No actions planned." ; console.print(message)
    planned_count = len([a for a in dry_run_actions_display_data if a["action"].plain not in ["Skip", "Fail"]])
    return dry_run_conflict_error, message, planned_count

# --- HELPER: Prepare Live Actions (Phase 0) ---
def _prepare_live_actions(
    plan: RenamePlan,
    cfg_helper,
    action_messages: List[str]
) -> Tuple[Optional[Path], Dict[Path, Path], Dict[Path, float], bool]:
    # (Function unchanged)
    conflict_mode = cfg_helper('on_conflict', 'skip')
    should_preserve_mtime = cfg_helper('preserve_mtime', False)
    created_dir: Optional[Path] = None
    resolved_target_map: Dict[Path, Path] = {}
    original_mtimes: Dict[Path, float] = {}
    success = True

    log.debug("Phase 0: Resolving final paths, checking conflicts, getting mtimes...")
    current_targets: Set[Path] = set()
    original_paths_in_plan: Set[Path] = {a.original_path.resolve() for a in plan.actions}

    try:
        if plan.created_dir_path and not plan.created_dir_path.exists():
            log.info(f"Creating folder: {plan.created_dir_path}")
            plan.created_dir_path.mkdir(parents=True, exist_ok=True)
            created_dir = plan.created_dir_path
        elif plan.created_dir_path:
            log.debug(f"Target directory already exists: {plan.created_dir_path}")

        for action in plan.actions:
            if not action.original_path.exists():
                log.warning(f"Phase 0 Skip: Original '{action.original_path.name}' not found.")
                continue
            orig_p_resolved = action.original_path.resolve()
            intended_final_p = action.new_path.resolve()
            final_target_p = intended_final_p
            if should_preserve_mtime and action.action_type != 'create_dir' :
                try: original_mtimes[orig_p_resolved] = action.original_path.stat().st_mtime; log.debug(f"  Stored original mtime for '{action.original_path.name}'")
                except OSError as stat_err: log.warning(f"Could not get mtime for '{action.original_path.name}': {stat_err}. Cannot preserve.")
            external_conflict = (final_target_p.exists() and final_target_p not in original_paths_in_plan)
            internal_conflict = final_target_p in current_targets
            if external_conflict or internal_conflict:
                stage_dir_arg = getattr(cfg_helper.args, 'stage_dir', None)
                if stage_dir_arg and action.action_type == 'move': 
                    stage_target_p = stage_dir_arg / intended_final_p.name
                    log.debug(f"  -> Checking conflict for staging target: {stage_target_p}")
                    final_target_p = _handle_conflict(action.original_path, stage_target_p, conflict_mode)
                else:
                    final_target_p = _handle_conflict(action.original_path, final_target_p, conflict_mode)
            if sys.platform == 'win32' and len(str(final_target_p)) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD: log.warning(f"Potential long path issue on Windows for target: '{final_target_p}'")
            if final_target_p in current_targets: raise FileOperationError(f"Internal Error: Multiple files resolve to target '{final_target_p.name}' after conflict resolution.")
            resolved_target_map[orig_p_resolved] = final_target_p; current_targets.add(final_target_p); log.debug(f"  Resolved: '{action.original_path.name}' -> '{final_target_p.name}'")
    except (FileOperationError, FileExistsError) as e:
        log.error(f"Conflict check or dir creation failed: {e}")
        action_messages.append(f"ERROR: {e}")
        success = False
        if isinstance(e, FileExistsError) and conflict_mode == 'fail': raise
    return created_dir, resolved_target_map, original_mtimes, success

# --- HELPER: Backup Action ---
def _perform_backup_action(
    plan: RenamePlan,
    backup_dir: Path,
    action_messages: List[str]
) -> None:
    # (Function unchanged)
    if not backup_dir: raise FileOperationError("Backup directory not specified.")
    backup_dir.mkdir(parents=True, exist_ok=True)
    backed_up_count = 0
    log.info(f"Starting backup phase to {backup_dir}...")
    for action in plan.actions:
        orig_p = action.original_path
        if not orig_p.exists(): log.warning(f"Cannot backup non-existent file: '{orig_p.name}'. Skipping backup."); continue
        backup_target = backup_dir / orig_p.name
        if sys.platform == 'win32' and len(str(backup_target.resolve())) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD: log.warning(f"Potential long path issue on Windows for backup target: '{backup_target.resolve()}'.")
        shutil.copy2(str(orig_p), str(backup_target)); log.debug(f"Backed up '{orig_p.name}'"); backed_up_count+=1
    if backed_up_count > 0: action_messages.append(f"Backed up {backed_up_count} files.")


# --- HELPER: Trash Action ---
def _perform_trash_action(
    plan: RenamePlan,
    run_batch_id: str,
    undo_manager: UndoManager,
    action_messages: List[str]
) -> int:
    # (Function unchanged)
    if not SEND2TRASH_AVAILABLE: raise FileOperationError("'send2trash' library not installed or available.")
    log.info("Starting trash phase..."); trash_count = 0
    for action in plan.actions:
        orig_p=action.original_path; final_p_intended = action.new_path
        if not orig_p.exists(): log.warning(f"Cannot trash non-existent file: '{orig_p.name}'. Skipping."); continue
        undo_manager.log_action(batch_id=run_batch_id, original_path=orig_p, new_path=final_p_intended, item_type='file', status='trashed')
        send2trash.send2trash(str(orig_p)); action_messages.append(f"TRASHED: '{orig_p.name}' (intended: '{final_p_intended.name}')"); trash_count+=1
    return trash_count


# --- HELPER: Stage Action ---
def _perform_stage_action(
    plan: RenamePlan,
    stage_dir: Path,
    run_batch_id: str,
    undo_manager: UndoManager,
    resolved_target_map: Dict[Path, Path],
    original_mtimes: Dict[Path, float],
    should_preserve_mtime: bool,
    action_messages: List[str]
) -> int:
    # (Function unchanged)
    if not stage_dir: raise FileOperationError("Staging directory not specified.")
    stage_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Starting staging phase to {stage_dir}..."); stage_count = 0
    for action in plan.actions:
        orig_p = action.original_path; orig_p_resolved = orig_p.resolve();
        final_staged_path = resolved_target_map.get(orig_p_resolved)
        if not final_staged_path: log.warning(f"Stage Skip: Could not find resolved target path for '{orig_p.name}'."); continue
        if not orig_p.exists(): log.warning(f"Cannot stage non-existent file: '{orig_p.name}'. Skipping stage."); continue
        if sys.platform == 'win32' and len(str(final_staged_path.resolve())) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD: log.warning(f"Potential long path issue on Windows for staged target: '{final_staged_path.resolve()}'.")
        undo_manager.log_action(batch_id=run_batch_id, original_path=orig_p, new_path=final_staged_path, item_type='file', status='moved')
        shutil.move(str(orig_p), str(final_staged_path)); action_messages.append(f"MOVED to stage: '{orig_p.name}' -> '{final_staged_path}'"); stage_count+=1
        if should_preserve_mtime:
            original_mtime = original_mtimes.get(orig_p_resolved)
            if original_mtime is not None:
                 try: log.debug(f"  -> Preserving mtime ({original_mtime:.2f}) for staged file '{final_staged_path.name}'"); os.utime(str(final_staged_path), (original_mtime, original_mtime))
                 except OSError as utime_err: log.warning(f"  -> Failed to preserve mtime for staged file '{final_staged_path.name}': {utime_err}")
            else: log.debug(f"  -> Could not preserve mtime for staged file '{final_staged_path.name}': Original mtime not found.")
    return stage_count


# --- REVISED HELPER: Transactional Rename/Move ---
def _perform_transactional_rename_move(
    plan: RenamePlan,
    run_batch_id: str,
    undo_manager: UndoManager,
    resolved_target_map: Dict[Path, Path], # Map of original_resolved_path -> final_target_path
    original_mtimes: Dict[Path, float],    # Map of original_resolved_path -> mtime
    should_preserve_mtime: bool,
    conflict_mode: str, # For P2, though ideally not needed if P0 handles all conflicts
    action_messages: List[str]
) -> Tuple[int, bool]: # Returns (actions_taken_count, phase2_errors_occurred)

    original_to_temp_map: Dict[Path, Path] = {} # original_resolved_path -> temp_path
    temp_to_final_map: Dict[Path, Path] = {}    # temp_path -> final_target_path (from resolved_target_map)
    phase1_ok = True
    actions_taken_count = 0

    # --- Phase 1: Move original files to temporary paths ---
    log.debug(f"Starting Phase 1: Move to temporary paths for run {run_batch_id}")
    for action in plan.actions:
        orig_p_resolved = action.original_path.resolve() # Key for maps
        final_p = resolved_target_map.get(orig_p_resolved)

        if not final_p:
            log.error(f"P1 Skip: Missing resolved final path for '{action.original_path.name}'. This indicates an internal error or issue in _prepare_live_actions.")
            continue
        if not action.original_path.exists(): # Check existence of the actual original file
            log.warning(f"P1 Skip: Original file '{action.original_path.name}' missing before move to temp.")
            # Update undo log if an entry was made for this original_path earlier (e.g. by _prepare_live_actions)
            # This depends on whether _prepare_live_actions logs anything itself.
            # Assuming 'pending_final' is only logged *during* Phase 1 if successful.
            continue

        try:
            # Generate a unique temporary path in the *final target directory*
            # This is crucial for cross-device moves; temp must be on the same filesystem as final.
            temp_uuid = uuid.uuid4().hex[:8]
            temp_path = final_p.parent / f"{final_p.stem}{TEMP_SUFFIX_PREFIX}{temp_uuid}{final_p.suffix}"
            while temp_path.exists() or temp_path.is_symlink(): # Ensure temp name is truly unique
                temp_uuid = uuid.uuid4().hex[:8]
                temp_path = final_p.parent / f"{final_p.stem}{TEMP_SUFFIX_PREFIX}{temp_uuid}{final_p.suffix}"

            # Log 'pending_final' with the INTENDED final path, not the temp path.
            # The original_path in the log is the true original. new_path is the intended final.
            if undo_manager.is_enabled:
                undo_manager.log_action(
                    batch_id=run_batch_id,
                    original_path=action.original_path, # The very first original path
                    new_path=final_p,                   # The *intended* final destination
                    item_type='file',
                    status='pending_final'
                )

            log.debug(f"  P1 Moving '{action.original_path}' -> Temp '{temp_path}' (Final Target Dir: {final_p.parent})")
            shutil.move(str(action.original_path), str(temp_path)) # Move original to temp

            original_to_temp_map[orig_p_resolved] = temp_path
            temp_to_final_map[temp_path] = final_p
            log.debug(f"  P1 Success: '{action.original_path.name}' moved to temp '{temp_path.name}'.")

        except Exception as e:
            msg = f"P1 Error moving '{action.original_path.name}' to temp: {e}"
            log.error(msg, exc_info=True)
            action_messages.append(f"ERROR: {msg}")
            if undo_manager.is_enabled:
                 # Update status to 'failed_pending' if it was 'pending_final'
                 undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(action.original_path), new_status='failed_pending')
            phase1_ok = False
            break # Stop Phase 1 on first error

    # --- Phase 1 Rollback (if errors occurred) ---
    if not phase1_ok:
        log.warning(f"Rolling back Phase 1 for run {run_batch_id} due to error...")
        rollback_success_count = 0
        rollback_fail_count = 0
        
        # Rollback files moved to temp
        for orig_p_res_rb, temp_p_rb in original_to_temp_map.items():
            # original_path_for_rollback is the key from original_to_temp_map (already resolved Path object)
            original_path_for_rollback = Path(orig_p_res_rb) 
            log.debug(f"  Attempting P1 rollback: Temp '{temp_p_rb}' -> Original '{original_path_for_rollback}'")
            try:
                if temp_p_rb.exists():
                    shutil.move(str(temp_p_rb), str(original_path_for_rollback))
                    log.info(f"  P1 Rollback successful: '{temp_p_rb.name}' -> '{original_path_for_rollback.name}'")
                    if undo_manager.is_enabled:
                        # Status was 'pending_final', now it's 'failed_pending' (or 'reverted' if we consider this a revert)
                        # 'failed_pending' seems more appropriate as the overall operation failed.
                        undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(original_path_for_rollback), new_status='failed_pending')
                    rollback_success_count += 1
                else:
                    log.warning(f"  P1 Rollback skipped for '{original_path_for_rollback.name}', temp file '{temp_p_rb}' not found (already rolled back or vanished).")
                    # Ensure status is failed if it was pending
                    if undo_manager.is_enabled:
                        undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(original_path_for_rollback), new_status='failed_pending')

            except Exception as e_rb:
                log.error(f"  P1 Rollback Error moving '{temp_p_rb.name}' to '{original_path_for_rollback.name}': {e_rb}")
                action_messages.append(f"CRITICAL: P1 Rollback failed for '{temp_p_rb.name}'. File may be at '{temp_p_rb}'. Original was '{original_path_for_rollback}'.")
                rollback_fail_count += 1
        
        # Rollback created directory if it's empty
        created_dir_for_rollback = plan.created_dir_path # From the plan
        if created_dir_for_rollback and created_dir_for_rollback.is_dir():
            try:
                if not any(created_dir_for_rollback.iterdir()): # Check if empty
                    log.debug(f"  Attempting P1 rollback: Removing created directory '{created_dir_for_rollback}'")
                    created_dir_for_rollback.rmdir()
                    log.info(f"  P1 Rollback successful: Removed directory '{created_dir_for_rollback}'")
                    if undo_manager.is_enabled:
                         undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(created_dir_for_rollback), new_status='reverted') # Or 'failed_dir_creation'
                else:
                    log.warning(f"  P1 Rollback: Directory '{created_dir_for_rollback}' not empty, not removed.")
            except OSError as e_rdir:
                log.error(f"  P1 Rollback could not remove created dir '{created_dir_for_rollback}': {e_rdir}")
                action_messages.append(f"CRITICAL: P1 Rollback failed to remove directory '{created_dir_for_rollback}'.")

        action_messages.append(f"Phase 1 Rollback Summary: {rollback_success_count} files restored, {rollback_fail_count} file restore failures.")
        return 0, True # 0 actions taken successfully, phase2_errors = True because P1 failed

    # --- Phase 2: Rename temporary paths to final paths ---
    log.debug(f"Starting Phase 2: Rename temporary paths to final for run {run_batch_id}")
    phase2_errors_occurred = False
    for temp_path, final_path in temp_to_final_map.items():
        original_path_for_log: Optional[Path] = None
        original_path_resolved_for_mtime: Optional[Path] = None # This should be the Path object key

        # Find the original resolved Path object that corresponds to this temp_path
        for orig_res_path_obj, mapped_temp_path in original_to_temp_map.items():
            if mapped_temp_path.resolve() == temp_path.resolve():
                original_path_for_log = Path(str(orig_res_path_obj)) # Ensure it's a Path object from string if needed
                original_path_resolved_for_mtime = orig_res_path_obj # This is the original resolved Path key
                break
        
        if not original_path_for_log:
            log.error(f"P2 CRITICAL: Could not find original path mapping for temp file '{temp_path}'. This indicates a serious internal inconsistency. Skipping.")
            action_messages.append(f"ERROR: P2 Internal error for temp file '{temp_path.name}'.")
            phase2_errors_occurred = True
            continue

        if not temp_path.exists():
            log.error(f"P2 Error: Temp file '{temp_path}' not found! Cannot complete rename for '{original_path_for_log.name}'.")
            action_messages.append(f"ERROR: P2 Temp file '{temp_path.name}' missing for '{original_path_for_log.name}'.")
            if undo_manager.is_enabled:
                undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(original_path_for_log), new_status='failed_pending')
            phase2_errors_occurred = True
            continue

        try:
            if final_path.exists() or final_path.is_symlink():
                # This check should ideally be redundant if _prepare_live_actions and conflict_mode worked.
                # 'overwrite' mode in _handle_conflict already allows target to exist.
                # If conflict_mode was 'skip' or 'fail', _prepare_live_actions should have raised an error.
                # If conflict_mode was 'suffix', final_path should be unique.
                # So, if we reach here and final_path exists, it's either overwrite or an issue.
                if conflict_mode == 'overwrite':
                    log.warning(f"  P2 Overwriting existing target (as per conflict_mode='overwrite'): '{final_path}'")
                    final_path.unlink(missing_ok=True) # missing_ok for safety if it vanishes between check and unlink
                else:
                    # This case implies an unexpected conflict.
                    log.error(f"  P2 Error: Target '{final_path}' exists unexpectedly! Conflict mode '{conflict_mode}' should have handled this or P0 failed. Original: '{original_path_for_log.name}'.")
                    action_messages.append(f"ERROR: P2 Target '{final_path.name}' exists unexpectedly for '{original_path_for_log.name}'.")
                    if undo_manager.is_enabled:
                        undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(original_path_for_log), new_status='failed_pending')
                    phase2_errors_occurred = True
                    continue
            
            rename_successful_p2 = False
            try:
                log.debug(f"  P2 Attempting (os.rename): '{temp_path.name}' -> '{final_path.name}'")
                os.rename(str(temp_path), str(final_path))
                log.debug(f"  P2 Success (os.rename)")
                rename_successful_p2 = True
            except OSError as e_os_rename_p2:
                log.warning(f"  P2 os.rename failed ('{e_os_rename_p2}'), attempting shutil.move for '{temp_path.name}' -> '{final_path.name}'...")
                try:
                    shutil.move(str(temp_path), str(final_path))
                    log.debug(f"  P2 Success (shutil.move)")
                    rename_successful_p2 = True
                except Exception as e_shutil_move_p2:
                    log.error(f"  P2 shutil.move also failed for '{temp_path.name}': {e_shutil_move_p2}")
            except Exception as e_generic_rename_p2:
                log.error(f"  P2 Unexpected error during os.rename for '{temp_path.name}': {e_generic_rename_p2}")

            if rename_successful_p2:
                actions_taken_count += 1
                log.info(f"  P2 Successfully renamed/moved '{original_path_for_log.name}' (from temp) to '{final_path}'")

                if should_preserve_mtime and original_path_resolved_for_mtime:
                    original_mtime = original_mtimes.get(original_path_resolved_for_mtime)
                    if original_mtime is not None:
                        try:
                            log.debug(f"    P2 Preserving mtime ({original_mtime:.2f}) for '{final_path.name}'")
                            os.utime(str(final_path), (original_mtime, original_mtime))
                        except OSError as utime_err:
                            log.warning(f"    P2 Failed to preserve mtime for '{final_path.name}': {utime_err}")
                    else:
                        log.debug(f"    P2 Could not preserve mtime for '{final_path.name}': Original mtime not found in map using key {original_path_resolved_for_mtime}.")
                
                if undo_manager.is_enabled:
                    final_status = 'moved' if final_path.parent.resolve() != original_path_for_log.parent.resolve() else 'renamed'
                    # Update the status of the log entry (original -> final)
                    if not undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(original_path_for_log), new_status=final_status):
                        log.error(f"  P2 Success, but FAILED to update undo log status for '{original_path_for_log}' to '{final_status}'")
                        action_messages.append(f"ACTION LOG UPDATE FAILED? '{original_path_for_log.name}' -> '{final_path}'")
                    else:
                        action_messages.append(f"{final_status.upper()}D: '{original_path_for_log.name}' -> '{final_path}'")
            else: # rename_successful_p2 is False
                msg = f"P2 All rename attempts FAILED for '{original_path_for_log.name}' (from temp '{temp_path.name}') to '{final_path.name}'."
                log.error(msg)
                action_messages.append(f"ERROR: {msg} File remains at '{temp_path}'.")
                if undo_manager.is_enabled:
                    undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(original_path_for_log), new_status='failed_pending')
                phase2_errors_occurred = True
        
        except Exception as e_outer_p2:
            msg = f"P2 Outer Error processing temp '{temp_path.name}' to '{final_path.name}' (Original: '{original_path_for_log.name}'): {e_outer_p2}"
            log.critical(msg, exc_info=True)
            action_messages.append(f"ERROR: {msg}. File may remain at '{temp_path}'.")
            if undo_manager.is_enabled and original_path_for_log: # original_path_for_log should be valid here
                undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(original_path_for_log), new_status='failed_pending')
            phase2_errors_occurred = True

    if phase2_errors_occurred:
        log.error(f"Phase 2 completed with errors for run {run_batch_id}. Some files may be in temporary state.")
    else:
        log.debug(f"Phase 2 completed successfully for run {run_batch_id}.")
        
    return actions_taken_count, phase2_errors_occurred
# --- END REVISED HELPER ---


# --- MAIN FUNCTION: perform_file_actions (Orchestrator) ---
def perform_file_actions(
    plan: RenamePlan,
    args_ns: argparse.Namespace,
    cfg_helper,
    undo_manager: UndoManager,
    run_batch_id: str,
    media_info: Optional[MediaInfo] = None
) -> Dict[str, Any]:
    results = {'success': True, 'message': "", 'actions_taken': 0}
    action_messages: List[str] = []

    if not getattr(args_ns, 'live', False):
        conflict_error_dry_run, msg, planned_actions_count = _display_dry_run_plan(plan, cfg_helper, media_info)
        results['success'] = not conflict_error_dry_run
        results['message'] = msg
        results['actions_taken'] = planned_actions_count # For dry run, this is "planned"
        return results

    # --- Live Run ---
    log.info(f"--- LIVE RUN for Run ID: {run_batch_id} (Plan ID: {plan.batch_id}) ---")
    action_type = 'rename' 
    if getattr(args_ns, 'stage_dir', None): action_type = 'stage'
    elif getattr(args_ns, 'trash', False): action_type = 'trash'
    elif getattr(args_ns, 'backup_dir', None): action_type = 'backup'

    try:
        created_dir, resolved_target_map, original_mtimes, prep_success = _prepare_live_actions(
            plan, cfg_helper, action_messages
        )
        if not prep_success:
            results['success'] = False; results['message'] = "\n".join(action_messages); return results

        if created_dir:
            if undo_manager.is_enabled:
                undo_manager.log_action(batch_id=run_batch_id, original_path=created_dir, new_path=created_dir, item_type='dir', status='created_dir')
            action_messages.append(f"CREATED DIR: '{created_dir}'")
            # Do not add to results['actions_taken'] here if it's counted by the main transactional logic later.
            # For now, transactional logic only counts file moves/renames.

        if action_type == 'backup':
            _perform_backup_action(plan, args_ns.backup_dir, action_messages)
            action_type = 'rename' # Proceed to rename after backup

        actions_done_this_type = 0
        if action_type == 'trash':
            actions_done_this_type = _perform_trash_action(plan, run_batch_id, undo_manager, action_messages)
        elif action_type == 'stage':
            actions_done_this_type = _perform_stage_action(
                plan, args_ns.stage_dir, run_batch_id, undo_manager,
                resolved_target_map, original_mtimes,
                cfg_helper('preserve_mtime', False), action_messages
            )
        elif action_type == 'rename':
            # This is the main transactional rename/move
            actions_done_this_type, phase2_errors = _perform_transactional_rename_move(
                plan, run_batch_id, undo_manager,
                resolved_target_map, original_mtimes,
                cfg_helper('preserve_mtime', False),
                cfg_helper('on_conflict', 'skip'),
                action_messages
            )
            if phase2_errors:
                action_messages.append("CRITICAL: Errors occurred during final rename phase (Phase 2). Some files might be in temporary state or not fully processed.")
                results['success'] = False # Mark overall as not fully successful
        else:
            raise RenamerError(f"Internal Error: Unknown live action type '{action_type}'")
        
        results['actions_taken'] += actions_done_this_type # Accumulate file actions

    except FileExistsError as e:
        log.critical(f"Stopping due to FileExistsError (conflict_mode='fail'): {e}")
        action_messages.append(f"STOPPED: {e}")
        results['success'] = False
    except FileOperationError as e:
        log.error(f"File operation error during live run for plan {plan.batch_id}: {e}", exc_info=True)
        action_messages.append(f"ERROR: {e}")
        results['success'] = False
    except Exception as e:
        log.exception(f"Unhandled error during file actions for run {run_batch_id}, plan {plan.batch_id}: {e}")
        results['success'] = False
        action_messages.append(f"CRITICAL UNHANDLED ERROR: {e}")

    # Final message assembly
    if results['success'] and action_type == 'rename' and getattr(args_ns, 'backup_dir', None) and results['actions_taken'] > 0 :
         backup_msg_index = -1
         for i, msg_item in enumerate(action_messages):
             if msg_item.startswith("Backed up"): backup_msg_index = i; break
         summary_msg = f"Renamed/Moved {results['actions_taken']} files after backup."
         if backup_msg_index != -1: action_messages.insert(backup_msg_index + 1, summary_msg)
         else: action_messages.insert(0, summary_msg)

    # Add count of created directories to total actions if not already included
    # and if we want to represent it in the summary count.
    # For now, actions_taken primarily reflects file renames/moves.
    if created_dir and results['success']:
        # results['actions_taken'] += 1 # Optional: if dir creation counts as a main "action"
        pass


    results['message'] = "\n".join(action_messages) if action_messages else "Live run completed, no specific messages."
    if results['success'] and not results['actions_taken'] and not created_dir: # Check created_dir too
        if not any(err_kw in m.upper() for m in action_messages for err_kw in ["ERROR", "CRITICAL", "STOPPED"]):
            results['message'] = "No file operations were performed (files may already be correct or skipped by configuration)."

    return results

# --- END OF FILE file_system_ops.py ---