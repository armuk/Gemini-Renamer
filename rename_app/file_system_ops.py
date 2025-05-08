# --- START OF FILE file_system_ops.py ---

import logging
import shutil
import uuid
from pathlib import Path
import argparse
import sys
import os # Keep os import for os.rename and os.utime
import time # Import time for mtime comparison/setting if needed
from typing import Dict, Callable, Set, Optional, Any, List, Tuple

# RICH Imports
import builtins
try:
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    # Minimal fallbacks if rich isn't available
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
        def plain(self): return self.text # Add plain property fallback
# --- End RICH Imports ---

from .models import RenamePlan, RenameAction, MediaMetadata, MediaInfo
from .exceptions import FileOperationError, RenamerError
from .undo_manager import UndoManager # Import for type hint

try: import send2trash; SEND2TRASH_AVAILABLE = True
except ImportError: SEND2TRASH_AVAILABLE = False

log = logging.getLogger(__name__)
TEMP_SUFFIX_PREFIX = ".renametmp_"
WINDOWS_PATH_LENGTH_WARNING_THRESHOLD = 240
# Define MTIME_TOLERANCE if used for comparisons (might not be needed here)
# MTIME_TOLERANCE = 1.0

def _compare_and_format(
    field_name: str,
    guess_value: Optional[Any],
    final_value: Optional[Any],
    is_numeric: bool = False
) -> Optional[Text]:
    """Compares guess vs final value and returns formatted Text if different."""
    # Normalize empty strings/None for comparison
    g_val = str(guess_value) if guess_value is not None else ""
    f_val = str(final_value) if final_value is not None else ""

    # Special handling for numeric comparison (treat None/empty string as unequal to 0)
    if is_numeric:
        try:
            g_num = int(g_val) if g_val else None
            f_num = int(f_val) if f_val else None
            if g_num == f_num:
                return None # Numeric values are the same
        except (ValueError, TypeError):
            pass # Fall back to string comparison if conversion fails

    # String comparison after potential numeric check
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

UndoLogCallable = Callable[[str, Path, Path, str, str], None]

# --- MODIFIED: Added run_batch_id argument ---
def perform_file_actions(
    plan: RenamePlan,
    args_ns: argparse.Namespace,
    cfg_helper,
    undo_manager: UndoManager,
    run_batch_id: str,
    media_info: Optional[MediaInfo] = None # Add MediaInfo object
) -> Dict[str, Any]:
    results = {'success': True, 'message': "", 'actions_taken': 0}
    action_messages = []
    conflict_mode = cfg_helper('on_conflict', 'skip')
    should_preserve_mtime = cfg_helper('preserve_mtime', False)
    console = Console()

    # --- Dry Run ---
    if not getattr(args_ns, 'live', False):
        log.info(f"--- DRY RUN for Run ID: {run_batch_id} (Plan ID: {plan.batch_id}) ---")
        dry_run_actions = []
        original_paths_in_plan_dry = {a.original_path.resolve() for a in plan.actions}
        current_targets_dry: Set[Path] = set()
        dry_run_conflict_error = False

        # --- Prepare data for comparison (only needed in dry run) ---
        original_guess: Dict[str, Any] = {}
        final_metadata: Optional[MediaMetadata] = None
        final_file_type: str = 'unknown'
        if media_info: # Check if media_info was provided
             original_guess = media_info.guess_info if media_info.guess_info else {}
             final_metadata = media_info.metadata
             final_file_type = media_info.file_type
        # --- End data preparation ---

        # --- Create Dir Action ---
        reason_text_dir = Text("") # Placeholder
        if plan.created_dir_path:
             status_text_dir = Text("OK", style="green")
             action_text_dir = Text("Create Dir", style="bold green")
             new_path_text_dir = Text(str(plan.created_dir_path), style="green")
             if plan.created_dir_path.exists():
                 status_text_dir = Text("Exists", style="yellow")
                 action_text_dir = Text("Create Dir", style="bold yellow")
                 new_path_text_dir = Text(str(plan.created_dir_path), style="yellow")
             dry_run_actions.append({
                 "original": Text("-", style="dim"),
                 "arrow": Text("->", style="dim"),
                 "new": new_path_text_dir,
                 "action": action_text_dir,
                 "status": status_text_dir,
                 "reason": reason_text_dir # Add reason column
             })

        # --- File Actions ---
        for action in plan.actions:
            simulated_final_target = action.new_path.resolve()
            target_exists_externally = (simulated_final_target.exists() and simulated_final_target not in original_paths_in_plan_dry) or simulated_final_target in current_targets_dry

            status_text = Text("OK", style="green")
            action_text = Text(action.action_type.capitalize(), style="blue")
            new_path_text = Text(str(action.new_path))
            reason_details: List[Text] = []

            # Check for long path
            if sys.platform == 'win32' and len(str(simulated_final_target)) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
                 status_text = Text(f"Long Path (> {WINDOWS_PATH_LENGTH_WARNING_THRESHOLD})", style="bold yellow")

            # Check for conflicts
            if target_exists_externally:
                try:
                    # Simulate conflict resolution, but don't actually fail the dry run on 'fail' mode
                    temp_conflict_mode = conflict_mode if conflict_mode != 'fail' else 'skip'
                    resolved_target_dry_sim = _handle_conflict(action.original_path, simulated_final_target, temp_conflict_mode)

                    if resolved_target_dry_sim != simulated_final_target:
                        status_text = Text(f"Conflict: Suffix -> '{resolved_target_dry_sim.name}'", style="yellow")
                        new_path_text = Text(str(resolved_target_dry_sim), style="yellow")
                    elif conflict_mode == 'skip':
                        status_text = Text("Conflict: Skip", style="bold yellow")
                        action_text = Text("Skip", style="bold yellow")
                        new_path_text = Text(str(action.new_path), style="dim yellow")
                    elif conflict_mode == 'overwrite':
                        status_text = Text("Conflict: Overwrite", style="bold yellow")
                        new_path_text = Text(str(action.new_path), style="yellow")
                    # Add a note if 'fail' mode would have been triggered
                    elif conflict_mode == 'fail':
                         status_text = Text("Conflict: Would Fail", style="bold red")
                         action_text = Text("Fail", style="bold red")
                         new_path_text = Text(str(action.new_path), style="dim red")
                         dry_run_conflict_error = True # Flag potential failure

                    simulated_final_target = resolved_target_dry_sim # Use the potentially resolved target

                except FileOperationError as e_dry_skip:
                    status_text = Text(f"Conflict: Skip ({e_dry_skip})", style="bold yellow")
                    action_text = Text("Skip", style="bold yellow")
                    new_path_text = Text(str(action.new_path), style="dim yellow")
                    simulated_final_target = None # Cannot proceed
                except Exception as e_dry: # Catch other potential errors during _handle_conflict simulation
                    status_text = Text(f"Conflict Error ({e_dry})", style="bold red")
                    action_text = Text("Fail", style="bold red")
                    new_path_text = Text(str(action.new_path), style="dim red")
                    simulated_final_target = None
                    dry_run_conflict_error = True

            # Check for internal collisions (multiple sources mapping to the same final target)
            if simulated_final_target:
                if simulated_final_target in current_targets_dry:
                    status_text = Text("Conflict: Target Collision", style="bold red")
                    action_text = Text("Fail", style="bold red")
                    new_path_text = Text(str(action.new_path), style="dim red")
                    dry_run_conflict_error = True
                else:
                    current_targets_dry.add(simulated_final_target)

            # --- Generate Reason Details (Only for the main video file action) ---
            # We assume associated files change *because* the video file changed.
            if media_info and action.original_path.resolve() == media_info.original_path.resolve():
                g_title = original_guess.get('title', '')
                g_year = original_guess.get('year')
                g_season = original_guess.get('season')
                # Handle guess episode (can be list or int)
                g_ep_raw = original_guess.get('episode')
                g_ep = g_ep_raw[0] if isinstance(g_ep_raw, list) and g_ep_raw else g_ep_raw

                f_title, f_year, f_season, f_ep = None, None, None, None
                if final_metadata:
                    if final_file_type == 'movie':
                        f_title = final_metadata.movie_title
                        f_year = final_metadata.movie_year
                    elif final_file_type == 'series':
                        f_title = final_metadata.show_title
                        f_year = final_metadata.show_year
                        f_season = final_metadata.season
                        f_ep = final_metadata.episode_list[0] if final_metadata.episode_list else None
                # Fallback to guess if no metadata
                f_title = f_title if f_title is not None else g_title
                f_year = f_year if f_year is not None else g_year
                f_season = f_season if f_season is not None else g_season
                f_ep = f_ep if f_ep is not None else g_ep

                reason_details.append(_compare_and_format("Title", g_title, f_title))
                reason_details.append(_compare_and_format("Year", g_year, f_year, is_numeric=True))
                if final_file_type == 'series':
                     reason_details.append(_compare_and_format("Season", g_season, f_season, is_numeric=True))
                     reason_details.append(_compare_and_format("Episode", g_ep, f_ep, is_numeric=True))

                # Check folder change
                if action.new_path.parent.resolve() != action.original_path.parent.resolve():
                     reason_details.append(Text("Folder Change", style="dim blue"))

            elif not media_info: # Handle case where media_info wasn't passed
                 reason_details.append(Text("Reason N/A (internal error)", style="yellow"))
            else: # For associated files
                 reason_details.append(Text("(matches video)", style="dim"))

            # Filter out None reasons and join with newlines
            reason_text = Text("\n").join(filter(None, reason_details))

            # Preserve mtime info
            preserve_mtime_info = ""
            if should_preserve_mtime and action.action_type in ['rename', 'move']:
                 preserve_mtime_info = Text(" (mtime preserved)", style="italic dim")

            dry_run_actions.append({
                "original": Text(str(action.original_path.name)),
                "arrow": Text("->", style="dim" if action_text.plain != "Fail" else "red"),
                "new": Text.assemble(new_path_text, preserve_mtime_info),
                "action": action_text,
                "status": status_text,
                "reason": reason_text # Add the generated reason text
            })

        if dry_run_actions:
            table = Table(
                title=f"Dry Run Plan - Batch ID (approx): {plan.batch_id[:15]}",
                show_header=True, header_style="bold magenta"
            )
            table.add_column("Original Name", style="dim cyan", no_wrap=False, min_width=20)
            table.add_column(" ", justify="center", width=2)
            table.add_column("New Path / Name", style="cyan", no_wrap=False, min_width=30)
            table.add_column("Action", justify="center")
            table.add_column("Status / Conflict", justify="left", min_width=15)
            # --- ADD REASON COLUMN ---
            table.add_column("Reason / Changes", justify="left", min_width=20)

            for item in dry_run_actions:
                table.add_row(
                    item["original"], item["arrow"], item["new"],
                    item["action"], item["status"], item["reason"] # Add reason data
                )
            console.print(table)
            results['message'] = f"Dry Run plan displayed above ({len(dry_run_actions)} potential actions)."
        else:
            results['message'] = "DRY RUN: No actions planned."
            console.print(results['message'])

        results['success'] = not dry_run_conflict_error
        # Store planned actions count for summary, even if there were conflicts
        results['actions_taken'] = len([a for a in dry_run_actions if a["action"].plain not in ["Skip", "Fail"]])
        return results

    # --- Live Run ---
    log.info(f"--- LIVE RUN for Run ID: {run_batch_id} (Plan ID: {plan.batch_id}) ---") # Log both IDs for clarity
    action_type = 'rename'; original_to_temp_map: Dict[Path, Path] = {}; temp_to_final_map: Dict[Path, Path] = {}; resolved_target_map: Dict[Path, Path] = {}; created_dir: Optional[Path] = None
    original_mtimes: Dict[Path, float] = {} # Store original mtimes here

    if args_ns.stage_dir: action_type = 'stage'
    elif args_ns.trash: action_type = 'trash'
    elif args_ns.backup_dir: action_type = 'backup'

    # --- Phase 0: Resolve Paths, Check Conflicts, Get mtimes ---
    if action_type in ['rename', 'backup', 'stage']:
        try:
            log.debug("Phase 0: Resolving final paths, checking conflicts, getting mtimes...")
            current_targets: Set[Path] = set(); original_paths_in_plan: Set[Path] = {a.original_path.resolve() for a in plan.actions}
            for action in plan.actions:
                if not action.original_path.exists(): log.warning(f"Phase 0 Skip: Original '{action.original_path.name}' not found."); continue

                orig_p_resolved = action.original_path.resolve(); intended_final_p = action.new_path.resolve()
                final_target_p = intended_final_p

                # Get original mtime BEFORE conflict check
                if should_preserve_mtime and action.action_type != 'create_dir' :
                    try:
                        orig_mtime = action.original_path.stat().st_mtime
                        original_mtimes[orig_p_resolved] = orig_mtime
                        log.debug(f"  Stored original mtime {orig_mtime:.2f} for '{action.original_path.name}'")
                    except OSError as stat_err: log.warning(f"Could not get mtime for '{action.original_path.name}': {stat_err}. Cannot preserve.")

                # Conflict check logic adjusted
                external_conflict = (final_target_p.exists() and final_target_p not in original_paths_in_plan)
                internal_conflict = final_target_p in current_targets

                if external_conflict or internal_conflict:
                     # For staging, conflicts are checked against the *staging directory* target
                     if action_type == 'stage':
                         if not args_ns.stage_dir: raise FileOperationError("Staging directory not specified in args for Phase 0.")
                         stage_target_p = args_ns.stage_dir / intended_final_p.name # Target within staging dir
                         # Re-check if *this specific stage target* exists or conflicts internally
                         external_conflict_stage = stage_target_p.exists() # Already checked against staging dir? No, need to check now.
                         # Internal conflicts check uses the final_target_p which points to stage dir now.
                         # This logic might need refinement - let _handle_conflict manage stage dir conflicts.
                         log.debug(f"  -> Checking conflict for staging target: {stage_target_p}")
                         final_target_p = _handle_conflict(action.original_path, stage_target_p, conflict_mode)
                     else: # Rename/backup - conflict checked against original target dir
                         final_target_p = _handle_conflict(action.original_path, final_target_p, conflict_mode)


                if sys.platform == 'win32' and len(str(final_target_p)) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD: log.warning(f"Potential long path issue on Windows for target: '{final_target_p}' (Length: {len(str(final_target_p))}).")
                if final_target_p in current_targets: raise FileOperationError(f"Internal Error: Multiple files resolve to target '{final_target_p.name}' after conflict resolution.")
                resolved_target_map[orig_p_resolved] = final_target_p; current_targets.add(final_target_p); log.debug(f"  Resolved: '{action.original_path.name}' -> '{final_target_p.name}'")

        except (FileOperationError, FileExistsError) as e:
             # Use run_batch_id in error message
             log.error(f"Conflict check failed for run {run_batch_id}: {e}")
             results['success'] = False; action_messages.append(f"ERROR: {e}")
             results['message'] = "\n".join(action_messages)
             if isinstance(e, FileExistsError) and conflict_mode == 'fail': raise e
             return results

    # --- Proceed with Actions ---
    try:
        # Pre-action setup
        if action_type == 'backup' and args_ns.backup_dir: args_ns.backup_dir.mkdir(parents=True, exist_ok=True)
        if action_type == 'stage' and args_ns.stage_dir: args_ns.stage_dir.mkdir(parents=True, exist_ok=True)

        # Create target directory if needed
        if plan.created_dir_path:
             target_dir = plan.created_dir_path
             if not target_dir.exists():
                 log.info(f"Creating folder: {target_dir}"); target_dir.mkdir(parents=True, exist_ok=True)
                 created_dir = target_dir
                 # --- MODIFIED: Use run_batch_id ---
                 undo_manager.log_action(batch_id=run_batch_id, original_path=target_dir, new_path=target_dir, item_type='dir', status='created_dir')
                 action_messages.append(f"CREATED DIR: '{target_dir}'")
             else: log.debug(f"Target directory already exists: {target_dir}")

        # Backup Logic
        if action_type == 'backup':
            if not args_ns.backup_dir: raise FileOperationError("Backup directory specified in args is missing.")
            backed_up_count = 0; log.info(f"Starting backup phase to {args_ns.backup_dir}...")
            for action in plan.actions:
                orig_p = action.original_path
                if not orig_p.exists(): log.warning(f"Cannot backup non-existent file: '{orig_p.name}'. Skipping backup."); continue
                backup_target = args_ns.backup_dir / orig_p.name
                if sys.platform == 'win32' and len(str(backup_target.resolve())) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD: log.warning(f"Potential long path issue on Windows for backup target: '{backup_target.resolve()}'.")
                shutil.copy2(str(orig_p), str(backup_target)); log.debug(f"Backed up '{orig_p.name}'"); backed_up_count+=1
            if backed_up_count > 0: action_messages.append(f"Backed up {backed_up_count} files.")
            action_type = 'rename' # Proceed to rename

        # --- Perform Main Action ---
        if action_type == 'trash':
            # Trash logic (unchanged)
            if not SEND2TRASH_AVAILABLE: raise FileOperationError("'send2trash' library not installed or available.")
            log.info("Starting trash phase..."); trash_count = 0
            for action in plan.actions:
                orig_p=action.original_path; final_p_intended = action.new_path
                if not orig_p.exists(): log.warning(f"Cannot trash non-existent file: '{orig_p.name}'. Skipping."); continue
                # --- MODIFIED: Use run_batch_id ---
                undo_manager.log_action(batch_id=run_batch_id, original_path=orig_p, new_path=final_p_intended, item_type='file', status='trashed')
                send2trash.send2trash(str(orig_p)); action_messages.append(f"TRASHED: '{orig_p.name}' (intended: '{final_p_intended.name}')"); trash_count+=1
            results['actions_taken'] = trash_count
        elif action_type == 'stage':
             # Staging logic (updated for mtime)
            if not args_ns.stage_dir: raise FileOperationError("Staging directory specified in args is missing.")
            log.info(f"Starting staging phase to {args_ns.stage_dir}..."); stage_count = 0
            for action in plan.actions:
                orig_p = action.original_path; orig_p_resolved = orig_p.resolve();
                final_staged_path = resolved_target_map.get(orig_p_resolved) # Path already resolved in Phase 0

                if not final_staged_path: log.warning(f"Stage Skip: Could not find resolved target path for '{orig_p.name}'."); continue
                if not orig_p.exists(): log.warning(f"Cannot stage non-existent file: '{orig_p.name}'. Skipping stage."); continue

                if sys.platform == 'win32' and len(str(final_staged_path.resolve())) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD: log.warning(f"Potential long path issue on Windows for staged target: '{final_staged_path.resolve()}'.")

                # --- MODIFIED: Use run_batch_id ---
                undo_manager.log_action(batch_id=run_batch_id, original_path=orig_p, new_path=final_staged_path, item_type='file', status='moved')
                shutil.move(str(orig_p), str(final_staged_path)); action_messages.append(f"MOVED to stage: '{orig_p.name}' -> '{final_staged_path}'"); stage_count+=1

                # Preserve mtime for staged files
                if should_preserve_mtime:
                    original_mtime = original_mtimes.get(orig_p_resolved)
                    if original_mtime is not None:
                         try:
                             log.debug(f"  -> Preserving mtime ({original_mtime:.2f}) for staged file '{final_staged_path.name}'")
                             os.utime(str(final_staged_path), (original_mtime, original_mtime)) # Use (mtime, mtime)
                         except OSError as utime_err: log.warning(f"  -> Failed to preserve mtime for staged file '{final_staged_path.name}': {utime_err}")
                    else: log.debug(f"  -> Could not preserve mtime for staged file '{final_staged_path.name}': Original mtime not found.")
            results['actions_taken'] = stage_count

        elif action_type == 'rename': # Transactional Rename/Move Logic
            # Phase 1
            phase1_ok = True; original_to_temp_map.clear(); temp_to_final_map.clear()
            log.debug(f"Starting Phase 1: Move to temporary paths for run {run_batch_id}")
            for action in plan.actions:
                orig_p = action.original_path; orig_p_resolved = orig_p.resolve()
                final_p = resolved_target_map.get(orig_p_resolved)
                if not final_p:
                     if not orig_p.exists(): log.warning(f"P1 Skip: Original file '{orig_p.name}' missing before Phase 1 move.")
                     else: log.error(f"P1 Skip: Missing resolved path for '{orig_p.name}'. Internal error?")
                     continue
                if not orig_p.exists(): log.warning(f"P1 Skip: Original file '{orig_p.name}' missing just before move to temp."); continue
                try:
                    temp_uuid = uuid.uuid4().hex[:8]; temp_path = final_p.parent / f"{final_p.stem}{TEMP_SUFFIX_PREFIX}{temp_uuid}{final_p.suffix}"
                    while temp_path.exists() or temp_path.is_symlink(): temp_uuid = uuid.uuid4().hex[:8]; temp_path = final_p.parent / f"{final_p.stem}{TEMP_SUFFIX_PREFIX}{temp_uuid}{final_p.suffix}"
                    # --- MODIFIED: Use run_batch_id ---
                    undo_manager.log_action(batch_id=run_batch_id, original_path=orig_p, new_path=final_p, item_type='file', status='pending_final')
                    log.debug(f"Phase 1: Moving '{orig_p}' -> Temp '{temp_path}'"); shutil.move(str(orig_p), str(temp_path))
                    original_to_temp_map[orig_p_resolved] = temp_path; temp_to_final_map[temp_path] = final_p
                except Exception as e: msg = f"Phase 1 Error moving '{orig_p.name}' to temp: {e}"; log.error(msg, exc_info=True); action_messages.append(f"ERROR: {msg}"); phase1_ok = False; break

            # Phase 1 Rollback
            if not phase1_ok:
                 log.warning(f"Rolling back Phase 1 for run {run_batch_id} due to error..."); rollback_success_count = 0; rollback_fail_count = 0
                 for orig_p_res_rb, temp_p_rb in original_to_temp_map.items():
                     orig_p_rb = Path(orig_p_res_rb); log.debug(f"Attempting rollback: '{temp_p_rb}' -> '{orig_p_rb}'")
                     try:
                         if temp_p_rb.exists():
                             shutil.move(str(temp_p_rb), str(orig_p_rb))
                             # --- MODIFIED: Use run_batch_id ---
                             if undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(orig_p_rb), new_status='failed_pending'): log.info(f"Rollback successful: '{temp_p_rb.name}' -> '{orig_p_rb.name}'")
                             else: log.error(f"Rollback successful for '{orig_p_rb.name}', but failed to update undo status.")
                             rollback_success_count += 1
                         else:
                             log.warning(f"Rollback skipped for '{orig_p_rb.name}', temp file '{temp_p_rb}' not found.")
                             # --- MODIFIED: Use run_batch_id ---
                             undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(orig_p_rb), new_status='failed_pending')
                     except Exception as e_rb: log.error(f"Rollback Error moving '{temp_p_rb.name}' to '{orig_p_rb.name}': {e_rb}"); rollback_fail_count += 1; action_messages.append(f"CRITICAL: Rollback failed for '{temp_p_rb.name}'.")
                 if created_dir and created_dir.is_dir() and not any(created_dir.iterdir()):
                     try:
                         log.debug(f"Attempting rollback: Removing created directory '{created_dir}'"); created_dir.rmdir()
                         # --- MODIFIED: Use run_batch_id ---
                         if undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(created_dir), new_status='reverted'): log.info(f"Rollback successful: Removed directory '{created_dir}'")
                         else: log.error(f"Rollback removed directory '{created_dir}', but failed to update undo status.")
                     except OSError as e_rdir: log.error(f"Rollback could not remove created dir '{created_dir}': {e_rdir}"); action_messages.append(f"CRITICAL: Rollback failed to remove directory '{created_dir}'.")
                 action_messages.append(f"Phase 1 Rollback Summary: {rollback_success_count} succeeded, {rollback_fail_count} failed.")
                 results['success'] = False; results['message'] = "\n".join(action_messages); return results

            # --- Phase 2: Rename Temporary to Final (Corrected mtime and undo update placement) ---
            log.debug(f"Starting Phase 2: Rename temporary paths to final for run {run_batch_id}")
            phase2_errors = False; action_count_phase2 = 0
            for temp_path, final_path in temp_to_final_map.items():
                original_path_for_log: Optional[Path] = None
                original_path_resolved: Optional[Path] = None # Store resolved path for mtime lookup
                for orig_res, temp in original_to_temp_map.items():
                    if temp.resolve() == temp_path.resolve():
                        original_path_for_log = Path(orig_res)
                        original_path_resolved = orig_res # Keep the resolved version
                        break

                try:
                    # ... (Error checking: temp_path exists, final_path doesn't exist or overwrite allowed) ...
                    if not temp_path.exists(): # ... handle error ...
                        continue
                    if final_path.exists() or final_path.is_symlink(): # ... handle error or unlink ...
                        continue
                    rename_successful = False
                    try:
                        # ... (os.rename with shutil.move fallback) ...
                        os.rename(str(temp_path), str(final_path)); log.debug(f"  -> P2 Success (os.rename): '{temp_path.name}' -> '{final_path.name}'"); rename_successful = True
                    except OSError as e_os_rename:
                        log.warning(f"  -> P2 os.rename failed ({e_os_rename}), attempting shutil.move fallback...");
                        try: shutil.move(str(temp_path), str(final_path)); log.debug(f"  -> P2 Success (shutil.move): '{temp_path.name}' -> '{final_path.name}'"); rename_successful = True
                        except Exception as e_shutil: msg = f"P2 Error: Both os.rename and shutil.move failed for '{temp_path.name}' -> '{final_path.name}': {e_shutil}"; log.critical(msg, exc_info=True); action_messages.append(f"ERROR: {msg}"); phase2_errors = True
                    except Exception as e_generic_rename: msg = f"P2 Error: Unexpected issue during os.rename for '{temp_path.name}' -> '{final_path.name}': {e_generic_rename}"; log.critical(msg, exc_info=True); action_messages.append(f"ERROR: {msg}"); phase2_errors = True

                     # --- MOVED LOGIC INSIDE 'if rename_successful' ---
                    if rename_successful:
                        action_count_phase2 += 1
                        if original_path_for_log and original_path_resolved: # Check we found the original path
                            # Preserve mtime AFTER successful rename/move
                            if should_preserve_mtime:
                                original_mtime = original_mtimes.get(original_path_resolved) # Use RESOLVED path for lookup
                                if original_mtime is not None:
                                    try:
                                        log.debug(f"  -> Preserving mtime ({original_mtime:.2f}) for '{final_path.name}'")
                                        os.utime(str(final_path), (original_mtime, original_mtime))
                                    except OSError as utime_err: log.warning(f"  -> Failed to preserve mtime for '{final_path.name}': {utime_err}")
                                else: log.debug(f"  -> Could not preserve mtime for '{final_path.name}': Original mtime not found in map.")

                            # Update undo status to success status
                            final_status = 'moved' if final_path.parent.resolve() != original_path_for_log.parent.resolve() else 'renamed'
                            if undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(original_path_for_log), new_status=final_status):
                                action_messages.append(f"{final_status.upper()}D: '{original_path_for_log.name}' -> '{final_path}'")
                                results['actions_taken'] += 1
                            else:
                                log.error(f"Could not update undo log status for '{original_path_for_log}' to '{final_status}'")
                                action_messages.append(f"ACTION UNLOGGED? '{original_path_for_log.name}' -> '{final_path}'")
                        else: log.error(f"Internal error finding original path for '{temp_path}'."); action_messages.append(f"RENAMED (orig?): temp -> '{final_path.name}'")
                     # --- END MOVED LOGIC ---
                     # --- FIX: Mark as failed in undo log ONLY if rename FAILED ---
                    elif original_path_for_log: # Check if rename failed and we have original path
                        log.error(f"P2 Rename failed for '{original_path_for_log.name}'. Marking undo status as failed.")
                        # --- MODIFIED: Use run_batch_id ---
                        undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(original_path_for_log), new_status='failed_pending')
                    # --- END FIX ---


                except Exception as e_outer_p2:
                    msg = f"P2 Outer Error processing temp '{temp_path.name}' to '{final_path.name}': {e_outer_p2}"; log.critical(msg, exc_info=True); action_messages.append(f"ERROR: {msg}"); phase2_errors = True
                    # Mark as failed if an outer error occurred
                    # --- MODIFIED: Use run_batch_id ---
                    if original_path_for_log: undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(original_path_for_log), new_status='failed_pending')

            # Final checks (unchanged)
            if phase2_errors: action_messages.append("CRITICAL: Errors occurred during final rename phase."); results['success'] = False
            elif not phase1_ok: results['success'] = False
            else: results['success'] = True
            if args_ns.backup_dir and results['actions_taken'] > 0 and not phase2_errors:
                 backup_msg_index = -1;
                 for i, msg in enumerate(action_messages):
                     if msg.startswith("Backed up"): backup_msg_index = i; break
                 summary_msg = f"Renamed/Moved {results['actions_taken']} files after backup."
                 if backup_msg_index != -1: action_messages.insert(backup_msg_index + 1, summary_msg)
                 else: action_messages.insert(0, summary_msg)

    # --- Exception Handling and Rollback ---
    except Exception as e:
        # Use run_batch_id in log message
        log.exception(f"Unhandled error during file actions for run {run_batch_id}: {e}")
        results['success'] = False; action_messages.append(f"CRITICAL UNHANDLED ERROR: {e}")
        if action_type == 'rename' and original_to_temp_map: # Rollback Phase 1
            log.critical(f"Rolling back Phase 1 for run {run_batch_id} due to unhandled exception: {e}"); rollback_success_count = 0; rollback_fail_count = 0
            for orig_p_res_rb, temp_p_rb in original_to_temp_map.items():
                 orig_p_rb = Path(orig_p_res_rb); log.debug(f"Attempting rollback (exception): '{temp_p_rb}' -> '{orig_p_rb}'")
                 try:
                     if temp_p_rb.exists():
                         shutil.move(str(temp_p_rb), str(orig_p_rb))
                         # --- MODIFIED: Use run_batch_id ---
                         if undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(orig_p_rb), new_status='failed_pending'): log.info(f"Rollback successful (exception): '{temp_p_rb.name}' -> '{orig_p_rb.name}'")
                         else: log.error(f"Rollback successful (exception) for '{orig_p_rb.name}', but failed to update undo status.")
                         rollback_success_count += 1
                     else:
                         log.warning(f"Rollback skipped (exception) for '{orig_p_rb.name}', temp file '{temp_p_rb}' not found.")
                         # --- MODIFIED: Use run_batch_id ---
                         undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(orig_p_rb), new_status='failed_pending')
                 except Exception as e_rb: log.error(f"Rollback Error (exception) moving '{temp_p_rb.name}' to '{orig_p_rb.name}': {e_rb}"); rollback_fail_count += 1; action_messages.append(f"CRITICAL: Rollback failed (exception) for '{temp_p_rb.name}'.")
            if created_dir and created_dir.is_dir() and not any(created_dir.iterdir()):
                 try:
                     log.debug(f"Attempting rollback (exception): Removing created directory '{created_dir}'"); created_dir.rmdir()
                     # --- MODIFIED: Use run_batch_id ---
                     if undo_manager.update_action_status(batch_id=run_batch_id, original_path=str(created_dir), new_status='reverted'): log.info(f"Rollback successful (exception): Removed directory '{created_dir}'")
                     else: log.error(f"Rollback removed directory (exception) '{created_dir}', but failed to update undo status.")
                 except OSError as e_rdir: log.error(f"Rollback could not remove created dir (exception) '{created_dir}': {e_rdir}"); action_messages.append(f"CRITICAL: Rollback failed (exception) to remove directory '{created_dir}'.")
            action_messages.append(f"Unhandled Exception Rollback Summary: {rollback_success_count} succeeded, {rollback_fail_count} failed.")

    results['message'] = "\n".join(action_messages) if action_messages else "Live run completed."
    return results

# --- END OF FILE file_system_ops.py ---