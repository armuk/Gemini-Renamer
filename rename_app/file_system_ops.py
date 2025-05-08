# rename_app/file_system_ops.py

import logging
import shutil
import uuid
from pathlib import Path
import argparse
import sys
import os # Keep os import for os.rename
from typing import Dict, Callable, Set, Optional, Any

# --- RICH Imports ---
import builtins # Keep fallback
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
    class Table:
        def __init__(self, *args, **kwargs): pass
        def add_column(self, *args, **kwargs): pass
        def add_row(self, *args, **kwargs): pass
    class Text:
        def __init__(self, text="", style=""): self.text = text; self.style = style
        def __str__(self): return self.text
# --- End RICH Imports ---

from .models import RenamePlan, RenameAction
from .exceptions import FileOperationError, RenamerError
from .undo_manager import UndoManager

try: import send2trash; SEND2TRASH_AVAILABLE = True
except ImportError: SEND2TRASH_AVAILABLE = False

log = logging.getLogger(__name__)
TEMP_SUFFIX_PREFIX = ".renametmp_"
WINDOWS_PATH_LENGTH_WARNING_THRESHOLD = 240

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

# Ensure run_batch_id is accepted
def perform_file_actions(plan: RenamePlan, run_batch_id: str, args_ns: argparse.Namespace, cfg_helper, undo_manager: UndoManager) -> Dict[str, Any]:
    results = {'success': True, 'message': "", 'actions_taken': 0}
    action_messages = []
    batch_id = run_batch_id
    conflict_mode = cfg_helper('on_conflict', 'skip')
    # Instantiate console for potential rich output
    console = Console()

    # --- Dry Run ---
    if not getattr(args_ns, 'live', False): # Check the final 'live' status
        log.info(f"--- DRY RUN for Run ID: {batch_id} ---")
        
        dry_run_actions = [] # Store dicts for table rows
        original_paths_in_plan_dry = {a.original_path.resolve() for a in plan.actions}
        current_targets_dry: Set[Path] = set()
        dry_run_conflict_error = False
        
        # Add planned directory creation first
        if plan.created_dir_path and not plan.created_dir_path.exists():
            dry_run_actions.append({
                "original": Text("-", style="dim"),
                "arrow": Text("->", style="dim"),
                "new": Text(str(plan.created_dir_path), style="green"),
                "action": Text("Create Dir", style="bold green"),
                "status": Text("OK", style="green")
            })
        elif plan.created_dir_path:
             dry_run_actions.append({
                "original": Text("-", style="dim"),
                "arrow": Text("->", style="dim"),
                "new": Text(str(plan.created_dir_path), style="yellow"),
                "action": Text("Create Dir", style="bold yellow"),
                "status": Text("Exists", style="yellow")
            })


        for action in plan.actions:
            simulated_final_target = action.new_path.resolve()
            target_exists_externally = (simulated_final_target.exists() and simulated_final_target not in original_paths_in_plan_dry) or simulated_final_target in current_targets_dry
            status_text = Text("OK", style="green")
            action_text = Text(action.action_type.capitalize(), style="blue")
            new_path_text = Text(str(action.new_path)) # Default style

            # Check for potential long path issues (Windows)
            if sys.platform == 'win32' and len(str(simulated_final_target)) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
                 status_text = Text(f"Long Path (> {WINDOWS_PATH_LENGTH_WARNING_THRESHOLD})", style="bold yellow")

            if target_exists_externally:
                try:
                    temp_conflict_mode = conflict_mode if conflict_mode != 'fail' else 'skip'
                    resolved_target_dry_sim = _handle_conflict(action.original_path, simulated_final_target, temp_conflict_mode)

                    if resolved_target_dry_sim != simulated_final_target:
                         status_text = Text(f"Conflict: Suffix -> '{resolved_target_dry_sim.name}'", style="yellow")
                         new_path_text = Text(str(resolved_target_dry_sim), style="yellow") # Show suffixed name
                    elif conflict_mode == 'skip':
                         status_text = Text("Conflict: Skip", style="bold yellow")
                         action_text = Text("Skip", style="bold yellow")
                         new_path_text = Text(str(action.new_path), style="dim yellow")
                    elif conflict_mode == 'overwrite':
                         status_text = Text("Conflict: Overwrite", style="bold yellow")
                         new_path_text = Text(str(action.new_path), style="yellow")
                    
                    simulated_final_target = resolved_target_dry_sim # Use resolved target for next check

                except FileOperationError as e_dry_skip: # Explicitly handle skip error
                     status_text = Text(f"Conflict: Skip ({e_dry_skip})", style="bold yellow")
                     action_text = Text("Skip", style="bold yellow")
                     new_path_text = Text(str(action.new_path), style="dim yellow")
                     simulated_final_target = None
                except FileExistsError as e_dry_fail: # Explicitly handle fail error
                     status_text = Text(f"Conflict: Fail ({e_dry_fail})", style="bold red")
                     action_text = Text("Fail", style="bold red")
                     new_path_text = Text(str(action.new_path), style="dim red")
                     simulated_final_target = None
                     dry_run_conflict_error = True
                except Exception as e_dry: # Catch other conflict resolution errors
                     status_text = Text(f"Error ({e_dry})", style="bold red")
                     simulated_final_target = None
                     dry_run_conflict_error = True

            # Check for internal conflicts (multiple sources to same final target)
            if simulated_final_target:
                 if simulated_final_target in current_targets_dry:
                      status_text = Text("Conflict: Target Collision", style="bold red")
                      action_text = Text("Fail", style="bold red")
                      new_path_text = Text(str(action.new_path), style="dim red")
                      dry_run_conflict_error=True
                 else:
                      current_targets_dry.add(simulated_final_target)

            dry_run_actions.append({
                "original": Text(str(action.original_path.name)),
                "arrow": Text("->", style="dim" if action_text.plain != "Fail" else "red"),
                "new": new_path_text,
                "action": action_text,
                "status": status_text
            })

        # --- Display the table ---
        if dry_run_actions:
            table = Table(title=f"Dry Run Plan - Batch ID (approx): {plan.batch_id[:15]}", show_header=True, header_style="bold magenta")
            table.add_column("Original Name", style="dim cyan", no_wrap=True, min_width=20)
            table.add_column(" ", justify="center", width=2) # Arrow column
            table.add_column("New Path / Name", style="cyan", no_wrap=True, min_width=30)
            table.add_column("Action", justify="center")
            table.add_column("Status / Conflict", justify="left", min_width=15)

            for item in dry_run_actions:
                table.add_row(
                    item["original"],
                    item["arrow"],
                    item["new"],
                    item["action"],
                    item["status"]
                )
            console.print(table)
            # Store message for return, but primary output is the table
            results['message'] = f"Dry Run plan displayed above ({len(dry_run_actions)} potential actions)." 
        else:
            results['message'] = "DRY RUN: No actions planned."
            console.print(results['message'])


        results['success'] = not dry_run_conflict_error
        # We don't need to return the old message format, the table is the output
        return results

    # --- Live Run ---
    # ...(rest of the live run code remains unchanged)...
    log.info(f"--- LIVE RUN for Run ID: {batch_id} ---")
    action_type = 'rename'; original_to_temp_map: Dict[Path, Path] = {}; temp_to_final_map: Dict[Path, Path] = {}; resolved_target_map: Dict[Path, Path] = {}; created_dir: Optional[Path] = None
    # Determine effective action type based on args
    if args_ns.stage_dir: action_type = 'stage'
    elif args_ns.trash: action_type = 'trash'
    elif args_ns.backup_dir: action_type = 'backup' # Backup happens first, then defaults to rename

    # --- Phase 0: Resolve Paths & Check Conflicts (for rename/backup action) ---
    if action_type in ['rename', 'backup']: # Also check conflicts if backup is involved before rename
        try:
            log.debug("Phase 0: Resolving final paths and checking conflicts...")
            current_targets: Set[Path] = set(); original_paths_in_plan: Set[Path] = {a.original_path.resolve() for a in plan.actions}
            for action in plan.actions:
                # --- Add check for original path existence before resolving ---
                if not action.original_path.exists():
                    log.warning(f"Phase 0 Skip: Original file '{action.original_path.name}' not found before conflict check. Skipping this action.")
                    # We need a way to signal this action should be skipped later.
                    # For now, we won't add it to resolved_target_map.
                    # The later loops already check for existence before acting.
                    continue # Skip to the next action in Phase 0
                # --- End Add ---

                orig_p_resolved = action.original_path.resolve(); intended_final_p = action.new_path.resolve()
                final_target_p = intended_final_p
                # Check if target exists OUTSIDE the set of files being renamed in this plan
                if (final_target_p.exists() and final_target_p not in original_paths_in_plan) \
                   or final_target_p in current_targets: # Or if it conflicts with another target in THIS plan
                    final_target_p = _handle_conflict(action.original_path, final_target_p, conflict_mode) # Raises skip/fail here

                # Check for long paths after potential conflict resolution
                if sys.platform == 'win32' and len(str(final_target_p)) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
                    log.warning(f"Potential long path issue on Windows for target: '{final_target_p}' (Length: {len(str(final_target_p))}). Ensure long path support is enabled if problems occur.")

                # Final check for internal plan conflicts after suffixing etc.
                if final_target_p in current_targets:
                     # This indicates a fundamental planning error or >100 suffix attempts failed
                     raise FileOperationError(f"Internal Error: Multiple files resolve to target '{final_target_p.name}' after conflict resolution.")
                resolved_target_map[orig_p_resolved] = final_target_p; current_targets.add(final_target_p); log.debug(f"  Resolved: '{action.original_path.name}' -> '{final_target_p.name}'")

        except (FileOperationError, FileExistsError) as e:
             log.error(f"Conflict check failed for run {batch_id}: {e}")
             results['success'] = False; action_messages.append(f"ERROR: {e}")
             results['message'] = "\n".join(action_messages)
             if isinstance(e, FileExistsError) and conflict_mode == 'fail': raise e # Re-raise if mode is 'fail'
             return results # Return failure result for skip mode or other FileOperationErrors


    # --- Proceed with Actions ---
    try:
        # --- Pre-action setup ---
        if action_type == 'backup' and args_ns.backup_dir: args_ns.backup_dir.mkdir(parents=True, exist_ok=True)
        if action_type == 'stage' and args_ns.stage_dir: args_ns.stage_dir.mkdir(parents=True, exist_ok=True)

        # Create target directory if needed (applies to rename/backup/stage)
        if plan.created_dir_path:
             target_dir = plan.created_dir_path
             if not target_dir.exists():
                 log.info(f"Creating folder: {target_dir}"); target_dir.mkdir(parents=True, exist_ok=True)
                 created_dir = target_dir # Track that we created it for potential rollback
                 # Log directory creation for undo
                 undo_manager.log_action(batch_id=batch_id, original_path=target_dir, new_path=target_dir, item_type='dir', status='created_dir')
                 action_messages.append(f"CREATED DIR: '{target_dir}'")
             else: log.debug(f"Target directory already exists: {target_dir}")

        # --- Backup Logic ---
        if action_type == 'backup':
            if not args_ns.backup_dir: raise FileOperationError("Backup directory specified in args is missing.") # Should not happen if pre-check passed
            backed_up_count = 0
            log.info(f"Starting backup phase to {args_ns.backup_dir}...")
            for action in plan.actions:
                orig_p = action.original_path
                # --- Check already exists here ---
                if not orig_p.exists():
                    log.warning(f"Cannot backup non-existent file: '{orig_p.name}'. Skipping backup for this file.")
                    continue
                backup_target = args_ns.backup_dir / orig_p.name
                if sys.platform == 'win32' and len(str(backup_target.resolve())) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
                    log.warning(f"Potential long path issue on Windows for backup target: '{backup_target.resolve()}' (Length: {len(str(backup_target.resolve()))}).")
                # Use shutil.copy2 to preserve metadata
                shutil.copy2(str(orig_p), str(backup_target)); log.debug(f"Backed up '{orig_p.name}'"); backed_up_count+=1
            if backed_up_count > 0: action_messages.append(f"Backed up {backed_up_count} files.")
            action_type = 'rename' # Set action type to proceed to the rename/move phase

        # --- Perform Main Action ---
        if action_type == 'trash':
            if not SEND2TRASH_AVAILABLE: raise FileOperationError("'send2trash' library not installed or available.")
            log.info("Starting trash phase...")
            for action in plan.actions:
                orig_p=action.original_path; final_p_intended = action.new_path
                # --- Check already exists here ---
                if not orig_p.exists():
                    log.warning(f"Cannot trash non-existent file: '{orig_p.name}'. Skipping trash for this file.");
                    continue
                undo_manager.log_action(batch_id=batch_id, original_path=orig_p, new_path=final_p_intended, item_type='file', status='trashed')
                send2trash.send2trash(str(orig_p)); action_messages.append(f"TRASHED: '{orig_p.name}' (intended: '{final_p_intended.name}')");
                results['actions_taken'] += 1
        elif action_type == 'stage':
            if not args_ns.stage_dir: raise FileOperationError("Staging directory specified in args is missing.")
            log.info(f"Starting staging phase to {args_ns.stage_dir}...")
            for action in plan.actions:
                orig_p=action.original_path; final_p_intended=action.new_path
                # --- Check already exists here ---
                if not orig_p.exists():
                    log.warning(f"Cannot stage non-existent file: '{orig_p.name}'. Skipping stage for this file.")
                    continue
                staged_path = args_ns.stage_dir / final_p_intended.name # Use intended name in stage dir
                # Handle potential conflicts *within the staging directory*
                final_staged_path = _handle_conflict(orig_p, staged_path, conflict_mode)
                if sys.platform == 'win32' and len(str(final_staged_path.resolve())) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
                    log.warning(f"Potential long path issue on Windows for staged target: '{final_staged_path.resolve()}' (Length: {len(str(final_staged_path.resolve()))}).")
                # Log before moving
                undo_manager.log_action(batch_id=batch_id, original_path=orig_p, new_path=final_staged_path, item_type='file', status='moved')
                # Perform the move
                shutil.move(str(orig_p), str(final_staged_path)); action_messages.append(f"MOVED to stage: '{orig_p.name}' -> '{final_staged_path}'");
                results['actions_taken'] += 1

        elif action_type == 'rename': # Transactional Rename/Move Logic
            phase1_ok = True; original_to_temp_map.clear(); temp_to_final_map.clear()
            log.debug(f"Starting Phase 1: Move to temporary paths for run {batch_id}")
            for action in plan.actions:
                orig_p = action.original_path; orig_p_resolved = orig_p.resolve()
                # Use resolved path from Phase 0 conflict check
                final_p = resolved_target_map.get(orig_p_resolved)
                # Check if skipped during phase 0 (e.g., original disappeared before check)
                if not final_p:
                     # Check if original path disappeared *after* Phase 0 but before move
                     if not orig_p.exists():
                         log.warning(f"P1 Skip: Original file '{orig_p.name}' missing before Phase 1 move.")
                     else:
                         log.error(f"P1 Skip: Missing resolved path for '{orig_p.name}' despite file existing (skipped in Phase 0?). Internal error?")
                     continue # Skip this action

                # --- Check original exists before move (redundant but safe) ---
                if not orig_p.exists():
                    log.warning(f"P1 Skip: Original file '{orig_p.name}' missing just before move to temp.")
                    continue # Skip if source disappeared between Phase 0 and now

                try:
                    # Generate unique temp name in the *final* target directory
                    temp_uuid = uuid.uuid4().hex[:8]
                    temp_path = final_p.parent / f"{final_p.stem}{TEMP_SUFFIX_PREFIX}{temp_uuid}{final_p.suffix}"
                    # Ensure temp name doesn't exist (highly unlikely)
                    while temp_path.exists() or temp_path.is_symlink():
                         temp_uuid = uuid.uuid4().hex[:8]
                         temp_path = final_p.parent / f"{final_p.stem}{TEMP_SUFFIX_PREFIX}{temp_uuid}{final_p.suffix}"

                    # Log intention before move
                    undo_manager.log_action(batch_id=batch_id, original_path=orig_p, new_path=final_p, item_type='file', status='pending_final')

                    log.debug(f"Phase 1: Moving '{orig_p}' -> Temp '{temp_path}'")
                    # Use shutil.move for Phase 1 as it handles cross-filesystem if needed temporarily
                    shutil.move(str(orig_p), str(temp_path))
                    original_to_temp_map[orig_p_resolved] = temp_path
                    temp_to_final_map[temp_path] = final_p
                except Exception as e:
                    msg = f"Phase 1 Error moving '{orig_p.name}' to temp: {e}"; log.error(msg, exc_info=True); action_messages.append(f"ERROR: {msg}"); phase1_ok = False; break # Stop Phase 1 on error

            # Phase 1 Rollback
            if not phase1_ok:
                 log.warning(f"Rolling back Phase 1 for run {batch_id} due to error during temp move...")
                 rollback_success_count = 0; rollback_fail_count = 0
                 for orig_p_res_rb, temp_p_rb in original_to_temp_map.items():
                     orig_p_rb = Path(orig_p_res_rb) # Recreate Path object
                     log.debug(f"Attempting rollback: '{temp_p_rb}' -> '{orig_p_rb}'")
                     try:
                         # --- Check temp exists before rollback move ---
                         if temp_p_rb.exists():
                             # Use shutil.move for rollback consistency
                             shutil.move(str(temp_p_rb), str(orig_p_rb))
                             if undo_manager.update_action_status(batch_id=batch_id, original_path=str(orig_p_rb), new_status='failed_pending'):
                                 log.info(f"Rollback successful: '{temp_p_rb.name}' -> '{orig_p_rb.name}'")
                             else: log.error(f"Rollback successful for '{orig_p_rb.name}', but failed to update undo status.")
                             rollback_success_count += 1
                         else:
                             log.warning(f"Rollback skipped for '{orig_p_rb.name}', temp file '{temp_p_rb}' not found (already rolled back or deleted?).")
                             # Optionally try to update status anyway? Risky. Let's mark as failed.
                             undo_manager.update_action_status(batch_id=batch_id, original_path=str(orig_p_rb), new_status='failed_pending')
                     except Exception as e_rb:
                         log.error(f"Rollback Error moving '{temp_p_rb.name}' to '{orig_p_rb.name}': {e_rb}")
                         rollback_fail_count += 1
                         action_messages.append(f"CRITICAL: Rollback failed for '{temp_p_rb.name}'. File may be stuck in temp state.")
                 # Rollback directory creation if needed
                 if created_dir and created_dir.is_dir() and not any(created_dir.iterdir()):
                     try:
                         log.debug(f"Attempting rollback: Removing created directory '{created_dir}'")
                         created_dir.rmdir()
                         if undo_manager.update_action_status(batch_id=batch_id, original_path=str(created_dir), new_status='reverted'):
                             log.info(f"Rollback successful: Removed directory '{created_dir}'")
                         else: log.error(f"Rollback removed directory '{created_dir}', but failed to update undo status.")
                     except OSError as e_rdir:
                         log.error(f"Rollback could not remove created dir '{created_dir}': {e_rdir}")
                         action_messages.append(f"CRITICAL: Rollback failed to remove directory '{created_dir}'.")

                 action_messages.append(f"Phase 1 Rollback Summary: {rollback_success_count} succeeded, {rollback_fail_count} failed.")
                 results['success'] = False; results['message'] = "\n".join(action_messages); return results

            # Phase 2: Rename Temporary to Final
            log.debug(f"Starting Phase 2: Rename temporary paths to final for run {batch_id}")
            phase2_errors = False; action_count_phase2 = 0
            for temp_path, final_path in temp_to_final_map.items():
                 original_path_for_log = None; # Find original path for logging status
                 for orig_res, temp in original_to_temp_map.items():
                     if temp.resolve() == temp_path.resolve(): original_path_for_log = Path(orig_res); break

                 try:
                     log.debug(f"Phase 2: Attempting move Temp '{temp_path}' -> Final '{final_path}'")
                     # --- Check temp exists before final rename ---
                     if not temp_path.exists():
                         log.error(f"P2 Error: Temp file {temp_path} not found! Cannot complete rename for original '{original_path_for_log.name if original_path_for_log else 'UNKNOWN'}'.")
                         phase2_errors = True;
                         # Attempt to mark the corresponding action as failed in undo log
                         if original_path_for_log:
                              undo_manager.update_action_status(batch_id=batch_id, original_path=str(original_path_for_log), new_status='failed_pending')
                         continue # Critical error if temp file vanished

                     if final_path.exists() or final_path.is_symlink():
                          # This check should ideally be unnecessary due to Phase 0, but acts as a safeguard
                          if conflict_mode == 'overwrite':
                              log.warning(f"P2 Overwriting existing target: {final_path}");
                              final_path.unlink(missing_ok=True) # Ensure it's gone before rename/move
                          else:
                              # Should not happen if Phase 0 worked, unless file appeared between phases
                              log.error(f"P2 Error: Target '{final_path}' exists unexpectedly! Conflict mode '{conflict_mode}' prevents overwrite."); phase2_errors = True; continue

                     # --- ATOMIC RENAME IMPLEMENTATION ---
                     rename_successful = False
                     try:
                         # Try atomic os.rename first
                         os.rename(str(temp_path), str(final_path))
                         log.debug(f"  -> P2 Success (os.rename): '{temp_path.name}' -> '{final_path.name}'")
                         rename_successful = True
                     except OSError as e_os_rename:
                         # OSError might indicate cross-device link or other issues
                         log.warning(f"  -> P2 os.rename failed ({e_os_rename}), attempting shutil.move fallback...")
                         try:
                             # Fallback to shutil.move (handles cross-device)
                             shutil.move(str(temp_path), str(final_path))
                             log.debug(f"  -> P2 Success (shutil.move): '{temp_path.name}' -> '{final_path.name}'")
                             rename_successful = True
                         except Exception as e_shutil:
                             # If shutil.move also fails, it's a critical error
                             msg = f"P2 Error: Both os.rename and shutil.move failed for '{temp_path.name}' -> '{final_path.name}': {e_shutil}"; log.critical(msg, exc_info=True); action_messages.append(f"ERROR: {msg}"); phase2_errors = True
                     except Exception as e_generic_rename:
                          # Catch other potential errors during os.rename
                          msg = f"P2 Error: Unexpected issue during os.rename for '{temp_path.name}' -> '{final_path.name}': {e_generic_rename}"; log.critical(msg, exc_info=True); action_messages.append(f"ERROR: {msg}"); phase2_errors = True
                     # --- END ATOMIC RENAME IMPLEMENTATION ---

                     # Update status only if rename/move succeeded
                     if rename_successful:
                         action_count_phase2 += 1
                         if original_path_for_log:
                              final_status = 'moved' if final_path.parent.resolve() != original_path_for_log.parent.resolve() else 'renamed'
                              if undo_manager.update_action_status(batch_id=batch_id, original_path=str(original_path_for_log), new_status=final_status):
                                  action_messages.append(f"{final_status.upper()}D: '{original_path_for_log.name}' -> '{final_path}'")
                                  results['actions_taken'] += 1
                              else:
                                  log.error(f"Could not update undo log status for '{original_path_for_log}'")
                                  action_messages.append(f"ACTION UNLOGGED? '{original_path_for_log.name}' -> '{final_path}'")
                         else: # Should not happen
                             log.error(f"Internal error finding original path for '{temp_path}' during status update.")
                             action_messages.append(f"RENAMED (orig?): temp -> '{final_path.name}'")

                 except Exception as e_outer_p2:
                     # Catch unexpected errors in the outer loop for this action
                     msg = f"P2 Outer Error processing temp '{temp_path.name}' to '{final_path.name}': {e_outer_p2}"; log.critical(msg, exc_info=True); action_messages.append(f"ERROR: {msg}"); phase2_errors = True

            # Check Phase 2 summary
            if phase2_errors: action_messages.append("CRITICAL: Errors occurred during final rename phase."); results['success'] = False
            elif not phase1_ok: results['success'] = False # If Phase 1 failed but somehow didn't return early
            else: results['success'] = True

            # Add backup message if relevant
            if args_ns.backup_dir and results['actions_taken'] > 0 and not phase2_errors:
                 # Find the original backup count message if it exists
                 backup_msg_index = -1
                 for i, msg in enumerate(action_messages):
                     if msg.startswith("Backed up"):
                          backup_msg_index = i; break
                 # Insert summary after backup message or at start
                 summary_msg = f"Renamed/Moved {results['actions_taken']} files after backup."
                 if backup_msg_index != -1: action_messages.insert(backup_msg_index + 1, summary_msg)
                 else: action_messages.insert(0, summary_msg)


    # --- Catch unexpected Exceptions during the whole live run ---
    except Exception as e:
        log.exception(f"Unhandled error during file actions for run {batch_id}: {e}")
        results['success'] = False; action_messages.append(f"CRITICAL UNHANDLED ERROR: {e}")
        # --- Rollback Phase 1 if needed on unhandled exception ---
        if action_type == 'rename' and original_to_temp_map: # Check if we were in rename phase
            log.critical(f"Rolling back Phase 1 for run {batch_id} due to unhandled exception: {e}")
            rollback_success_count = 0; rollback_fail_count = 0
            for orig_p_res_rb, temp_p_rb in original_to_temp_map.items():
                 orig_p_rb = Path(orig_p_res_rb)
                 log.debug(f"Attempting rollback (exception): '{temp_p_rb}' -> '{orig_p_rb}'")
                 try:
                     # --- Check temp exists before rollback move ---
                     if temp_p_rb.exists():
                         shutil.move(str(temp_p_rb), str(orig_p_rb))
                         if undo_manager.update_action_status(batch_id=batch_id, original_path=str(orig_p_rb), new_status='failed_pending'):
                              log.info(f"Rollback successful (exception): '{temp_p_rb.name}' -> '{orig_p_rb.name}'")
                         else: log.error(f"Rollback successful (exception) for '{orig_p_rb.name}', but failed to update undo status.")
                         rollback_success_count += 1
                     else:
                         log.warning(f"Rollback skipped (exception) for '{orig_p_rb.name}', temp file '{temp_p_rb}' not found (already rolled back or deleted?).")
                         # Mark status as failed here too
                         undo_manager.update_action_status(batch_id=batch_id, original_path=str(orig_p_rb), new_status='failed_pending')
                 except Exception as e_rb:
                     log.error(f"Rollback Error (exception) moving '{temp_p_rb.name}' to '{orig_p_rb.name}': {e_rb}")
                     rollback_fail_count += 1
                     action_messages.append(f"CRITICAL: Rollback failed (exception) for '{temp_p_rb.name}'. File may be stuck in temp state.")
            # Rollback directory creation
            if created_dir and created_dir.is_dir() and not any(created_dir.iterdir()):
                 try:
                     log.debug(f"Attempting rollback (exception): Removing created directory '{created_dir}'")
                     created_dir.rmdir()
                     if undo_manager.update_action_status(batch_id=batch_id, original_path=str(created_dir), new_status='reverted'):
                          log.info(f"Rollback successful (exception): Removed directory '{created_dir}'")
                     else: log.error(f"Rollback removed directory (exception) '{created_dir}', but failed to update undo status.")
                 except OSError as e_rdir:
                     log.error(f"Rollback could not remove created dir (exception) '{created_dir}': {e_rdir}")
                     action_messages.append(f"CRITICAL: Rollback failed (exception) to remove directory '{created_dir}'.")
            action_messages.append(f"Unhandled Exception Rollback Summary: {rollback_success_count} succeeded, {rollback_fail_count} failed.")

    # --- Finalize results ---
    # Combine action messages into a single string for the message field
    results['message'] = "\n".join(action_messages) if action_messages else "Live run completed."
    return results