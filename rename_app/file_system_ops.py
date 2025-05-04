# --- START OF FILE file_system_ops.py ---

# rename_app/file_system_ops.py

import logging
import shutil
import uuid
from pathlib import Path
import argparse
import sys
import os # Keep os import for os.rename
from typing import Dict, Callable, Set, Optional, Any

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

    # --- Dry Run ---
    if not getattr(args_ns, 'live', False): # Check the final 'live' status
        # (Dry Run logic unchanged)
        log.info(f"--- DRY RUN for Run ID: {batch_id} ---"); dry_run_messages = []
        original_paths_in_plan_dry = {a.original_path.resolve() for a in plan.actions}; current_targets_dry: Set[Path] = set(); dry_run_conflict_error = False
        if plan.created_dir_path and not plan.created_dir_path.exists(): dry_run_messages.append(f"DRY RUN: Would create directory '{plan.created_dir_path}'")
        for action in plan.actions:
            simulated_final_target = action.new_path.resolve(); target_exists_externally = (simulated_final_target.exists() and simulated_final_target not in original_paths_in_plan_dry) or simulated_final_target in current_targets_dry; conflict_msg = ""
            if sys.platform == 'win32' and len(str(simulated_final_target)) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
                 conflict_msg += f" (WARNING: Potential long path >{WINDOWS_PATH_LENGTH_WARNING_THRESHOLD} chars)"
            if target_exists_externally:
                try:
                    temp_conflict_mode = conflict_mode if conflict_mode != 'fail' else 'skip'; resolved_target_dry_sim = _handle_conflict(action.original_path, simulated_final_target, temp_conflict_mode)
                    if resolved_target_dry_sim != simulated_final_target: conflict_msg += f" (WARNING: Target exists - mode: {conflict_mode} -> likely '{resolved_target_dry_sim.name}')"
                    else: conflict_msg += f" (WARNING: Target exists - mode: {conflict_mode})";
                    if conflict_mode == 'skip': conflict_msg += " - Action would be skipped.)"
                    elif conflict_mode == 'overwrite': conflict_msg += " - Action would overwrite.)"
                    else: conflict_msg += ")" # Close paren for suffix
                    simulated_final_target = resolved_target_dry_sim
                except FileOperationError as e_dry_skip:
                     conflict_msg += f" (WARNING: Target exists - mode: skip - Action would be skipped.)"; simulated_final_target = None
                except FileExistsError as e_dry_fail:
                     conflict_msg += f" (ERROR: Target exists - mode: fail - Action would fail.)"; simulated_final_target = None; dry_run_conflict_error = True
                except Exception as e_dry: conflict_msg += f" (ERROR during dry run check: {e_dry})"; simulated_final_target = None; dry_run_conflict_error = True
            if simulated_final_target:
                 if simulated_final_target in current_targets_dry: conflict_msg += " (ERROR: Multiple files map to same target!)"; simulated_final_target = None; dry_run_conflict_error=True
                 else: current_targets_dry.add(simulated_final_target)
            action_verb = action.action_type if action.action_type == 'move' else 'rename'; action_desc = f"{action_verb} '{action.original_path.name}' -> '{action.new_path}'{conflict_msg}"; dry_run_messages.append(f"DRY RUN: Would {action_desc}")
        results['success'] = not dry_run_conflict_error; results['message'] = "\n".join(dry_run_messages) if dry_run_messages else "DRY RUN: No actions planned."; return results

    # --- Live Run ---
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
                if not orig_p.exists(): log.warning(f"Cannot backup non-existent file: {orig_p}"); continue
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
                if not orig_p.exists(): log.warning(f"Cannot trash non-existent: {orig_p}"); continue
                undo_manager.log_action(batch_id=batch_id, original_path=orig_p, new_path=final_p_intended, item_type='file', status='trashed')
                send2trash.send2trash(str(orig_p)); action_messages.append(f"TRASHED: '{orig_p.name}' (intended: '{final_p_intended.name}')");
                results['actions_taken'] += 1
        elif action_type == 'stage':
            if not args_ns.stage_dir: raise FileOperationError("Staging directory specified in args is missing.")
            log.info(f"Starting staging phase to {args_ns.stage_dir}...")
            for action in plan.actions:
                orig_p=action.original_path; final_p_intended=action.new_path
                if not orig_p.exists(): log.warning(f"Cannot stage non-existent: {orig_p}"); continue
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
                if not final_p: log.error(f"P1 Skip: Missing resolved path for {orig_p}"); phase1_ok = False; break # Should not happen if Phase 0 passed
                if not orig_p.exists(): log.warning(f"Cannot process non-existent P1: {orig_p}"); continue # Skip if source disappeared

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
                 # (Rollback logic unchanged - moves temp back to original, updates undo log)
                 log.warning(f"Rolling back Phase 1 for run {batch_id} due to error during temp move...")
                 rollback_success_count = 0; rollback_fail_count = 0
                 for orig_p_res_rb, temp_p_rb in original_to_temp_map.items():
                     orig_p_rb = Path(orig_p_res_rb) # Recreate Path object
                     log.debug(f"Attempting rollback: '{temp_p_rb}' -> '{orig_p_rb}'")
                     try:
                         if temp_p_rb.exists():
                             # Use shutil.move for rollback consistency
                             shutil.move(str(temp_p_rb), str(orig_p_rb))
                             if undo_manager.update_action_status(batch_id=batch_id, original_path=str(orig_p_rb), new_status='failed_pending'):
                                 log.info(f"Rollback successful: '{temp_p_rb.name}' -> '{orig_p_rb.name}'")
                             else: log.error(f"Rollback successful for '{orig_p_rb.name}', but failed to update undo status.")
                             rollback_success_count += 1
                         else: log.warning(f"Rollback skipped for '{orig_p_rb.name}', temp file '{temp_p_rb}' not found.")
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
                     if not temp_path.exists():
                         log.error(f"P2 Error: Temp file {temp_path} not found! Cannot complete rename."); phase2_errors = True; continue # Critical error if temp file vanished

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
                     if temp_p_rb.exists():
                         shutil.move(str(temp_p_rb), str(orig_p_rb))
                         if undo_manager.update_action_status(batch_id=batch_id, original_path=str(orig_p_rb), new_status='failed_pending'):
                              log.info(f"Rollback successful (exception): '{temp_p_rb.name}' -> '{orig_p_rb.name}'")
                         else: log.error(f"Rollback successful (exception) for '{orig_p_rb.name}', but failed to update undo status.")
                         rollback_success_count += 1
                     else: log.warning(f"Rollback skipped (exception) for '{orig_p_rb.name}', temp file '{temp_p_rb}' not found.")
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
    results['message'] = "\n".join(action_messages)
    return results
# --- End file_system_ops.py ---