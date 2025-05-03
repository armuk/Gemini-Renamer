# --- START OF FILE file_system_ops.py ---

# rename_app/file_system_ops.py

import logging
import shutil
import uuid
from pathlib import Path
import argparse
import sys # Import sys for platform check
import os  # Keep os import (might be needed by Path methods implicitly)
from typing import Dict, Callable, Set, Optional, Any

from .models import RenamePlan, RenameAction
from .exceptions import FileOperationError, RenamerError
from .undo_manager import UndoManager

try: import send2trash; SEND2TRASH_AVAILABLE = True
except ImportError: SEND2TRASH_AVAILABLE = False

log = logging.getLogger(__name__)
TEMP_SUFFIX_PREFIX = ".renametmp_"
# Define threshold for Windows path length warning
WINDOWS_PATH_LENGTH_WARNING_THRESHOLD = 240


# (_handle_conflict unchanged)
def _handle_conflict(original_path: Path, target_path: Path, conflict_mode: str) -> Path:
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
    # Use the consistent run_batch_id for all logging within this function call
    batch_id = run_batch_id
    conflict_mode = cfg_helper('on_conflict', 'skip')

    # --- Dry Run ---
    if args_ns.dry_run:
        # Use run_batch_id in log message
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
    # Use run_batch_id in log message
    log.info(f"--- LIVE RUN for Run ID: {batch_id} ---")
    action_type = 'rename'; original_to_temp_map: Dict[Path, Path] = {}; temp_to_final_map: Dict[Path, Path] = {}; resolved_target_map: Dict[Path, Path] = {}; created_dir: Optional[Path] = None
    if args_ns.stage_dir: action_type = 'stage'
    elif args_ns.trash: action_type = 'trash'
    elif args_ns.backup_dir: action_type = 'backup'

    # --- Phase 0: Resolve Paths & Check Conflicts (for rename action) ---
    if action_type == 'rename':
        try:
            log.debug("Phase 0: Resolving final paths and checking conflicts...")
            current_targets: Set[Path] = set(); original_paths_in_plan: Set[Path] = {a.original_path.resolve() for a in plan.actions}
            for action in plan.actions:
                orig_p_resolved = action.original_path.resolve(); intended_final_p = action.new_path.resolve()
                final_target_p = intended_final_p
                if (final_target_p.exists() and final_target_p not in original_paths_in_plan) or final_target_p in current_targets:
                    final_target_p = _handle_conflict(action.original_path, final_target_p, conflict_mode) # Raises skip/fail here

                if sys.platform == 'win32' and len(str(final_target_p)) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
                    log.warning(f"Potential long path issue on Windows for target: '{final_target_p}' (Length: {len(str(final_target_p))}). Ensure long path support is enabled if problems occur.")

                if final_target_p in current_targets: raise FileOperationError(f"Internal Error: Multiple files resolve to target '{final_target_p.name}'")
                resolved_target_map[orig_p_resolved] = final_target_p; current_targets.add(final_target_p); log.debug(f"  Resolved: '{action.original_path.name}' -> '{final_target_p.name}'")

        except (FileOperationError, FileExistsError) as e:
             log.error(f"Conflict check failed for run {batch_id}: {e}") # Use run_batch_id
             results['success'] = False; action_messages.append(f"ERROR: {e}")
             results['message'] = "\n".join(action_messages)
             if isinstance(e, FileExistsError) and conflict_mode == 'fail': raise e
             return results

    # --- Proceed with Actions ---
    try:
        # --- Pre-action setup ---
        if action_type == 'backup' and args_ns.backup_dir: args_ns.backup_dir.mkdir(parents=True, exist_ok=True)
        if action_type == 'stage' and args_ns.stage_dir: args_ns.stage_dir.mkdir(parents=True, exist_ok=True)
        if plan.created_dir_path:
             target_dir = plan.created_dir_path
             if not target_dir.exists():
                 log.info(f"Creating folder: {target_dir}"); target_dir.mkdir(parents=True, exist_ok=True)
                 created_dir = target_dir
                 # Use run_batch_id for logging
                 undo_manager.log_action(batch_id=batch_id, original_path=target_dir, new_path=target_dir, item_type='dir', status='created_dir')
                 action_messages.append(f"CREATED DIR: '{target_dir}'")
             else: log.debug(f"Target directory already exists: {target_dir}")

        # --- Backup Logic ---
        if action_type == 'backup':
            if not args_ns.backup_dir: raise FileOperationError("Backup directory missing.")
            backed_up_count = 0
            for action in plan.actions:
                orig_p = action.original_path
                if not orig_p.exists(): log.warning(f"Cannot backup non-existent: {orig_p}"); continue
                backup_target = args_ns.backup_dir / orig_p.name
                if sys.platform == 'win32' and len(str(backup_target.resolve())) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
                    log.warning(f"Potential long path issue on Windows for backup target: '{backup_target.resolve()}' (Length: {len(str(backup_target.resolve()))}).")
                shutil.copy2(str(orig_p), str(backup_target)); log.debug(f"Backed up '{orig_p.name}'"); backed_up_count+=1
            if backed_up_count > 0: action_messages.append(f"Backed up {backed_up_count} files.")
            action_type = 'rename' # Proceed to rename phase

        # --- Perform Main Action ---
        if action_type == 'trash':
            if not SEND2TRASH_AVAILABLE: raise FileOperationError("Send2Trash missing.")
            for action in plan.actions:
                orig_p=action.original_path; final_p_intended = action.new_path
                if not orig_p.exists(): log.warning(f"Cannot trash non-existent: {orig_p}"); continue
                # Use run_batch_id for logging
                undo_manager.log_action(batch_id=batch_id, original_path=orig_p, new_path=final_p_intended, item_type='file', status='trashed')
                send2trash.send2trash(str(orig_p)); action_messages.append(f"TRASHED: '{orig_p.name}' (intended: '{final_p_intended.name}')");
                results['actions_taken'] += 1
        elif action_type == 'stage':
            if not args_ns.stage_dir: raise FileOperationError("Staging directory missing.")
            for action in plan.actions:
                orig_p=action.original_path; final_p_intended=action.new_path
                if not orig_p.exists(): log.warning(f"Cannot stage non-existent: {orig_p}"); continue
                staged_path = args_ns.stage_dir / final_p_intended.name
                final_staged_path = _handle_conflict(orig_p, staged_path, conflict_mode)
                if sys.platform == 'win32' and len(str(final_staged_path.resolve())) > WINDOWS_PATH_LENGTH_WARNING_THRESHOLD:
                    log.warning(f"Potential long path issue on Windows for staged target: '{final_staged_path.resolve()}' (Length: {len(str(final_staged_path.resolve()))}).")
                # Use run_batch_id for logging
                undo_manager.log_action(batch_id=batch_id, original_path=orig_p, new_path=final_staged_path, item_type='file', status='moved')
                shutil.move(str(orig_p), str(final_staged_path)); action_messages.append(f"MOVED to stage: '{orig_p.name}' -> '{final_staged_path}'");
                results['actions_taken'] += 1

        elif action_type == 'rename': # Transactional Rename Logic
            phase1_ok = True; original_to_temp_map.clear(); temp_to_final_map.clear()
            # Use run_batch_id in log message
            log.debug(f"Starting Phase 1: Move to temporary paths for run {batch_id}")
            for action in plan.actions:
                orig_p = action.original_path; orig_p_resolved = orig_p.resolve()
                final_p = resolved_target_map.get(orig_p_resolved) # Use resolved path from Phase 0
                if not final_p: log.error(f"P1 Skip: Missing resolved path {orig_p}"); phase1_ok = False; break
                if not orig_p.exists(): log.warning(f"Cannot process non-existent P1: {orig_p}"); continue
                try:
                    temp_uuid = uuid.uuid4().hex[:8]; temp_path = final_p.parent / f"{final_p.stem}{TEMP_SUFFIX_PREFIX}{temp_uuid}{final_p.suffix}"
                    while temp_path.exists() or temp_path.is_symlink(): temp_uuid = uuid.uuid4().hex[:8]; temp_path = final_p.parent / f"{final_p.stem}{TEMP_SUFFIX_PREFIX}{temp_uuid}{final_p.suffix}"
                    # Use run_batch_id for logging
                    undo_manager.log_action(batch_id=batch_id, original_path=orig_p, new_path=final_p, item_type='file', status='pending_final')
                    log.debug(f"Phase 1: Moving '{orig_p}' -> Temp '{temp_path}'")
                    shutil.move(str(orig_p), str(temp_path))
                    original_to_temp_map[orig_p_resolved] = temp_path; temp_to_final_map[temp_path] = final_p
                except Exception as e: msg = f"Phase 1 Error moving '{orig_p.name}' to temp: {e}"; log.error(msg, exc_info=True); action_messages.append(f"ERROR: {msg}"); phase1_ok = False; break

            # Phase 1 Rollback
            if not phase1_ok:
                 # Use run_batch_id in log message
                 log.warning(f"Rolling back Phase 1 for run {batch_id} due to error during temp move...")
                 rollback_success_count = 0; rollback_fail_count = 0
                 for orig_p_res_rb, temp_p_rb in original_to_temp_map.items():
                     orig_p_rb = Path(orig_p_res_rb)
                     log.debug(f"Attempting rollback: '{temp_p_rb}' -> '{orig_p_rb}'")
                     try:
                         if temp_p_rb.exists():
                             shutil.move(str(temp_p_rb), str(orig_p_rb))
                             # Use run_batch_id for status update
                             if undo_manager.update_action_status(batch_id=batch_id, original_path=str(orig_p_rb), new_status='failed_pending'):
                                 log.info(f"Rollback successful: '{temp_p_rb.name}' -> '{orig_p_rb.name}'")
                             else: log.error(f"Rollback successful for '{orig_p_rb.name}', but failed to update undo status.")
                             rollback_success_count += 1
                         else: log.warning(f"Rollback skipped for '{orig_p_rb.name}', temp file '{temp_p_rb}' not found.")
                     except Exception as e_rb:
                         log.error(f"Rollback Error moving '{temp_p_rb.name}' to '{orig_p_rb.name}': {e_rb}")
                         rollback_fail_count += 1
                         action_messages.append(f"CRITICAL: Rollback failed for '{temp_p_rb.name}'. File may be stuck in temp state.")
                 if created_dir and created_dir.is_dir() and not any(created_dir.iterdir()):
                     try:
                         log.debug(f"Attempting rollback: Removing created directory '{created_dir}'")
                         created_dir.rmdir()
                         # Use run_batch_id for status update
                         if undo_manager.update_action_status(batch_id=batch_id, original_path=str(created_dir), new_status='reverted'):
                             log.info(f"Rollback successful: Removed directory '{created_dir}'")
                         else: log.error(f"Rollback removed directory '{created_dir}', but failed to update undo status.")
                     except OSError as e_rdir:
                         log.error(f"Rollback could not remove created dir '{created_dir}': {e_rdir}")
                         action_messages.append(f"CRITICAL: Rollback failed to remove directory '{created_dir}'.")

                 action_messages.append(f"Phase 1 Rollback Summary: {rollback_success_count} succeeded, {rollback_fail_count} failed.")
                 results['success'] = False; results['message'] = "\n".join(action_messages); return results

            # Phase 2: Rename Temporary to Final
            # Use run_batch_id in log message
            log.debug(f"Starting Phase 2: Rename temporary paths to final for run {batch_id}")
            phase2_errors = False; action_count_phase2 = 0
            for temp_path, final_path in temp_to_final_map.items():
                 original_path_for_log = None;
                 for orig_res, temp in original_to_temp_map.items():
                     if temp.resolve() == temp_path.resolve(): original_path_for_log = Path(orig_res); break
                 try:
                     log.debug(f"Phase 2: Renaming Temp '{temp_path}' -> Final '{final_path}'")
                     if not temp_path.exists(): log.error(f"P2 Error: Temp file {temp_path} not found!"); phase2_errors = True; continue
                     if final_path.exists() or final_path.is_symlink():
                          if conflict_mode == 'overwrite': log.warning(f"Overwriting existing: {final_path}"); final_path.unlink(missing_ok=True)
                          else: log.error(f"P2 Error: Target '{final_path}' exists unexpectedly!"); phase2_errors = True; continue
                     temp_path.rename(final_path); action_count_phase2 += 1
                     if original_path_for_log:
                          final_status = 'moved' if final_path.parent.resolve() != original_path_for_log.parent.resolve() else 'renamed'
                          # Use run_batch_id for status update
                          if undo_manager.update_action_status(batch_id=batch_id, original_path=str(original_path_for_log), new_status=final_status): action_messages.append(f"{final_status.upper()}D: '{original_path_for_log.name}' -> '{final_path}'"); results['actions_taken'] += 1
                          else: log.error(f"Could not update undo log status for '{original_path_for_log}'"); action_messages.append(f"ACTION UNLOGGED? '{original_path_for_log.name}' -> '{final_path}'")
                     else: log.error(f"Internal error finding original path for '{temp_path}'"); action_messages.append(f"RENAMED (orig?): temp -> '{final_path.name}'")
                 except Exception as e: msg = f"P2 Error renaming temp '{temp_path.name}' to '{final_path.name}': {e}"; log.critical(msg, exc_info=True); action_messages.append(f"ERROR: {msg}"); phase2_errors = True
            if phase2_errors: action_messages.append("CRITICAL: Errors during final rename."); results['success'] = False
            elif not phase1_ok: results['success'] = False
            else: results['success'] = True
            if args_ns.backup_dir and results['actions_taken'] > 0: action_messages.insert(0, f"Backed up {results['actions_taken']} original files.")

    # --- Catch unexpected Exceptions ---
    except Exception as e:
        # Use run_batch_id in log message
        log.exception(f"Unhandled error during file actions for run {batch_id}: {e}")
        results['success'] = False; action_messages.append(f"CRITICAL UNHANDLED ERROR: {e}")
        # Rollback Phase 1 if needed
        if action_type == 'rename' and original_to_temp_map:
            # Use run_batch_id in log message
            log.critical(f"Rolling back Phase 1 for run {batch_id} due to unhandled exception: {e}")
            rollback_success_count = 0; rollback_fail_count = 0
            for orig_p_res_rb, temp_p_rb in original_to_temp_map.items():
                 orig_p_rb = Path(orig_p_res_rb)
                 log.debug(f"Attempting rollback: '{temp_p_rb}' -> '{orig_p_rb}'")
                 try:
                     if temp_p_rb.exists():
                         shutil.move(str(temp_p_rb), str(orig_p_rb))
                         # Use run_batch_id for status update
                         if undo_manager.update_action_status(batch_id=batch_id, original_path=str(orig_p_rb), new_status='failed_pending'):
                              log.info(f"Rollback successful: '{temp_p_rb.name}' -> '{orig_p_rb.name}'")
                         else: log.error(f"Rollback successful for '{orig_p_rb.name}', but failed to update undo status.")
                         rollback_success_count += 1
                     else: log.warning(f"Rollback skipped for '{orig_p_rb.name}', temp file '{temp_p_rb}' not found.")
                 except Exception as e_rb:
                     log.error(f"Rollback Error moving '{temp_p_rb.name}' to '{orig_p_rb.name}': {e_rb}")
                     rollback_fail_count += 1
                     action_messages.append(f"CRITICAL: Rollback failed for '{temp_p_rb.name}'. File may be stuck in temp state.")
            if created_dir and created_dir.is_dir() and not any(created_dir.iterdir()):
                 try:
                     log.debug(f"Attempting rollback: Removing created directory '{created_dir}'")
                     created_dir.rmdir()
                     # Use run_batch_id for status update
                     if undo_manager.update_action_status(batch_id=batch_id, original_path=str(created_dir), new_status='reverted'):
                          log.info(f"Rollback successful: Removed directory '{created_dir}'")
                     else: log.error(f"Rollback removed directory '{created_dir}', but failed to update undo status.")
                 except OSError as e_rdir:
                     log.error(f"Rollback could not remove created dir '{created_dir}': {e_rdir}")
                     action_messages.append(f"CRITICAL: Rollback failed to remove directory '{created_dir}'.")
            action_messages.append(f"Unhandled Exception Rollback Summary: {rollback_success_count} succeeded, {rollback_fail_count} failed.")

    # --- Finalize results ---
    results['message'] = "\n".join(action_messages)
    return results
# --- End file_system_ops.py ---