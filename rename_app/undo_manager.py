# --- START OF FILE undo_manager.py ---

import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
import time # Needed for integrity check comparison delta
import os # Needed for integrity check comparison delta
import fnmatch # Needed for finding temp files
import hashlib # Needed for hash check
from typing import Optional, Tuple # Added Tuple

from .exceptions import RenamerError

log = logging.getLogger(__name__)
TEMP_SUFFIX_PREFIX = ".renametmp_"
# Tolerance for mtime comparison in seconds (adjust as needed)
MTIME_TOLERANCE = 1.0

class UndoManager:
    def __init__(self, cfg_helper):
        self.cfg = cfg_helper
        self.db_path = None
        self.is_enabled = False
        self.check_integrity = False
        self.hash_check_bytes = 0 # Default to disabled

        try:
            self.is_enabled = self.cfg('enable_undo', False)
            if self.is_enabled:
                self.db_path = self._resolve_db_path()
                self._init_db()
                self.check_integrity = self.cfg('undo_check_integrity', False)
                # Get hash check config value
                try:
                    hash_bytes_cfg = self.cfg('undo_integrity_hash_bytes', 0)
                    self.hash_check_bytes = int(hash_bytes_cfg) if hash_bytes_cfg else 0
                    if self.hash_check_bytes < 0:
                        log.warning("undo_integrity_hash_bytes cannot be negative. Disabling hash check.")
                        self.hash_check_bytes = 0
                except (ValueError, TypeError):
                    log.warning(f"Invalid 'undo_integrity_hash_bytes' value ('{hash_bytes_cfg}'). Disabling hash check.")
                    self.hash_check_bytes = 0

                log_msg = f"UndoManager initialized (DB: {self.db_path}, Integrity: {self.check_integrity}"
                if self.check_integrity and self.hash_check_bytes > 0:
                    log_msg += f", Hash Check: {self.hash_check_bytes} bytes)"
                else:
                    log_msg += ")"
                log.info(log_msg)
            else:
                log.info("Undo feature disabled by configuration.")
        except Exception as e:
            self.is_enabled = False
            log.exception(f"Failed to initialize UndoManager: {e}")

    def _resolve_db_path(self):
        # (Function unchanged)
        db_path_config = self.cfg('undo_db_path', None)
        try:
            if db_path_config:
                path = Path(db_path_config).resolve()
            else:
                path = Path(__file__).parent.parent / "rename_log.db"
                path = path.resolve()
            path.parent.mkdir(parents=True, exist_ok=True)
            return path
        except Exception as e:
            raise RenamerError(f"Cannot resolve undo database path: {e}") from e

    def _connect(self):
        # (Function unchanged)
        if not self.db_path:
            raise RenamerError("Cannot connect to undo database: path not resolved.")
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            try: conn.execute("PRAGMA journal_mode=WAL;")
            except sqlite3.Error as pe: log.warning(f"Could not set PRAGMA journal_mode=WAL for undo DB ({self.db_path}): {pe}")
            try: conn.execute("PRAGMA busy_timeout=5000;")
            except sqlite3.Error as pe: log.warning(f"Could not set PRAGMA busy_timeout=5000 for undo DB ({self.db_path}): {pe}")
            return conn
        except sqlite3.Error as e:
            raise RenamerError(f"Cannot connect to undo database '{self.db_path}': {e}") from e

    def _init_db(self):
        # (Function unchanged)
        try:
            with self._connect() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS rename_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        batch_id TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        original_path TEXT NOT NULL UNIQUE,
                        new_path TEXT NOT NULL,
                        type TEXT CHECK(type IN ('file', 'dir')) NOT NULL,
                        status TEXT CHECK(status IN ('renamed', 'moved', 'trashed', 'reverted', 'created_dir', 'pending_final', 'failed_pending')) NOT NULL,
                        original_size INTEGER,
                        original_mtime REAL,
                        original_hash TEXT NULL
                    )
                """)
                try:
                    conn.execute("ALTER TABLE rename_log ADD COLUMN original_hash TEXT NULL;")
                    log.info("Added 'original_hash' column to undo log table.")
                except sqlite3.OperationalError as e:
                    if "duplicate column name" in str(e).lower(): pass
                    else: raise
                conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_id ON rename_log(batch_id)")
                conn.commit()
        except sqlite3.Error as e: raise RenamerError(f"Failed to initialize undo database schema: {e}") from e
        except RenamerError as e: raise RenamerError(f"Failed to connect during undo database initialization: {e}") from e

    def _calculate_partial_hash(self, file_path: Path, num_bytes: int) -> Optional[str]:
        # (Function unchanged)
        if num_bytes <= 0: return None
        try:
            hasher = hashlib.sha256()
            with open(file_path, 'rb') as f: chunk = f.read(num_bytes); hasher.update(chunk if chunk else b'')
            return hasher.hexdigest()
        except FileNotFoundError: log.warning(f"Cannot calculate hash: File not found '{file_path}'"); return None
        except OSError as e: log.warning(f"Cannot calculate hash for '{file_path}': {e}"); return None
        except Exception as e: log.exception(f"Unexpected error calculating hash for '{file_path}': {e}"); return None

    def log_action(self, batch_id, original_path, new_path, item_type, status):
        # (Function unchanged)
        if not self.is_enabled: return False
        orig_p = Path(original_path); original_size, original_mtime, original_hash = None, None, None
        can_stat = item_type == 'file' and status in {'pending_final', 'renamed', 'moved', 'trashed'}
        if can_stat:
            try:
                if orig_p.is_file():
                    stat = orig_p.stat(); original_size = stat.st_size; original_mtime = stat.st_mtime
                    if self.hash_check_bytes > 0: original_hash = self._calculate_partial_hash(orig_p, self.hash_check_bytes)
            except OSError as e: log.warning(f"Could not stat original file during log_action for '{original_path}': {e}")
            except Exception as e: log.exception(f"Unexpected error getting stats/hash for '{original_path}' during log_action: {e}")
        try:
            with self._connect() as conn:
                conn.execute("INSERT INTO rename_log (batch_id, timestamp, original_path, new_path, type, status, original_size, original_mtime, original_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (batch_id, datetime.now(timezone.utc).isoformat(), str(original_path), str(new_path), item_type, status, original_size, original_mtime, original_hash))
                conn.commit(); return True
        except sqlite3.IntegrityError as e: log.warning(f"Duplicate entry prevented in rename log for '{original_path}' (batch '{batch_id}'): {e}"); return False
        except sqlite3.Error as e: log.error(f"Database error during log_action for '{original_path}' (batch '{batch_id}'): {e}"); return False
        except Exception as e: log.exception(f"Unexpected error logging undo action for '{original_path}' (batch '{batch_id}'): {e}"); return False

    def update_action_status(self, batch_id, original_path, new_status, conn: Optional[sqlite3.Connection] = None):
        # (Function unchanged)
        if not self.is_enabled: return False
        log.debug(f"Updating status to '{new_status}' for '{original_path}' in batch '{batch_id}'")
        manage_connection = conn is None; _conn = None
        try:
            if manage_connection: _conn = self._connect(); cursor = _conn.cursor()
            else: _conn = conn; cursor = _conn.cursor()
            cursor.execute("UPDATE rename_log SET status = ? WHERE batch_id = ? AND original_path = ? AND status != 'reverted'", (new_status, batch_id, str(original_path)))
            updated_count = cursor.rowcount
            if manage_connection: _conn.commit()
            if updated_count > 0: log.debug(f"Successfully updated status for '{original_path}'"); return True
            else: log.warning(f"No matching record found or status already 'reverted' for update: '{original_path}' (batch '{batch_id}')"); return False
        except sqlite3.Error as e: log.error(f"Failed updating undo status for '{original_path}' (batch '{batch_id}') to '{new_status}': {e}"); return False
        except Exception as e: log.exception(f"Unexpected error updating undo status for '{original_path}' (batch '{batch_id}') to '{new_status}': {e}"); return False
        finally:
            if manage_connection and _conn: _conn.close()


    def prune_old_batches(self):
        # (Function unchanged)
        if not self.is_enabled: return
        expire_days_cfg = self.cfg('undo_expire_days', 30)
        try:
            expire_days = int(expire_days_cfg)
            if expire_days < 0: log.warning("Undo expiration days cannot be negative. Skipping prune."); return
        except (ValueError, TypeError):
            log.warning(f"Invalid 'undo_expire_days' config value ('{expire_days_cfg}'). Using default 30."); expire_days = 30
        if expire_days == 0: cutoff = datetime.now(timezone.utc) + timedelta(seconds=1)
        else: cutoff = datetime.now(timezone.utc) - timedelta(days=expire_days)
        cutoff_iso = cutoff.isoformat(); log.debug(f"Pruning undo records older than {cutoff_iso} ({expire_days} days)")
        deleted_rows = 0
        try:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM rename_log WHERE timestamp < ?", (cutoff_iso,))
                deleted_rows = cur.rowcount if cur else 0; conn.commit()
                if deleted_rows > 0: log.info(f"Pruned {deleted_rows} old undo log records.")
                else: log.debug("No expired entries found to prune.")
        except sqlite3.Error as e: log.error(f"Error during undo log pruning: {e}")
        except Exception as e: log.exception(f"Unexpected error during undo log pruning: {e}")


    def _find_temp_file(self, final_dest_path: Path) -> Optional[Path]:
        # (Function unchanged)
        temp_pattern = f"{final_dest_path.stem}{TEMP_SUFFIX_PREFIX}*{final_dest_path.suffix}"
        try:
            matches = list(final_dest_path.parent.glob(temp_pattern))
            if len(matches) == 1: log.debug(f"Found temp file for {final_dest_path}: {matches[0]}"); return matches[0]
            elif len(matches) > 1: log.warning(f"Multiple temp files found for {final_dest_path}: {matches}. Cannot determine correct one."); return None
            else: log.debug(f"No temp file found matching pattern '{temp_pattern}' in {final_dest_path.parent}"); return None
        except OSError as e: log.error(f"Error searching for temp file for {final_dest_path}: {e}"); return None


    def _check_file_integrity(self, current_path: Path, logged_size: Optional[int], logged_mtime: Optional[float], logged_hash: Optional[str]) -> Tuple[bool, str]:
        # (Function unchanged)
        if not self.check_integrity: return True, "Skipped (Check Disabled)"
        size_ok, mtime_ok, hash_ok = True, True, True; reasons = []
        has_size = logged_size is not None; has_mtime = logged_mtime is not None
        has_hash = logged_hash is not None and self.hash_check_bytes > 0
        if not has_size and not has_mtime and not has_hash: return True, "Skipped (no stats logged)"
        try: current_stat = current_path.stat(); current_size = current_stat.st_size; current_mtime = current_stat.st_mtime
        except OSError as e: return False, f"FAIL (Cannot stat: {e})"
        except Exception as e: log.exception(f"Unexpected error during integrity check stat for {current_path}"); return False, "FAIL (Stat Check Error)"
        if has_size:
            size_ok = current_size == logged_size
            if not size_ok: reasons.append(f"Size ({current_size} != {logged_size})")
        if has_mtime:
             mtime_ok = abs(current_mtime - logged_mtime) < MTIME_TOLERANCE
             if not mtime_ok: reasons.append(f"MTime ({current_mtime:.2f} !~= {logged_mtime:.2f})")
        if has_hash:
            current_hash = self._calculate_partial_hash(current_path, self.hash_check_bytes)
            if current_hash is None: hash_ok = False; reasons.append("Hash (Cannot calc current)")
            else:
                hash_ok = current_hash == logged_hash
                if not hash_ok: reasons.append(f"Hash ({current_hash[:8]}... != {logged_hash[:8]}...)")
        passed = size_ok and mtime_ok and hash_ok
        if passed: return True, "OK"
        else: return False, f"FAIL ({', '.join(reasons)})"


    def perform_undo(self, batch_id):
        # (Initial checks, Fetch unchanged)
        if not self.is_enabled: print("Error: Undo logging was not enabled or manager failed initialization."); return False
        if not self.db_path or not self.db_path.exists(): print(f"Error: Undo database not found at {self.db_path}"); return False
        print(f"--- Starting UNDO for batch '{batch_id}' ---"); actions = []; conn = None
        try:
            conn = self._connect()
            cursor = conn.execute("SELECT * FROM rename_log WHERE batch_id = ? AND status NOT IN ('reverted', 'trashed') ORDER BY id DESC", (batch_id,))
            actions = cursor.fetchall()
        except sqlite3.Error as e: log.error(f"Error accessing undo database trying to fetch actions for batch '{batch_id}': {e}"); print(f"Error accessing undo database: {e}"); return False
        except RenamerError as e: log.error(f"Error connecting to undo database for batch '{batch_id}': {e}"); print(f"Error connecting to undo database: {e}"); return False
        finally:
             if conn and not actions: conn.close()
        if not actions: print(f"No revertible actions found for batch '{batch_id}'."); return False

        # (Preview unchanged)
        print("Operations to be reverted (new -> original):")
        for action in actions:
            orig_p = Path(action['original_path']); new_p = Path(action['new_path']); status = action['status']
            prefix = f"  [{action['id']}] {status}: "; temp_p = None
            if status == 'pending_final': temp_p = self._find_temp_file(new_p)
            if status in ('renamed', 'moved'): print(f"{prefix}{new_p.name} -> {orig_p.name}  (in {orig_p.parent})")
            elif status == 'pending_final': print(f"{prefix}{temp_p.name if temp_p else f'TEMP_FILE_FOR({new_p.name})'} -> {orig_p.name} (in {orig_p.parent}){'' if temp_p else ' (Temp file maybe missing?)'}")
            elif status == 'created_dir': print(f"{prefix}Remove directory '{orig_p}'")
            else: print(f"{prefix}Unknown/Skipped Status '{status}' for '{new_p}' -> '{orig_p}'")

        # --- Confirmation Phase ---
        try:
            confirm = input("Proceed with UNDO operation? (y/N): ").strip().lower()
            if confirm != 'y':
                print("Undo operation cancelled by user.")
                if conn: conn.close() # Close connection on user cancel
                return False
        # --- REMOVED MISPLACED BLOCK ---
        except (EOFError, Exception) as e:
            log.error(f"Error reading confirmation input: {e}")
            print("Undo operation cancelled (Error reading input).")
            if conn: conn.close() # Close connection on input error
            return False

        # --- Execution Phase with Transaction ---
        print("--- Performing Revert ---")
        success_count, db_fail_count, fs_fail_count, skip_count = 0, 0, 0, 0
        critical_fs_error = False
        overall_success = False

        try: # Wrap the main loop for commit/rollback
            for action in actions:
                action_id = action['id']; orig_p = Path(action['original_path']); new_p = Path(action['new_path'])
                status = action['status']; item_type = action['type']; log_prefix = f"[Undo ID {action_id}] "
                fs_op_succeeded = False

                try: # Inner try for individual action processing
                    # (File/Dir revert logic - unchanged from previous step)
                    if status in ('renamed', 'moved'):
                        # ... (integrity, pre-checks, rename attempt) ...
                        current_src = new_p; target_dest = orig_p
                        log.debug(f"{log_prefix}Processing revert: {current_src} -> {target_dest}")
                        integrity_passed, integrity_msg = True, "Skipped (Check Disabled)"
                        if self.check_integrity and item_type == 'file':
                            integrity_passed, integrity_msg = self._check_file_integrity(current_src, action['original_size'], action['original_mtime'], action['original_hash'])
                            print(f"  Integrity check for '{current_src.name}': {integrity_msg}"); log.info(f"{log_prefix}Integrity check for '{current_src}': {integrity_msg}")
                        if not integrity_passed: print(f"  Skipping revert due to integrity check failure."); log.warning(f"{log_prefix}Skipping revert for '{current_src}' due to integrity check failure."); skip_count += 1; continue
                        if not current_src.exists(): print(f"  Skipped revert: File to revert from does not exist: '{current_src}'"); log.warning(f"{log_prefix}Skipped revert: Source '{current_src}' does not exist."); skip_count += 1; continue
                        if target_dest.exists(): print(f"  Skipped revert: Cannot revert '{current_src.name}'. Original path '{target_dest}' already exists."); log.warning(f"{log_prefix}Skipped revert: Target '{target_dest}' already exists."); skip_count += 1; continue
                        try:
                            log.info(f"{log_prefix}Attempting rename: '{current_src}' -> '{target_dest}'"); target_dest.parent.mkdir(parents=True, exist_ok=True)
                            current_src.rename(target_dest); print(f"  Success: '{current_src.name}' reverted to '{target_dest.name}'"); log.info(f"{log_prefix}Revert successful.")
                            fs_op_succeeded = True
                        except OSError as e: print(f"  Error reverting '{current_src.name}' to '{target_dest.name}': {e}"); log.error(f"{log_prefix}OSError reverting '{current_src}' to '{target_dest}': {e}"); fs_fail_count += 1; critical_fs_error = True
                        except Exception as e: print(f"  Unexpected error reverting '{current_src.name}': {e}"); log.exception(f"{log_prefix}Unexpected error reverting '{current_src}' to '{target_dest}': {e}"); fs_fail_count += 1; critical_fs_error = True
                    elif status == 'pending_final':
                        # ... (find temp, pre-checks, rename attempt) ...
                        final_dest = new_p; orig_src = orig_p
                        log.debug(f"{log_prefix}Processing revert for pending_final: TEMP({final_dest}) -> {orig_src}")
                        temp_src = self._find_temp_file(final_dest)
                        if not temp_src: print(f"  Skipped revert: Cannot find temp file for '{final_dest}'"); log.warning(f"{log_prefix}Skipped revert: Temp file for '{final_dest}' not found."); skip_count += 1; continue
                        if not temp_src.exists(): print(f"  Skipped revert: Temp file '{temp_src}' does not exist."); log.warning(f"{log_prefix}Skipped revert: Temp source '{temp_src}' does not exist."); skip_count += 1; continue
                        if orig_src.exists(): print(f"  Skipped revert: Cannot revert temp file '{temp_src.name}'. Original path '{orig_src}' already exists."); log.warning(f"{log_prefix}Skipped revert: Target '{orig_src}' already exists."); skip_count += 1; continue
                        try:
                            log.info(f"{log_prefix}Attempting rename: '{temp_src}' -> '{orig_src}'"); orig_src.parent.mkdir(parents=True, exist_ok=True)
                            temp_src.rename(orig_src); print(f"  Success: Temp file '{temp_src.name}' reverted to '{orig_src.name}'"); log.info(f"{log_prefix}Temp file revert successful.")
                            fs_op_succeeded = True
                        except OSError as e: print(f"  Error reverting temp file '{temp_src.name}' to '{orig_src.name}': {e}"); log.error(f"{log_prefix}OSError reverting temp file '{temp_src}' to '{orig_src}': {e}"); fs_fail_count += 1; critical_fs_error = True
                        except Exception as e: print(f"  Unexpected error reverting temp file '{temp_src.name}': {e}"); log.exception(f"{log_prefix}Unexpected error reverting temp file '{temp_src}' to '{orig_src}': {e}"); fs_fail_count += 1; critical_fs_error = True
                    elif status == 'created_dir':
                        # ... (checks, rmdir attempt) ...
                        dir_to_remove = orig_p
                        log.debug(f"{log_prefix}Processing revert for created_dir: Remove '{dir_to_remove}'")
                        if not dir_to_remove.exists(): print(f"  Skipped removal: Directory '{dir_to_remove}' does not exist."); log.debug(f"{log_prefix}Skipped removal: Directory '{dir_to_remove}' does not exist."); fs_op_succeeded = True
                        elif not dir_to_remove.is_dir(): print(f"  Skipped removal: Path '{dir_to_remove}' exists but is not a directory."); log.warning(f"{log_prefix}Skipped removal: '{dir_to_remove}' is not a directory."); skip_count += 1; continue
                        else:
                            try: is_empty = not any(dir_to_remove.iterdir())
                            except OSError as e: print(f"  Error checking if directory '{dir_to_remove}' is empty: {e}"); log.error(f"{log_prefix}OSError checking emptiness of '{dir_to_remove}': {e}"); fs_fail_count += 1; critical_fs_error = True; continue
                            if not is_empty: print(f"  Skipped removal: Directory '{dir_to_remove}' is not empty."); log.warning(f"{log_prefix}Skipped removal: Directory '{dir_to_remove}' is not empty."); skip_count += 1; continue
                            try:
                                log.info(f"{log_prefix}Attempting rmdir: '{dir_to_remove}'"); dir_to_remove.rmdir(); print(f"  Success: Directory '{dir_to_remove}' removed."); log.info(f"{log_prefix}Directory removal successful.")
                                fs_op_succeeded = True
                            except OSError as e: print(f"  Error removing directory '{dir_to_remove}': {e}"); log.error(f"{log_prefix}OSError removing directory '{dir_to_remove}': {e}"); fs_fail_count += 1; critical_fs_error = True
                            except Exception as e: print(f"  Unexpected error removing directory '{dir_to_remove}': {e}"); log.exception(f"{log_prefix}Unexpected error removing directory '{dir_to_remove}': {e}"); fs_fail_count += 1; critical_fs_error = True
                    else:
                        print(f"  Skipping action with unknown status '{status}' for '{new_p}' -> '{orig_p}'")
                        log.warning(f"{log_prefix}Skipping action with unknown status '{status}'")
                        skip_count += 1
                        continue

                    # Update Status (if FS operation succeeded)
                    if fs_op_succeeded:
                         if self.update_action_status(batch_id, str(orig_p), 'reverted', conn=conn):
                             success_count += 1
                         else:
                             log.error(f"{log_prefix}FS operation successful, but FAILED to update status to 'reverted' for '{orig_p}'")
                             db_fail_count += 1 # Count DB update failures separately

                except Exception as e_inner:
                    print(f"  Unexpected error processing action ID {action_id} ({status} for {new_p}): {e_inner}")
                    log.exception(f"{log_prefix}Unexpected error processing action: {e_inner}")
                    fs_fail_count += 1
                    critical_fs_error = True

            # --- After loop: Commit or Rollback based ONLY on critical_fs_error ---
            if critical_fs_error:
                 log.error(f"Critical file system errors occurred during undo for batch '{batch_id}'. Rolling back database changes.")
                 print("  ERROR: Critical file system errors occurred. Rolling back database changes.")
                 if conn: conn.rollback()
            else:
                 log.info(f"Undo database updates committing for batch '{batch_id}'.")
                 if conn: conn.commit()
            # Overall success depends on NO critical FS errors AND no DB update errors
            overall_success = not critical_fs_error and db_fail_count == 0

        except Exception as e_outer:
            log.exception(f"Critical error during undo transaction management for batch '{batch_id}': {e_outer}")
            print(f"CRITICAL DATABASE ERROR during undo finalization: {e_outer}")
            if conn:
                try: conn.rollback()
                except Exception as e_rb: log.error(f"Rollback failed after outer error: {e_rb}")
            overall_success = False # Ensure failure on outer exception
        finally:
            if conn: conn.close(); log.debug("Undo database connection closed.")

        print(f"--- UNDO Complete for batch '{batch_id}' ---")
        print(f"Summary: {success_count} succeeded (FS+DB), {db_fail_count} DB update errors, {fs_fail_count} FS errors, {skip_count} skipped.")

        return overall_success

# --- END OF FILE undo_manager.py ---