import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
import time # Needed for integrity check comparison delta
import os # Needed for integrity check comparison delta
import fnmatch # Needed for finding temp files

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

        try:
            self.is_enabled = self.cfg('enable_undo', False)
            if self.is_enabled:
                self.db_path = self._resolve_db_path()
                self._init_db()
                self.check_integrity = self.cfg('undo_check_integrity', False)
                log.info(f"UndoManager initialized (Integrity Check: {self.check_integrity}). DB: {self.db_path}")
            else:
                log.info("Undo feature disabled by configuration.")
        except Exception as e:
            # Set disabled status FIRST in case logging fails
            self.is_enabled = False
            log.exception(f"Failed to initialize UndoManager: {e}")

    def _resolve_db_path(self):
        db_path_config = self.cfg('undo_db_path', None)
        try:
            if db_path_config:
                path = Path(db_path_config).resolve()
            else:
                # Consider using platformdirs or similar for default location
                # For now, keep relative path for simplicity in this context
                path = Path(__file__).parent.parent / "rename_log.db"
                path = path.resolve()
            path.parent.mkdir(parents=True, exist_ok=True)
            return path
        except Exception as e:
            # Raise the specific error type expected by tests
            raise RenamerError(f"Cannot resolve undo database path: {e}") from e

    def _connect(self):
        if not self.db_path:
            raise RenamerError("Cannot connect to undo database: path not resolved.")
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            # Apply PRAGMAs individually and log warnings on failure
            try:
                conn.execute("PRAGMA journal_mode=WAL;")
            except sqlite3.Error as pe:
                log.warning(f"Could not set PRAGMA journal_mode=WAL for undo DB ({self.db_path}): {pe}")
            try:
                conn.execute("PRAGMA busy_timeout=5000;")
            except sqlite3.Error as pe:
                log.warning(f"Could not set PRAGMA busy_timeout=5000 for undo DB ({self.db_path}): {pe}")
            return conn
        except sqlite3.Error as e:
            # Raise specific error expected by tests
            raise RenamerError(f"Cannot connect to undo database '{self.db_path}': {e}") from e

    def _init_db(self):
        # No change needed here based on failures, but ensure error propagation
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
                        original_mtime REAL
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_id ON rename_log(batch_id)")
                conn.commit()
                # log.info(f"Undo database schema checked/initialized: {self.db_path}") # Reduced verbosity
        except sqlite3.Error as e:
            # Raise specific error expected by tests
            raise RenamerError(f"Failed to initialize undo database schema: {e}") from e
        except RenamerError as e: # Catch connection errors
             raise RenamerError(f"Failed to connect during undo database initialization: {e}") from e

    def log_action(self, batch_id, original_path, new_path, item_type, status):
        if not self.is_enabled:
            return False
        
        orig_p = Path(original_path)
        original_size = None
        original_mtime = None

        # Only attempt stat for specific types/statuses where original file existed
        if item_type == 'file' and status in {'pending_final', 'renamed', 'moved', 'trashed'}:
            try:
                # Check existence *before* statting
                if orig_p.is_file():
                    stat = orig_p.stat()
                    original_size = stat.st_size
                    original_mtime = stat.st_mtime
                    # log.debug(f"Captured stats for {original_path}: size={original_size}, mtime={original_mtime}")
                # else:
                    # Optional: Log if file not found when expected
                    # log.debug(f"Original file not found or not a file, skipping stats: {original_path}")
            except OSError as e:
                # Log the specific OS error
                log.warning(f"Could not stat original file during log_action for '{original_path}': {e}")
            except Exception as e:
                log.exception(f"Unexpected error getting stats for '{original_path}' during log_action: {e}")

        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT INTO rename_log (batch_id, timestamp, original_path, new_path, type, status, original_size, original_mtime)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    batch_id, datetime.now(timezone.utc).isoformat(), str(original_path), str(new_path), item_type, status, original_size, original_mtime
                ))
                conn.commit()
                return True # Success
        except sqlite3.IntegrityError as e:
            log.warning(f"Duplicate entry prevented in rename log for '{original_path}' (batch '{batch_id}'): {e}")
            return False # Indicate failure due to constraint
        except sqlite3.Error as e:
            log.error(f"Database error during log_action for '{original_path}' (batch '{batch_id}'): {e}")
            return False # Indicate other DB failure
        except Exception as e:
            log.exception(f"Unexpected error logging undo action for '{original_path}' (batch '{batch_id}'): {e}")
            return False # Indicate unexpected failure


    def update_action_status(self, batch_id, original_path, new_status):
        if not self.is_enabled:
            return False

        log.debug(f"Updating status to '{new_status}' for '{original_path}' in batch '{batch_id}'")
        try:
            with self._connect() as conn:
                cur = conn.execute("""
                    UPDATE rename_log
                    SET status = ?
                    WHERE batch_id = ? AND original_path = ? AND status != 'reverted'
                """, (new_status, batch_id, str(original_path)))
                conn.commit() # Ensure commit after update
                updated_count = cur.rowcount if cur else 0
                if updated_count > 0:
                    log.debug(f"Successfully updated status for '{original_path}'")
                    return True
                else:
                    log.warning(f"No matching record found or status already 'reverted' for update: '{original_path}' (batch '{batch_id}')")
                    return False
        except sqlite3.Error as e:
            log.error(f"Failed updating undo status for '{original_path}' (batch '{batch_id}') to '{new_status}': {e}")
            return False
        except Exception as e:
            log.exception(f"Unexpected error updating undo status for '{original_path}' (batch '{batch_id}') to '{new_status}': {e}")
            return False


    def prune_old_batches(self):
        if not self.is_enabled:
            return

        expire_days_cfg = self.cfg('undo_expire_days', 30)
        try:
            expire_days = int(expire_days_cfg)
            if expire_days < 0:
                # Match test assertion string
                log.warning("Undo expiration days cannot be negative. Skipping prune.")
                return
        except (ValueError, TypeError):
             # Match test assertion string
            log.warning(f"Invalid 'undo_expire_days' config value ('{expire_days_cfg}'). Using default 30.")
            expire_days = 30

        # Use 0 days for testing immediate prune
        if expire_days == 0:
             cutoff = datetime.now(timezone.utc) + timedelta(seconds=1) # Ensure anything just logged is included for 0 days
        else:
            cutoff = datetime.now(timezone.utc) - timedelta(days=expire_days)
        cutoff_iso = cutoff.isoformat()
        log.debug(f"Pruning undo records older than {cutoff_iso} ({expire_days} days)")

        deleted_rows = 0
        try:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM rename_log WHERE timestamp < ?", (cutoff_iso,))
                # Use cur.rowcount directly
                deleted_rows = cur.rowcount if cur else 0
                conn.commit()
                if deleted_rows > 0:
                    log.info(f"Pruned {deleted_rows} old undo log records.")
                else:
                    # Match test assertion string
                    log.debug("No expired entries found to prune.")
        except sqlite3.Error as e:
            log.error(f"Error during undo log pruning: {e}")
        except Exception as e:
            log.exception(f"Unexpected error during undo log pruning: {e}")


    def _find_temp_file(self, final_dest_path: Path) -> Path | None:
        """Finds the corresponding temporary file for a final destination."""
        temp_pattern = f"{final_dest_path.stem}{TEMP_SUFFIX_PREFIX}*{final_dest_path.suffix}"
        try:
            matches = list(final_dest_path.parent.glob(temp_pattern))
            if len(matches) == 1:
                log.debug(f"Found temp file for {final_dest_path}: {matches[0]}")
                return matches[0]
            elif len(matches) > 1:
                log.warning(f"Multiple temp files found for {final_dest_path}: {matches}. Cannot determine correct one.")
                return None
            else:
                log.debug(f"No temp file found matching pattern '{temp_pattern}' in {final_dest_path.parent}")
                return None
        except OSError as e:
            log.error(f"Error searching for temp file for {final_dest_path}: {e}")
            return None


    def _check_file_integrity(self, current_path: Path, logged_size: int | None, logged_mtime: float | None) -> tuple[bool, str]:
        """Checks if current file matches logged stats."""
        if logged_size is None or logged_mtime is None:
            return True, "Skipped (no stats)"
        try:
            current_stat = current_path.stat()
            size_match = current_stat.st_size == logged_size
            # Compare mtime with tolerance
            mtime_match = abs(current_stat.st_mtime - logged_mtime) < MTIME_TOLERANCE

            if size_match and mtime_match:
                return True, "OK"
            else:
                reason = []
                if not size_match: reason.append(f"Size ({current_stat.st_size} != {logged_size})")
                if not mtime_match: reason.append(f"MTime ({current_stat.st_mtime:.2f} !~= {logged_mtime:.2f})")
                return False, f"FAIL ({', '.join(reason)})"
        except OSError as e:
            return False, f"FAIL (Cannot stat: {e})"
        except Exception as e:
            log.exception(f"Unexpected error during integrity check for {current_path}")
            return False, "FAIL (Check Error)"

    def perform_undo(self, batch_id):
        if not self.is_enabled:
            print("Error: Undo logging was not enabled or manager failed initialization.")
            return False
        if not self.db_path or not self.db_path.exists():
            print(f"Error: Undo database not found at {self.db_path}")
            return False

        print(f"--- Starting UNDO for batch '{batch_id}' ---")
        actions = []
        try:
            with self._connect() as conn:
                cur = conn.execute("""
                    SELECT * FROM rename_log
                    WHERE batch_id = ?
                    AND status NOT IN ('reverted', 'trashed')
                    ORDER BY id DESC -- Process in reverse order of logging
                """, (batch_id,))
                actions = cur.fetchall()
        except sqlite3.Error as e:
            # Log error, print message, return False
            log.error(f"Error accessing undo database trying to fetch actions for batch '{batch_id}': {e}")
            print(f"Error accessing undo database: {e}")
            return False
        except RenamerError as e: # Catch connection error
             log.error(f"Error connecting to undo database for batch '{batch_id}': {e}")
             print(f"Error connecting to undo database: {e}")
             return False


        if not actions:
            # Match test assertion string
            print(f"No revertible actions found for batch '{batch_id}'.")
            return False # Indicate nothing to do, but not strictly an error

        # --- Preview Phase ---
        print("Operations to be reverted (new -> original):")
        preview_ok = True
        for action in actions:
            orig_p = Path(action['original_path'])
            new_p = Path(action['new_path'])
            status = action['status']
            item_type = action['type']
            prefix = f"  [{action['id']}] {status}: "

            if status in ('renamed', 'moved'):
                print(f"{prefix}{new_p.name} -> {orig_p.name}  (in {orig_p.parent})")
            elif status == 'pending_final':
                # Try to find temp file for preview clarity
                temp_p = self._find_temp_file(new_p)
                if temp_p:
                    print(f"{prefix}{temp_p.name} -> {orig_p.name} (in {orig_p.parent})")
                else:
                    print(f"{prefix}TEMP_FILE_FOR({new_p.name}) -> {orig_p.name} (in {orig_p.parent}) (Temp file maybe missing?)")
            elif status == 'created_dir':
                 print(f"{prefix}Remove directory '{orig_p}'") # orig_p == new_p here
            else:
                 # Match test assertion string
                 print(f"{prefix}Unknown/Skipped Status '{status}' for '{new_p}' -> '{orig_p}'")
                 # Optionally log here if needed by tests like test_perform_undo_preview_unknown_status
                 # log.warning(f"Skipping preview for unknown/unhandled status '{status}' for action ID {action['id']}")

        # --- Confirmation Phase ---
        try:
            confirm = input("Proceed with UNDO operation? (y/N): ").strip().lower()
            if confirm != 'y':
                print("Undo operation cancelled by user.")
                return False # User cancelled
        except (EOFError, Exception) as e:
            # Log the specific error, print message, return False
            log.error(f"Error reading confirmation input: {e}")
            print("Undo operation cancelled (Error reading input).")
            return False # Treat input error as cancellation


        # --- Execution Phase ---
        print("--- Performing Revert ---")
        success_count = 0
        fail_count = 0
        skip_count = 0

        for action in actions:
            action_id = action['id']
            orig_p = Path(action['original_path'])
            new_p = Path(action['new_path'])
            status = action['status']
            item_type = action['type']
            log_prefix = f"[Undo ID {action_id}] "

            try:
                if status in ('renamed', 'moved'):
                    # --- Handle File/Dir Rename/Move Revert ---
                    current_src = new_p
                    target_dest = orig_p
                    log.debug(f"{log_prefix}Processing revert: {current_src} -> {target_dest}")

                    # 1. Integrity Check (if enabled)
                    integrity_passed = True
                    integrity_msg = "Skipped (disabled)"
                    if self.check_integrity and item_type == 'file':
                        integrity_passed, integrity_msg = self._check_file_integrity(current_src, action['original_size'], action['original_mtime'])
                        print(f"  Integrity check for '{current_src.name}': {integrity_msg}")
                        log.info(f"{log_prefix}Integrity check for '{current_src}': {integrity_msg}")

                    if not integrity_passed:
                        print(f"  Skipping revert due to integrity check failure.")
                        log.warning(f"{log_prefix}Skipping revert for '{current_src}' due to integrity check failure.")
                        skip_count += 1
                        continue # Skip this action

                    # 2. Pre-condition Checks
                    if not current_src.exists():
                        # Match test assertion string (test_undo_with_missing_current_file)
                        print(f"  Skipped revert: File to revert from does not exist: '{current_src}'")
                        log.warning(f"{log_prefix}Skipped revert: Source '{current_src}' does not exist.")
                        skip_count += 1
                        continue
                    if target_dest.exists():
                         # Match test assertion string (test_undo_target_already_exists)
                        print(f"  Skipped revert: Cannot revert '{current_src.name}'. Original path '{target_dest}' already exists.")
                        log.warning(f"{log_prefix}Skipped revert: Target '{target_dest}' already exists.")
                        skip_count += 1
                        continue

                    # 3. Perform Revert
                    try:
                        log.info(f"{log_prefix}Attempting rename: '{current_src}' -> '{target_dest}'")
                        target_dest.parent.mkdir(parents=True, exist_ok=True)
                        current_src.rename(target_dest)
                        print(f"  Success: '{current_src.name}' reverted to '{target_dest.name}'")
                        log.info(f"{log_prefix}Revert successful.")
                        # 4. Update Status
                        if self.update_action_status(batch_id, str(target_dest), 'reverted'):
                            success_count += 1
                        else:
                            # Log error if status update failed, but count rename as success
                            log.error(f"{log_prefix}Revert rename successful, but FAILED to update status to 'reverted' for '{target_dest}'")
                            success_count += 1 # Count the file operation success
                            fail_count +=1 # Count the status update failure implicitly

                    except OSError as e:
                         # Match test assertion string (test_perform_undo_revert_os_error)
                        print(f"  Error reverting '{current_src.name}' to '{target_dest.name}': {e}")
                        log.error(f"{log_prefix}OSError reverting '{current_src}' to '{target_dest}': {e}")
                        fail_count += 1
                    except Exception as e:
                         # Match test assertion string (test_perform_undo_revert_exception)
                        print(f"  Unexpected error reverting '{current_src.name}': {e}")
                        log.exception(f"{log_prefix}Unexpected error reverting '{current_src}' to '{target_dest}': {e}")
                        fail_count += 1


                elif status == 'pending_final':
                     # --- Handle Temp File Revert ---
                    final_dest = new_p
                    orig_src = orig_p
                    log.debug(f"{log_prefix}Processing revert for pending_final: TEMP({final_dest}) -> {orig_src}")

                    # 1. Find Temp File
                    temp_src = self._find_temp_file(final_dest)
                    if not temp_src:
                        print(f"  Skipped revert: Cannot find temp file for '{final_dest}'")
                        log.warning(f"{log_prefix}Skipped revert: Temp file for '{final_dest}' not found.")
                        skip_count += 1
                        continue

                    # 2. Pre-condition Checks
                    if not temp_src.exists():
                        print(f"  Skipped revert: Temp file '{temp_src}' does not exist.")
                        log.warning(f"{log_prefix}Skipped revert: Temp source '{temp_src}' does not exist.")
                        skip_count += 1
                        continue
                    if orig_src.exists():
                        print(f"  Skipped revert: Cannot revert temp file '{temp_src.name}'. Original path '{orig_src}' already exists.")
                        log.warning(f"{log_prefix}Skipped revert: Target '{orig_src}' already exists.")
                        skip_count += 1
                        continue

                    # 3. Perform Revert
                    try:
                        log.info(f"{log_prefix}Attempting rename: '{temp_src}' -> '{orig_src}'")
                        orig_src.parent.mkdir(parents=True, exist_ok=True)
                        temp_src.rename(orig_src)
                        print(f"  Success: Temp file '{temp_src.name}' reverted to '{orig_src.name}'")
                        log.info(f"{log_prefix}Temp file revert successful.")
                        # 4. Update Status
                        if self.update_action_status(batch_id, str(orig_src), 'reverted'):
                             success_count += 1
                        else:
                            log.error(f"{log_prefix}Temp file revert successful, but FAILED to update status to 'reverted' for '{orig_src}'")
                            success_count += 1
                            fail_count += 1

                    except OSError as e:
                        print(f"  Error reverting temp file '{temp_src.name}' to '{orig_src.name}': {e}")
                        log.error(f"{log_prefix}OSError reverting temp file '{temp_src}' to '{orig_src}': {e}")
                        fail_count += 1
                    except Exception as e:
                        print(f"  Unexpected error reverting temp file '{temp_src.name}': {e}")
                        log.exception(f"{log_prefix}Unexpected error reverting temp file '{temp_src}' to '{orig_src}': {e}")
                        fail_count += 1


                elif status == 'created_dir':
                    # --- Handle Created Directory Removal ---
                    dir_to_remove = orig_p # orig_p == new_p for created_dir
                    log.debug(f"{log_prefix}Processing revert for created_dir: Remove '{dir_to_remove}'")

                    # 1. Pre-condition Checks
                    if not dir_to_remove.exists():
                         # Match test assertion string (test_perform_undo_dir_does_not_exist_on_cleanup)
                        print(f"  Skipped removal: Directory '{dir_to_remove}' does not exist.")
                        log.debug(f"{log_prefix}Skipped removal: Directory '{dir_to_remove}' does not exist.")
                        # Update status even if dir is already gone, as the state matches 'reverted'
                        if self.update_action_status(batch_id, str(dir_to_remove), 'reverted'):
                             skip_count += 1 # Skipped the operation, but outcome is correct
                        else:
                             log.error(f"{log_prefix}Directory already gone, but FAILED to update status to 'reverted' for '{dir_to_remove}'")
                             fail_count += 1 # Failed status update
                        continue # Move to next action

                    if not dir_to_remove.is_dir():
                        print(f"  Skipped removal: Path '{dir_to_remove}' exists but is not a directory.")
                        log.warning(f"{log_prefix}Skipped removal: '{dir_to_remove}' is not a directory.")
                        skip_count += 1
                        continue

                    try:
                        # Check if directory is empty *before* trying to remove
                        is_empty = not any(dir_to_remove.iterdir())
                    except OSError as e:
                        print(f"  Error checking if directory '{dir_to_remove}' is empty: {e}")
                        log.error(f"{log_prefix}OSError checking emptiness of '{dir_to_remove}': {e}")
                        fail_count += 1
                        continue # Cannot proceed if we can't check emptiness

                    if not is_empty:
                         # Match test assertion string (test_undo_created_dir_not_empty)
                        print(f"  Skipped removal: Directory '{dir_to_remove}' is not empty.")
                        log.warning(f"{log_prefix}Skipped removal: Directory '{dir_to_remove}' is not empty.")
                        skip_count += 1
                        continue

                    # 2. Perform Removal
                    try:
                        log.info(f"{log_prefix}Attempting rmdir: '{dir_to_remove}'")
                        dir_to_remove.rmdir()
                        print(f"  Success: Directory '{dir_to_remove}' removed.")
                        log.info(f"{log_prefix}Directory removal successful.")
                         # 3. Update Status
                        if self.update_action_status(batch_id, str(dir_to_remove), 'reverted'):
                            success_count += 1
                        else:
                            # This case is tested by test_perform_undo_dir_cleanup_exception
                            log.error(f"{log_prefix}Directory removed successfully, but FAILED to update status to 'reverted' for '{dir_to_remove}'")
                            success_count += 1 # Count dir removal
                            fail_count += 1 # Count status update failure

                    except OSError as e:
                         # Match test assertion string (test_perform_undo_dir_removal_os_error)
                        print(f"  Error removing directory '{dir_to_remove}': {e}")
                        log.error(f"{log_prefix}OSError removing directory '{dir_to_remove}': {e}")
                        fail_count += 1
                    except Exception as e:
                        # Should be caught by outer try/except, but good practice here too
                        print(f"  Unexpected error removing directory '{dir_to_remove}': {e}")
                        # Use log.exception here as needed by test_perform_undo_dir_cleanup_exception
                        log.exception(f"{log_prefix}Unexpected error removing directory '{dir_to_remove}': {e}")
                        fail_count += 1


                else:
                    # --- Handle Unknown Status ---
                    print(f"  Skipping action with unknown status '{status}' for '{new_p}' -> '{orig_p}'")
                    log.warning(f"{log_prefix}Skipping action with unknown status '{status}'")
                    skip_count += 1

            except Exception as e:
                # Catch-all for unexpected errors during the processing of a single action
                print(f"  Unexpected error processing action ID {action_id} ({status} for {new_p}): {e}")
                log.exception(f"{log_prefix}Unexpected error processing action: {e}")
                fail_count += 1

        print(f"--- UNDO Complete for batch '{batch_id}' ---")
        print(f"Summary: {success_count} succeeded, {fail_count} failed, {skip_count} skipped.")

        # --- FIX: Return True if there were no failures ---
        # This considers the undo successful if all actions either succeeded
        # or were correctly skipped (like dir already gone), and no errors occurred.
        return fail_count == 0
        # --- End FIX ---

        # Old logic:
        # return fail_count == 0 and success_count > 0 # Return True if anything succeeded and nothing failed