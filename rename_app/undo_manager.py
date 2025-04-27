# rename_app/undo_manager.py

import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from .exceptions import RenamerError, FileOperationError # Keep custom exception

log = logging.getLogger(__name__)
TEMP_SUFFIX_PREFIX = ".renametmp_" # Ensure this matches file_system_ops

class UndoManager:
    def __init__(self, cfg_helper):
        self.cfg = cfg_helper
        self.db_path = self._resolve_db_path()
        # Use cfg helper to get final enable_undo decision
        self.is_enabled = self.cfg('enable_undo', False)
        if self.is_enabled:
            self._init_db()

    def _resolve_db_path(self):
        # Allow configuring DB path? For now, keep it simple.
        # Use cfg helper to get potential configured path
        db_path_config = self.cfg('undo_db_path', None)
        if db_path_config:
            return Path(db_path_config).resolve()
        else:
            # Default location relative to project root (assuming standard structure)
            try:
                 script_dir = Path(__file__).parent.parent.resolve()
                 return script_dir / "rename_log.db"
            except NameError: # Fallback if __file__ not defined
                 return Path.cwd() / "rename_log.db"


    def _connect(self):
        """Connects to the SQLite database."""
        # (Implementation as before)
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0) # Added timeout
            conn.row_factory = sqlite3.Row
            # Optional: Enable WAL mode for potentially better concurrency
            # try:
            #     conn.execute("PRAGMA journal_mode=WAL;")
            # except sqlite3.Error as e:
            #     log.warning(f"Could not enable WAL mode for undo DB: {e}")
            return conn
        except sqlite3.Error as e:
            raise RenamerError(f"Failed to connect to undo database '{self.db_path}': {e}") from e


    def _init_db(self):
        """Initializes the database schema if it doesn't exist."""
        # (Implementation as before)
        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                # Added original_size and original_mtime
                cursor.execute("""
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
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_batch_id ON rename_log (batch_id)")
                # Original path is unique, index might not be strictly needed but helps lookups
                # cursor.execute("CREATE INDEX IF NOT EXISTS idx_original_path ON rename_log (original_path)")
                conn.commit() # Explicit commit though connect context handles it
                log.info(f"Undo database initialized/verified at {self.db_path}")
        except sqlite3.Error as e:
            log.error(f"Failed to initialize undo database schema: {e}")
            self.is_enabled = False # Disable if schema init fails


    def log_action(self, batch_id, original_path, new_path, item_type, status):
        """Logs a file operation action if undo is enabled."""
        # (Implementation as before)
        if not self.is_enabled: return

        original_size, original_mtime = None, None
        orig_p = Path(original_path)
        # Get stats only for files *before* they are potentially moved/deleted
        # Log stats only for the initial relevant statuses
        if item_type == 'file' and status in ['pending_final', 'renamed', 'moved', 'trashed', 'created_dir']:
            try:
                if orig_p.is_file(): # Ensure it exists and is a file
                    stats = orig_p.stat()
                    original_size = stats.st_size
                    original_mtime = stats.st_mtime
            except OSError as e: log.warning(f"Could not stat original '{orig_p}' for undo log: {e}")

        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT INTO rename_log (batch_id, timestamp, original_path, new_path, type, status, original_size, original_mtime)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (batch_id, datetime.now(timezone.utc).isoformat(), str(original_path), str(new_path), item_type, status, original_size, original_mtime))
                log.debug(f"Logged undo action ({status}): '{original_path}' -> '{new_path}'")
        except sqlite3.Error as e:
            log.error(f"Failed logging undo action for '{original_path}': {e}")


    def update_action_status(self, batch_id, original_path, new_status):
        """Updates the status of a previously logged action."""
        # (Implementation as before)
        if not self.is_enabled: return False
        try:
            with self._connect() as conn:
               cursor = conn.execute("""
                    UPDATE rename_log SET status = ? WHERE original_path = ? AND batch_id = ? AND status != 'reverted'
                """, (new_status, str(original_path), batch_id))
               updated = cursor.rowcount > 0
               if updated: log.debug(f"Updated undo status to '{new_status}' for '{original_path}' in batch {batch_id}")
               return updated
        except sqlite3.Error as e:
            log.error(f"Failed updating undo status for '{original_path}': {e}")
            return False


    def perform_undo(self, batch_id_to_undo):
        """Reverts actions for a specific batch ID."""
        if not self.is_enabled:
             print("Error: Undo logging was not enabled for this run or DB is inaccessible.")
             return
        if not self.db_path.exists():
             print(f"Error: Undo database not found at {self.db_path}")
             return

        # Use cfg helper directly since UndoManager has it
        check_integrity = self.cfg('undo_check_integrity', False)
        log.info(f"Starting undo for batch: {batch_id_to_undo} (Integrity Check: {check_integrity})")
        actions_to_undo = []
        try:
            with self._connect() as conn:
                cursor = conn.execute("""
                    SELECT id, original_path, new_path, type, status, original_size, original_mtime
                    FROM rename_log WHERE batch_id = ? AND status NOT IN ('reverted', 'trashed') ORDER BY id DESC
                """, (batch_id_to_undo,)) # Exclude already reverted or trashed (cannot revert trash)
                actions_to_undo = cursor.fetchall()
        except sqlite3.Error as e:
            log.error(f"Failed retrieving undo actions for batch '{batch_id_to_undo}': {e}")
            print(f"Error accessing undo database: {e}")
            return

        if not actions_to_undo:
            print(f"No revertible actions found for batch '{batch_id_to_undo}'.")
            return

        # Preview Actions
        print(f"Found {len(actions_to_undo)} actions to revert for batch '{batch_id_to_undo}'.")
        print("Proposed reversions:")
        dirs_to_remove_plan = []
        for action in actions_to_undo:
            status = action['status']
            if status in ['renamed', 'moved', 'pending_final', 'failed_pending']:
                print(f"  - Restore: '{action['new_path']}' -> '{action['original_path']}'")
            elif status == 'created_dir':
                print(f"  - Remove Dir (if empty): '{action['original_path']}'")
                dirs_to_remove_plan.append(Path(action['original_path']))
            else:
                 print(f"  - Unknown/Skipped Status '{status}' for original '{action['original_path']}'")

        try:
             confirm = input(f"Proceed with reverting?{' (Integrity check: ' + ('ON' if check_integrity else 'OFF') + ')'} (y/N): ")
             if confirm.lower() != 'y': print("Undo operation cancelled."); return
        except EOFError: print("Undo operation cancelled (no input)."); return


        print("Attempting to revert...")
        reverted_count, error_count, integrity_mismatch = 0, 0, 0
        # Use the list planned during preview
        dirs_to_remove = dirs_to_remove_plan

        # Process file reverts first
        for action in actions_to_undo:
             orig_p, new_p = Path(action['original_path']), Path(action['new_path'])
             type, status = action['type'], action['status']
             orig_size, orig_mtime = action['original_size'], action['original_mtime']
             log_id = action['id']

             if status == 'created_dir': continue # Handle dirs later

             current_path_to_check = new_p
             # Handle cases where rename failed mid-transaction
             if status == 'pending_final' or status == 'failed_pending':
                 # Construct potential temp path pattern
                 temp_pattern = f"{new_p.stem}{TEMP_SUFFIX_PREFIX}*{new_p.suffix}"
                 possible_temps = list(new_p.parent.glob(temp_pattern))
                 if possible_temps:
                      current_path_to_check = possible_temps[0]
                      log.warning(f"Found potential temp file '{current_path_to_check}' for failed action, will attempt revert from temp.")
                 # --- Corrected Indentation Starts Here ---
                 elif not new_p.exists(): # Neither final nor temp exists
                     print(f"Skipped revert: File not found at expected path '{new_p}' or related temp path.")
                     error_count += 1
                     continue
                 # --- Corrected Indentation Ends Here ---
                 # If new_p *does* exist despite pending/failed status, proceed with checking it
                 else:
                      current_path_to_check = new_p # Stick with new_p if it exists


             if not current_path_to_check.exists():
                 print(f"Skipped revert: File to revert from does not exist: '{current_path_to_check}'")
                 error_count += 1
                 continue

             # Integrity Check
             integrity_ok = True
             if check_integrity and type == 'file' and orig_size is not None and orig_mtime is not None:
                try:
                     current_stats = current_path_to_check.stat()
                     # Allow tolerance for size/mtime? Filesystems can vary slightly.
                     size_diff = abs(current_stats.st_size - orig_size)
                     time_diff = abs(current_stats.st_mtime - orig_mtime)
                     size_ok = size_diff == 0 # Require exact size match for now
                     time_ok = time_diff < 2 # Allow < 2 sec difference for mtime

                     if not size_ok:
                         print(f"Integrity FAIL (Size): '{current_path_to_check.name}' ({current_stats.st_size} != {orig_size})")
                         integrity_ok = False; integrity_mismatch += 1
                     elif not time_ok:
                         print(f"Integrity FAIL (MTime): '{current_path_to_check.name}' ({current_stats.st_mtime:.0f} != {orig_mtime:.0f}, diff={time_diff:.2f}s)")
                         integrity_ok = False; integrity_mismatch += 1
                except OSError as e_stat:
                     print(f"Warning: Could not get stats for integrity check on '{current_path_to_check.name}': {e_stat}")
                     # Proceed without check if stat fails? Or count as error? Proceeding.

             if not integrity_ok and check_integrity:
                  print(f"Skipping revert for '{current_path_to_check.name}' due to integrity mismatch.")
                  error_count += 1 # Count as error/skipped
                  continue

             # Perform Revert (Move/Rename back)
             try:
                 if orig_p.exists():
                      # Maybe offer a suffix option for the revert target? For now, fail.
                      print(f"Error: Cannot revert '{current_path_to_check.name}'. Original path '{orig_p}' already exists."); error_count += 1; continue
                 # Ensure parent of original exists before moving back
                 orig_p.parent.mkdir(parents=True, exist_ok=True)
                 # Use rename which works for move across filesystems too on most OS
                 current_path_to_check.rename(orig_p)
                 print(f"Reverted: '{current_path_to_check.name}' -> '{orig_p.name}'")
                 # Pass self.cfg when calling update method
                 if self.update_action_status(batch_id_to_undo, action['original_path'], 'reverted'):
                      reverted_count += 1
                 else: # Should not happen if query worked
                      log.error(f"Failed to update status to reverted for '{action['original_path']}'")

             except OSError as e:
                 print(f"Error reverting '{current_path_to_check.name}' to '{orig_p.name}': {e}")
                 log.error(f"Error reverting action ID {log_id}: {e}")
                 error_count += 1
             except Exception as e: # Catch other unexpected errors
                 print(f"Unexpected error reverting '{current_path_to_check.name}': {e}")
                 log.exception(f"Unexpected error reverting action ID {log_id}")
                 error_count += 1


        # Attempt to remove directories marked for removal (only if empty)
        print("Cleaning up created directories (if empty)...")
        processed_dirs = set()
        # Iterate multiple times or sort to handle nested directories correctly? Sort reversed.
        dirs_to_remove.sort(key=lambda p: len(p.parts), reverse=True)

        for dir_path in dirs_to_remove:
             if dir_path in processed_dirs: continue
             try:
                 if dir_path.is_dir() and not any(dir_path.iterdir()):
                     dir_path.rmdir()
                     print(f"Removed empty directory: '{dir_path}'")
                     processed_dirs.add(dir_path)
                     # Mark dir revert in DB
                     self.update_action_status(batch_id_to_undo, str(dir_path), 'reverted')
                 elif dir_path.is_dir():
                      print(f"Skipped removal: Directory '{dir_path}' is not empty.")
             except OSError as e:
                 print(f"Error removing directory '{dir_path}': {e}")
                 error_count += 1 # Count as error

        # Final undo summary
        total_attempted = len(actions_to_undo)
        skipped_count = total_attempted - reverted_count - error_count - integrity_mismatch
        print("-" * 30)
        print("Undo Summary:")
        print(f"  Actions successfully reverted: {reverted_count}")
        print(f"  Actions skipped (not found/exists/integrity): {skipped_count + integrity_mismatch}")
        print(f"  Errors during revert: {error_count}")