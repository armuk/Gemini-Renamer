# rename_app/undo_manager.py
import sqlite3
import logging
import sys # For sys.stderr
import builtins # For builtins.print
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
import os # os.rename, os.utime
import fnmatch # Not used, but was in original. Can be removed if truly unused.
import hashlib
import shutil
from typing import Optional, Tuple, List, Dict, Any

# --- MODIFIED RICH IMPORTS ---
from rename_app.ui_utils import (
    ConsoleClass, TableClass, TextClass, ConfirmClass, # Added ConfirmClass
    RICH_AVAILABLE_UI as RICH_AVAILABLE, RichConsoleActual # Import RichConsoleActual for isinstance
)
# --- END MODIFIED RICH IMPORTS ---

from .exceptions import RenamerError, FileOperationError
from .config_manager import ConfigHelper # For type hinting cfg_helper

log = logging.getLogger(__name__)
# TEMP_SUFFIX_PREFIX = ".renametmp_" # Removed hardcoded constant
MTIME_TOLERANCE = 1.0
HASH_CHUNK_SIZE = 65536

# Helper to print to stderr, adapted for use within this module
def _print_stderr_message_undo(console_obj: ConsoleClass, message: Any, is_quiet: bool, is_rich_available: bool):
    if is_rich_available and isinstance(console_obj, RichConsoleActual):
        if not is_quiet:
            try:
                # Create a temporary Rich console for stderr to print styled message
                console_stderr_temp = RichConsoleActual(file=sys.stderr, width=console_obj.width) # type: ignore
                console_stderr_temp.print(message)
                return
            except Exception: # Fallback if temp console fails
                pass
        plain_message = message.plain if hasattr(message, 'plain') else str(message)
        builtins.print(plain_message, file=sys.stderr)
    else:
        console_obj.print(message, file=sys.stderr)


class UndoManager:
    def __init__(self, cfg_helper: ConfigHelper, quiet_mode: bool = False, console_instance: Optional[ConsoleClass] = None):
        self.cfg = cfg_helper
        self.db_path: Optional[Path] = None
        self.is_enabled: bool = False
        self.check_integrity: bool = False
        self.hash_check_bytes: int = 0
        self.use_full_hash: bool = False
        
        self.quiet_mode = quiet_mode
        if console_instance:
            self.console = console_instance
        else:
            self.console = ConsoleClass(quiet=self.quiet_mode)
        
        try:
            self.is_enabled = self.cfg('enable_undo', False)
            if self.is_enabled:
                self.db_path = self._resolve_db_path()
                if self.db_path:
                    self._init_db()
                else:
                    self.is_enabled = False
                    log.error("UndoManager: DB path not resolved, disabling undo.")

                if self.is_enabled:
                    self.check_integrity = self.cfg('undo_check_integrity', False)
                    self.use_full_hash = self.cfg('undo_integrity_hash_full', False)
                    try:
                        hash_bytes_cfg = self.cfg('undo_integrity_hash_bytes', 0)
                        self.hash_check_bytes = int(hash_bytes_cfg) if hash_bytes_cfg is not None else 0
                        if self.hash_check_bytes < 0:
                            log.warning("undo_integrity_hash_bytes negative. Disabling partial hash.")
                            self.hash_check_bytes = 0
                    except (ValueError, TypeError):
                        log.warning(f"Invalid 'undo_integrity_hash_bytes'. Disabling partial hash.")
                        self.hash_check_bytes = 0

                    log_msg = f"UndoManager initialized (DB: {self.db_path}, Integrity: {self.check_integrity}"
                    if self.check_integrity:
                        if self.use_full_hash: log_msg += ", Hash Check: FULL)"
                        elif self.hash_check_bytes > 0: log_msg += f", Hash Check: Partial ({self.hash_check_bytes} bytes))"
                        else: log_msg += ", Hash Check: Disabled)"
                    else: log_msg += ")"
                    log.info(log_msg)
            else:
                log.info("Undo feature disabled by configuration.")
        except Exception as e:
            self.is_enabled = False
            log.exception(f"Failed to initialize UndoManager: {e}")

    def _resolve_db_path(self) -> Optional[Path]:
        db_path_config = self.cfg('undo_db_path', None)
        path: Optional[Path] = None
        try:
            if db_path_config:
                path = Path(db_path_config).resolve()
            else:
                path = Path(__file__).parent.parent / "rename_log.db"
                path = path.resolve()
            
            if path:
                path.parent.mkdir(parents=True, exist_ok=True)
            return path
        except Exception as e:
            log.error(f"Cannot resolve or create undo database path: {e}")
            return None

    def _connect(self) -> Optional[sqlite3.Connection]:
        if not self.db_path:
            log.error("Cannot connect to undo database: path not resolved or invalid.")
            return None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            try: conn.execute("PRAGMA journal_mode=WAL;")
            except sqlite3.Error as pe: log.warning(f"Could not set PRAGMA journal_mode=WAL for undo DB ({self.db_path}): {pe}")
            try: conn.execute("PRAGMA busy_timeout=5000;")
            except sqlite3.Error as pe: log.warning(f"Could not set PRAGMA busy_timeout=5000 for undo DB ({self.db_path}): {pe}")
            return conn
        except sqlite3.Error as e:
            log.error(f"Cannot connect to undo database '{self.db_path}': {e}")
            return None

    def _init_db(self):
        if not self.is_enabled or not self.db_path: return

        conn = None
        try:
            conn = self._connect()
            if not conn:
                self.is_enabled = False
                log.error("UndoManager: Database connection failed during init. Disabling undo.")
                return

            conn.execute("""
                CREATE TABLE IF NOT EXISTS rename_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL, timestamp TEXT NOT NULL,
                    original_path TEXT NOT NULL, new_path TEXT NOT NULL, type TEXT CHECK(type IN ('file', 'dir')) NOT NULL,
                    status TEXT CHECK(status IN ('renamed', 'moved', 'trashed', 'reverted', 'created_dir', 'pending_final', 'failed_pending')) NOT NULL,
                    original_size INTEGER, original_mtime REAL, original_hash TEXT NULL, UNIQUE(batch_id, original_path)
                )""")
            try:
                cursor = conn.execute("PRAGMA table_info(rename_log)")
                columns = [row['name'] for row in cursor.fetchall()]
                if 'original_hash' not in columns:
                    conn.execute("ALTER TABLE rename_log ADD COLUMN original_hash TEXT NULL;")
                    log.info("Added 'original_hash' column to undo log table.")
            except sqlite3.Error as e_alter:
                log.error(f"Error altering table to add original_hash: {e_alter}")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_id ON rename_log(batch_id)")
            conn.commit()
        except sqlite3.Error as e:
            log.error(f"Failed to initialize undo database schema: {e}")
            self.is_enabled = False
        except Exception as e:
            log.exception(f"Unexpected error during undo database initialization: {e}")
            self.is_enabled = False
        finally:
            if conn:
                conn.close()

    def _calculate_file_hash(self, file_path: Path, full_hash: bool) -> Optional[str]:
        hasher = hashlib.sha256()
        try:
            with open(file_path, 'rb') as f:
                if full_hash:
                    log.debug(f"Calculating FULL hash for {file_path.name}")
                    while True:
                        chunk = f.read(HASH_CHUNK_SIZE)
                        if not chunk:
                            break
                        hasher.update(chunk)
                elif self.hash_check_bytes > 0:
                    log.debug(f"Calculating PARTIAL hash ({self.hash_check_bytes} bytes) for {file_path.name}")
                    chunk = f.read(self.hash_check_bytes)
                    hasher.update(chunk if chunk else b'')
                else:
                    return None
            return hasher.hexdigest()
        except FileNotFoundError:
            log.warning(f"Cannot calculate hash: File not found '{file_path}'")
            return None
        except OSError as e:
            log.warning(f"Cannot calculate hash for '{file_path}': {e}")
            return None
        except Exception as e:
            log.exception(f"Unexpected error calculating hash for '{file_path}': {e}")
            return None

    def log_action(self, batch_id: str, original_path: Path, new_path: Path, item_type: str, status: str) -> bool:
        if not self.is_enabled: return False
        
        orig_p = Path(original_path)
        original_size: Optional[int] = None
        original_mtime: Optional[float] = None
        original_hash: Optional[str] = None
        
        can_stat = item_type == 'file' and status in {'pending_final', 'renamed', 'moved', 'trashed'}

        if can_stat:
            try:
                if orig_p.is_file():
                    stat_info = orig_p.stat()
                    original_size = stat_info.st_size
                    original_mtime = stat_info.st_mtime
                    if self.use_full_hash or self.hash_check_bytes > 0:
                        original_hash = self._calculate_file_hash(orig_p, full_hash=self.use_full_hash)
            except OSError as e:
                log.warning(f"Could not stat original file for log_action '{original_path}': {e}")
            except Exception as e:
                log.exception(f"Unexpected error getting stats/hash for '{original_path}': {e}")
        
        conn = None
        try:
            conn = self._connect()
            if not conn: return False

            conn.execute(
                "INSERT INTO rename_log (batch_id, timestamp, original_path, new_path, type, status, original_size, original_mtime, original_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (batch_id, datetime.now(timezone.utc).isoformat(), str(original_path), str(new_path), item_type, status, original_size, original_mtime, original_hash)
            )
            conn.commit()
            log.debug(f"Logged action for '{original_path}' (batch '{batch_id}') status '{status}'.")
            return True
        except sqlite3.IntegrityError as e:
            log.warning(f"Duplicate entry in rename log for '{original_path}' ('{batch_id}'): {e}.")
            return False
        except sqlite3.Error as e:
            log.error(f"DB error log_action for '{original_path}' ('{batch_id}'): {e}")
            return False
        except Exception as e:
            log.exception(f"Unexpected error logging undo for '{original_path}' ('{batch_id}'): {e}")
            return False
        finally:
            if conn:
                conn.close()

    def update_action_status(self, batch_id: str, original_path: str, new_status: str, conn: Optional[sqlite3.Connection] = None) -> bool:
        if not self.is_enabled: return False
        log.debug(f"Updating status to '{new_status}' for '{original_path}' in batch '{batch_id}'")
        
        manage_connection = conn is None
        _conn = None
        try:
            _conn = self._connect() if manage_connection else conn
            if not _conn: return False

            cursor = _conn.cursor()
            cursor.execute("UPDATE rename_log SET status = ? WHERE batch_id = ? AND original_path = ? AND status != 'reverted'", (new_status, batch_id, str(original_path)))
            updated_count = cursor.rowcount
            if manage_connection:
                _conn.commit()
            
            if updated_count > 0:
                log.debug(f"Successfully updated status for '{original_path}'")
                return True
            else:
                log.warning(f"No matching record or status 'reverted' for update: '{original_path}' ('{batch_id}')")
                return False
        except sqlite3.Error as e:
            log.error(f"Failed updating undo status for '{original_path}' ('{batch_id}') to '{new_status}': {e}")
            return False
        except Exception as e:
            log.exception(f"Unexpected error updating undo status for '{original_path}' ('{batch_id}') to '{new_status}': {e}")
            return False
        finally:
            if manage_connection and _conn:
                _conn.close()

    def prune_old_batches(self):
        if not self.is_enabled: return
        
        expire_days_cfg = self.cfg('undo_expire_days', 30)
        try:
            expire_days = int(expire_days_cfg if expire_days_cfg is not None else 30)
            if expire_days < 0:
                log.info("Undo expiration days set to -1 (forever). Skipping prune.")
                return
            if expire_days == 0:
                log.info("Undo expiration days set to 0 (session only). Pruning all except current session if identifiable.")
                pass
        except (ValueError, TypeError):
            log.warning(f"Invalid 'undo_expire_days' ('{expire_days_cfg}'). Defaulting to 30.")
            expire_days = 30

        cutoff = datetime.now(timezone.utc) - timedelta(days=expire_days)
        cutoff_iso = cutoff.isoformat()
        log.debug(f"Pruning undo records older than {cutoff_iso} ({expire_days} days)")
        
        deleted_rows = 0
        conn = None
        try:
            conn = self._connect()
            if not conn: return

            cur = conn.execute("DELETE FROM rename_log WHERE timestamp < ?", (cutoff_iso,))
            deleted_rows = cur.rowcount if cur else 0
            conn.commit()
            if deleted_rows > 0:
                log.info(f"Pruned {deleted_rows} old undo log records.")
            else:
                log.debug("No expired entries found to prune.")
        except sqlite3.Error as e:
            log.error(f"Error during undo log pruning: {e}")
        except Exception as e:
            log.exception(f"Unexpected error during undo log pruning: {e}")
        finally:
            if conn:
                conn.close()

    def _find_temp_file(self, final_dest_path: Path) -> Optional[Path]:
        # Fetch temp_suffix_prefix from config
        temp_suffix_prefix_val = self.cfg('temp_file_suffix_prefix', ".renametmp_")
        temp_pattern = f"{final_dest_path.stem}{temp_suffix_prefix_val}*{final_dest_path.suffix}"
        try:
            matches = list(final_dest_path.parent.glob(temp_pattern))
            if len(matches) == 1:
                log.debug(f"Found temp file for {final_dest_path}: {matches[0]}")
                return matches[0]
            elif len(matches) > 1:
                log.warning(f"Multiple temp files for {final_dest_path}: {matches}. Cannot reliably choose.")
                return None
            else:
                log.debug(f"No temp file for pattern '{temp_pattern}' in {final_dest_path.parent}")
                return None
        except OSError as e:
            log.error(f"Error searching for temp file for {final_dest_path}: {e}")
            return None

    def _check_file_integrity(self, current_path: Path, logged_size: Optional[int], logged_mtime: Optional[float], logged_hash: Optional[str]) -> Tuple[bool, str]:
        if not self.check_integrity:
            return True, "Skipped (Check Disabled)"
        
        size_ok, mtime_ok, hash_ok = True, True, True
        reasons: List[str] = []
        
        has_size_log = logged_size is not None
        has_mtime_log = logged_mtime is not None
        has_hash_log = logged_hash is not None

        if not has_size_log and not has_mtime_log and not has_hash_log:
            return True, "Skipped (no stats logged)"

        try:
            current_stat = current_path.stat()
            current_size = current_stat.st_size
            current_mtime = current_stat.st_mtime
        except OSError as e:
            return False, f"FAIL (Cannot stat: {e})"
        except Exception as e: 
            log.exception(f"Unexpected error during integrity check stat for {current_path}")
            return False, "FAIL (Check Error)"

        if has_size_log and current_size != logged_size:
            size_ok = False
            reasons.append(f"Size ({current_size} != {logged_size})")
        
        if has_mtime_log and abs(current_mtime - logged_mtime) >= MTIME_TOLERANCE:
            mtime_ok = False
            reasons.append(f"MTime ({current_mtime:.2f} !~= {logged_mtime:.2f})")

        if has_hash_log:
            current_hash_val = self._calculate_file_hash(current_path, full_hash=self.use_full_hash)
            
            if current_hash_val is None and (self.use_full_hash or self.hash_check_bytes > 0):
                hash_ok = False
                reasons.append("Hash (Cannot calc current)")
            elif current_hash_val is not None and current_hash_val != logged_hash:
                hash_ok = False
                reasons.append(f"Hash ({current_hash_val[:8]}... != {logged_hash[:8]}...)")
            elif current_hash_val is None and not (self.use_full_hash or self.hash_check_bytes > 0):
                reasons.append("Hash (Check Disabled Now)")

        passed = size_ok and mtime_ok and hash_ok
        return (True, "OK") if passed else (False, f"FAIL ({', '.join(reasons)})")

    def list_batches(self) -> List[Dict[str, Any]]:
        if not self.is_enabled or not self.db_path or not self.db_path.exists():
            log.error("Cannot list batches: Undo disabled or DB not found.")
            return []
        
        query = """
            SELECT batch_id, MIN(timestamp) as first_timestamp, MAX(timestamp) as last_timestamp, COUNT(*) as action_count
            FROM rename_log
            GROUP BY batch_id
            ORDER BY last_timestamp DESC
        """
        batches: List[Dict[str, Any]] = []
        conn = None
        try:
            conn = self._connect()
            if not conn: return []
            batches = [dict(row) for row in conn.execute(query).fetchall()]
            log.info(f"Found {len(batches)} batches in undo log.")
        except sqlite3.Error as e:
            log.error(f"Database error listing undo batches: {e}")
        except Exception as e:
            log.exception(f"Unexpected error listing undo batches: {e}")
        finally:
            if conn:
                conn.close()
        return batches

    def _fetch_undo_actions_from_db(self, batch_id: str, conn: sqlite3.Connection) -> List[sqlite3.Row]:
        try:
            cursor = conn.execute(
                "SELECT * FROM rename_log WHERE batch_id = ? AND status NOT IN ('reverted', 'trashed', 'failed_pending') ORDER BY id DESC",
                (batch_id,)
            )
            return cursor.fetchall()
        except sqlite3.Error as e:
            log.error(f"Error fetching undo actions for batch '{batch_id}': {e}")
            raise

    def _display_undo_preview_table(self, actions: List[sqlite3.Row], batch_id: str):
        # self.console is already quiet-aware from __init__
        self.console.print("Operations to be reverted (new -> original):")
        preview_table = TableClass(title=f"Undo Plan for Batch: {batch_id}", show_header=True, header_style="bold magenta")
        preview_table.add_column("ID", style="dim", width=5, justify="right")
        preview_table.add_column("Status", style="yellow", width=10)
        preview_table.add_column("Type", width=4)
        preview_table.add_column("Current Path / Item", style="cyan", no_wrap=True, min_width=30)
        preview_table.add_column("->", justify="center", width=2)
        preview_table.add_column("Target Path / Action", style="green", no_wrap=True, min_width=30)
        preview_table.add_column("Integrity", width=25)

        for action in actions:
            orig_p = Path(action['original_path'])
            new_p = Path(action['new_path'])
            status = action['status']
            item_type = action['type']
            action_id = action['id']
            current_path_str: str = "?"
            target_path_str: str = "?"
            integrity_msg: str = "N/A"

            if status in ('renamed', 'moved'):
                current_path_str = str(new_p)
                target_path_str = str(orig_p)
                if self.check_integrity and item_type == 'file':
                    _, integrity_msg = self._check_file_integrity(new_p, action['original_size'], action['original_mtime'], action['original_hash'])
            elif status == 'pending_final':
                temp_p = self._find_temp_file(new_p)
                current_path_str = str(temp_p) if temp_p else f"[red]TEMP NOT FOUND for {new_p.name}[/red]"
                target_path_str = str(orig_p)
                integrity_msg = "N/A (Temp File)"
            elif status == 'created_dir':
                current_path_str = str(orig_p)
                target_path_str = "[red]Remove Directory[/red]"
                integrity_msg = "N/A (Directory)"
            else:
                current_path_str = f"[red]Unknown Status '{status}'[/red]"
                integrity_msg = "[red]Unknown[/red]"

            preview_table.add_row(str(action_id), status.capitalize(), item_type.capitalize(), current_path_str, "->", target_path_str, integrity_msg)
        self.console.print(preview_table)

    def _confirm_undo_with_user(self) -> bool:
        if self.quiet_mode:
            log.info("Quiet mode: Skipping undo confirmation. Undo will NOT proceed by default.")
            return False
        try:
            # Use ConfirmClass from ui_utils for consistency
            if ConfirmClass.ask("Proceed with UNDO operation?", default=False):
                return True
            self.console.print(TextClass("[yellow]Undo operation cancelled by user.[/yellow]", style="yellow"))
            return False
        except (EOFError, KeyboardInterrupt) as e:
            log.warning(f"Undo confirmation aborted by user ({type(e).__name__}).")
            # Use _print_stderr_message_undo helper for this module
            _print_stderr_message_undo(self.console, TextClass("\n[yellow]Undo operation cancelled by user.[/yellow]", style="yellow"), self.quiet_mode, RICH_AVAILABLE)
            return False
        except Exception as e: # Other prompt errors
            log.error(f"Error reading confirmation input for undo: {e}")
            _print_stderr_message_undo(self.console, TextClass("[bold red]Undo operation cancelled (Error reading input).[/bold red]", style="bold red"), self.quiet_mode, RICH_AVAILABLE)
            return False

    def _revert_single_file_undo_action(self, action_log: sqlite3.Row, batch_id: str, conn: sqlite3.Connection) -> Tuple[bool, bool, str]:
        orig_p = Path(action_log['original_path'])
        new_p = Path(action_log['new_path'])
        status = action_log['status']
        item_type = action_log['type']
        action_id = action_log['id']
        
        log_prefix = f"[Undo ID {action_id}] "
        console_prefix = f"  {log_prefix}"
        
        current_src: Optional[Path] = None
        if status in ('renamed', 'moved'):
            current_src = new_p
        elif status == 'pending_final':
            current_src = self._find_temp_file(new_p)
        
        target_dest: Path = orig_p

        if current_src is None:
            if status == 'pending_final':
                msg = f"Skipped revert: Cannot find temp file for '{new_p.name}'"
                log.warning(f"{log_prefix}{msg}")
                return False, False, f"{console_prefix}[yellow]{msg}[/yellow]"
            else:
                msg = f"Internal error: Could not determine source for status '{status}' for '{new_p.name}'."
                log.error(f"{log_prefix}{msg}")
                return False, False, f"{console_prefix}[red]{msg}[/red]"

        action_desc = f"Reverting '{current_src.name}' to '{target_dest.name}'"
        log.debug(f"{log_prefix}Processing revert: {action_desc}")

        if self.check_integrity and item_type == 'file' and status != 'pending_final':
            integrity_passed, integrity_msg_raw = self._check_file_integrity(current_src, action_log['original_size'], action_log['original_mtime'], action_log['original_hash'])
            if not self.quiet_mode: # Only print integrity to console if not quiet
                self.console.print(f"{console_prefix}Integrity check for '[cyan]{current_src.name}[/]': {integrity_msg_raw}")
            log.info(f"{log_prefix}Integrity check for '{current_src}': {integrity_msg_raw}")
            if not integrity_passed:
                msg = f"Skipping revert for '{current_src.name}' due to integrity failure."
                log.warning(f"{log_prefix}{msg}")
                return False, False, f"{console_prefix}[yellow]{msg}[/yellow]"

        if not current_src.exists():
            msg = f"Skipped revert: Source file '{current_src}' does not exist."
            log.warning(f"{log_prefix}{msg}")
            return False, False, f"{console_prefix}[yellow]{msg}[/yellow]"
        
        if target_dest.exists():
            msg = f"Skipped revert: Target '{target_dest}' already exists."
            log.warning(f"{log_prefix}{msg}")
            return False, False, f"{console_prefix}[yellow]{msg}[/yellow]"

        try:
            log.info(f"{log_prefix}Attempting rename/move: '{current_src}' -> '{target_dest}'")
            target_dest.parent.mkdir(parents=True, exist_ok=True)
            
            try:
                os.rename(str(current_src), str(target_dest))
            except OSError:
                log.debug(f"{log_prefix}os.rename failed, trying shutil.move for '{current_src.name}'.")
                shutil.move(str(current_src), str(target_dest))
            
            db_updated = self.update_action_status(batch_id, str(orig_p), 'reverted', conn=conn)
            if not db_updated:
                log.error(f"{log_prefix}FS op OK, but FAILED DB update to 'reverted' for '{orig_p.name}'")
            msg = f"Success: '{current_src.name}' reverted to '{target_dest.name}'"
            log.info(f"{log_prefix}{msg}")
            return True, db_updated, f"{console_prefix}[green]{msg}[/green]"
        except OSError as e:
            msg = f"Error reverting '{current_src.name}': {e}"
            log.error(f"{log_prefix}{msg}")
            return False, False, f"{console_prefix}[bold red]{msg}[/bold red]"
        except Exception as e:
            msg = f"Unexpected error reverting '{current_src.name}': {e}"
            log.exception(f"{log_prefix}{msg}")
            return False, False, f"{console_prefix}[bold red]{msg}[/bold red]"

    def _revert_created_dir_undo_action(self, action_log: sqlite3.Row, batch_id: str, conn: sqlite3.Connection, removed_dirs_list: List[Path]) -> Tuple[bool, bool, str]:
        dir_to_remove = Path(action_log['original_path'])
        action_id = action_log['id']
        
        log_prefix = f"[Undo ID {action_id}] "
        log.debug(f"{log_prefix}Reverting created_dir: Attempt to remove '{dir_to_remove}'")
        console_prefix = f"  {log_prefix}"

        if not dir_to_remove.exists():
            msg = f"Skipped removal: Dir '{dir_to_remove}' not found."
            log.debug(f"{log_prefix}{msg}")
            db_updated = self.update_action_status(batch_id, str(dir_to_remove), 'reverted', conn=conn)
            return True, db_updated, f"{console_prefix}[dim]{msg}[/dim]"
        
        if not dir_to_remove.is_dir():
            msg = f"Skipped removal: '{dir_to_remove}' is not a directory."
            log.warning(f"{log_prefix}{msg}")
            return False, False, f"{console_prefix}[yellow]{msg}[/yellow]"
        
        try:
            if any(dir_to_remove.iterdir()):
                msg = f"Skipped removal: Dir '{dir_to_remove}' not empty."
                log.warning(f"{log_prefix}{msg}")
                return False, False, f"{console_prefix}[yellow]{msg}[/yellow]"
        except OSError as e:
            msg = f"Error checking dir '{dir_to_remove}' emptiness: {e}"
            log.error(f"{log_prefix}{msg}")
            return False, False, f"{console_prefix}[bold red]{msg}[/bold red]"

        try:
            log.info(f"{log_prefix}Attempting rmdir: '{dir_to_remove}'")
            dir_to_remove.rmdir()
            removed_dirs_list.append(dir_to_remove)
            
            db_updated = self.update_action_status(batch_id, str(dir_to_remove), 'reverted', conn=conn)
            if not db_updated:
                log.error(f"{log_prefix}Dir removal OK, but FAILED DB update for '{dir_to_remove}'")
            msg = f"Success: Dir '{dir_to_remove}' removed."
            log.info(f"{log_prefix}{msg}")
            return True, db_updated, f"{console_prefix}[green]{msg}[/green]"
        except OSError as e:
            msg = f"Error removing dir '{dir_to_remove}': {e}"
            log.error(f"{log_prefix}{msg}")
            return False, False, f"{console_prefix}[bold red]{msg}[/bold red]"
        except Exception as e:
            msg = f"Unexpected error removing dir '{dir_to_remove}': {e}"
            log.exception(f"{log_prefix}{msg}")
            return False, False, f"{console_prefix}[bold red]{msg}[/bold red]"

    def _cleanup_empty_parent_dirs_after_undo(self, removed_dirs: List[Path]):
        if not removed_dirs: return
        
        self.console.print("--- Attempting to clean up empty parent directories ---")
        sorted_dirs = sorted(removed_dirs, key=lambda p: len(p.parts), reverse=True)
        processed_parents = set()

        for d_path in sorted_dirs:
            parent = d_path.parent
            while parent != parent.parent:
                if parent in processed_parents or not parent.is_dir():
                    break
                
                try:
                    if not any(parent.iterdir()):
                        log.info(f"[Undo Cleanup] Attempting rmdir on empty parent: '{parent}'")
                        try:
                            parent.rmdir()
                            self.console.print(f"  [green]Success:[/green] Removed empty parent '[cyan]{parent}[/]'")
                        except OSError as e_p:
                            self.console.print(f"  [dim]Info: Could not remove parent '[cyan]{parent}[/]': {e_p}[/dim]")
                            log.warning(f"[Undo Cleanup] Failed parent rmdir '{parent}': {e_p}")
                            break
                    else:
                        log.debug(f"[Undo Cleanup] Parent '{parent}' not empty.")
                        break
                except OSError as e_chk:
                    log.warning(f"[Undo Cleanup] Error checking or removing parent '{parent}': {e_chk}")
                    break
                
                processed_parents.add(parent)
                parent = parent.parent

    def perform_undo(self, batch_id: str, dry_run: bool = False):
        if not self.is_enabled:
            _print_stderr_message_undo(self.console, TextClass("[bold red]Error: Undo disabled.[/bold red]", style="bold red"), self.quiet_mode, RICH_AVAILABLE)
            return False
        if not self.db_path or not self.db_path.exists():
            _print_stderr_message_undo(self.console, TextClass(f"[bold red]Error: Undo DB not found at {self.db_path}[/cyan]", style="bold red"), self.quiet_mode, RICH_AVAILABLE)
            return False

        action_word = "DRY RUN UNDO" if dry_run else "UNDO"
        self.console.print(f"--- Starting {action_word} for batch '[cyan]{batch_id}[/cyan]' ---")
        
        actions_to_revert: List[sqlite3.Row] = []
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self._connect()
            if not conn:
                _print_stderr_message_undo(self.console, TextClass("[bold red]Error: Could not connect to undo database.[/bold red]", style="bold red"), self.quiet_mode, RICH_AVAILABLE)
                return False
            actions_to_revert = self._fetch_undo_actions_from_db(batch_id, conn)
        except (sqlite3.Error, RenamerError) as e:
            _print_stderr_message_undo(self.console, TextClass(f"[bold red]Error accessing undo database:[/bold red] {e}", style="bold red"), self.quiet_mode, RICH_AVAILABLE)
            if conn: conn.close()
            return False
        
        if not actions_to_revert:
            self.console.print(f"No revertible actions found for batch '[cyan]{batch_id}[/cyan]'.")
            if conn: conn.close()
            return False

        self._display_undo_preview_table(actions_to_revert, batch_id)
        if dry_run:
            self.console.print(f"\n--- {action_word} Preview Complete. No changes made. ---")
            if conn: conn.close()
            return True
        
        if not self._confirm_undo_with_user():
            if conn: conn.close()
            return False

        self.console.print("--- Performing Revert ---")
        s_count, db_err, fs_err, sk_count = 0, 0, 0, 0
        crit_fs_err_flag = False
        removed_dirs: List[Path] = []

        try:
            for item in actions_to_revert:
                fs_ok, db_ok, msg = False, False, ""
                status = item['status']
                
                if status in ('renamed', 'moved', 'pending_final'):
                    fs_ok, db_ok, msg = self._revert_single_file_undo_action(item, batch_id, conn)
                elif status == 'created_dir':
                    fs_ok, db_ok, msg = self._revert_created_dir_undo_action(item, batch_id, conn, removed_dirs)
                else:
                    action_id_for_msg = item['id']
                    console_prefix_unknown = f"  [Undo ID {action_id_for_msg}] "
                    msg = f"{console_prefix_unknown}[yellow]Skipping action ID {action_id_for_msg} with unexpected status '{status}'[/yellow]"
                    log.warning(f"[Undo ID {action_id_for_msg}] Skipping unknown status '{status}'")
                    sk_count += 1
                
                self.console.print(msg)
                
                if fs_ok and db_ok: s_count += 1
                elif fs_ok and not db_ok: db_err += 1
                elif not fs_ok:
                    if "error" in msg.lower() or "fail" in msg.lower() or "[red]" in msg.lower():
                        fs_err += 1
                        crit_fs_err_flag = True
                    else:
                        sk_count +=1
            
            if not crit_fs_err_flag and db_err == 0:
                if conn: conn.commit()
                log.info(f"Undo DB commit for batch '{batch_id}'.")
                self._cleanup_empty_parent_dirs_after_undo(removed_dirs)
                overall_ok = True
            else:
                if conn: conn.rollback()
                log.error(f"Undo DB rollback for batch '{batch_id}'. CritFS: {crit_fs_err_flag}, DBFails: {db_err}")
                _print_stderr_message_undo(self.console, TextClass("[bold red]WARNING: Problems occurred. DB changes rolled back.[/bold red]", style="bold red"), self.quiet_mode, RICH_AVAILABLE)
                if crit_fs_err_flag: _print_stderr_message_undo(self.console, TextClass("[bold red]Critical FS errors. Files may be inconsistent.[/bold red]", style="bold red"), self.quiet_mode, RICH_AVAILABLE)
                if db_err > 0: _print_stderr_message_undo(self.console, TextClass(f"[bold red]{db_err} DB updates failed for successful FS ops. Log may be inconsistent.[/bold red]", style="bold red"), self.quiet_mode, RICH_AVAILABLE)
                overall_ok = False
        except Exception as e_outer:
            log.exception(f"Critical error during undo transaction for '{batch_id}': {e_outer}")
            _print_stderr_message_undo(self.console, TextClass(f"[bold red]CRITICAL UNEXPECTED ERROR during undo: {e_outer}[/bold red]", style="bold red"), self.quiet_mode, RICH_AVAILABLE)
            if conn:
                try: conn.rollback()
                except Exception as e_rb_final: log.error(f"Error during final rollback attempt: {e_rb_final}")
            overall_ok = False
        finally:
            if conn: conn.close()
            log.debug("Undo DB connection closed.")

        self.console.print(f"--- {action_word} Complete for batch '[cyan]{batch_id}[/cyan]' ---")
        summary_parts = []
        if s_count > 0: summary_parts.append(f"[green]{s_count} succeeded[/]")
        if sk_count > 0: summary_parts.append(f"[yellow]{sk_count} skipped[/]")
        if fs_err > 0: summary_parts.append(f"[red]{fs_err} FS errors[/]")
        if db_err > 0: summary_parts.append(f"[red]{db_err} DB errors[/]")
        
        summary = ", ".join(summary_parts) if summary_parts else "No actions tallied."
        self.console.print(f"Summary: {summary}.")
        if not overall_ok:
             _print_stderr_message_undo(self.console, TextClass("[bold red]Undo operation finished with errors. Please check logs.[/bold red]", style="bold red"), self.quiet_mode, RICH_AVAILABLE)
        return overall_ok