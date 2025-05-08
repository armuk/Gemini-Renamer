# --- START OF FILE undo_manager.py ---

import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
import time
import os
import fnmatch
import hashlib
import shutil
from typing import Optional, Tuple, List, Dict, Any

# --- RICH IMPORTS ---
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
        def input(self, *args, **kwargs) -> str: return builtins.input(*args, **kwargs) # Add input fallback
    class Table:
        def __init__(self, *args, **kwargs): pass
        def add_column(self, *args, **kwargs): pass
        def add_row(self, *args, **kwargs): pass
    class Text:
        def __init__(self, text="", style=""): self.text = text; self.style = style
        def __str__(self): return self.text
# --- END RICH IMPORTS ---

from .exceptions import RenamerError, FileOperationError

log = logging.getLogger(__name__)
TEMP_SUFFIX_PREFIX = ".renametmp_"
MTIME_TOLERANCE = 1.0

class UndoManager:
    # --- (__init__, _resolve_db_path, _connect, _init_db, _calculate_partial_hash unchanged) ---
    def __init__(self, cfg_helper):
        self.cfg = cfg_helper; self.db_path = None; self.is_enabled = False; self.check_integrity = False; self.hash_check_bytes = 0
        try:
            self.is_enabled = self.cfg('enable_undo', False)
            if self.is_enabled:
                self.db_path = self._resolve_db_path(); self._init_db()
                self.check_integrity = self.cfg('undo_check_integrity', False)
                try:
                    hash_bytes_cfg = self.cfg('undo_integrity_hash_bytes', 0); self.hash_check_bytes = int(hash_bytes_cfg) if hash_bytes_cfg else 0
                    if self.hash_check_bytes < 0: log.warning("undo_integrity_hash_bytes cannot be negative. Disabling hash check."); self.hash_check_bytes = 0
                except (ValueError, TypeError): log.warning(f"Invalid 'undo_integrity_hash_bytes'. Disabling hash check."); self.hash_check_bytes = 0
                log_msg = f"UndoManager initialized (DB: {self.db_path}, Integrity: {self.check_integrity}"
                if self.check_integrity and self.hash_check_bytes > 0: log_msg += f", Hash Check: {self.hash_check_bytes} bytes)"
                else: log_msg += ")"
                log.info(log_msg)
            else: log.info("Undo feature disabled by configuration.")
        except Exception as e: self.is_enabled = False; log.exception(f"Failed to initialize UndoManager: {e}")
    def _resolve_db_path(self):
        db_path_config = self.cfg('undo_db_path', None)
        try:
            if db_path_config: path = Path(db_path_config).resolve()
            else: path = Path(__file__).parent.parent / "rename_log.db"; path = path.resolve()
            path.parent.mkdir(parents=True, exist_ok=True); return path
        except Exception as e: raise RenamerError(f"Cannot resolve undo database path: {e}") from e
    def _connect(self):
        if not self.db_path: raise RenamerError("Cannot connect to undo database: path not resolved.")
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0); conn.row_factory = sqlite3.Row
            try: conn.execute("PRAGMA journal_mode=WAL;")
            except sqlite3.Error as pe: log.warning(f"Could not set PRAGMA journal_mode=WAL for undo DB ({self.db_path}): {pe}")
            try: conn.execute("PRAGMA busy_timeout=5000;")
            except sqlite3.Error as pe: log.warning(f"Could not set PRAGMA busy_timeout=5000 for undo DB ({self.db_path}): {pe}")
            return conn
        except sqlite3.Error as e: raise RenamerError(f"Cannot connect to undo database '{self.db_path}': {e}") from e
    def _init_db(self):
        try:
            with self._connect() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS rename_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL, timestamp TEXT NOT NULL,
                        original_path TEXT NOT NULL, new_path TEXT NOT NULL, type TEXT CHECK(type IN ('file', 'dir')) NOT NULL,
                        status TEXT CHECK(status IN ('renamed', 'moved', 'trashed', 'reverted', 'created_dir', 'pending_final', 'failed_pending')) NOT NULL,
                        original_size INTEGER, original_mtime REAL, original_hash TEXT NULL, UNIQUE(batch_id, original_path)
                    )""")
                try:
                    cursor = conn.execute("PRAGMA table_info(rename_log)"); columns = [row['name'] for row in cursor.fetchall()]
                    if 'original_hash' not in columns: conn.execute("ALTER TABLE rename_log ADD COLUMN original_hash TEXT NULL;"); log.info("Added 'original_hash' column to undo log table.")
                except sqlite3.Error as e_alter: log.error(f"Error altering table to add original_hash: {e_alter}")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_id ON rename_log(batch_id)"); conn.commit()
        except sqlite3.Error as e: raise RenamerError(f"Failed to initialize undo database schema: {e}") from e
        except RenamerError as e: raise RenamerError(f"Failed to connect during undo database initialization: {e}") from e
    def _calculate_partial_hash(self, file_path: Path, num_bytes: int) -> Optional[str]:
        if num_bytes <= 0: return None
        try:
            hasher = hashlib.sha256();
            with open(file_path, 'rb') as f: chunk = f.read(num_bytes); hasher.update(chunk if chunk else b'')
            return hasher.hexdigest()
        except FileNotFoundError: log.warning(f"Cannot calculate hash: File not found '{file_path}'"); return None
        except OSError as e: log.warning(f"Cannot calculate hash for '{file_path}': {e}"); return None
        except Exception as e: log.exception(f"Unexpected error calculating hash for '{file_path}': {e}"); return None

    # --- (log_action, update_action_status, prune_old_batches, _find_temp_file, _check_file_integrity, list_batches unchanged) ---
    def log_action(self, batch_id, original_path, new_path, item_type, status):
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
                cursor = conn.execute(
                    "INSERT INTO rename_log (batch_id, timestamp, original_path, new_path, type, status, original_size, original_mtime, original_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (batch_id, datetime.now(timezone.utc).isoformat(), str(original_path), str(new_path), item_type, status, original_size, original_mtime, original_hash)
                )
                conn.commit(); log.debug(f"Logged action for '{original_path}' (batch '{batch_id}') with status '{status}'."); return True
        except sqlite3.IntegrityError as e:
             # Enhanced log message for UNIQUE constraint violation
             log.warning(f"Duplicate entry prevented in rename log for '{original_path}' (batch '{batch_id}'): {e}."); return False
        except sqlite3.Error as e: log.error(f"Database error during log_action for '{original_path}' (batch '{batch_id}'): {e}"); return False
        except Exception as e: log.exception(f"Unexpected error logging undo action for '{original_path}' (batch '{batch_id}'): {e}"); return False

    def update_action_status(self, batch_id, original_path, new_status, conn: Optional[sqlite3.Connection] = None):
        if not self.is_enabled: return False
        log.debug(f"Updating status to '{new_status}' for '{original_path}' in batch '{batch_id}'")
        manage_connection = conn is None; _conn = None
        try:
            _conn = self._connect() if manage_connection else conn
            # Use a dedicated cursor for thread safety if manager becomes shared (though likely not an issue here)
            cursor = _conn.cursor()
            # Update based on original path and batch_id, EXCLUDING already reverted actions
            cursor.execute("UPDATE rename_log SET status = ? WHERE batch_id = ? AND original_path = ? AND status != 'reverted'", (new_status, batch_id, str(original_path)))
            updated_count = cursor.rowcount
            if manage_connection: _conn.commit() # Commit only if managing the connection
            if updated_count > 0: log.debug(f"Successfully updated status for '{original_path}'"); return True
            else: log.warning(f"No matching record found or status already 'reverted' for update: '{original_path}' (batch '{batch_id}')"); return False
        except sqlite3.Error as e: log.error(f"Failed updating undo status for '{original_path}' (batch '{batch_id}') to '{new_status}': {e}"); return False
        except Exception as e: log.exception(f"Unexpected error updating undo status for '{original_path}' (batch '{batch_id}') to '{new_status}': {e}"); return False
        finally:
            # Ensure connection is closed only if it was opened by this method
            if manage_connection and _conn: _conn.close()

    def prune_old_batches(self):
        if not self.is_enabled: return
        expire_days_cfg = self.cfg('undo_expire_days', 30)
        try:
            expire_days = int(expire_days_cfg)
            if expire_days < 0: log.warning("Undo expiration days cannot be negative. Skipping prune."); return
        except (ValueError, TypeError): log.warning(f"Invalid 'undo_expire_days' config value ('{expire_days_cfg}'). Using default 30."); expire_days = 30
        if expire_days == 0: cutoff = datetime.now(timezone.utc) + timedelta(seconds=1) # Ensure even just-logged items are included
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
        temp_pattern = f"{final_dest_path.stem}{TEMP_SUFFIX_PREFIX}*{final_dest_path.suffix}"
        try:
            matches = list(final_dest_path.parent.glob(temp_pattern))
            if len(matches) == 1: log.debug(f"Found temp file for {final_dest_path}: {matches[0]}"); return matches[0]
            elif len(matches) > 1: log.warning(f"Multiple temp files found for {final_dest_path}: {matches}. Cannot determine correct one."); return None
            else: log.debug(f"No temp file found matching pattern '{temp_pattern}' in {final_dest_path.parent}"); return None
        except OSError as e: log.error(f"Error searching for temp file for {final_dest_path}: {e}"); return None

    def _check_file_integrity(self, current_path: Path, logged_size: Optional[int], logged_mtime: Optional[float], logged_hash: Optional[str]) -> Tuple[bool, str]:
        if not self.check_integrity: return True, "Skipped (Check Disabled)"
        size_ok, mtime_ok, hash_ok = True, True, True; reasons = []
        has_size = logged_size is not None; has_mtime = logged_mtime is not None
        has_hash = logged_hash is not None and self.hash_check_bytes > 0
        # Require at least one logged stat to perform check
        if not has_size and not has_mtime and not has_hash: return True, "Skipped (no stats logged)"
        try: current_stat = current_path.stat(); current_size = current_stat.st_size; current_mtime = current_stat.st_mtime
        except OSError as e: return False, f"FAIL (Cannot stat: {e})"
        except Exception as e: log.exception(f"Unexpected error during integrity check stat for {current_path}"); return False, "FAIL (Check Error)"
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

    def list_batches(self) -> List[Dict[str, Any]]:
        if not self.is_enabled or not self.db_path or not self.db_path.exists():
            log.error("Cannot list batches: Undo disabled or DB not found.")
            return []
        query = """ SELECT batch_id, MIN(timestamp) as first_timestamp, MAX(timestamp) as last_timestamp, COUNT(*) as action_count
                    FROM rename_log GROUP BY batch_id ORDER BY last_timestamp DESC """
        batches = []
        try:
            with self._connect() as conn:
                cursor = conn.execute(query)
                batches = [dict(row) for row in cursor.fetchall()]
            log.info(f"Found {len(batches)} batches in undo log.")
        except sqlite3.Error as e: log.error(f"Database error listing undo batches: {e}")
        except Exception as e: log.exception(f"Unexpected error listing undo batches: {e}")
        return batches

    # --- MODIFIED: perform_undo Method ---
    def perform_undo(self, batch_id: str, dry_run: bool = False):
        if not self.is_enabled:
            # Use Console for user output if rich is available
            console = Console()
            console.print("[bold red]Error:[/bold red] Undo logging was not enabled or manager failed initialization.")
            return False
        if not self.db_path or not self.db_path.exists():
            console = Console()
            console.print(f"[bold red]Error:[/bold red] Undo database not found at [cyan]{self.db_path}[/cyan]")
            return False

        action_word = "DRY RUN UNDO" if dry_run else "UNDO"
        console = Console() # Use rich console for output
        console.print(f"--- Starting {action_word} for batch '[cyan]{batch_id}[/cyan]' ---")
        actions = []
        conn = None

        # 1. Fetch Actions (unchanged)
        try:
            conn = self._connect()
            cursor = conn.execute(
                "SELECT * FROM rename_log WHERE batch_id = ? AND status NOT IN ('reverted', 'trashed', 'failed_pending') ORDER BY id DESC",
                 (batch_id,)
            )
            actions = cursor.fetchall()
        except sqlite3.Error as e:
            log.error(f"Error accessing undo database trying to fetch actions for batch '{batch_id}': {e}")
            console.print(f"[bold red]Error accessing undo database:[/bold red] {e}")
            if conn: conn.close()
            return False
        except RenamerError as e:
            log.error(f"Error connecting to undo database for batch '{batch_id}': {e}")
            console.print(f"[bold red]Error connecting to undo database:[/bold red] {e}")
            return False

        if not actions:
            console.print(f"No revertible actions found for batch '[cyan]{batch_id}[/cyan]'.")
            if conn: conn.close()
            return False

        # --- 2. Preview Actions using Rich Table ---
        console.print("Operations to be reverted (new -> original):")
        preview_table = Table(title=f"Undo Plan for Batch: {batch_id}", show_header=True, header_style="bold magenta")
        preview_table.add_column("ID", style="dim", width=5, justify="right")
        preview_table.add_column("Status", style="yellow", width=10)
        preview_table.add_column("Type", width=4)
        preview_table.add_column("Current Path / Item", style="cyan", no_wrap=True, min_width=30)
        preview_table.add_column("->", justify="center", width=2)
        preview_table.add_column("Target Path / Action", style="green", no_wrap=True, min_width=30)
        preview_table.add_column("Integrity", width=25) # For integrity check results

        for action in actions:
            orig_p = Path(action['original_path'])
            new_p = Path(action['new_path'])
            status = action['status']
            item_type = action['type']
            action_id = action['id']
            current_path_str = "?"
            target_path_str = "?"
            action_desc = "?"
            integrity_msg = "N/A" # Default for dirs or when check disabled/fails early

            if status in ('renamed', 'moved'):
                current_path_str = str(new_p)
                target_path_str = str(orig_p)
                action_desc = f"Rename/Move Back ({item_type})"
                if self.check_integrity and item_type == 'file':
                    # Perform check *during* preview for dry run and live run
                    _, integrity_msg = self._check_file_integrity(new_p, action['original_size'], action['original_mtime'], action['original_hash'])
            elif status == 'pending_final':
                temp_p = self._find_temp_file(new_p)
                current_path_str = str(temp_p) if temp_p else f"[red]TEMP NOT FOUND for {new_p.name}[/red]"
                target_path_str = str(orig_p)
                action_desc = f"Rename Temp Back ({item_type})"
                # Integrity check is not applicable here as temp file wasn't the original
                integrity_msg = "N/A (Temp File)"
            elif status == 'created_dir':
                current_path_str = str(orig_p) # The dir that was created
                target_path_str = "[red]Remove Directory[/red]"
                action_desc = "Remove Created Dir"
                integrity_msg = "N/A (Directory)"
            else:
                current_path_str = f"[red]Unknown Status '{status}'[/red]"
                target_path_str = str(orig_p)
                action_desc = "[red]Unknown Action[/red]"
                integrity_msg = "[red]Unknown[/red]"

            preview_table.add_row(
                str(action_id),
                status.capitalize(),
                item_type.capitalize(),
                current_path_str,
                "->",
                target_path_str,
                integrity_msg
            )

        console.print(preview_table)
        # --- END Preview Table ---

        if dry_run:
            console.print(f"\n--- {action_word} Preview Complete ---")
            console.print("[yellow]No changes were made.[/yellow]")
            if conn: conn.close()
            return True

        try:
            # Use rich's input for consistency if available
            confirm = console.input("Proceed with UNDO operation? ([bold green]y[/]/[bold red]N[/]): ").strip().lower()
            if confirm != 'y':
                console.print("[yellow]Undo operation cancelled by user.[/yellow]");
                if conn: conn.close(); return False
        except (EOFError, Exception) as e:
            log.error(f"Error reading confirmation input: {e}")
            console.print("[bold red]Undo operation cancelled (Error reading input).[/bold red]")
            if conn: conn.close(); return False

        console.print("--- Performing Revert ---")
        success_count, db_fail_count, fs_fail_count, skip_count = 0, 0, 0, 0
        critical_fs_error = False
        overall_success = False
        removed_dirs_in_batch: List[Path] = []

        try: # Transaction management
            for action in actions:
                action_id = action['id']; orig_p = Path(action['original_path']); new_p = Path(action['new_path'])
                status = action['status']; item_type = action['type']; log_prefix = f"[Undo ID {action_id}] "
                fs_op_succeeded = False

                try:
                    if status in ('renamed', 'moved'):
                        current_src = new_p; target_dest = orig_p
                        log.debug(f"{log_prefix}Processing revert: {current_src} -> {target_dest}")
                        integrity_passed, integrity_msg = True, "Skipped (Check Disabled)"
                        if self.check_integrity and item_type == 'file':
                            integrity_passed, integrity_msg = self._check_file_integrity(current_src, action['original_size'], action['original_mtime'], action['original_hash'])
                            console.print(f"  Integrity check for '[cyan]{current_src.name}[/]': {integrity_msg}"); log.info(f"{log_prefix}Integrity check for '{current_src}': {integrity_msg}")
                        if not integrity_passed: console.print(f"  [yellow]Skipping revert due to integrity check failure.[/yellow]"); log.warning(f"{log_prefix}Skipping revert for '{current_src}' due to integrity check failure."); skip_count += 1; continue
                        if not current_src.exists(): console.print(f"  [yellow]Skipped revert: File to revert from does not exist: '[cyan]{current_src}[/]'[/yellow]"); log.warning(f"{log_prefix}Skipped revert: Source '{current_src}' does not exist."); skip_count += 1; continue
                        if target_dest.exists(): console.print(f"  [yellow]Skipped revert: Cannot revert '[cyan]{current_src.name}[/]'. Original path '[cyan]{target_dest}[/]' already exists.[/yellow]"); log.warning(f"{log_prefix}Skipped revert: Target '{target_dest}' already exists."); skip_count += 1; continue
                        try:
                            log.info(f"{log_prefix}Attempting rename: '{current_src}' -> '{target_dest}'"); target_dest.parent.mkdir(parents=True, exist_ok=True)
                            try: os.rename(str(current_src), str(target_dest)); log.debug(f"  -> Revert successful (os.rename)")
                            except OSError as e_os: log.warning(f"  -> os.rename failed ({e_os}), attempting shutil.move..."); shutil.move(str(current_src), str(target_dest)); log.debug(f"  -> Revert successful (shutil.move)")
                            console.print(f"  [green]Success:[/green] '[cyan]{current_src.name}[/]' reverted to '[cyan]{target_dest.name}[/]'"); log.info(f"{log_prefix}Revert successful.")
                            fs_op_succeeded = True
                        except OSError as e: console.print(f"  [bold red]Error reverting '[cyan]{current_src.name}[/]' to '[cyan]{target_dest.name}[/': {e}"); log.error(f"{log_prefix}OSError reverting '{current_src}' to '{target_dest}': {e}"); fs_fail_count += 1; critical_fs_error = True
                        except Exception as e: console.print(f"  [bold red]Unexpected error reverting '[cyan]{current_src.name}[/': {e}"); log.exception(f"{log_prefix}Unexpected error reverting '{current_src}' to '{target_dest}': {e}"); fs_fail_count += 1; critical_fs_error = True

                    elif status == 'pending_final':
                        final_dest = new_p; orig_target = orig_p
                        log.debug(f"{log_prefix}Processing revert for pending_final: TEMP({final_dest}) -> {orig_target}")
                        temp_src = self._find_temp_file(final_dest)
                        if not temp_src: console.print(f"  [yellow]Skipped revert: Cannot find temp file for '[cyan]{final_dest}[/]'[/yellow]"); log.warning(f"{log_prefix}Skipped revert: Temp file for '{final_dest}' not found."); skip_count += 1; continue
                        if not temp_src.exists(): console.print(f"  [yellow]Skipped revert: Temp file '[cyan]{temp_src}[/]' does not exist.[/yellow]"); log.warning(f"{log_prefix}Skipped revert: Temp source '{temp_src}' does not exist."); skip_count += 1; continue
                        if orig_target.exists(): console.print(f"  [yellow]Skipped revert: Cannot revert temp file '[cyan]{temp_src.name}[/]'. Original path '[cyan]{orig_target}[/]' already exists.[/yellow]"); log.warning(f"{log_prefix}Skipped revert: Target '{orig_target}' already exists."); skip_count += 1; continue
                        try:
                            log.info(f"{log_prefix}Attempting rename: '{temp_src}' -> '{orig_target}'"); orig_target.parent.mkdir(parents=True, exist_ok=True)
                            try: os.rename(str(temp_src), str(orig_target)); log.debug(f"  -> Temp file revert successful (os.rename)")
                            except OSError as e_os: log.warning(f"  -> os.rename failed for temp file ({e_os}), attempting shutil.move..."); shutil.move(str(temp_src), str(orig_target)); log.debug(f"  -> Temp file revert successful (shutil.move)")
                            console.print(f"  [green]Success:[/green] Temp file '[cyan]{temp_src.name}[/]' reverted to '[cyan]{orig_target.name}[/]'"); log.info(f"{log_prefix}Temp file revert successful.")
                            fs_op_succeeded = True
                        except OSError as e: console.print(f"  [bold red]Error reverting temp file '[cyan]{temp_src.name}[/]' to '[cyan]{orig_target.name}[/': {e}"); log.error(f"{log_prefix}OSError reverting temp file '{temp_src}' to '{orig_target}': {e}"); fs_fail_count += 1; critical_fs_error = True
                        except Exception as e: console.print(f"  [bold red]Unexpected error reverting temp file '[cyan]{temp_src.name}[/': {e}"); log.exception(f"{log_prefix}Unexpected error reverting temp file '{temp_src}' to '{orig_target}': {e}"); fs_fail_count += 1; critical_fs_error = True

                    elif status == 'created_dir':
                        dir_to_remove = orig_p
                        log.debug(f"{log_prefix}Processing revert for created_dir: Remove '{dir_to_remove}'")
                        if not dir_to_remove.exists(): console.print(f"  [dim]Skipped removal: Directory '[cyan]{dir_to_remove}[/]' does not exist.[/dim]"); log.debug(f"{log_prefix}Skipped removal: Directory '{dir_to_remove}' does not exist."); fs_op_succeeded = True
                        elif not dir_to_remove.is_dir(): console.print(f"  [yellow]Skipped removal: Path '[cyan]{dir_to_remove}[/]' exists but is not a directory.[/yellow]"); log.warning(f"{log_prefix}Skipped removal: '{dir_to_remove}' is not a directory."); skip_count += 1; continue
                        else:
                            try: is_empty = not any(dir_to_remove.iterdir())
                            except OSError as e: console.print(f"  [bold red]Error checking if directory '[cyan]{dir_to_remove}[/]' is empty: {e}"); log.error(f"{log_prefix}OSError checking emptiness of '{dir_to_remove}': {e}"); fs_fail_count += 1; critical_fs_error = True; continue
                            if not is_empty: console.print(f"  [yellow]Skipped removal: Directory '[cyan]{dir_to_remove}[/]' is not empty.[/yellow]"); log.warning(f"{log_prefix}Skipped removal: Directory '{dir_to_remove}' is not empty."); skip_count += 1; continue
                            try:
                                log.info(f"{log_prefix}Attempting rmdir: '{dir_to_remove}'"); dir_to_remove.rmdir(); console.print(f"  [green]Success:[/green] Directory '[cyan]{dir_to_remove}[/]' removed."); log.info(f"{log_prefix}Directory removal successful.")
                                fs_op_succeeded = True; removed_dirs_in_batch.append(dir_to_remove)
                            except OSError as e: console.print(f"  [bold red]Error removing directory '[cyan]{dir_to_remove}[/': {e}"); log.error(f"{log_prefix}OSError removing directory '{dir_to_remove}': {e}"); fs_fail_count += 1; critical_fs_error = True
                            except Exception as e: console.print(f"  [bold red]Unexpected error removing directory '[cyan]{dir_to_remove}[/': {e}"); log.exception(f"{log_prefix}Unexpected error removing directory '{dir_to_remove}': {e}"); fs_fail_count += 1; critical_fs_error = True
                    else:
                        console.print(f"  [yellow]Skipping action with unexpected status '[bold]{status}[/bold]' for '[cyan]{new_p}[/]' -> '[cyan]{orig_p}[/]'")
                        log.warning(f"{log_prefix}Skipping action with unknown/unhandled status '{status}'")
                        skip_count += 1
                        continue

                    if fs_op_succeeded:
                         if self.update_action_status(batch_id, str(orig_p), 'reverted', conn=conn):
                             success_count += 1
                         else:
                             log.error(f"{log_prefix}FS operation successful, but FAILED to update status to 'reverted' for '{orig_p}'")
                             db_fail_count += 1 # Count DB update failures

                except Exception as e_inner:
                    console.print(f"  [bold red]Unexpected error processing action ID {action_id} ({status} for {new_p}): {e_inner}")
                    log.exception(f"{log_prefix}Unexpected error processing action: {e_inner}")
                    fs_fail_count += 1
                    critical_fs_error = True

            # Attempt to clean up empty parent directories (unchanged logic)
            if not critical_fs_error and removed_dirs_in_batch:
                console.print("--- Attempting to clean up empty parent directories ---")
                sorted_removed_dirs = sorted(removed_dirs_in_batch, key=lambda p: len(p.parts), reverse=True)
                processed_parents = set()
                for removed_dir in sorted_removed_dirs:
                    parent_dir = removed_dir.parent
                    if parent_dir in processed_parents: continue
                    if parent_dir.is_dir():
                        try:
                            is_empty = not any(parent_dir.iterdir())
                            if is_empty:
                                log.info(f"[Undo Cleanup] Attempting rmdir on now-empty parent: '{parent_dir}'")
                                try:
                                    parent_dir.rmdir()
                                    console.print(f"  [green]Success:[/green] Removed empty parent directory '[cyan]{parent_dir}[/]'")
                                    processed_parents.add(parent_dir)
                                except OSError as e_p:
                                    console.print(f"  [dim]Info: Could not remove parent '[cyan]{parent_dir}[/]' (likely permissions or not empty): {e_p}[/dim]")
                                    log.warning(f"[Undo Cleanup] Failed to remove parent dir '{parent_dir}': {e_p}")
                                    processed_parents.add(parent_dir)
                            else:
                                log.debug(f"[Undo Cleanup] Parent directory '{parent_dir}' is not empty. Skipping removal.")
                                processed_parents.add(parent_dir)
                        except OSError as e_p_check:
                            log.warning(f"[Undo Cleanup] Error checking parent directory '{parent_dir}': {e_p_check}")
                            processed_parents.add(parent_dir)
                    else: log.debug(f"[Undo Cleanup] Parent directory '{parent_dir}' does not exist. Skipping."); processed_parents.add(parent_dir)

            # Commit or Rollback
            if critical_fs_error:
                 log.error(f"Critical file system errors occurred during undo for batch '{batch_id}'. Rolling back database changes.")
                 console.print("[bold red]  ERROR: Critical file system errors occurred. Rolling back database changes.[/bold red]")
                 if conn: conn.rollback()
            else:
                 log.info(f"Undo database updates committing for batch '{batch_id}'.")
                 if conn: conn.commit()

            overall_success = not critical_fs_error and db_fail_count == 0

        except Exception as e_outer:
            log.exception(f"Critical error during undo transaction management for batch '{batch_id}': {e_outer}")
            console.print(f"[bold red]CRITICAL DATABASE ERROR during undo finalization:[/bold red] {e_outer}")
            if conn:
                try: conn.rollback()
                except Exception as e_rb: log.error(f"Rollback failed after outer error: {e_rb}")
            overall_success = False
        finally:
            if conn: conn.close(); log.debug("Undo database connection closed.")

        console.print(f"--- {action_word} Complete for batch '[cyan]{batch_id}[/cyan]' ---")
        console.print(f"Summary: [green]{success_count} succeeded[/], [red]{db_fail_count} DB errors[/], [red]{fs_fail_count} FS errors[/], [yellow]{skip_count} skipped[/].")
        return overall_success

# --- END OF FILE undo_manager.py ---