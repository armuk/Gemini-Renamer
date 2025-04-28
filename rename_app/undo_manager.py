import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from .exceptions import RenamerError

log = logging.getLogger(__name__)
TEMP_SUFFIX_PREFIX = ".renametmp_"

class UndoManager:
    def __init__(self, cfg_helper):
        self.cfg = cfg_helper
        self.db_path = None
        self.is_enabled = False

        try:
            self.is_enabled = self.cfg('enable_undo', False)
            if self.is_enabled:
                self.db_path = self._resolve_db_path()
                self._init_db()
            else:
                log.info("Undo feature disabled by configuration.")
        except Exception as e:
            log.exception(f"Failed to initialize UndoManager: {e}")
            self.is_enabled = False

    def _resolve_db_path(self):
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
        if not self.db_path:
            raise RenamerError("Cannot connect to undo database: path not resolved.")
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            return conn
        except sqlite3.Error as e:
            raise RenamerError(f"Cannot connect to undo database: {e}") from e

    def _init_db(self):
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
                log.info(f"Undo database initialized: {self.db_path}")
        except sqlite3.Error as e:
            raise RenamerError(f"Failed to initialize undo database: {e}") from e

    def log_action(self, batch_id, original_path, new_path, item_type, status):
        if not self.is_enabled:
            return

        orig_p = Path(original_path)
        original_size = original_mtime = None

        if item_type == 'file' and status in {'pending_final', 'renamed', 'moved', 'trashed'}:
            try:
                if orig_p.is_file():
                    stat = orig_p.stat()
                    original_size = stat.st_size
                    original_mtime = stat.st_mtime
            except OSError:
                log.warning(f"Could not stat file during log_action: {original_path}")

        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT INTO rename_log (batch_id, timestamp, original_path, new_path, type, status, original_size, original_mtime)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    batch_id, datetime.now(timezone.utc).isoformat(), str(original_path), str(new_path), item_type, status, original_size, original_mtime
                ))
        except sqlite3.IntegrityError:
            log.warning(f"Duplicate entry in rename log: {original_path} (batch {batch_id})")
        except sqlite3.Error as e:
            log.error(f"Database error during log_action: {e}")

    def update_action_status(self, batch_id, original_path, new_status):
        if not self.is_enabled:
            return False

        try:
            with self._connect() as conn:
                cur = conn.execute("""
                    UPDATE rename_log
                    SET status = ?
                    WHERE batch_id = ? AND original_path = ? AND status != 'reverted'
                """, (new_status, batch_id, str(original_path)))
                return (cur.rowcount or 0) > 0
        except sqlite3.Error as e:
            log.error(f"Failed to update action status: {e}")
            return False

    def prune_old_batches(self):
        if not self.is_enabled:
            return

        try:
            expire_days = int(self.cfg('undo_expire_days', 30))
            if expire_days < 0:
                log.warning("undo_expire_days cannot be negative. Skipping prune.")
                return
        except (ValueError, TypeError):
            log.warning("Invalid undo_expire_days configuration. Using default 30.")
            expire_days = 30

        cutoff = datetime.now(timezone.utc) - timedelta(days=expire_days)
        cutoff_iso = cutoff.isoformat()

        try:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM rename_log WHERE timestamp < ?", (cutoff_iso,))
                conn.commit()
                deleted_rows = cur.rowcount or 0
                if deleted_rows > 0:
                    log.info(f"Pruned {deleted_rows} old undo records.")
                else:
                    log.debug("No old undo records to prune.")
        except sqlite3.Error as e:
            log.error(f"Error during undo record pruning: {e}")

    def perform_undo(self, batch_id):
        if not self.is_enabled or not self.db_path or not self.db_path.exists():
            print("Undo not available: database missing or disabled.")
            return False

        print(f"Undo started for batch '{batch_id}'...")

        try:
            with self._connect() as conn:
                cur = conn.execute("""
                    SELECT * FROM rename_log
                    WHERE batch_id = ?
                    AND status NOT IN ('reverted', 'trashed')
                    ORDER BY id DESC
                """, (batch_id,))
                actions = cur.fetchall()
        except sqlite3.Error as e:
            log.error(f"Failed to fetch undo records: {e}")
            return False

        if not actions:
            print("No undoable actions found.")
            return False

        for action in actions:
            print(f"Plan: {action['new_path']} → {action['original_path']}")

        confirm = input("Proceed with undo? (y/N): ").strip().lower()
        if confirm != 'y':
            print("Undo cancelled.")
            return False

        for action in actions:
            new_p = Path(action['new_path'])
            orig_p = Path(action['original_path'])

            if new_p.exists() and not orig_p.exists():
                try:
                    orig_p.parent.mkdir(parents=True, exist_ok=True)
                    new_p.rename(orig_p)
                    print(f"Undo success: {new_p} → {orig_p}")
                    self.update_action_status(batch_id, orig_p, 'reverted')
                except Exception as e:
                    print(f"Undo failed for {new_p}: {e}")
                    log.error(f"Undo failed for {new_p}: {e}")
            else:
                print(f"Skipping undo: {new_p} (destination exists or missing source)")

        return True
