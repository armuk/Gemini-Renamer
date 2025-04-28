# tests/test_undo_manager.py

import pytest
import sqlite3
import time
import sys
import os
import logging
from pathlib import Path
from unittest.mock import MagicMock, call, patch, ANY
from datetime import datetime, timezone, timedelta

# Ensure imports work correctly relative to the project structure
# Assuming tests are run from the project root
from rename_app.undo_manager import UndoManager, TEMP_SUFFIX_PREFIX, MTIME_TOLERANCE
from rename_app.exceptions import RenamerError

# Configure logging for tests
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

# --- Fixture for Basic Undo Manager ---
@pytest.fixture
def basic_undo_manager(tmp_path):
    db_path = tmp_path / "test_undo.db"
    cfg = lambda k, d=None: {
        'enable_undo': True,
        'undo_db_path': str(db_path),
        'undo_expire_days': 30,
        'undo_check_integrity': False,
    }.get(k, d)
    # Ensure db doesn't exist from previous failed run if tmp_path is reused
    if db_path.exists():
        db_path.unlink()
    manager = UndoManager(cfg_helper=cfg)
    yield manager # Use yield to allow cleanup if needed
    # Optional cleanup: close connection if manager holds one? (Current code uses context managers)
    # if manager.is_enabled and hasattr(manager,'_conn') and manager._conn: manager._conn.close()


# --- Fixture for Integrity Check Manager ---
@pytest.fixture
def manager_integrity_check(tmp_path):
    db_path = tmp_path / "integrity_undo.db"
    cfg = lambda k, d=None: {
        'enable_undo': True,
        'undo_db_path': str(db_path),
        'undo_expire_days': 30,
        'undo_check_integrity': True, # Enable integrity check
    }.get(k, d)
    if db_path.exists():
        db_path.unlink()
    manager = UndoManager(cfg_helper=cfg)
    yield manager

# --- Fixture for Custom Config Manager ---
@pytest.fixture
def custom_config_manager(tmp_path):
    # Ensure unique DB for each custom config test
    db_counter = 0
    def _create_manager(config_dict):
        nonlocal db_counter
        db_name = f"custom_undo_{db_counter}.db"
        db_counter += 1
        db_path = tmp_path / db_name
        full_config = {
            'enable_undo': True,
            'undo_db_path': str(db_path),
            'undo_expire_days': 30,
            'undo_check_integrity': False,
            **config_dict
        }
        if db_path.exists():
            db_path.unlink()
        cfg = lambda k, d=None: full_config.get(k, d)
        manager = UndoManager(cfg_helper=cfg)
        return manager
    return _create_manager

# --- Helper Functions ---
def _query_db(db_path, query, params=()):
    if not Path(db_path).exists():
        # Don't fail here, allow tests to check for non-existence
        # pytest.fail(f"Database file not found at {db_path}")
        return [] # Return empty list if DB doesn't exist
    try:
        # Use a new connection each time to avoid conflicts with manager's connection
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return cursor.fetchall()
    except sqlite3.Error as e:
        pytest.fail(f"Database query failed: {e}\nQuery: {query}\nParams: {params}")

def _get_log_count(db_path, batch_id=None):
    if not Path(db_path).exists(): return 0
    query = "SELECT COUNT(*) FROM rename_log"
    params = ()
    if batch_id:
        query += " WHERE batch_id = ?"
        params = (batch_id,)
    # Use try-except as DB might be locked during test
    try:
        result = _query_db(db_path, query, params)
        return result[0][0] if result else 0
    except IndexError: # Handle case where query returns nothing unexpectedly
         return 0
    except Exception as e:
         log.error(f"Error getting log count for {db_path}: {e}")
         return -1 # Indicate error


# ======================= TESTS START =======================

# --- __init__ Tests ---
def test_init_unexpected_error(mocker):
    """Test that manager is disabled if _resolve_db_path raises unexpected error."""
    mock_cfg = MagicMock()
    # Configure mock to return True for 'enable_undo' and raise error for 'undo_db_path' indirectly
    mock_cfg.side_effect = lambda k, d=None: {'enable_undo': True}.get(k, d)
    # Mock the method that causes the error
    mocker.patch('rename_app.undo_manager.UndoManager._resolve_db_path', side_effect=Exception("Unexpected init error"))
    # Patch logger to check log message
    mock_log = mocker.patch('rename_app.undo_manager.log')

    manager = UndoManager(cfg_helper=mock_cfg)

    assert manager.is_enabled is False
    # Check that the exception was logged
    mock_log.exception.assert_called_once_with(mocker.ANY) # Check if exception was logged
    assert "Failed to initialize UndoManager: Unexpected init error" in mock_log.exception.call_args[0][0]

# Add these to test_undo_manager.py

def test_connect_sqlite_error(mocker):
    """Test _connect handles sqlite3.Error during the connect call itself."""
    # This covers lines like 69-71 if the connect fails before PRAGMAs
    db_path = Path("dummy/connect_fail.db")
    mock_cfg = MagicMock()
    mock_cfg.side_effect = lambda k, d=None: {
        'enable_undo': True, 'undo_db_path': str(db_path),
        'undo_check_integrity': False, 'undo_expire_days': 30
    }.get(k, d)

    mocker.patch('rename_app.undo_manager.UndoManager._resolve_db_path', return_value=db_path)
    # Make sqlite3.connect raise an error
    mocker.patch('sqlite3.connect', side_effect=sqlite3.Error("Connection refused"))
    mock_log = mocker.patch('rename_app.undo_manager.log')

    # Instantiating should fail during _init_db -> _connect
    manager = UndoManager(cfg_helper=mock_cfg)

    assert manager.is_enabled is False
    mock_log.exception.assert_called_once()
    assert "Failed to connect during undo database initialization" in mock_log.exception.call_args[0][0]
    assert "Connection refused" in mock_log.exception.call_args[0][0]

def test_perform_undo_pending_final_rename_oserror(basic_undo_manager, tmp_path, mocker, capsys):
    """Test OSError during rename for pending_final revert."""
    # Covers lines 418-421
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    src = tmp_path / "pf_os_err.txt"; final_dest = tmp_path / "pf_os_dest.txt"
    unique_suffix = str(time.time()).replace('.', '')
    temp_path = final_dest.parent / f"{final_dest.stem}{TEMP_SUFFIX_PREFIX}{unique_suffix}{final_dest.suffix}"
    src.write_text("pf_content")
    basic_undo_manager.log_action("batch_pf_os", str(src), str(final_dest), 'file', 'pending_final')
    src.rename(temp_path)
    assert not src.exists() and temp_path.exists()

    mocker.patch("builtins.input", return_value="y")
    # Mock rename to fail specifically for the temp -> src rename
    error_msg = "Cannot rename temp file back"
    original_rename = Path.rename
    def rename_side_effect(self, target):
        if self == temp_path and target == src:
            raise OSError(error_msg)
        return original_rename(self, target)
    mocker.patch('pathlib.Path.rename', side_effect=rename_side_effect, autospec=True)

    result = basic_undo_manager.perform_undo("batch_pf_os")

    assert result is False # Rename failed
    assert not src.exists() # Should not have been recreated
    assert temp_path.exists() # Temp file should remain
    assert f"Error reverting temp file '{temp_path.name}' to '{src.name}': {error_msg}" in capsys.readouterr().out
    mock_logger.error.assert_called_once()
    assert f"OSError reverting temp file '{temp_path}' to '{src}': {error_msg}" in mock_logger.error.call_args[0][0]
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_pf_os",))[0]
    assert log_entry['status'] == 'pending_final' # Status unchanged

def test_perform_undo_pending_final_rename_exception(basic_undo_manager, tmp_path, mocker, capsys):
    """Test generic Exception during rename for pending_final revert."""
    # Covers lines 425-428
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    src = tmp_path / "pf_exc.txt"; final_dest = tmp_path / "pf_exc_dest.txt"
    unique_suffix = str(time.time()).replace('.', '')
    temp_path = final_dest.parent / f"{final_dest.stem}{TEMP_SUFFIX_PREFIX}{unique_suffix}{final_dest.suffix}"
    src.write_text("pf_content")
    basic_undo_manager.log_action("batch_pf_exc", str(src), str(final_dest), 'file', 'pending_final')
    src.rename(temp_path)
    assert not src.exists() and temp_path.exists()

    mocker.patch("builtins.input", return_value="y")
    # Mock rename to fail specifically for the temp -> src rename
    error_msg = "Weird error renaming temp file back"
    original_rename = Path.rename
    def rename_side_effect(self, target):
        if self == temp_path and target == src:
            raise ValueError(error_msg) # Non-OSError
        return original_rename(self, target)
    mocker.patch('pathlib.Path.rename', side_effect=rename_side_effect, autospec=True)

    result = basic_undo_manager.perform_undo("batch_pf_exc")

    assert result is False # Rename failed
    assert not src.exists() # Should not have been recreated
    assert temp_path.exists() # Temp file should remain
    assert f"Unexpected error reverting temp file '{temp_path.name}': {error_msg}" in capsys.readouterr().out
    mock_logger.exception.assert_called_once()
    assert f"Unexpected error reverting temp file '{temp_path}' to '{src}'" in mock_logger.exception.call_args[0][0]
    assert error_msg in mock_logger.exception.call_args[0][0]
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_pf_exc",))[0]
    assert log_entry['status'] == 'pending_final' # Status unchanged

# In test_undo_manager.py

def test_perform_undo_created_dir_main_exception(basic_undo_manager, tmp_path, mocker, capsys):
    """Test the main undo loop's generic exception handler via created_dir status update failure."""
    # Covers lines 530-538
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    created_dir = tmp_path / "cd_outer_exc"
    # Log the action, but DO NOT create the directory
    basic_undo_manager.log_action("batch_cd_outer", str(created_dir), str(created_dir), 'dir', 'created_dir')
    assert not created_dir.exists()

    # Mock update_action_status to raise an unexpected error *only* for this specific call
    error_msg = "Unexpected status update error for non-existent dir"
    original_update = basic_undo_manager.update_action_status
    def update_side_effect(batch_id, path, status):
        if batch_id == "batch_cd_outer" and path == str(created_dir) and status == 'reverted':
            raise ValueError(error_msg)
        # Allow other calls (if any) to proceed normally
        return original_update(batch_id, path, status)

    mocker.patch.object(basic_undo_manager, 'update_action_status', side_effect=update_side_effect)
    mocker.patch("builtins.input", return_value="y")

    result = basic_undo_manager.perform_undo("batch_cd_outer")

    assert result is False # Failed due to the exception during update
    assert not created_dir.exists() # Dir still shouldn't exist

    # Check the generic exception log for the action processing
    mock_logger.exception.assert_called_once()
    log_call_args = mock_logger.exception.call_args[0][0]
    # FIX: Check for the correct prefix and message content
    assert "[Undo ID 1] Unexpected error processing action" in log_call_args
    assert error_msg in log_call_args

    # Check print output for the specific error
    # Note: The print output comes from the except block in perform_undo
    captured = capsys.readouterr()
    assert f"Unexpected error processing action ID 1 (created_dir for {created_dir}): {error_msg}" in captured.out

    # Check DB status unchanged (because update failed)
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_cd_outer",))[0]
    assert log_entry['status'] == 'created_dir'

# --- _resolve_db_path Tests ---
def test_resolve_db_path_from_config(custom_config_manager, tmp_path):
    config_path = tmp_path / "config_dir" / "my_undo.db"
    # Ensure parent does not exist initially to test creation
    if config_path.parent.exists():
        config_path.parent.rmdir()

    manager = custom_config_manager({'undo_db_path': str(config_path)})

    # Check if enabled first, as resolve failure disables it
    assert manager.is_enabled is True, "Manager should be enabled if path resolution succeeds"
    assert manager.db_path == config_path, "Resolved path should match config"
    assert config_path.parent.is_dir(), "Parent directory should have been created"

def test_resolve_db_path_config_error(custom_config_manager, tmp_path, mocker):
    """Test that manager is disabled if configured path resolution fails."""
    # Mock Path.resolve to simulate an OS error during resolution
    mocker.patch('pathlib.Path.resolve', side_effect=OSError("Resolve failed"))
    mock_log = mocker.patch('rename_app.undo_manager.log')
    config_path = tmp_path / "config_dir" / "my_undo.db" # Path doesn't need to exist

    manager = custom_config_manager({'undo_db_path': str(config_path)})

    assert manager.is_enabled is False, "Manager should be disabled on resolve error"
    assert manager.db_path is None, "db_path should be None on resolve error"
    mock_log.exception.assert_called_once()
    assert "Failed to initialize UndoManager: Cannot resolve undo database path: Resolve failed" in mock_log.exception.call_args[0][0]


def test_resolve_db_path_default_error(mocker):
    """Test that manager is disabled if default path creation fails."""
    mock_cfg = MagicMock()
    mock_cfg.side_effect = lambda k, d=None: {'enable_undo': True, 'undo_db_path': None}.get(k, d) # Use default path
    # Mock Path.mkdir to simulate inability to create the default parent directory
    mocker.patch('pathlib.Path.mkdir', side_effect=OSError("Cannot create dir"))
    mock_log = mocker.patch('rename_app.undo_manager.log')

    manager = UndoManager(cfg_helper=mock_cfg)

    assert manager.is_enabled is False, "Manager should be disabled on default dir creation error"
    assert manager.db_path is None, "db_path should be None on error"
    mock_log.exception.assert_called_once()
    assert "Failed to initialize UndoManager: Cannot resolve undo database path: Cannot create dir" in mock_log.exception.call_args[0][0]


# --- _connect Tests ---
def test_connect_db_path_none(basic_undo_manager):
    # Temporarily disable manager and set db_path to None to test _connect directly
    original_state = basic_undo_manager.is_enabled
    original_path = basic_undo_manager.db_path
    basic_undo_manager.is_enabled = True # Pretend it's enabled for the test
    basic_undo_manager.db_path = None

    with pytest.raises(RenamerError, match="Cannot connect to undo database: path not resolved."):
        basic_undo_manager._connect()

    # Restore original state
    basic_undo_manager.db_path = original_path
    basic_undo_manager.is_enabled = original_state

def test_connect_pragma_warning(basic_undo_manager, mocker, caplog):
    """Test that PRAGMA errors are logged as warnings but connection still returned."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")

    mock_conn = MagicMock(spec=sqlite3.Connection)
    # Simulate failure only on the second PRAGMA
    mock_conn.execute.side_effect = [
        None, # PRAGMA journal_mode=WAL succeeds
        sqlite3.Error("PRAGMA busy_timeout failed") # PRAGMA busy_timeout fails
    ]
    # Mock sqlite3.connect to return our connection mock
    mocker.patch('sqlite3.connect', return_value=mock_conn)
    mock_log = mocker.patch('rename_app.undo_manager.log')

    # Call _connect
    conn_result = basic_undo_manager._connect()

    # Assertions
    assert conn_result is mock_conn, "Connection object should still be returned"
    # Check that execute was called twice (for the two PRAGMAs)
    assert mock_conn.execute.call_count == 2
    mock_conn.execute.assert_has_calls([
        call("PRAGMA journal_mode=WAL;"),
        call("PRAGMA busy_timeout=5000;")
    ])
    # Check that the warning was logged for the failed PRAGMA
    mock_log.warning.assert_called_once_with(mocker.ANY)
    assert f"Could not set PRAGMA busy_timeout=5000 for undo DB ({basic_undo_manager.db_path}): PRAGMA busy_timeout failed" in mock_log.warning.call_args[0][0]
    # Ensure no error was logged
    mock_log.error.assert_not_called()


# --- _init_db Tests ---
def test_init_db_schema_error_disables_manager(mocker): # Removed caplog, use mock_log
    """Test that manager is disabled if schema creation fails during init."""
    mock_cfg = MagicMock()
    mock_cfg.side_effect = lambda k, d=None: {
        'enable_undo': True,
        'undo_db_path': 'dummy/path/db'
    }.get(k, d)

    # Mock resolve_db_path first
    mocker.patch('rename_app.undo_manager.UndoManager._resolve_db_path', return_value=Path('dummy/path/db'))

    # Mock the connection to raise error on execute
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    # Make the *first* execute call (CREATE TABLE) raise the error
    mock_conn.execute.side_effect = sqlite3.Error("Schema creation failed")

    # Mock _connect to return the failing connection
    mock_connect = mocker.patch('rename_app.undo_manager.UndoManager._connect', return_value=mock_conn)

    # Patch the logger *within the undo_manager module*
    mock_log = mocker.patch('rename_app.undo_manager.log')

    # Instantiate the manager - this should trigger the error path
    manager = UndoManager(cfg_helper=mock_cfg)

    # Assertions
    assert manager.is_enabled is False, "Manager should be disabled after init failure"

    # Verify mocks were called as expected
    mock_connect.assert_called_once() # _init_db should call _connect
    mock_conn.execute.assert_called_once() # CREATE TABLE should be attempted

    # Verify the exception was logged correctly in __init__'s except block
    mock_log.exception.assert_called_once()
    log_msg = mock_log.exception.call_args[0][0]
    assert "Failed to initialize UndoManager" in log_msg
    assert "Failed to initialize undo database schema" in log_msg
    assert "Schema creation failed" in log_msg

def test_init_db_creates_table_and_index(basic_undo_manager, tmp_path):
    """Verify database schema is correctly created on first initialization."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")

    db_path = basic_undo_manager.db_path
    assert db_path.exists(), "Database file should be created"

    # Check table exists
    tables = _query_db(db_path, "SELECT name FROM sqlite_master WHERE type='table' AND name='rename_log'")
    assert len(tables) == 1, "Table 'rename_log' should exist"

    # Check essential columns exist
    cols_info = _query_db(db_path, "PRAGMA table_info(rename_log)")
    col_names = {col['name'] for col in cols_info}
    expected_cols = {'id', 'batch_id', 'timestamp', 'original_path', 'new_path', 'type', 'status', 'original_size', 'original_mtime'}
    assert expected_cols <= col_names, f"Missing columns: {expected_cols - col_names}"

    # Check UNIQUE constraint on original_path (best effort check in SQL definition)
    constraints = _query_db(db_path, "SELECT sql FROM sqlite_master WHERE name='rename_log'")
    assert constraints, "Could not fetch table SQL definition"
    sql_def = constraints[0]['sql'].lower()
    # Simple check for UNIQUE keyword associated with the column
    assert 'original_path' in sql_def
    assert 'unique' in sql_def # Assuming 'original_path TEXT NOT NULL UNIQUE' or similar

    # Check index exists
    indexes = _query_db(db_path, "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_batch_id'")
    assert len(indexes) == 1, "Index 'idx_batch_id' should exist"


# --- log_action Tests ---
def test_log_action_skips_stats_for_dir(basic_undo_manager, tmp_path, caplog):
    """Stats should not be collected for directories."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    db_path = basic_undo_manager.db_path
    dir_path = tmp_path / "my_dir"
    dir_path.mkdir()

    basic_undo_manager.log_action("batch_dir_log", str(dir_path), str(dir_path), 'dir', 'created_dir')

    assert _get_log_count(db_path, "batch_dir_log") == 1
    log_entry = _query_db(db_path, "SELECT * FROM rename_log WHERE batch_id = ?", ("batch_dir_log",))[0]
    assert log_entry['original_size'] is None
    assert log_entry['original_mtime'] is None
    # Check logs don't mention capturing stats (use DEBUG level)
    # assert "Captured stats" not in caplog.text # Too specific, check None values instead


def test_log_action_skips_stats_for_wrong_status(basic_undo_manager, tmp_path, caplog):
    """Stats should only be collected for relevant statuses ('pending_final', 'renamed', 'moved', 'trashed')."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    db_path = basic_undo_manager.db_path
    src = tmp_path / "file_log_reverted.txt"
    src.touch()

    # Use a status ('reverted') for which stats shouldn't be collected
    basic_undo_manager.log_action("batch_revert_log", str(src), str(src), 'file', 'reverted')

    assert _get_log_count(db_path, "batch_revert_log") == 1
    log_entry = _query_db(db_path, "SELECT * FROM rename_log WHERE batch_id = ?", ("batch_revert_log",))[0]
    assert log_entry['original_size'] is None
    assert log_entry['original_mtime'] is None


def test_log_action_stat_target_not_file(basic_undo_manager, tmp_path, caplog):
    """Stats should be skipped if the original path is not a file (even if type='file')."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    db_path = basic_undo_manager.db_path
    dir_path = tmp_path / "stat_dir_target"
    dir_path.mkdir() # Create a directory

    # Log action with type='file' but path is a directory
    basic_undo_manager.log_action("batch_stat_dir", str(dir_path), str(dir_path), 'file', 'moved')

    assert _get_log_count(db_path, "batch_stat_dir") == 1
    log_entry = _query_db(db_path, "SELECT * FROM rename_log WHERE batch_id = ?", ("batch_stat_dir",))[0]
    assert log_entry['original_size'] is None
    assert log_entry['original_mtime'] is None
    # Verify no warning about *failing* to stat, just that it was skipped.
    # Check logs if specific debug message was added for skipping non-files.
    # assert "Original file not found or not a file, skipping stats" in caplog.text # If debug log added
    assert "Could not stat original file" not in caplog.text # Ensure no OS error was logged


def test_log_action_integrity_error(basic_undo_manager, tmp_path, caplog):
    """Test handling of UNIQUE constraint violation."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    db_path = basic_undo_manager.db_path
    src = tmp_path / "duplicate.txt"
    src.touch()

    # First log action - should succeed
    basic_undo_manager.log_action("batch_duplicate", str(src), "new1.txt", 'file', 'moved')
    assert _get_log_count(db_path, "batch_duplicate") == 1

    # Second log action with the same original_path - should fail UNIQUE constraint
    caplog.clear() # Clear previous logs
    with caplog.at_level(logging.WARNING): # Ensure warnings are captured
        basic_undo_manager.log_action("batch_duplicate", str(src), "new2.txt", 'file', 'renamed')

    # Assertions
    # Check that the warning message includes the specific error
    assert "Duplicate entry prevented in rename log" in caplog.text
    assert str(src) in caplog.text
    assert "UNIQUE constraint failed" in caplog.text # Check the specific DB error is mentioned
    # Check that the second log action did not add a new row
    assert _get_log_count(db_path, "batch_duplicate") == 1


def test_log_action_stat_os_error(basic_undo_manager, tmp_path, mocker, caplog):
    """Test logging when Path.stat() raises OSError."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    db_path = basic_undo_manager.db_path
    src_str = str(tmp_path / "stat_error.txt")
    Path(src_str).touch() # Create the file

    # Mock Path instance for the specific source file to raise OSError on stat
    mock_path_instance = MagicMock(spec=Path)
    mock_path_instance.is_file.return_value = True # Pretend it's a file
    mock_path_instance.stat.side_effect = OSError("Stat failed") # Make stat fail
    mock_path_instance.__str__.return_value = src_str # Ensure string representation is correct
    mock_path_instance.name = Path(src_str).name # Needed for logs/output sometimes

    # Patch the Path constructor to return our mock *only* for the source path
    original_path_constructor = Path
    def path_side_effect(p):
        if str(p) == src_str:
            return mock_path_instance
        else:
            # Important: return a real Path object for other paths
            return original_path_constructor(p)

    mock_path_constructor = mocker.patch('rename_app.undo_manager.Path', side_effect=path_side_effect)

    # Log the action, triggering the mocked stat failure
    with caplog.at_level(logging.WARNING): # Capture warnings
        basic_undo_manager.log_action("batch_stat_error", src_str, "new.txt", 'file', 'moved')

    # Assertions
    # assert mock_path_instance.stat.called # Redundant if checking log
    # Check the log message matches the updated code format
    assert f"Could not stat original file during log_action for '{src_str}': Stat failed" in caplog.text
    # Ensure entry was still logged, but without stats
    assert _get_log_count(db_path, "batch_stat_error") == 1
    log_entry = _query_db(db_path, "SELECT * FROM rename_log WHERE batch_id = ?", ("batch_stat_error",))[0]
    assert log_entry['original_size'] is None
    assert log_entry['original_mtime'] is None


def test_log_action_generic_db_error(basic_undo_manager, tmp_path, mocker, caplog):
    """Test logging when DB connection execute raises a generic sqlite3.Error."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "generic_db_error.txt"
    src.touch()

    # Mock the connection context manager's execute method
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_conn.__enter__.return_value = mock_conn # Return self for context manager
    mock_conn.__exit__.return_value = None
    mock_conn.execute.side_effect = sqlite3.Error("Generic DB Error on Insert")
    # Patch the _connect method to return this mock
    mocker.patch.object(basic_undo_manager, '_connect', return_value=mock_conn)

    # Log action, triggering the error
    with caplog.at_level(logging.ERROR): # Capture errors
        basic_undo_manager.log_action("batch_generic_db", str(src), "new.txt", 'file', 'moved')

    # Assertions
    # Check the log message matches the updated code format
    assert f"Database error during log_action for '{src}' (batch 'batch_generic_db'): Generic DB Error on Insert" in caplog.text
    # Ensure no record was added
    assert _get_log_count(basic_undo_manager.db_path, "batch_generic_db") == 0


def test_log_action_generic_exception(basic_undo_manager, tmp_path, mocker, caplog):
    """Test logging when an unexpected Exception occurs during log_action."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "generic_exception.txt"
    src.touch()

    # Mock the connection execute method to raise a generic Exception
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.execute.side_effect = Exception("Something unexpected happened")
    mocker.patch.object(basic_undo_manager, '_connect', return_value=mock_conn)
    mock_log = mocker.patch('rename_app.undo_manager.log') # Patch log for exception check

    # Log action, triggering the error
    basic_undo_manager.log_action("batch_generic_exception", str(src), "new.txt", 'file', 'moved')

    # Assertions
    # Check that log.exception was called with the correct message format
    mock_log.exception.assert_called_once_with(mocker.ANY)
    assert f"Unexpected error logging undo action for '{src}' (batch 'batch_generic_exception'): Something unexpected happened" in mock_log.exception.call_args[0][0]

    # Ensure no record was added
    assert _get_log_count(basic_undo_manager.db_path, "batch_generic_exception") == 0


# --- update_action_status Tests ---
def test_update_action_status_success(basic_undo_manager, tmp_path):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    db_path = basic_undo_manager.db_path
    src = tmp_path / "one_update.txt"
    src.touch()
    src_str = str(src)
    basic_undo_manager.log_action("batch_update", src_str, str(tmp_path / "two_update.txt"), 'file', 'pending_final')

    result = basic_undo_manager.update_action_status("batch_update", src_str, "moved")

    assert result is True
    log_entry = _query_db(db_path, "SELECT status FROM rename_log WHERE original_path = ?", (src_str,))[0]
    assert log_entry['status'] == 'moved'

def test_update_action_status_failure(basic_undo_manager, caplog):
    """Test updating a non-existent record returns False."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")

    with caplog.at_level(logging.WARNING):
        result = basic_undo_manager.update_action_status("fakebatch", "missing.txt", "reverted")

    assert result is False
    assert "No matching record found or status already 'reverted' for update" in caplog.text

def test_update_action_status_db_error(basic_undo_manager, tmp_path, mocker, caplog):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "update_db_error.txt"
    src_str = str(src)
    src.touch()
    basic_undo_manager.log_action("batch_update_db_error", src_str, "new.txt", 'file', 'pending_final')

    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.execute.side_effect = sqlite3.Error("DB Error on Update")
    mocker.patch.object(basic_undo_manager, '_connect', return_value=mock_conn)

    with caplog.at_level(logging.ERROR):
        result = basic_undo_manager.update_action_status("batch_update_db_error", src_str, "moved")

    assert result is False
    # Check the log message matches the updated code format
    assert f"Failed updating undo status for '{src_str}' (batch 'batch_update_db_error') to 'moved': DB Error on Update" in caplog.text

def test_update_action_status_exception(basic_undo_manager, tmp_path, mocker, caplog):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "update_exception.txt"
    src_str = str(src)
    src.touch()
    basic_undo_manager.log_action("batch_update_exception", src_str, "new.txt", 'file', 'pending_final')

    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.execute.side_effect = Exception("Update Exception")
    mocker.patch.object(basic_undo_manager, '_connect', return_value=mock_conn)
    mock_log = mocker.patch('rename_app.undo_manager.log') # Patch log for exception check

    result = basic_undo_manager.update_action_status("batch_update_exception", src_str, "moved")

    assert result is False
    # Check log.exception was called with the right format
    mock_log.exception.assert_called_once()
    assert f"Unexpected error updating undo status for '{src_str}' (batch 'batch_update_exception') to 'moved': Update Exception" in mock_log.exception.call_args[0][0]

# --- prune_old_batches Tests ---
def test_prune_skip_negative_days(custom_config_manager, tmp_path, caplog):
    manager = custom_config_manager({'undo_expire_days': -5})
    if not manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "neg.txt"
    src.touch()
    manager.log_action("batch_neg", str(src), "new_neg.txt", "file", "moved")
    initial_count = _get_log_count(manager.db_path)

    with caplog.at_level(logging.WARNING):
        manager.prune_old_batches()

    # Check assertion message matches code
    assert "Undo expiration days cannot be negative. Skipping prune." in caplog.text
    assert _get_log_count(manager.db_path) == initial_count

def test_prune_invalid_days_config(custom_config_manager, tmp_path, caplog):
    invalid_value = 'invalid_string'
    manager = custom_config_manager({'undo_expire_days': invalid_value})
    if not manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "inv.txt"
    src.touch()
    manager.log_action("batch_invalid", str(src), "new_inv.txt", "file", "moved")
    initial_count = _get_log_count(manager.db_path)

    with caplog.at_level(logging.WARNING):
        manager.prune_old_batches() # Should use default 30 days

    # Check assertion message matches code
    assert f"Invalid 'undo_expire_days' config value ('{invalid_value}'). Using default 30." in caplog.text
    assert _get_log_count(manager.db_path) == initial_count # Default 30 days won't prune recent item

def test_prune_db_error(basic_undo_manager, tmp_path, mocker, caplog):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "pde.txt"
    src.touch()
    basic_undo_manager.log_action("batch_prune_db_err", str(src), "new_pde.txt", "file", "moved")
    initial_count = _get_log_count(basic_undo_manager.db_path)

    # Mock execute to fail during DELETE
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.execute.side_effect = sqlite3.Error("Prune Delete Error")
    mocker.patch.object(basic_undo_manager, '_connect', return_value=mock_conn)

    with caplog.at_level(logging.ERROR):
        basic_undo_manager.prune_old_batches()

    assert "Error during undo log pruning: Prune Delete Error" in caplog.text
    assert _get_log_count(basic_undo_manager.db_path) == initial_count # Ensure count unchanged on error

def test_prune_exception(basic_undo_manager, tmp_path, mocker, caplog):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "pe.txt"
    src.touch()
    basic_undo_manager.log_action("batch_prune_exc", str(src), "new_pe.txt", "file", "moved")
    initial_count = _get_log_count(basic_undo_manager.db_path)

    # Mock execute to raise generic exception
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.execute.side_effect = Exception("Prune Exception")
    mocker.patch.object(basic_undo_manager, '_connect', return_value=mock_conn)
    mock_log = mocker.patch('rename_app.undo_manager.log') # Patch log for exception check

    basic_undo_manager.prune_old_batches()

    # Check log.exception
    mock_log.exception.assert_called_once()
    assert "Unexpected error during undo log pruning: Prune Exception" in mock_log.exception.call_args[0][0]
    assert _get_log_count(basic_undo_manager.db_path) == initial_count # Count unchanged

def test_prune_no_entries_deleted(custom_config_manager, tmp_path, caplog):
    # Use a long expiry time
    manager = custom_config_manager({'undo_expire_days': 365})
    if not manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "np.txt"
    src.touch()
    manager.log_action("batch_no_prune", str(src), "new_np.txt", "file", "moved")

    with caplog.at_level(logging.DEBUG): # Ensure debug messages are captured
        manager.prune_old_batches()

    # Check assertion message matches code
    assert "No expired entries found to prune." in caplog.text

def test_prune_expired_batches(tmp_path):
    db_path = tmp_path / "prune_undo.db"
    # Use expire_days = 0 to prune everything
    cfg_prune = lambda k, d=None: {
        'enable_undo': True,
        'undo_db_path': str(db_path),
        'undo_expire_days': 0, # Expire immediately
        'undo_check_integrity': False
        }.get(k, d)

    if db_path.exists(): db_path.unlink()
    undo_manager_prune = UndoManager(cfg_helper=cfg_prune)

    if not undo_manager_prune.is_enabled: pytest.skip("Undo disabled")

    # Log some actions
    (tmp_path/"o1.txt").touch(); undo_manager_prune.log_action("old1", str(tmp_path/"o1.txt"), "n1.txt", 'file', 'moved')
    (tmp_path/"o2.txt").touch(); undo_manager_prune.log_action("old2", str(tmp_path/"o2.txt"), "n2.txt", 'file', 'moved')
    (tmp_path/"c.txt").touch(); undo_manager_prune.log_action("curr", str(tmp_path/"c.txt"), "nc.txt", 'file', 'renamed')

    # Ensure some time passes so timestamps are definitely in the past for expire_days=0
    time.sleep(0.01)

    assert _get_log_count(db_path) == 3, "Should have 3 records before prune"
    undo_manager_prune.prune_old_batches()
    assert _get_log_count(db_path) == 0, "All records should be pruned with expiry_days=0"

# --- _find_temp_file Tests ---

def test_find_temp_file_os_error(basic_undo_manager, tmp_path, mocker):
    """Test _find_temp_file handles OSError during glob."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_log = mocker.patch('rename_app.undo_manager.log')
    final_dest = tmp_path / "final.txt"
    mock_parent = MagicMock(spec=Path)
    mock_parent.glob.side_effect = OSError("Glob failed")
    # Mock the final_dest path object itself to return the mocked parent
    mock_final_dest = MagicMock(spec=Path)
    mock_final_dest.parent = mock_parent
    mock_final_dest.stem = "final"
    mock_final_dest.suffix = ".txt"

    result = basic_undo_manager._find_temp_file(mock_final_dest)

    assert result is None
    mock_log.error.assert_called_once()
    assert "Error searching for temp file" in mock_log.error.call_args[0][0]
    assert "Glob failed" in mock_log.error.call_args[0][0]

def test_find_temp_file_multiple_matches(basic_undo_manager, tmp_path, mocker):
    """Test _find_temp_file handles multiple matches."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_log = mocker.patch('rename_app.undo_manager.log')
    final_dest = tmp_path / "final.txt"
    # Create multiple potential temp files
    unique_suffix1 = str(time.time()).replace('.', '') + "_1"
    unique_suffix2 = str(time.time()).replace('.', '') + "_2"
    temp1 = final_dest.parent / f"{final_dest.stem}{TEMP_SUFFIX_PREFIX}{unique_suffix1}{final_dest.suffix}"
    temp2 = final_dest.parent / f"{final_dest.stem}{TEMP_SUFFIX_PREFIX}{unique_suffix2}{final_dest.suffix}"
    temp1.touch()
    temp2.touch()

    result = basic_undo_manager._find_temp_file(final_dest)

    assert result is None
    mock_log.warning.assert_called_once()
    assert "Multiple temp files found" in mock_log.warning.call_args[0][0]

def test_find_temp_file_success(basic_undo_manager, tmp_path, mocker):
    """Test _find_temp_file finds a single unique temp file."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_log = mocker.patch('rename_app.undo_manager.log')
    final_dest = tmp_path / "final_ok.txt"
    # Create the expected temp file
    unique_suffix = str(time.time()).replace('.', '')
    temp_path = final_dest.parent / f"{final_dest.stem}{TEMP_SUFFIX_PREFIX}{unique_suffix}{final_dest.suffix}"
    temp_path.touch()
    # Create another unrelated file to ensure glob is specific
    (tmp_path / "other.txt").touch()

    result = basic_undo_manager._find_temp_file(final_dest)

    assert result == temp_path
    mock_log.debug.assert_called_with(f"Found temp file for {final_dest}: {temp_path}")

def test_find_temp_file_no_match(basic_undo_manager, tmp_path, mocker):
    """Test _find_temp_file finds no matching temp file."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_log = mocker.patch('rename_app.undo_manager.log')
    final_dest = tmp_path / "final_nomatch.txt"
    # Create an unrelated file
    (tmp_path / "unrelated.txt").touch()

    result = basic_undo_manager._find_temp_file(final_dest)

    assert result is None
    # Check that the specific "No temp file found" debug message was logged
    found_log = any(
        f"No temp file found matching pattern" in call_args[0][0]
        for call_args in mock_log.debug.call_args_list
    )
    assert found_log, "Expected 'No temp file found' debug log"

# --- _check_file_integrity Tests ---

def test_check_file_integrity_stat_os_error(basic_undo_manager, tmp_path, mocker):
    """Test _check_file_integrity handles OSError during stat."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_path = MagicMock(spec=Path)
    mock_path.stat.side_effect = OSError("Cannot stat")

    # Provide some dummy logged data
    result, msg = basic_undo_manager._check_file_integrity(mock_path, 100, time.time())

    assert result is False
    assert "FAIL (Cannot stat: Cannot stat)" in msg

def test_check_file_integrity_stat_exception(basic_undo_manager, tmp_path, mocker):
    """Test _check_file_integrity handles generic Exception during stat."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_log = mocker.patch('rename_app.undo_manager.log')
    mock_path = MagicMock(spec=Path)
    mock_path.stat.side_effect = Exception("Unexpected stat error")

    # Provide some dummy logged data
    result, msg = basic_undo_manager._check_file_integrity(mock_path, 100, time.time())

    assert result is False
    assert "FAIL (Check Error)" in msg
    mock_log.exception.assert_called_once()
    assert "Unexpected error during integrity check" in mock_log.exception.call_args[0][0]

def test_check_file_integrity_no_stats(basic_undo_manager, tmp_path):
    """Test _check_file_integrity skips if no stats logged."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_path = MagicMock(spec=Path) # Path doesn't need to exist

    result, msg = basic_undo_manager._check_file_integrity(mock_path, None, time.time())
    assert result is True
    assert "Skipped (no stats)" in msg

    result, msg = basic_undo_manager._check_file_integrity(mock_path, 100, None)
    assert result is True
    assert "Skipped (no stats)" in msg

def test_check_file_integrity_fail_both(manager_integrity_check, tmp_path):
    """Test integrity check fails both size and mtime."""
    if not manager_integrity_check.is_enabled: pytest.skip("Undo disabled")
    p = tmp_path / "both_fail.txt"
    p.write_text("Original short content")
    original_stat = p.stat()
    time.sleep(MTIME_TOLERANCE + 0.1) # Ensure time difference exceeds tolerance

    # Logged stats are different
    logged_size = original_stat.st_size + 10
    logged_mtime = original_stat.st_mtime - (MTIME_TOLERANCE * 5)

    result, msg = manager_integrity_check._check_file_integrity(p, logged_size, logged_mtime)

    assert result is False
    assert "FAIL (Size" in msg
    assert "MTime" in msg
    assert f"{original_stat.st_size} != {logged_size}" in msg # Check size part
    assert f"{original_stat.st_mtime:.2f} !~= {logged_mtime:.2f}" in msg # Check mtime part

# --- perform_undo Tests - More Error Paths and Edge Cases ---

def test_init_db_connect_error(mocker):
    """Test that manager init fails if _connect fails during _init_db."""
    mock_cfg = MagicMock()
    mock_cfg.side_effect = lambda k, d=None: {'enable_undo': True, 'undo_db_path': 'dummy/path/db'}.get(k,d)
    mocker.patch('rename_app.undo_manager.UndoManager._resolve_db_path', return_value=Path('dummy/path/db'))
    # Make _connect raise RenamerError when called by _init_db
    mock_connect = mocker.patch('rename_app.undo_manager.UndoManager._connect', side_effect=RenamerError("Connection failed during init"))
    mock_log = mocker.patch('rename_app.undo_manager.log')

    manager = UndoManager(cfg_helper=mock_cfg)

    assert manager.is_enabled is False
    mock_connect.assert_called_once() # Check _connect was called
    mock_log.exception.assert_called_once() # Check the final exception log
    # Check that the log message contains the specific error from RenamerError
    assert "Failed to initialize UndoManager: Failed to connect during undo database initialization: Connection failed during init" in mock_log.exception.call_args[0][0]

def test_perform_undo_failed_status_update_pending_final(basic_undo_manager, tmp_path, mocker):
    """Test error logging when status update fails after successful pending_final revert."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    src = tmp_path / "suf_pf.txt"; final_dest = tmp_path / "sum_pf.txt"
    unique_suffix = str(time.time()).replace('.', '')
    temp_path = final_dest.parent / f"{final_dest.stem}{TEMP_SUFFIX_PREFIX}{unique_suffix}{final_dest.suffix}"
    src.write_text("pf_content")
    basic_undo_manager.log_action("batch_suf_pf", str(src), str(final_dest), 'file', 'pending_final')
    src.rename(temp_path) # Simulate move to temp path
    assert not src.exists() and temp_path.exists()

    # Mock update_action_status to *always* return False
    mocker.patch.object(basic_undo_manager, 'update_action_status', return_value=False)
    mocker.patch("builtins.input", return_value="y")
    spy_rename = mocker.spy(Path, "rename")

    result = basic_undo_manager.perform_undo("batch_suf_pf")

    assert src.exists(), "Source file should be restored"
    assert not temp_path.exists(), "Temp file should be gone"
    assert result is False, "Result should indicate failure if status update fails"
    spy_rename.assert_called_once_with(temp_path, src) # Verify rename was attempted

    # Check that the error for the failed status update was logged
    found_log = False
    for call_args in mock_logger.error.call_args_list:
        if "Temp file revert successful, but FAILED to update status to 'reverted'" in call_args[0][0] and f"for '{src}'" in call_args[0][0]:
             found_log = True
             break
    assert found_log, "Expected log message about failed status update for pending_final not found"

def test_perform_undo_failed_status_update_created_dir(basic_undo_manager, tmp_path, mocker):
    """Test error logging when status update fails after successful created_dir revert."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    created_dir = tmp_path / "suf_cd"
    basic_undo_manager.log_action("batch_suf_cd", str(created_dir), str(created_dir), 'dir', 'created_dir')
    created_dir.mkdir()
    assert created_dir.exists()

    # Mock update_action_status to *always* return False
    mocker.patch.object(basic_undo_manager, 'update_action_status', return_value=False)
    mocker.patch("builtins.input", return_value="y")
    spy_rmdir = mocker.spy(Path, "rmdir")

    result = basic_undo_manager.perform_undo("batch_suf_cd")

    assert not created_dir.exists(), "Directory should be removed"
    assert result is False, "Result should indicate failure if status update fails"
    spy_rmdir.assert_called_once_with(created_dir) # Verify rmdir was attempted

    # Check that the error for the failed status update was logged
    found_log = False
    for call_args in mock_logger.error.call_args_list:
        if "Directory removed successfully, but FAILED to update status to 'reverted'" in call_args[0][0] and f"for '{created_dir}'" in call_args[0][0]:
             found_log = True
             break
    assert found_log, "Expected log message about failed status update for created_dir not found"

def test_perform_undo_created_dir_iterdir_oserror(basic_undo_manager, tmp_path, mocker, capsys):
    """Test perform_undo handles OSError during iterdir check for created_dir."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    created_dir = tmp_path / "cd_iter_err"
    basic_undo_manager.log_action("batch_cd_iter", str(created_dir), str(created_dir), 'dir', 'created_dir')
    created_dir.mkdir() # Create the dir

    # Mock iterdir on the specific path object to raise OSError
    mock_created_dir_path = MagicMock(spec=Path)
    mock_created_dir_path.exists.return_value = True
    mock_created_dir_path.is_dir.return_value = True
    mock_created_dir_path.iterdir.side_effect = OSError("Cannot list directory")
    mock_created_dir_path.__str__.return_value = str(created_dir) # For logging/comparison

    mocker.patch("builtins.input", return_value="y")
    # Patch Path constructor to return our mock only for created_dir
    original_path = Path
    def path_side_effect(p):
        return mock_created_dir_path if str(p) == str(created_dir) else original_path(p)
    mocker.patch('rename_app.undo_manager.Path', side_effect=path_side_effect)

    result = basic_undo_manager.perform_undo("batch_cd_iter")
    captured = capsys.readouterr()

    assert result is False
    assert f"Error checking if directory '{created_dir}' is empty: Cannot list directory" in captured.out
    assert created_dir.exists() # Dir should still exist
    mock_logger.error.assert_called_once()
    assert f"OSError checking emptiness of '{created_dir}': Cannot list directory" in mock_logger.error.call_args[0][0]
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_cd_iter",))[0]
    assert log_entry['status'] == 'created_dir' # Status unchanged

def test_perform_undo_created_dir_is_file(basic_undo_manager, tmp_path, mocker, capsys):
    """Test undo skips created_dir if path exists but is a file."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    created_dir_path_str = str(tmp_path / "cd_is_file")
    # Log dir creation
    basic_undo_manager.log_action("batch_cd_file", created_dir_path_str, created_dir_path_str, 'dir', 'created_dir')
    # Create a FILE at that path instead of a directory
    Path(created_dir_path_str).touch()
    assert Path(created_dir_path_str).is_file()

    mocker.patch("builtins.input", return_value="y")
    result = basic_undo_manager.perform_undo("batch_cd_file")
    captured = capsys.readouterr()

    assert result is True # No errors, just skipped
    assert f"Skipped removal: Path '{created_dir_path_str}' exists but is not a directory." in captured.out
    # Check warning log
    found_log = any(
        f"Skipped removal: '{created_dir_path_str}' is not a directory." in call_args[0][0]
        for call_args in mock_logger.warning.call_args_list
    )
    assert found_log
    assert Path(created_dir_path_str).exists() # File should still exist
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_cd_file",))[0]
    assert log_entry['status'] == 'created_dir' # Status unchanged

def test_perform_undo_pending_final_find_temp_fails(basic_undo_manager, tmp_path, mocker, capsys):
    """Test undo skips pending_final if _find_temp_file returns None."""
    # Covers line 410
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    src = tmp_path / "pf_find_fail.txt"; final_dest = tmp_path / "pf_find_dest.txt"
    src.touch() # Src exists, but no temp file will be found
    basic_undo_manager.log_action("batch_pf_find", str(src), str(final_dest), 'file', 'pending_final')

    # Mock _find_temp_file to return None
    mocker.patch.object(basic_undo_manager, '_find_temp_file', return_value=None)
    mocker.patch("builtins.input", return_value="y")

    result = basic_undo_manager.perform_undo("batch_pf_find")
    captured = capsys.readouterr()

    assert result is True # No errors, just skipped
    assert f"Skipped revert: Cannot find temp file for '{final_dest}'" in captured.out
    # Check warning log
    found_log = any(
        f"Skipped revert: Temp file for '{final_dest}' not found." in call_args[0][0]
        for call_args in mock_logger.warning.call_args_list
    )
    assert found_log
    assert src.exists() # Original file should still exist
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_pf_find",))[0]
    assert log_entry['status'] == 'pending_final' # Status unchanged


def test_perform_undo_pending_final_temp_missing(basic_undo_manager, tmp_path, mocker, capsys):
    """Test undo skips pending_final if found temp file doesn't exist."""
    # Covers line 415
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    src = tmp_path / "pf_temp_miss.txt"; final_dest = tmp_path / "pf_temp_miss_dest.txt"
    # Create a path for the temp file, but don't create the file itself
    unique_suffix = str(time.time()).replace('.', '')
    temp_path = final_dest.parent / f"{final_dest.stem}{TEMP_SUFFIX_PREFIX}{unique_suffix}{final_dest.suffix}"
    src.touch()
    basic_undo_manager.log_action("batch_pf_temp_miss", str(src), str(final_dest), 'file', 'pending_final')
    assert not temp_path.exists()

    # Mock _find_temp_file to return the non-existent path
    mocker.patch.object(basic_undo_manager, '_find_temp_file', return_value=temp_path)
    mocker.patch("builtins.input", return_value="y")

    result = basic_undo_manager.perform_undo("batch_pf_temp_miss")
    captured = capsys.readouterr()

    assert result is True # No errors, just skipped
    assert f"Skipped revert: Temp file '{temp_path}' does not exist." in captured.out
    # Check warning log
    found_log = any(
        f"Skipped revert: Temp source '{temp_path}' does not exist." in call_args[0][0]
        for call_args in mock_logger.warning.call_args_list
    )
    assert found_log
    assert src.exists() # Original file should still exist
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_pf_temp_miss",))[0]
    assert log_entry['status'] == 'pending_final' # Status unchanged


def test_perform_undo_pending_final_target_exists(basic_undo_manager, tmp_path, mocker, capsys):
    """Test undo skips pending_final if original target path already exists."""
    # Covers line 420
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    src = tmp_path / "pf_target_exists.txt"; final_dest = tmp_path / "pf_target_dest.txt"
    unique_suffix = str(time.time()).replace('.', '')
    temp_path = final_dest.parent / f"{final_dest.stem}{TEMP_SUFFIX_PREFIX}{unique_suffix}{final_dest.suffix}"
    # Create BOTH the original source and the temp file
    src.write_text("original content")
    temp_path.write_text("temp content")
    basic_undo_manager.log_action("batch_pf_target", str(src), str(final_dest), 'file', 'pending_final')
    assert src.exists() and temp_path.exists()

    mocker.patch("builtins.input", return_value="y")
    # _find_temp_file should work normally

    result = basic_undo_manager.perform_undo("batch_pf_target")
    captured = capsys.readouterr()

    assert result is True # No errors, just skipped
    assert f"Skipped revert: Cannot revert temp file '{temp_path.name}'. Original path '{src}' already exists." in captured.out
    # Check warning log
    found_log = any(
        f"Skipped revert: Target '{src}' already exists." in call_args[0][0]
        for call_args in mock_logger.warning.call_args_list
    )
    assert found_log
    # Ensure files are untouched
    assert src.read_text() == "original content"
    assert temp_path.read_text() == "temp content"
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_pf_target",))[0]
    assert log_entry['status'] == 'pending_final' # Status unchanged

# Review assertion for test_perform_undo_failed_status_update_created_dir
def test_perform_undo_failed_status_update_created_dir_refined(basic_undo_manager, tmp_path, mocker):
    """Refined check for error log when status update fails after successful created_dir revert."""
    # Covers line 452-453
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    created_dir = tmp_path / "suf_cd_refined"
    basic_undo_manager.log_action("batch_suf_cd_ref", str(created_dir), str(created_dir), 'dir', 'created_dir')
    created_dir.mkdir()
    assert created_dir.exists()

    mocker.patch.object(basic_undo_manager, 'update_action_status', return_value=False)
    mocker.patch("builtins.input", return_value="y")
    spy_rmdir = mocker.spy(Path, "rmdir")

    result = basic_undo_manager.perform_undo("batch_suf_cd_ref")

    assert not created_dir.exists(), "Directory should be removed"
    assert result is False, "Result should indicate failure if status update fails"
    spy_rmdir.assert_called_once_with(created_dir)

    # Refined Check: Ensure the specific error log call happened
    found_log = False
    expected_msg_part_1 = "Directory removed successfully, but FAILED to update status to 'reverted'"
    expected_msg_part_2 = f"for '{created_dir}'"
    for call_args, call_kwargs in mock_logger.error.call_args_list:
        if expected_msg_part_1 in call_args[0] and expected_msg_part_2 in call_args[0]:
            found_log = True
            break
    assert found_log, "Expected log message about failed status update for created_dir not found"

# In test_undo_manager.py

def test_perform_undo_main_loop_exception(basic_undo_manager, tmp_path, mocker):
    """Test the main undo loop's generic exception handler by failing rename."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    src=tmp_path/"loop_exc.txt"; dest=tmp_path/"loop_moved.txt"
    src_str = str(src); dest_str = str(dest)
    src.write_text("test")
    basic_undo_manager.log_action("batch_loop_exc", src_str, dest_str, 'file', 'moved')
    src.rename(dest) # Perform the move

    error_msg = "Unexpected rename error during loop test"
    original_rename = Path.rename # Store original

    # --- FIX: Mock Path.rename instead of Path.exists ---
    def rename_side_effect(self, target):
        # Check if this is the specific rename call we expect inside the undo operation
        # 'self' is the Path instance being renamed (current_src, which is the dest object)
        # 'target' is the destination path (target_dest, which is the src object)
        if str(self) == dest_str and str(target) == src_str:
            # Raise an unexpected error *instead* of OSError
            raise TypeError(error_msg) # Using TypeError to ensure it hits the generic except block
        else:
            # Allow other rename calls (if any) to proceed normally
            # Note: This shouldn't happen in this specific test's flow
            return original_rename(self, target)

    # Patch the rename method
    mocker.patch('pathlib.Path.rename', side_effect=rename_side_effect, autospec=True)
    # --- End FIX ---

    mocker.patch("builtins.input", return_value="y")

    result = basic_undo_manager.perform_undo("batch_loop_exc")

    assert result is False # Undo failed due to the error
    # Check the generic exception log message caught *within* the rename try/except
    mock_logger.exception.assert_called_once()
    # Check the specific log message for rename failure
    assert f"Unexpected error reverting '{dest}' to '{src}'" in mock_logger.exception.call_args[0][0]
    assert error_msg in mock_logger.exception.call_args[0][0]
    # Check the file system state (should be unchanged from before undo attempt)
    assert not src.exists()
    assert dest.exists()
    # Check DB status unchanged
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_loop_exc",))[0]
    assert log_entry['status'] == 'moved'

# --- perform_undo Tests - Error Paths and Edge Cases ---
def test_perform_undo_disabled(tmp_path, capsys):
    cfg_disabled = lambda k, d=None: {'enable_undo': False}.get(k, d)
    manager_disabled = UndoManager(cfg_helper=cfg_disabled)
    assert not manager_disabled.is_enabled

    result = manager_disabled.perform_undo("some_batch")
    captured = capsys.readouterr()

    assert result is False
    # Check message printed by the updated code
    assert "Error: Undo logging was not enabled or manager failed initialization." in captured.out

def test_perform_undo_db_not_found(basic_undo_manager, capsys):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    db_path = basic_undo_manager.db_path
    assert db_path is not None

    # Delete the database file
    if db_path.exists():
        db_path.unlink()
    assert not db_path.exists()

    result = basic_undo_manager.perform_undo("some_batch")
    captured = capsys.readouterr()

    assert result is False
     # Check message printed by the updated code
    assert f"Error: Undo database not found at {db_path}" in captured.out

def test_perform_undo_db_fetch_error(basic_undo_manager, mocker, capsys):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")

    # Mock connection to fail during fetch (execute call)
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.execute.side_effect = sqlite3.Error("Fetch Error")
    mocker.patch.object(basic_undo_manager, '_connect', return_value=mock_conn)
    mock_log = mocker.patch('rename_app.undo_manager.log')

    result = basic_undo_manager.perform_undo("batch_fetch_error")
    captured = capsys.readouterr()

    assert result is False
    # Check printed output
    assert "--- Starting UNDO for batch 'batch_fetch_error' ---" in captured.out
    assert "Error accessing undo database: Fetch Error" in captured.out
    # Check logged error
    mock_log.error.assert_called_once()
    assert "Error accessing undo database trying to fetch actions" in mock_log.error.call_args[0][0]
    assert "Fetch Error" in mock_log.error.call_args[0][0]

def test_perform_undo_preview_unknown_status(basic_undo_manager, tmp_path, mocker, capsys):
    """Test preview handles unknown status gracefully."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")

    # Mock DB fetch to return an action with an unexpected status
    unknown_status = 'unexpected_status'
    mock_action_row = {
        'id': 1,
        'original_path': str(tmp_path / "unknown.txt"),
        'new_path': "new_unknown.txt",
        'type': 'file',
        'status': unknown_status,
        'original_size': None,
        'original_mtime': None
    }
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [mock_action_row] # Return list with one action
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    # Make execute return the cursor only for the SELECT query
    mock_conn.execute.return_value = mock_cursor
    mocker.patch.object(basic_undo_manager, '_connect', return_value=mock_conn)

    # Mock input to cancel after preview
    mocker.patch("builtins.input", return_value="n")
    mock_log = mocker.patch('rename_app.undo_manager.log') # Check logs if needed

    result = basic_undo_manager.perform_undo("batch_unknown")
    captured = capsys.readouterr()

    assert result is False # Cancelled
    # Check that the preview printed the unknown status message
    assert f"Unknown/Skipped Status '{unknown_status}'" in captured.out
    assert "Undo operation cancelled by user." in captured.out
    # Check logs if warning was added
    # mock_log.warning.assert_called_once_with(mocker.string.containing(f"Skipping preview for unknown/unhandled status '{unknown_status}'"))


def test_perform_undo_confirmation_eof(basic_undo_manager, tmp_path, mocker, capsys):
    """Test cancellation if input() raises EOFError."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "confirm_eof.txt"
    src.touch()
    basic_undo_manager.log_action("batch_eof", str(src), "new.txt", 'file', 'moved')

    # Mock input to raise EOFError
    mocker.patch("builtins.input", side_effect=EOFError)
    mock_log = mocker.patch('rename_app.undo_manager.log')

    result = basic_undo_manager.perform_undo("batch_eof")
    captured = capsys.readouterr()

    assert result is False # Should be cancelled
    # Check the cancellation message printed by the code's exception handler
    assert "Undo operation cancelled (Error reading input)." in captured.out
    # Check the error was logged
    mock_log.error.assert_called_once_with("Error reading confirmation input: ") # EOFError might have empty message

def test_perform_undo_confirmation_exception(basic_undo_manager, tmp_path, mocker):
    """Test cancellation if input() raises generic Exception."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log') # Use logger patch
    src = tmp_path / "confirm_exc.txt"
    dest = tmp_path / "new.txt"
    src.touch()
    basic_undo_manager.log_action("batch_exc", str(src), str(dest), 'file', 'moved')
    src.rename(dest) # Simulate the move

    # Mock input to raise a generic Exception
    mocker.patch("builtins.input", side_effect=Exception("Input kaboom"))

    result = basic_undo_manager.perform_undo("batch_exc")

    assert result is False # Should be cancelled due to input error
    # Check that the correct log method was called with the expected message
    mock_logger.error.assert_called_once_with("Error reading confirmation input: Input kaboom")
    # Check file state unchanged
    assert not src.exists()
    assert dest.exists()
    # Check DB state unchanged
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_exc",))[0]
    assert log_entry['status'] == 'moved'


def test_perform_undo_failed_status_update(basic_undo_manager, tmp_path, mocker):
    """Test error logging when status update fails after successful revert."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    src = tmp_path / "suf.txt"
    dest = tmp_path / "sum.txt"
    src.write_text("c")
    basic_undo_manager.log_action("batch_suf", str(src), str(dest), 'file', 'moved')
    src.rename(dest)
    assert not src.exists() and dest.exists()

    mocker.patch.object(basic_undo_manager, 'update_action_status', return_value=False)
    mocker.patch("builtins.input", return_value="y")

    result = basic_undo_manager.perform_undo("batch_suf")

    assert src.exists(), "Source file should be restored"
    assert not dest.exists(), "Destination file should be gone"
    assert result is False, "Result should indicate failure if status update fails"

    # Check that the error for the failed status update was logged
    found_log = False
    for call_args in mock_logger.error.call_args_list:
        # Check for the specific log message format from perform_undo
        if "FAILED to update status to 'reverted'" in call_args[0][0] and f"for '{src}'" in call_args[0][0]:
             found_log = True
             break
    assert found_log, "Expected log message about failed status update not found"


def test_perform_undo_revert_os_error(basic_undo_manager, tmp_path, mocker, capsys):
    """Test handling of OSError during file rename revert."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "roe.txt"
    dest = tmp_path / "rom.txt"
    src.write_text("c")
    basic_undo_manager.log_action("batch_roe", str(src), str(dest), 'file', 'moved')
    src.rename(dest)

    mocker.patch('pathlib.Path.rename', side_effect=OSError("Cannot rename back"))
    mocker.patch("builtins.input", return_value="y")
    mock_log = mocker.patch('rename_app.undo_manager.log')

    result = basic_undo_manager.perform_undo("batch_roe")
    captured = capsys.readouterr()

    assert result is False
    assert not src.exists()
    assert dest.exists()
    assert f"Error reverting '{dest.name}' to '{src.name}': Cannot rename back" in captured.out
    # Check logged error
    mock_log.error.assert_called_once()
    assert f"OSError reverting '{dest}' to '{src}': Cannot rename back" in mock_log.error.call_args[0][0]
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_roe",))[0]
    assert log_entry['status'] == 'moved'

def test_perform_undo_revert_exception(basic_undo_manager, tmp_path, mocker, capsys):
    """Test handling of generic Exception during file rename revert."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "re.txt"
    dest = tmp_path / "rem.txt"
    src.write_text("c")
    basic_undo_manager.log_action("batch_re", str(src), str(dest), 'file', 'moved')
    src.rename(dest)

    mocker.patch('pathlib.Path.rename', side_effect=Exception("Revert kaboom"))
    mocker.patch("builtins.input", return_value="y")
    mock_log = mocker.patch('rename_app.undo_manager.log')

    result = basic_undo_manager.perform_undo("batch_re")
    captured = capsys.readouterr()

    assert result is False
    assert not src.exists()
    assert dest.exists()
    assert f"Unexpected error reverting '{dest.name}': Revert kaboom" in captured.out
    # Check logged exception
    mock_log.exception.assert_called_once()
    assert f"Unexpected error reverting '{dest}' to '{src}': Revert kaboom" in mock_log.exception.call_args[0][0]
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_re",))[0]
    assert log_entry['status'] == 'moved'

def test_perform_undo_dir_removal_os_error(basic_undo_manager, tmp_path, mocker, capsys):
    """Test handling of OSError during created directory removal."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    created_dir = tmp_path / "drre"
    basic_undo_manager.log_action("batch_drre", str(created_dir), str(created_dir), 'dir', 'created_dir')
    created_dir.mkdir()

    mocker.patch('pathlib.Path.rmdir', side_effect=OSError("Cannot remove dir"))
    mocker.patch("builtins.input", return_value="y")
    mock_log = mocker.patch('rename_app.undo_manager.log')

    result = basic_undo_manager.perform_undo("batch_drre")
    captured = capsys.readouterr()

    assert result is False
    assert created_dir.exists()
    assert f"Error removing directory '{created_dir}': Cannot remove dir" in captured.out
    # Check logged error
    mock_log.error.assert_called_once()
    assert f"OSError removing directory '{created_dir}': Cannot remove dir" in mock_log.error.call_args[0][0]
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_drre",))[0]
    assert log_entry['status'] == 'created_dir'

def test_perform_undo_dir_does_not_exist_on_cleanup(basic_undo_manager, tmp_path, mocker, capsys):
    """Test handling when created directory is already gone during undo."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    created_dir = tmp_path / "dgone"
    basic_undo_manager.log_action("batch_dgone", str(created_dir), str(created_dir), 'dir', 'created_dir')
    assert not created_dir.exists()

    mocker.patch("builtins.input", return_value="y")
    mock_update = mocker.spy(basic_undo_manager, 'update_action_status')

    result = basic_undo_manager.perform_undo("batch_dgone")
    captured = capsys.readouterr()

    assert not created_dir.exists()
    assert f"Skipped removal: Directory '{created_dir}' does not exist." in captured.out
    # Check debug log message
    found_log = False
    for call_args in mock_logger.debug.call_args_list:
        if f"Skipped removal: Directory '{created_dir}' does not exist." in call_args[0][0]:
            found_log = True
            break
    assert found_log, "Expected debug log about skipping non-existent dir removal not found"
    mock_update.assert_called_once_with("batch_dgone", str(created_dir), 'reverted')
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_dgone",))[0]
    assert log_entry['status'] == 'reverted'
    assert result is True


def test_perform_undo_dir_cleanup_exception(basic_undo_manager, tmp_path, mocker, capsys):
    """Test error handling for generic Exception during dir removal attempt."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    mock_logger = mocker.patch('rename_app.undo_manager.log')
    created_dir = tmp_path / "dir_cleanup_exc"
    basic_undo_manager.log_action("batch_dir_cleanup_exc", str(created_dir), str(created_dir), 'dir', 'created_dir')
    created_dir.mkdir()

    mock_rmdir = mocker.patch('pathlib.Path.rmdir', side_effect=Exception("Cleanup rmdir kaboom"))
    mocker.patch("builtins.input", return_value="y")

    result = basic_undo_manager.perform_undo("batch_dir_cleanup_exc")
    captured = capsys.readouterr()

    assert result is False
    # Check the exception was logged using log.exception
    mock_logger.exception.assert_called_once()
    assert f"Unexpected error removing directory '{created_dir}'" in mock_logger.exception.call_args[0][0]
    # Check printed error message
    assert f"Unexpected error removing directory '{created_dir}': Cleanup rmdir kaboom" in captured.out
    mock_rmdir.assert_called_once()
    assert created_dir.exists()
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("batch_dir_cleanup_exc",))[0]
    assert log_entry['status'] == 'created_dir'


# --- Previously Passing Tests (Re-verify/Adjust based on code changes) ---

def test_connect_success(basic_undo_manager):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    conn = None
    try:
        conn = basic_undo_manager._connect()
        assert conn is not None
        # Check if it's a valid connection (e.g., try executing a simple query)
        cur = conn.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
    finally:
        if conn:
            conn.close()


def test_log_action_success(basic_undo_manager, tmp_path):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    db_path=basic_undo_manager.db_path
    src=tmp_path/"fl.txt"
    dest=tmp_path/"rl.txt"
    src.write_text("c") # Create file before logging 'renamed' status
    basic_undo_manager.log_action("bl",str(src),str(dest),'file','renamed')
    assert _get_log_count(db_path,"bl")==1
    log_entry=_query_db(db_path,"SELECT * FROM rename_log WHERE batch_id = ?",("bl",))[0]
    assert log_entry['original_path']==str(src)
    assert log_entry['new_path']==str(dest)
    assert log_entry['status']=='renamed'
    assert log_entry['type']=='file'
    # Check stats were captured for 'renamed' status
    assert log_entry['original_size'] is not None
    assert log_entry['original_mtime'] is not None


def test_log_action_stores_stats(basic_undo_manager, tmp_path):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    db_path=basic_undo_manager.db_path
    src=tmp_path/"fs.txt"
    dest=tmp_path/"rs.txt"
    content="s"
    src.write_text(content)
    # Get stats *before* logging
    stats=src.stat()
    basic_undo_manager.log_action("bs",str(src),str(dest),'file','moved')
    # src.rename(dest) # Rename doesn't affect the logged stats

    assert _get_log_count(db_path,"bs")==1
    log_entry=_query_db(db_path,"SELECT * FROM rename_log WHERE batch_id = ?",("bs",))[0]
    assert log_entry['original_size']==stats.st_size
    # Use MTIME_TOLERANCE for comparison
    assert abs(log_entry['original_mtime'] - stats.st_mtime) < MTIME_TOLERANCE


def test_record_and_undo_move(basic_undo_manager, tmp_path, mocker):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    src=tmp_path/"m.txt"
    dest=tmp_path/"im.txt"
    src.write_text("hu")
    src_str, dest_str = str(src), str(dest)
    # Log before move
    basic_undo_manager.log_action("bm",src_str,dest_str,'file','renamed')
    # Perform move
    src.rename(dest)
    assert not src.exists() and dest.exists()

    mocker.patch("builtins.input",return_value="y")
    result = basic_undo_manager.perform_undo("bm")

    assert result is True
    assert src.exists() and not dest.exists() and src.read_text()=="hu"
    log_entry=_query_db(basic_undo_manager.db_path,"SELECT status FROM rename_log WHERE batch_id = ?",("bm",))[0]
    assert log_entry['status']=='reverted'


def test_record_and_undo_move_into_created_dir(basic_undo_manager, tmp_path, mocker):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    # Setup paths
    src_parent=tmp_path/"ol"
    src_parent.mkdir()
    src=src_parent/"mmd.txt"
    dest_dir=tmp_path/"ND" # Does not exist initially
    dest=dest_dir/"imd.txt"
    src.write_text("hdu")
    src_str, dest_str, dest_dir_str = str(src), str(dest), str(dest_dir)

    # Log actions (order matters for undo)
    # 1. Log directory creation *before* it's created (simulating plan)
    basic_undo_manager.log_action("bd", dest_dir_str, dest_dir_str, 'dir', 'created_dir')
     # 2. Log file move *before* it happens
    basic_undo_manager.log_action("bd", src_str, dest_str, 'file', 'moved')

    # Perform actions
    dest_dir.mkdir()
    src.rename(dest)
    assert not src.exists() and dest.exists() and dest_dir.exists()

    mocker.patch("builtins.input",return_value="y")
    result = basic_undo_manager.perform_undo("bd") # Batch 'bd'

    assert result is True
    # Check final state: file back, dir removed
    assert src.exists() and not dest.exists() and not dest_dir.exists()
    assert src.read_text()=="hdu"

    # Check DB status for both actions
    logs = _query_db(basic_undo_manager.db_path,"SELECT original_path, status FROM rename_log WHERE batch_id = ?",("bd",))
    statuses = {log['original_path']: log['status'] for log in logs}
    assert statuses.get(src_str) == 'reverted'
    assert statuses.get(dest_dir_str) == 'reverted'


def test_batch_undo_multiple_actions(basic_undo_manager, tmp_path, mocker):
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    f1=tmp_path/"mf1.txt"; f1s=str(f1); m1=tmp_path/"mm1.txt"; m1s=str(m1)
    f2=tmp_path/"mf2.txt"; f2s=str(f2); m2=tmp_path/"mm2.txt"; m2s=str(m2)
    f1.write_text("one"); f2.write_text("two")

    # Log actions before performing them
    basic_undo_manager.log_action("bmm",f1s,m1s,'file','renamed')
    basic_undo_manager.log_action("bmm",f2s,m2s,'file','renamed')

    # Perform actions
    f1.rename(m1); f2.rename(m2)
    assert not f1.exists() and not f2.exists()
    assert m1.exists() and m2.exists()

    mocker.patch("builtins.input",return_value="y")
    result = basic_undo_manager.perform_undo("bmm")

    assert result is True
    assert f1.exists() and f1.read_text()=="one"
    assert f2.exists() and f2.read_text()=="two"
    assert not m1.exists() and not m2.exists()

    # Check DB status
    logs=_query_db(basic_undo_manager.db_path,"SELECT status FROM rename_log WHERE batch_id = ?",("bmm",))
    assert len(logs) == 2
    assert all(log['status']=='reverted' for log in logs)


def test_undo_with_missing_current_file(basic_undo_manager, tmp_path, mocker, capsys):
    """Undo should skip if the 'current' file (new_path) is missing."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    db_path=basic_undo_manager.db_path
    src=tmp_path/"om.txt"
    dest=tmp_path/"dm.txt"
    # Log the action, but DON'T perform the rename/move
    basic_undo_manager.log_action("bm",str(src),str(dest),'file','moved')
    # src.touch() # Original exists, but destination does not
    assert not dest.exists()
    assert _get_log_count(db_path,"bm")==1

    mocker.patch("builtins.input",return_value="y")
    result = basic_undo_manager.perform_undo("bm")
    captured = capsys.readouterr()

    assert result is True # No successful actions
    # Files should be unchanged (dest still doesn't exist)
    assert not dest.exists()
    # Check the skip message is printed
    assert f"Skipped revert: File to revert from does not exist: '{dest}'" in captured.out
    # Check DB status is unchanged
    log_entry=_query_db(db_path,"SELECT status FROM rename_log WHERE batch_id = ?",("bm",))[0]
    assert log_entry['status']=='moved'


def test_undo_does_not_crash_with_empty_log(basic_undo_manager, tmp_path, mocker, capsys):
    """Test perform_undo with a batch_id that has no records."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    # No actions logged for 'empty_batch_id'

    # No need to mock input as it won't be reached
    # mocker.patch("builtins.input",return_value="y")
    result = basic_undo_manager.perform_undo("empty_batch_id")
    captured = capsys.readouterr()

    assert result is False # No actions found
    # Check the message printed by the code
    assert f"No revertible actions found for batch 'empty_batch_id'." in captured.out


def test_undo_target_already_exists(basic_undo_manager, tmp_path, mocker, capsys):
    """Test undo skips if the original path already exists."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "oe.txt"
    dest = tmp_path / "me.txt"
    src.write_text("original content")
    # Log action
    basic_undo_manager.log_action("be", str(src), str(dest), 'file', 'moved')
    # Perform action
    src.rename(dest)
    # Create a *new* file at the original location
    src.write_text("new content at original location")
    assert src.exists() and dest.exists()

    mocker.patch("builtins.input", return_value="y")
    result = basic_undo_manager.perform_undo("be")
    captured = capsys.readouterr()

    assert result is True # Skipped action
    # Check the skip message
    assert f"Skipped revert: Cannot revert '{dest.name}'. Original path '{src}' already exists." in captured.out
    # Check files are unchanged from before undo attempt
    assert src.exists() and src.read_text() == "new content at original location"
    assert dest.exists() and dest.read_text() == "original content"
    # Check DB status unchanged
    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("be",))[0]
    assert log_entry['status'] == 'moved'


def test_undo_created_dir_empty(basic_undo_manager, tmp_path, mocker):
    """Test undo removes an empty created directory."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    db_path = basic_undo_manager.db_path
    created_dir = tmp_path / "nce"
    # Log creation
    basic_undo_manager.log_action("bcd", str(created_dir), str(created_dir), 'dir', 'created_dir')
    # Perform creation
    created_dir.mkdir()
    assert created_dir.is_dir()

    mocker.patch("builtins.input", return_value="y")
    result = basic_undo_manager.perform_undo("bcd")

    assert result is True
    # Directory should be removed
    assert not created_dir.exists()
    # Check DB status
    log_entry = _query_db(db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("bcd",))[0]
    assert log_entry['status'] == 'reverted'


def test_undo_created_dir_not_empty(basic_undo_manager, tmp_path, mocker, capsys):
    """Test undo skips removing a created directory if it's not empty."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    db_path = basic_undo_manager.db_path
    created_dir = tmp_path / "ncne"
    # Log creation
    basic_undo_manager.log_action("bne", str(created_dir), str(created_dir), 'dir', 'created_dir')
    # Perform creation and add a file
    created_dir.mkdir()
    (created_dir / "f.txt").touch()
    assert created_dir.is_dir()

    mocker.patch("builtins.input", return_value="y")
    result = basic_undo_manager.perform_undo("bne")
    captured = capsys.readouterr()

    assert result is True # Skipped action
    # Directory should still exist because it wasn't empty
    assert created_dir.exists()
    # Check skip message
    assert f"Skipped removal: Directory '{created_dir}' is not empty." in captured.out
    # Check DB status unchanged
    log_entry = _query_db(db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("bne",))[0]
    assert log_entry['status'] == 'created_dir'


def test_undo_integrity_check_success(manager_integrity_check, tmp_path, mocker):
    """Test successful undo when integrity check is enabled and passes."""
    if not manager_integrity_check.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "iok.txt"
    dest = tmp_path / "imok.txt"
    content = "ok"
    src.write_text(content)
    # Log before move (captures stats)
    manager_integrity_check.log_action("biok", str(src), str(dest), 'file', 'moved')
    # Perform move
    src.rename(dest)
    assert not src.exists() and dest.exists()

    mocker.patch("builtins.input", return_value="y")
    result = manager_integrity_check.perform_undo("biok")

    assert result is True
    assert src.exists() and src.read_text() == content and not dest.exists()
    log_entry = _query_db(manager_integrity_check.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("biok",))[0]
    assert log_entry['status'] == 'reverted'


def test_undo_integrity_check_fail_size(manager_integrity_check, tmp_path, mocker, capsys):
    """Test undo skip when integrity check fails due to size mismatch."""
    if not manager_integrity_check.is_enabled: pytest.skip("Undo disabled")
    src=tmp_path/"is.txt"
    dest=tmp_path/"ims.txt"
    src.write_text("s") # Original size 1
    # Log action (captures size 1)
    manager_integrity_check.log_action("bis",str(src),str(dest),'file','moved')
    # Perform move
    src.rename(dest)
    # Modify file so size changes
    dest.write_text("loooooonger") # New size > 1
    assert not src.exists() and dest.exists()

    mocker.patch("builtins.input",return_value="y")
    result = manager_integrity_check.perform_undo("bis")
    captured = capsys.readouterr()

    assert result is True # Skipped action
    # Files unchanged from before undo attempt
    assert not src.exists() and dest.exists()
    assert dest.read_text() == "loooooonger"
    # Check output for integrity fail message
    assert "Integrity check for 'ims.txt': FAIL (Size" in captured.out # Checks prefix
    assert "Skipping revert due to integrity check failure." in captured.out
    # Check DB status unchanged
    log_entry=_query_db(manager_integrity_check.db_path,"SELECT status FROM rename_log WHERE batch_id = ?",("bis",))[0]
    assert log_entry['status']=='moved'


def test_undo_integrity_check_fail_mtime(manager_integrity_check, tmp_path, mocker, capsys):
    """Test undo skip when integrity check fails due to mtime mismatch."""
    if not manager_integrity_check.is_enabled: pytest.skip("Undo disabled")
    src=tmp_path/"imt.txt"
    dest=tmp_path/"immt.txt"
    content="m"
    src.write_text(content)
    time.sleep(0.05) # Ensure original mtime is distinct enough
    manager_integrity_check.log_action("bim",str(src),str(dest),'file','moved')
    original_mtime = _query_db(manager_integrity_check.db_path,"SELECT original_mtime FROM rename_log WHERE batch_id = ?",("bim",))[0]['original_mtime']
    src.rename(dest)

    # --- FIX: Modify ONLY mtime using os.utime ---
    time.sleep(0.01) # Short sleep before touching
    current_time = time.time()
    # Set mtime significantly different (e.g., 5 seconds newer), keep atime same
    os.utime(dest, (dest.stat().st_atime, current_time + 5))
    # --- End FIX ---

    new_mtime=dest.stat().st_mtime
    assert abs(new_mtime - original_mtime) > MTIME_TOLERANCE, "MTime should differ significantly"
    assert dest.stat().st_size == len(content), "Size should not have changed"

    mocker.patch("builtins.input",return_value="y")
    result = manager_integrity_check.perform_undo("bim")
    captured = capsys.readouterr()

    assert result is True # Skipped action
    assert not src.exists() and dest.exists()
    # Check output for integrity fail message specific to MTime
    assert "Integrity check for 'immt.txt': FAIL (MTime" in captured.out
    # Ensure Size failure is NOT mentioned in the *same* integrity check line
    fail_line = [line for line in captured.out.splitlines() if "Integrity check for 'immt.txt': FAIL" in line][0]
    assert "Size" not in fail_line
    assert "Skipping revert due to integrity check failure." in captured.out
    log_entry=_query_db(manager_integrity_check.db_path,"SELECT status FROM rename_log WHERE batch_id = ?",("bim",))[0]
    assert log_entry['status']=='moved'

def test_undo_failed_transaction_temp_file(basic_undo_manager, tmp_path, mocker):
    """Test undoing a 'pending_final' action reverts the temp file."""
    if not basic_undo_manager.is_enabled: pytest.skip("Undo disabled")
    src = tmp_path / "of.txt"
    final_dest = tmp_path / "ff.txt"
    unique_suffix = str(time.time()).replace('.', '')
    temp_path = final_dest.parent / f"{final_dest.stem}{TEMP_SUFFIX_PREFIX}{unique_suffix}{final_dest.suffix}"
    src.write_text("failed")

    basic_undo_manager.log_action("bft", str(src), str(final_dest), 'file', 'pending_final')
    src.rename(temp_path)
    assert not src.exists() and temp_path.exists() and not final_dest.exists()

    mocker.patch("builtins.input", return_value="y")
    mock_find_temp = mocker.spy(basic_undo_manager, '_find_temp_file')
    result = basic_undo_manager.perform_undo("bft")

    assert result is True
    assert src.exists() and src.read_text() == "failed"
    assert not temp_path.exists()
    assert not final_dest.exists()

    # --- FIX: Use assert_any_call ---
    mock_find_temp.assert_any_call(final_dest)
    # --- End FIX ---

    log_entry = _query_db(basic_undo_manager.db_path, "SELECT status FROM rename_log WHERE batch_id = ?", ("bft",))[0]
    assert log_entry['status'] == 'reverted'

# --- END tests/test_undo_manager.py ---