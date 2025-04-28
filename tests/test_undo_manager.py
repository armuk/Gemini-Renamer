# tests/test_undo_manager.py

import pytest
from unittest.mock import MagicMock
from rename_app.undo_manager import UndoManager
from rename_app.exceptions import RenamerError

@pytest.fixture
def undo_manager(tmp_path):
    cfg = lambda k, d=None: {
        'enable_undo': True,
        'undo_db_path': str(tmp_path / "undo.db")
    }.get(k, d)
    return UndoManager(cfg_helper=cfg)

# -----------------------
# Connection Tests
# -----------------------
def test_connect_success(undo_manager):
    conn = undo_manager._connect()
    assert conn is not None
    conn.close()

def test_connect_failure(mocker, tmp_path):
    broken_cfg = MagicMock()
    broken_cfg.side_effect = lambda k, d=None: {
        'enable_undo': True,
        'undo_db_path': str(tmp_path / "broken.db")
    }.get(k, d)
    mocker.patch("sqlite3.connect", side_effect=Exception("Cannot create"))
    with pytest.raises(RenamerError):
        UndoManager(cfg_helper=broken_cfg)

# -----------------------
# Action Logging and Status Update
# -----------------------
def test_log_action_success(undo_manager, tmp_path):
    src = tmp_path / "file.txt"
    dest = tmp_path / "renamed.txt"
    src.write_text("hi")
    src.rename(dest)
    undo_manager.log_action("batch", dest, src, 'file', 'renamed')


def test_update_action_status_success(undo_manager, tmp_path):
    src = tmp_path / "one.txt"
    src.write_text("content")
    undo_manager.log_action("batch2", src, tmp_path / "two.txt", 'file', 'moved')
    undo_manager.update_action_status("batch2", str(src), "reverted")


def test_update_action_status_failure(undo_manager):
    with pytest.raises(RenamerError):
        undo_manager.update_action_status("fakebatch", "missing.txt", "reverted")

# -----------------------
# Undo Functionality Tests
# -----------------------

def test_record_and_undo_move(tmp_path, undo_manager, mocker):
    src = tmp_path / "move.txt"
    dest = tmp_path / "moved.txt"
    src.write_text("hello")
    src.rename(dest)
    undo_manager.log_action("batch3", dest, src, 'file', 'renamed')

    mocker.patch("builtins.input", return_value="y")
    undo_manager.perform_undo("batch3")

    assert src.exists()


def test_record_and_undo_trash(tmp_path, undo_manager, mocker):
    src = tmp_path / "deleted.txt"
    trash = tmp_path / "trash" / "deleted.txt"
    trash.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("goodbye")
    src.rename(trash)

    undo_manager.log_action("batch4", trash, src, 'file', 'moved')

    mocker.patch("builtins.input", return_value="y")
    undo_manager.perform_undo("batch4")

    assert src.exists()


def test_batch_undo_multiple_actions(tmp_path, undo_manager, mocker):
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    moved1 = tmp_path / "file1_moved.txt"
    moved2 = tmp_path / "file2_moved.txt"
    file1.write_text("one")
    file2.write_text("two")
    file1.rename(moved1)
    file2.rename(moved2)

    undo_manager.log_action("batch5", moved1, file1, 'file', 'renamed')
    undo_manager.log_action("batch5", moved2, file2, 'file', 'renamed')

    mocker.patch("builtins.input", return_value="y")
    undo_manager.perform_undo("batch5")

    assert file1.exists()
    assert file2.exists()


def test_undo_with_missing_file(tmp_path, undo_manager, mocker):
    fake_file = tmp_path / "missing.txt"
    undo_manager.log_action("batch6", fake_file, tmp_path / "dest.txt", 'file', 'renamed')
    mocker.patch("builtins.input", return_value="y")
    undo_manager.perform_undo("batch6")


def test_undo_does_not_crash_with_empty_log(undo_manager, mocker):
    mocker.patch("builtins.input", return_value="n")
    undo_manager.perform_undo("emptybatch")

# -----------------------
# Directory Utilities
# -----------------------

def test_remove_empty_directory(tmp_path, undo_manager):
    empty_dir = tmp_path / "empty_folder"
    empty_dir.mkdir()
    undo_manager._try_remove_dir_if_empty(empty_dir)
    assert not empty_dir.exists()


def test_skip_nonempty_directory_removal(tmp_path, undo_manager):
    non_empty = tmp_path / "non_empty"
    non_empty.mkdir()
    (non_empty / "file.txt").write_text("data")
    undo_manager._try_remove_dir_if_empty(non_empty)
    assert non_empty.exists()

# -----------------------
# Expiration Handling
# -----------------------

def test_prune_expired_batches(tmp_path):
    cfg = lambda k, d=None: {
        'enable_undo': True,
        'undo_db_path': str(tmp_path / "undo.db"),
        'undo_expire_days': 0  # Immediately expire
    }.get(k, d)
    undo_manager = UndoManager(cfg_helper=cfg)

    src = tmp_path / "oldfile.txt"
    dest = tmp_path / "newfile.txt"
    src.write_text("data")
    src.rename(dest)

    undo_manager.log_action("oldbatch", dest, src, 'file', 'renamed')
    undo_manager.prune_old_batches()

# END
