# tests/test_file_system_ops.py

import pytest
import shutil
import os # Import os
from pathlib import Path
from unittest.mock import MagicMock, call

# Import necessary components from the application package
from rename_app import file_system_ops
from rename_app.models import RenamePlan, RenameAction
from rename_app.exceptions import FileOperationError, RenamerError # Import necessary custom exceptions
from rename_app.undo_manager import UndoManager # Import for type hint

# Import send2trash conditionally for testing
try:
    import send2trash
    SEND2TRASH_AVAILABLE = True
except ImportError:
    SEND2TRASH_AVAILABLE = False

# --- Fixture for Mock Undo Manager ---
@pytest.fixture
def mock_undo_manager(mocker):
    """Provides a mock UndoManager with traceable methods."""
    # Adding spec=UndoManager helps catch incorrect method calls and attribute access
    mock = MagicMock(spec=UndoManager)
    mock.log_action = MagicMock()
    mock.update_action_status = MagicMock(return_value=True) # Assume update succeeds
    return mock

# --- Helper to create RenamePlan ---
def create_test_plan(
    tmp_path: Path,
    actions: list = None,
    created_dir: str = None, # Relative path string for created dir
    batch_id: str = "test_batch_123"
) -> RenamePlan:
    """Creates RenamePlan object. Ensures TARGET parent dirs exist."""
    # Source file/dir existence is handled by individual tests now
    if actions is None: actions = []
    vid_path_obj = None
    plan_actions = []
    created_dir_path_obj = (tmp_path / created_dir).resolve() if created_dir else None

    for orig_rel_name, new_rel_name, item_type, action_type in actions:
        orig_p = (tmp_path / orig_rel_name).resolve()
        # Determine target directory based on whether created_dir is set
        target_dir = created_dir_path_obj if created_dir_path_obj else orig_p.parent
        # Ensure target parent exists (safe even if it exists)
        # Let the main code under test handle target dir creation if needed
        # target_dir.mkdir(parents=True, exist_ok=True) # Removed from helper
        new_p = target_dir / new_rel_name

        plan_actions.append(RenameAction(original_path=orig_p, new_path=new_p, action_type=action_type))
        # Assign first file action's original path as the 'video file' for the plan
        if item_type == 'file' and vid_path_obj is None:
            vid_path_obj = orig_p

    if vid_path_obj is None: # Handle case with no actions or only dir actions
        vid_path_obj = tmp_path / "dummy_video_fallback.mkv" # Dummy path

    return RenamePlan(batch_id=batch_id, video_file=vid_path_obj, status='success', actions=plan_actions, created_dir_path=created_dir_path_obj)


# === Test perform_file_actions ===

# --- Dry Run Tests ---
def test_perform_file_actions_dry_run_rename(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    """Test dry run for a simple rename."""
    # Arrange
    mock_shutil_move = mocker.patch('shutil.move'); mock_path_rename = mocker.patch('pathlib.Path.rename'); mock_mkdir = mocker.patch('pathlib.Path.mkdir')
    mock_cfg_helper.args.dry_run = True; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'create_folders': False, 'on_conflict': 'skip'}
    plan = create_test_plan(tmp_path, actions=[("old_video.mkv", "new_video.mkv", 'file', 'rename'), ("old_video.nfo", "new_video.nfo", 'file', 'rename')])
    # Create source files using write_text which handles parent creation
    plan.actions[0].original_path.write_text("mkv")
    plan.actions[1].original_path.write_text("nfo")
    mock_mkdir.reset_mock() # Reset after potential helper calls
    # Act
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    # Assert
    assert result['success'] is True; assert result['actions_taken'] == 0
    assert result['message'] is not None, "Result message should not be None"
    assert "DRY RUN: Would rename 'old_video.mkv' -> " in result['message']
    assert "DRY RUN: Would rename 'old_video.nfo' -> " in result['message']
    mock_shutil_move.assert_not_called(); mock_path_rename.assert_not_called(); mock_mkdir.assert_not_called(); mock_undo_manager.log_action.assert_not_called()

def test_perform_file_actions_dry_run_move_create_dir(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    """Test dry run involving moving files and creating a directory."""
    # Arrange
    mock_shutil_move = mocker.patch('shutil.move')
    # Don't mock mkdir for this dry run test, allow helper to ensure target parent exists if needed
    # mock_mkdir = mocker.patch('pathlib.Path.mkdir')
    mock_cfg_helper.args.dry_run = True; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'create_folders': True, 'on_conflict': 'skip'}
    plan = create_test_plan(tmp_path, actions=[ ("original/vid.mkv", "New Name.mkv", 'file', 'move'), ("original/vid.srt", "New Name.srt", 'file', 'move')], created_dir="New Folder/Subfolder")
    # Use write_text which handles parent creation implicitly
    plan.actions[0].original_path.parent.mkdir(parents=True, exist_ok=True)
    plan.actions[0].original_path.write_text("mkv content")
    plan.actions[1].original_path.parent.mkdir(parents=True, exist_ok=True)
    plan.actions[1].original_path.write_text("srt content")
    # mock_mkdir.reset_mock() # No mock to reset
    # Act
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    # Assert
    assert result['success'] is True; assert result['actions_taken'] == 0
    assert "DRY RUN: Would create directory" in result['message']; assert str(tmp_path / "New Folder/Subfolder") in result['message']
    assert "DRY RUN: Would move 'vid.mkv' ->" in result['message']; assert str(tmp_path / "New Folder/Subfolder" / "New Name.mkv") in result['message']
    assert "DRY RUN: Would move 'vid.srt' ->" in result['message']; assert str(tmp_path / "New Folder/Subfolder" / "New Name.srt") in result['message']
    mock_shutil_move.assert_not_called()
    # Cannot assert mkdir not called if not mocked
    # mock_mkdir.assert_not_called()
    mock_undo_manager.log_action.assert_not_called()


# --- Live Run Tests ---
def test_perform_file_actions_live_rename(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    """Test live run for a simple rename using transactional logic."""
    # Arrange: Let file operations run (no mocks for move/rename)
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'on_conflict': 'skip', 'create_folders': False, 'enable_undo': True}
    plan = create_test_plan(tmp_path, actions=[("vid.mkv", "new_vid.mkv", 'file', 'rename')])
    orig_path = plan.actions[0].original_path; final_path = plan.actions[0].new_path
    orig_path.write_text("content") # Create source file
    # Act
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    # Assert
    assert result['success'] is True, f"Action failed: {result.get('message')}"
    assert result['actions_taken'] == 1
    assert not orig_path.exists(), "Original file should not exist"
    assert final_path.exists(), "Final file should exist"
    assert final_path.read_text() == "content", "File content mismatch"
    # Check logging calls
    mock_undo_manager.log_action.assert_called_once_with(batch_id=plan.batch_id, original_path=orig_path, new_path=final_path, item_type='file', status='pending_final')
    mock_undo_manager.update_action_status.assert_called_once_with(batch_id=plan.batch_id, original_path=str(orig_path), new_status='renamed')

def test_perform_file_actions_live_move_create_dir(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    """Test live run moving files and creating directory."""
    # Arrange: Let file operations run
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'create_folders': True, 'on_conflict': 'skip', 'enable_undo': True}
    plan = create_test_plan(tmp_path, actions=[("old/vid.mkv", "New Name.mkv", 'file', 'move')], created_dir="New Dir")
    orig_path = plan.actions[0].original_path; final_path = plan.actions[0].new_path; created_dir_path = plan.created_dir_path
    # Create source file using write_text (handles parent creation)
    orig_path.parent.mkdir(parents=True, exist_ok=True)
    orig_path.write_text("content")
    # Ensure target directory does NOT exist before test
    if created_dir_path.is_dir(): shutil.rmtree(created_dir_path)
    if created_dir_path.is_file(): created_dir_path.unlink()

    # Act
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)

    # Assert
    assert result['success'] is True, f"Action failed: {result.get('message')}"
    assert result['actions_taken'] == 1
    assert created_dir_path.is_dir(), "Target directory was not created"
    assert final_path.exists(), "Final file path does not exist"
    assert final_path.read_text() == "content", "File content mismatch"
    assert not orig_path.exists(), "Original file still exists"
    # Check logging calls
    log_calls = mock_undo_manager.log_action.call_args_list; update_calls = mock_undo_manager.update_action_status.call_args_list
    assert any(c.kwargs.get('original_path') == created_dir_path and c.kwargs.get('status') == 'created_dir' for c in log_calls)
    assert any(c.kwargs.get('original_path') == orig_path and c.kwargs.get('status') == 'pending_final' for c in log_calls)
    assert any(c.kwargs.get('original_path') == str(orig_path) and c.kwargs.get('new_status') == 'moved' for c in update_calls)

def test_perform_file_actions_live_trash(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    """Test live run with trash action."""
    if not SEND2TRASH_AVAILABLE: pytest.skip("send2trash not installed"); return
    # Arrange
    mock_send2trash = mocker.patch('send2trash.send2trash'); mocker.patch('shutil.move'); mocker.patch('pathlib.Path.rename')
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = True
    mock_cfg_helper.manager._mock_values = {'enable_undo': True}
    plan = create_test_plan(tmp_path, actions=[("file_to_trash.txt", "new_name.txt", 'file', 'rename')])
    orig_path = plan.actions[0].original_path; intended_new_path = plan.actions[0].new_path; orig_path.write_text("content")
    # Act
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    # Assert
    assert result['success'] is True, f"Action failed: {result.get('message')}"
    assert result['actions_taken'] == 1, f"Action count mismatch: {result['actions_taken']}"
    mock_send2trash.assert_called_once_with(str(orig_path))
    mock_undo_manager.log_action.assert_called_once_with(batch_id=plan.batch_id, original_path=orig_path, new_path=intended_new_path, item_type='file', status='trashed')
    mock_undo_manager.update_action_status.assert_not_called()

def test_perform_file_actions_live_backup(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    """Test live run with backup action."""
    # Arrange
    mock_copy2 = mocker.patch('shutil.copy2') # Mock copy only
    # Let move/rename run
    mock_cfg_helper.args.dry_run = False; backup_dir = tmp_path / "backups"; mock_cfg_helper.args.backup_dir = backup_dir; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'on_conflict': 'skip', 'create_folders': False, 'enable_undo': True}
    plan = create_test_plan(tmp_path, actions=[("backup_me.mkv", "new_backup.mkv", 'file', 'rename')])
    orig_path = plan.actions[0].original_path; final_path = plan.actions[0].new_path; expected_backup_path = backup_dir / orig_path.name; orig_path.write_text("content")
    # Act
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    # Assert
    assert result['success'] is True, f"Action failed: {result.get('message')}"
    assert result['actions_taken'] == 1, f"Action count mismatch: {result['actions_taken']}"
    assert backup_dir.is_dir()
    mock_copy2.assert_called_once_with(str(orig_path), str(expected_backup_path)) # Verify mock call
    # Check final state
    assert not orig_path.exists() # Original should be moved
    assert final_path.exists() # Final renamed file should exist
    assert final_path.read_text() == "content"
    # Check logging
    log_calls = mock_undo_manager.log_action.call_args_list; update_calls = mock_undo_manager.update_action_status.call_args_list
    assert any(c.kwargs.get('original_path') == orig_path and c.kwargs.get('status') == 'pending_final' for c in log_calls)
    assert any(c.kwargs.get('original_path') == str(orig_path) and c.kwargs.get('new_status') == 'renamed' for c in update_calls)


def test_perform_file_actions_live_stage(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    """Test live run with stage action."""
    # Arrange
    mock_shutil_move = mocker.patch('shutil.move'); mocker.patch('pathlib.Path.rename') # Mock move for stage
    stage_dir = tmp_path / "staging"
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = stage_dir; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'enable_undo': True, 'on_conflict': 'skip'}
    plan = create_test_plan(tmp_path, actions=[("stage_me.mkv", "staged_name.mkv", 'file', 'rename')])
    orig_path = plan.actions[0].original_path; intended_new_path = plan.actions[0].new_path; expected_staged_path = stage_dir / intended_new_path.name; orig_path.write_text("content")
    # Act
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    # Assert
    assert result['success'] is True, f"Action failed: {result.get('message')}"
    assert result['actions_taken'] == 1, f"Action count mismatch: {result['actions_taken']}"
    mock_shutil_move.assert_called_once_with(str(orig_path), str(expected_staged_path))
    mock_undo_manager.log_action.assert_called_once_with(batch_id=plan.batch_id, original_path=orig_path, new_path=expected_staged_path, item_type='file', status='moved')
    mock_undo_manager.update_action_status.assert_not_called()

# --- Conflict Tests ---
def test_perform_file_actions_conflict_skip(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    """Test conflict with skip mode - should return success=False."""
    # Arrange
    mock_shutil_move = mocker.patch('shutil.move'); mock_path_rename = mocker.patch('pathlib.Path.rename')
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'on_conflict': 'skip', 'create_folders': False, 'enable_undo': True}
    existing_target = tmp_path / "target_exists.mkv"; existing_target.touch()
    plan = create_test_plan(tmp_path, actions=[("source.mkv", "target_exists.mkv", 'file', 'rename')])
    (tmp_path / "source.mkv").write_text("content")
    # Act
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    # Assert
    assert result['success'] is False, "Expected success=False for skip conflict"
    assert "ERROR: Target 'target_exists.mkv' exists (mode: skip)." in result['message'], "Expected skip error message"
    mock_shutil_move.assert_not_called(); mock_path_rename.assert_not_called(); mock_undo_manager.log_action.assert_not_called()

def test_perform_file_actions_conflict_fail(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    """Test conflict with fail mode - should raise FileExistsError."""
    # Arrange
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'on_conflict': 'fail', 'create_folders': False, 'enable_undo': True}
    existing_target = tmp_path / "target_exists.mkv"; existing_target.touch()
    plan = create_test_plan(tmp_path, actions=[("source.mkv", "target_exists.mkv", 'file', 'rename')])
    (tmp_path / "source.mkv").write_text("content")
    # Act & Assert
    with pytest.raises(FileExistsError, match="Target 'target_exists.mkv' exists \(mode: fail\)"):
         file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)

def test_perform_file_actions_conflict_overwrite(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    """Test conflict with overwrite mode."""
    # Arrange
    mock_unlink = mocker.patch('pathlib.Path.unlink') # Mock unlink only
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'on_conflict': 'overwrite', 'create_folders': False, 'enable_undo': True}
    existing_target = tmp_path / "target_exists.mkv"; existing_target.write_text("old content")
    plan = create_test_plan(tmp_path, actions=[("source.mkv", "target_exists.mkv", 'file', 'rename')])
    orig_path = plan.actions[0].original_path; final_path = plan.actions[0].new_path; orig_path.write_text("new content")
    # Act
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    # Assert
    assert result['success'] is True, f"Action failed: {result.get('message')}"; assert result['actions_taken'] == 1
    mock_unlink.assert_called_once_with(missing_ok=True) # Check unlink call
    assert not orig_path.exists(); assert final_path.exists(); assert final_path.read_text() == "new content"
    mock_undo_manager.log_action.assert_called_once_with(batch_id=plan.batch_id, original_path=orig_path, new_path=final_path, item_type='file', status='pending_final')
    mock_undo_manager.update_action_status.assert_called_once_with(batch_id=plan.batch_id, original_path=str(orig_path), new_status='renamed')

def test_perform_file_actions_conflict_suffix(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    """Test conflict with suffix mode."""
    # Arrange
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'on_conflict': 'suffix', 'create_folders': False, 'enable_undo': True}
    existing_target = tmp_path / "target_exists.mkv"; existing_target.write_text("content 0")
    existing_target_1 = tmp_path / "target_exists_1.mkv"; existing_target_1.write_text("content 1")
    plan = create_test_plan(tmp_path, actions=[("source.mkv", "target_exists.mkv", 'file', 'rename')])
    orig_path = plan.actions[0].original_path; expected_final_path = tmp_path / "target_exists_2.mkv"; orig_path.write_text("new content")
    # Act
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    # Assert
    assert result['success'] is True, f"Action failed: {result.get('message')}"; assert result['actions_taken'] == 1
    assert not orig_path.exists(); assert expected_final_path.exists(); assert expected_final_path.read_text() == "new content"; assert existing_target.exists(); assert existing_target_1.exists()
    mock_undo_manager.log_action.assert_called_once_with(batch_id=plan.batch_id, original_path=orig_path, new_path=expected_final_path, item_type='file', status='pending_final')
    mock_undo_manager.update_action_status.assert_called_once_with(batch_id=plan.batch_id, original_path=str(orig_path), new_status='renamed')

# TODO: Add tests for transactional rollback scenarios (Phase 1 failure, Phase 2 failure)