# tests/test_file_system_ops.py

import pytest
import shutil
import os
from pathlib import Path
from unittest.mock import MagicMock, call

from rename_app import file_system_ops
from rename_app.models import RenamePlan, RenameAction
from rename_app.exceptions import FileOperationError, RenamerError
from rename_app.undo_manager import UndoManager
try: import send2trash; SEND2TRASH_AVAILABLE = True
except ImportError: SEND2TRASH_AVAILABLE = False

# --- Fixture for Mock Undo Manager ---
@pytest.fixture
def mock_undo_manager(mocker):
    mock = MagicMock(spec=UndoManager); mock.log_action = MagicMock(); mock.update_action_status = MagicMock(return_value=True)
    return mock

# --- Helper to create RenamePlan ---
def create_test_plan( tmp_path: Path, actions: list = None, created_dir: str = None, batch_id: str = "test_batch_123" ) -> RenamePlan:
    if actions is None: actions = []
    vid_path = None; plan_actions = []
    created_dir_path = (tmp_path / created_dir).resolve() if created_dir else None
    for orig_rel_name, new_rel_name, item_type, action_type in actions:
        orig_p = (tmp_path / orig_rel_name).resolve()
        target_dir = created_dir_path if created_dir_path else orig_p.parent
        target_dir.mkdir(parents=True, exist_ok=True)
        new_p = target_dir / new_rel_name
        # Ensure original parent dir exists implicitly via write_text later
        # if action_type != 'create_dir':
        #     orig_p.parent.mkdir(parents=True, exist_ok=True)
        plan_actions.append(RenameAction(original_path=orig_p, new_path=new_p, action_type=action_type))
        if item_type == 'file' and vid_path is None: vid_path = orig_p
    if vid_path is None: vid_path = tmp_path / "dummy_video_fallback.mkv"
    return RenamePlan(batch_id=batch_id, video_file=vid_path, status='success', actions=plan_actions, created_dir_path=created_dir_path)


# === Test perform_file_actions ===

# --- Dry Run Tests ---
# (test_perform_file_actions_dry_run_rename unchanged)
def test_perform_file_actions_dry_run_rename(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    mock_shutil_move = mocker.patch('shutil.move'); mock_path_rename = mocker.patch('pathlib.Path.rename'); mock_mkdir = mocker.patch('pathlib.Path.mkdir')
    mock_cfg_helper.args.dry_run = True; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'create_folders': False, 'on_conflict': 'skip'}
    plan = create_test_plan(tmp_path, actions=[("old_video.mkv", "new_video.mkv", 'file', 'rename'), ("old_video.nfo", "new_video.nfo", 'file', 'rename')])
    plan.actions[0].original_path.write_text("mkv"); plan.actions[1].original_path.write_text("nfo") # Use write_text
    mock_mkdir.reset_mock()
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    assert result['success'] is True; assert result['actions_taken'] == 0; assert "DRY RUN: Would rename 'old_video.mkv' -> " in result['message']; assert "DRY RUN: Would rename 'old_video.nfo' -> " in result['message']
    mock_shutil_move.assert_not_called(); mock_path_rename.assert_not_called(); mock_mkdir.assert_not_called(); mock_undo_manager.log_action.assert_not_called()

# --- FIX: test_perform_file_actions_dry_run_move_create_dir ---
def test_perform_file_actions_dry_run_move_create_dir(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    mock_shutil_move = mocker.patch('shutil.move'); mock_mkdir = mocker.patch('pathlib.Path.mkdir')
    mock_cfg_helper.args.dry_run = True; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'create_folders': True, 'on_conflict': 'skip'}
    plan = create_test_plan(tmp_path, actions=[ ("original/vid.mkv", "New Name.mkv", 'file', 'move'), ("original/vid.srt", "New Name.srt", 'file', 'move')], created_dir="New Folder/Subfolder")
    # Use write_text which handles parent creation
    plan.actions[0].original_path.write_text("mkv content")
    plan.actions[1].original_path.write_text("srt content")
    mock_mkdir.reset_mock()
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    assert result['success'] is True; assert result['actions_taken'] == 0
    assert "DRY RUN: Would create directory" in result['message']; assert str(tmp_path / "New Folder/Subfolder") in result['message']
    assert "DRY RUN: Would move 'vid.mkv' ->" in result['message']; assert str(tmp_path / "New Folder/Subfolder" / "New Name.mkv") in result['message']
    assert "DRY RUN: Would move 'vid.srt' ->" in result['message']; assert str(tmp_path / "New Folder/Subfolder" / "New Name.srt") in result['message']
    mock_shutil_move.assert_not_called(); mock_mkdir.assert_not_called(); mock_undo_manager.log_action.assert_not_called()
# --- End Fix ---

# --- Live Run Tests ---
# (test_perform_file_actions_live_rename unchanged)
def test_perform_file_actions_live_rename(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'on_conflict': 'skip', 'create_folders': False, 'enable_undo': True}
    plan = create_test_plan(tmp_path, actions=[("vid.mkv", "new_vid.mkv", 'file', 'rename')])
    orig_path = plan.actions[0].original_path; final_path = plan.actions[0].new_path; orig_path.write_text("content")
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    assert result['success'] is True, f"Action failed: {result.get('message')}"; assert result['actions_taken'] == 1
    assert not orig_path.exists(); assert final_path.exists()
    mock_undo_manager.log_action.assert_called_once_with(batch_id=plan.batch_id, original_path=orig_path, new_path=final_path, item_type='file', status='pending_final')
    mock_undo_manager.update_action_status.assert_called_once_with(batch_id=plan.batch_id, original_path=str(orig_path), new_status='renamed')

# --- FIX: test_perform_file_actions_live_move_create_dir ---
def test_perform_file_actions_live_move_create_dir(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    # Arrange: Let file operations run
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'create_folders': True, 'on_conflict': 'skip', 'enable_undo': True}
    plan = create_test_plan(tmp_path, actions=[("old/vid.mkv", "New Name.mkv", 'file', 'move')], created_dir="New Dir")
    orig_path = plan.actions[0].original_path; final_path = plan.actions[0].new_path; created_dir_path = plan.created_dir_path
    # Create source file using write_text (handles parent creation)
    orig_path.write_text("content")
    # Act
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    # Assert
    assert result['success'] is True, f"Action failed: {result.get('message')}"
    assert result['actions_taken'] == 1
    assert created_dir_path.is_dir()
    assert final_path.exists()
    assert not orig_path.exists()
    # Check logging calls
    log_calls = mock_undo_manager.log_action.call_args_list; update_calls = mock_undo_manager.update_action_status.call_args_list
    assert any(c.kwargs.get('original_path') == created_dir_path and c.kwargs.get('status') == 'created_dir' for c in log_calls)
    assert any(c.kwargs.get('original_path') == orig_path and c.kwargs.get('status') == 'pending_final' for c in log_calls)
    assert any(c.kwargs.get('original_path') == str(orig_path) and c.kwargs.get('new_status') == 'moved' for c in update_calls)
# --- End Fix ---

# (test_perform_file_actions_live_trash unchanged)
def test_perform_file_actions_live_trash(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    if not SEND2TRASH_AVAILABLE: pytest.skip("send2trash not installed"); return
    mock_send2trash = mocker.patch('send2trash.send2trash'); mocker.patch('shutil.move'); mocker.patch('pathlib.Path.rename')
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = True
    mock_cfg_helper.manager._mock_values = {'enable_undo': True}
    plan = create_test_plan(tmp_path, actions=[("file_to_trash.txt", "new_name.txt", 'file', 'rename')])
    orig_path = plan.actions[0].original_path; intended_new_path = plan.actions[0].new_path; orig_path.write_text("content")
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    assert result['success'] is True; assert result['actions_taken'] == 1; mock_send2trash.assert_called_once_with(str(orig_path))
    mock_undo_manager.log_action.assert_called_once_with(batch_id=plan.batch_id, original_path=orig_path, new_path=intended_new_path, item_type='file', status='trashed')
    mock_undo_manager.update_action_status.assert_not_called()

# --- FIX: test_perform_file_actions_live_backup ---
def test_perform_file_actions_live_backup(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
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
    assert result['actions_taken'] == 1
    assert backup_dir.is_dir()
    # Check mock copy call FIRST
    mock_copy2.assert_called_once_with(str(orig_path), str(expected_backup_path))
    # Check final state
    assert not orig_path.exists()
    assert final_path.exists()
    # Check logging
    log_calls = mock_undo_manager.log_action.call_args_list; update_calls = mock_undo_manager.update_action_status.call_args_list
    assert any(c.kwargs.get('original_path') == orig_path and c.kwargs.get('status') == 'pending_final' for c in log_calls)
    assert any(c.kwargs.get('original_path') == str(orig_path) and c.kwargs.get('new_status') == 'renamed' for c in update_calls)
# --- End Fix ---

# (test_perform_file_actions_live_stage unchanged)
def test_perform_file_actions_live_stage(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    mock_shutil_move = mocker.patch('shutil.move'); mocker.patch('pathlib.Path.rename')
    stage_dir = tmp_path / "staging"
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = stage_dir; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'enable_undo': True, 'on_conflict': 'skip'}
    plan = create_test_plan(tmp_path, actions=[("stage_me.mkv", "staged_name.mkv", 'file', 'rename')])
    orig_path = plan.actions[0].original_path; intended_new_path = plan.actions[0].new_path; expected_staged_path = stage_dir / intended_new_path.name; orig_path.write_text("content")
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    assert result['success'] is True; assert result['actions_taken'] == 1
    mock_shutil_move.assert_called_once_with(str(orig_path), str(expected_staged_path))
    mock_undo_manager.log_action.assert_called_once_with(batch_id=plan.batch_id, original_path=orig_path, new_path=expected_staged_path, item_type='file', status='moved')
    mock_undo_manager.update_action_status.assert_not_called()


# --- Conflict Tests ---
# (test_perform_file_actions_conflict_skip unchanged, should pass)
def test_perform_file_actions_conflict_skip(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    mock_shutil_move = mocker.patch('shutil.move'); mock_path_rename = mocker.patch('pathlib.Path.rename')
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'on_conflict': 'skip', 'create_folders': False, 'enable_undo': True}
    existing_target = tmp_path / "target_exists.mkv"; existing_target.write_text("old")
    plan = create_test_plan(tmp_path, actions=[("source.mkv", "target_exists.mkv", 'file', 'rename')])
    (tmp_path / "source.mkv").write_text("new")
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    assert result['success'] is False; assert "ERROR: Target 'target_exists.mkv' exists (mode: skip)." in result['message']
    mock_shutil_move.assert_not_called(); mock_path_rename.assert_not_called(); mock_undo_manager.log_action.assert_not_called()

# (test_perform_file_actions_conflict_fail unchanged, should pass)
def test_perform_file_actions_conflict_fail(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'on_conflict': 'fail', 'create_folders': False, 'enable_undo': True}
    existing_target = tmp_path / "target_exists.mkv"; existing_target.touch()
    plan = create_test_plan(tmp_path, actions=[("source.mkv", "target_exists.mkv", 'file', 'rename')])
    (tmp_path / "source.mkv").write_text("content")
    with pytest.raises(FileExistsError, match="Target 'target_exists.mkv' exists \(mode: fail\)"):
         file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)

# (test_perform_file_actions_conflict_overwrite unchanged)
def test_perform_file_actions_conflict_overwrite(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    mock_unlink = mocker.patch('pathlib.Path.unlink')
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'on_conflict': 'overwrite', 'create_folders': False, 'enable_undo': True}
    existing_target = tmp_path / "target_exists.mkv"; existing_target.write_text("old content")
    plan = create_test_plan(tmp_path, actions=[("source.mkv", "target_exists.mkv", 'file', 'rename')])
    orig_path = plan.actions[0].original_path; final_path = plan.actions[0].new_path; orig_path.write_text("new content")
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    assert result['success'] is True, f"Action failed: {result.get('message')}"; assert result['actions_taken'] == 1
    mock_unlink.assert_called_once_with(missing_ok=True)
    assert not orig_path.exists(); assert final_path.exists(); assert final_path.read_text() == "new content"
    mock_undo_manager.log_action.assert_called_once_with(batch_id=plan.batch_id, original_path=orig_path, new_path=final_path, item_type='file', status='pending_final')
    mock_undo_manager.update_action_status.assert_called_once_with(batch_id=plan.batch_id, original_path=str(orig_path), new_status='renamed')

# (test_perform_file_actions_conflict_suffix unchanged)
def test_perform_file_actions_conflict_suffix(tmp_path, mock_cfg_helper, mock_undo_manager, mocker):
    mock_cfg_helper.args.dry_run = False; mock_cfg_helper.args.backup_dir = None; mock_cfg_helper.args.stage_dir = None; mock_cfg_helper.args.use_trash = False
    mock_cfg_helper.manager._mock_values = {'on_conflict': 'suffix', 'create_folders': False, 'enable_undo': True}
    existing_target = tmp_path / "target_exists.mkv"; existing_target.write_text("content 0")
    existing_target_1 = tmp_path / "target_exists_1.mkv"; existing_target_1.write_text("content 1")
    plan = create_test_plan(tmp_path, actions=[("source.mkv", "target_exists.mkv", 'file', 'rename')])
    orig_path = plan.actions[0].original_path; expected_final_path = tmp_path / "target_exists_2.mkv"; orig_path.write_text("new content")
    result = file_system_ops.perform_file_actions(plan, mock_cfg_helper.args, mock_cfg_helper, mock_undo_manager)
    assert result['success'] is True, f"Action failed: {result.get('message')}"; assert result['actions_taken'] == 1
    assert not orig_path.exists(); assert expected_final_path.exists(); assert expected_final_path.read_text() == "new content"; assert existing_target.exists(); assert existing_target_1.exists()
    mock_undo_manager.log_action.assert_called_once_with(batch_id=plan.batch_id, original_path=orig_path, new_path=expected_final_path, item_type='file', status='pending_final')
    mock_undo_manager.update_action_status.assert_called_once_with(batch_id=plan.batch_id, original_path=str(orig_path), new_status='renamed')