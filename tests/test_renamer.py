# tests/test_renamer.py

import pytest
from pathlib import Path
from unittest.mock import MagicMock

from rename_app.renamer_engine import RenamerEngine
from rename_app.models import MediaInfo, MediaMetadata, RenamePlan, RenameAction
# No need to import ConfigHelper here if only using the fixture

# Assume conftest.py provides mock_cfg_helper and test_files fixtures

# --- Test Cases for RenamerEngine.plan_rename ---

def test_plan_rename_simple_series_no_folders(mock_cfg_helper, test_files):
    """Test basic series rename with default format, no folder creation."""
    # --- Arrange ---
    mock_cfg_helper.manager._mock_values = {
        'series_format': "{show_title} - S{season:0>2}E{episode:0>2} - {episode_title}", # No {ext}
        'create_folders': False,
        'scene_tags_in_filename': False,
        'subtitle_format': "{stem}.{lang_code}", # No {ext}
        'subtitle_extensions': ['.srt'],
        'on_conflict': 'skip'
    }
    mock_cfg_helper.args.directory = test_files
    engine = RenamerEngine(mock_cfg_helper)
    vid_path = test_files / "My Show S01E01 Episode Name.mkv"
    sub_path = test_files / "My Show S01E01 Episode Name.eng.srt"
    vid_path.touch(); sub_path.touch()
    media_info = MediaInfo(
        original_path=vid_path,
        guess_info={'type': 'episode', 'title': 'My Show', 'season': 1, 'episode': 1},
        metadata=MediaMetadata(is_series=True, show_title="My Show (API)", season=1, episode_list=[1], episode_titles={1: "The Pilot API"})
    )
    associated_paths = [sub_path]
    # Expected: Spaces/hyphens preserved, correct extensions added
    expected_vid_new_path = test_files / "My Show (API) - S01E01 - The Pilot API.mkv"
    expected_sub_new_path = test_files / "My Show (API) - S01E01 - The Pilot API.eng.srt"

    # --- Act ---
    plan = engine.plan_rename(vid_path, associated_paths, media_info)

    # --- Assert ---
    assert plan is not None, "Plan should not be None"
    assert plan.status == 'success', f"Status: {plan.status}, Msg: {plan.message}"
    assert plan.created_dir_path is None, "Folder path should be None"
    assert len(plan.actions) == 2, "Should be 2 actions"
    vid_action = next((a for a in plan.actions if a.original_path == vid_path), None); assert vid_action is not None
    assert vid_action.action_type == 'rename'; assert vid_action.new_path == expected_vid_new_path, f"Expected Vid: {expected_vid_new_path}\nActual Vid:   {vid_action.new_path}"
    sub_action = next((a for a in plan.actions if a.original_path == sub_path), None); assert sub_action is not None
    assert sub_action.action_type == 'rename'; assert sub_action.new_path == expected_sub_new_path, f"Expected Sub: {expected_sub_new_path}\nActual Sub:   {sub_action.new_path}"

def test_plan_rename_movie_with_folders(mock_cfg_helper, test_files):
    """Test movie rename with folder creation enabled."""
    mock_cfg_helper.manager._mock_values = {
        'movie_format': "{movie_title} ({movie_year})", # No {ext}
        'create_folders': True,
        'folder_format_movie': "{movie_title} ({movie_year})",
        'scene_tags_in_filename': False,
        'associated_extensions': [],
        'on_conflict': 'skip'
    }
    mock_cfg_helper.args.directory = test_files
    engine = RenamerEngine(mock_cfg_helper)
    vid_path = test_files / "Some Movie Title 1080p.mp4"; vid_path.touch()
    media_info = MediaInfo(original_path=vid_path, guess_info={'type': 'movie', 'title': 'Some Movie Title'}, metadata=MediaMetadata(is_movie=True, movie_title="Some Movie Title - Definitive", movie_year=2021))
    expected_folder = test_files / "Some Movie Title - Definitive (2021)"
    expected_vid_new_path = expected_folder / "Some Movie Title - Definitive (2021).mp4" # Ext added correctly
    plan = engine.plan_rename(vid_path, [], media_info)
    assert plan is not None; assert plan.status == 'success', f"Status: {plan.status}, Msg: {plan.message}"; assert plan.created_dir_path == expected_folder; assert len(plan.actions) == 1
    vid_action = plan.actions[0]; assert vid_action.original_path == vid_path; assert vid_action.action_type == 'move'; assert vid_action.new_path == expected_vid_new_path, f"Expected Vid: {expected_vid_new_path}\nActual Vid:   {vid_action.new_path}"

def test_plan_rename_multi_episode_series_folders(mock_cfg_helper, test_files):
    """Test multi-episode file with series folder creation."""
    mock_cfg_helper.manager._mock_values = {
        'series_format': "{show_title} - S{season:0>2}{episode_range}", # No {ext}
        'create_folders': True,
        'folder_format_series': "{show_title}/Season {season:0>2}",
        'scene_tags_in_filename': False,
        'associated_extensions': [],
        'on_conflict': 'skip'
    }
    mock_cfg_helper.args.directory = test_files
    engine = RenamerEngine(mock_cfg_helper)
    vid_path = test_files / "The.Show.S03E01-E03.HDTV.mkv"; vid_path.touch()
    media_info = MediaInfo(original_path=vid_path, guess_info={'type': 'episode', 'title': 'The Show', 'season': 3, 'episode': 1, 'episode_list': [1, 2, 3]}, metadata=MediaMetadata(is_series=True, show_title="The Show (API)", season=3, episode_list=[1, 2, 3], episode_titles={1: "One", 2: "Two", 3: "Three"}))
    expected_folder = test_files / "The Show (API)" / "Season 03"
    expected_vid_new_path = expected_folder / "The Show (API) - S03E01-E03.mkv" # Ext added correctly
    plan = engine.plan_rename(vid_path, [], media_info)
    assert plan is not None; assert plan.status == 'success', f"Status: {plan.status}, Msg: {plan.message}"; assert plan.created_dir_path == expected_folder; assert len(plan.actions) == 1
    vid_action = plan.actions[0]; assert vid_action.action_type == 'move'; assert vid_action.new_path == expected_vid_new_path, f"Expected Vid: {expected_vid_new_path}\nActual Vid:   {vid_action.new_path}"

def test_plan_rename_no_change(mock_cfg_helper, test_files):
    """Test scenario where the filename and path are already correct."""
    vid_filename = "MyShow S01E01.mkv"
    sub_filename = "MyShow S01E01.eng.srt"
    mock_cfg_helper.manager._mock_values = {
        'series_format': "{show_title} S{season:0>2}E{episode:0>2}", # No {ext}
        'create_folders': False,
        'scene_tags_in_filename': False,
        'associated_extensions': ['.srt'],
        'subtitle_format': "{stem}.{lang_code}", # No {ext}
        'on_conflict': 'skip'
    }
    mock_cfg_helper.args.directory = test_files
    engine = RenamerEngine(mock_cfg_helper)
    vid_path = test_files / vid_filename; vid_path.touch()
    sub_path = test_files / sub_filename; sub_path.touch()
    media_info = MediaInfo(original_path=vid_path, guess_info={'type': 'episode', 'title': 'MyShow', 'season': 1, 'episode': 1})
    associated_paths = [sub_path]

    plan = engine.plan_rename(vid_path, associated_paths, media_info)

    # This test should now pass because the comparison logic is fixed
    assert plan is not None, "Plan should not be None"
    assert plan.status == 'skipped', f"Expected 'skipped', got '{plan.status}'. Msg: {plan.message}"
    assert plan.message == "Path already correct.", "Incorrect skip message: " + str(plan.message)
    assert len(plan.actions) == 0, "Actions list should be empty"
    assert plan.created_dir_path is None, "Folder path should be None"


def test_plan_rename_conflict_skip(mock_cfg_helper, test_files):
    """Test conflict detection when mode is 'skip'."""
    mock_cfg_helper.manager._mock_values = {
        'series_format': "New Name S01E01", # No {ext}
        'create_folders': False,
        'scene_tags_in_filename': False,
        'associated_extensions': [],
        'on_conflict': 'skip'
    }
    mock_cfg_helper.args.directory = test_files
    engine = RenamerEngine(mock_cfg_helper)
    vid_path = test_files / "Original.Name.S01E01.mkv"; vid_path.touch()
    existing_target_path = test_files / "New Name S01E01.mkv"; existing_target_path.touch()
    media_info = MediaInfo(original_path=vid_path, guess_info={'type': 'episode', 'season': 1, 'episode': 1})

    plan = engine.plan_rename(vid_path, [], media_info)

    # Conflict should be detected in planning stage for skip/fail modes
    assert plan is not None, "Plan should not be None"
    assert plan.status == 'conflict_unresolved', f"Expected 'conflict_unresolved', got '{plan.status}'"
    assert "Target 'New Name S01E01.mkv' exists" in plan.message, "Incorrect conflict message"
    assert len(plan.actions) == 0, "Actions should be empty on unresolved conflict"


def test_plan_rename_conflict_fail(mock_cfg_helper, test_files):
    """Test conflict detection when mode is 'fail'."""
    mock_cfg_helper.manager._mock_values = {
        'series_format': "New Name S01E01", # No {ext}
        'create_folders': False,
        'on_conflict': 'fail'
    }
    mock_cfg_helper.args.directory = test_files
    engine = RenamerEngine(mock_cfg_helper)
    vid_path = test_files / "Original.Name.S01E01.mkv"; vid_path.touch()
    existing_target_path = test_files / "New Name S01E01.mkv"; existing_target_path.touch()
    media_info = MediaInfo(original_path=vid_path, guess_info={'season': 1, 'episode': 1})

    plan = engine.plan_rename(vid_path, [], media_info)

    # Conflict should be detected in planning stage
    assert plan is not None, "Plan should not be None"
    assert plan.status == 'conflict_unresolved', f"Expected 'conflict_unresolved', got '{plan.status}'"
    assert "Target 'New Name S01E01.mkv' exists" in plan.message, "Incorrect conflict message"


def test_plan_rename_conflict_overwrite_suffix(mock_cfg_helper, test_files):
    """Test conflict planning when mode allows proceeding (overwrite/suffix)."""
    mock_cfg_helper.manager._mock_values = {
        'series_format': "New Name S01E01", # No {ext}
        'create_folders': False,
        'on_conflict': 'suffix' # or 'overwrite'
    }
    mock_cfg_helper.args.directory = test_files
    engine = RenamerEngine(mock_cfg_helper)
    vid_path = test_files / "Original.Name.S01E01.mkv"; vid_path.touch()
    existing_target_path = test_files / "New Name S01E01.mkv"; existing_target_path.touch()
    media_info = MediaInfo(original_path=vid_path, guess_info={'season': 1, 'episode': 1})

    plan = engine.plan_rename(vid_path, [], media_info)

    # Plan should succeed now, conflict handled later by file ops
    assert plan is not None, "Plan should not be None"
    assert plan.status == 'success', f"Expected status 'success', got '{plan.status}'. Message: {plan.message}"
    assert len(plan.actions) == 1, "Should be 1 action"
    # Planned path remains the original conflicting one
    assert plan.actions[0].new_path == existing_target_path, "Planned path mismatch"


def test_plan_rename_include_scene_tags(mock_cfg_helper, test_files):
    """Test that scene tags are correctly appended when configured."""
    mock_cfg_helper.manager._mock_values = {
        'series_format': "{show_title}.S{season:0>2}E{episode:0>2}", # No ext
        'create_folders': False,
        'scene_tags_in_filename': True,
        'scene_tags_to_preserve': ["PROPER", "REPACK"],
        'associated_extensions': [],
        'on_conflict': 'skip'
    }
    mock_cfg_helper.args.directory = test_files
    engine = RenamerEngine(mock_cfg_helper)
    vid_path = test_files / "MyShow.S01E01.OtherStuff.PROPER.REPACK.1080p.mkv"; vid_path.touch()
    media_info = MediaInfo(original_path=vid_path, guess_info={'type': 'episode', 'title': 'MyShow', 'season': 1, 'episode': 1})
    # Expected: OS sanitized title, original ext, tags appended before ext
    expected_vid_new_path = test_files / "MyShow.S01E01.PROPER.REPACK.mkv" # Tags added by _format_new_name

    plan = engine.plan_rename(vid_path, [], media_info)

    # This should pass now
    assert plan is not None, "Plan should not be None"
    assert plan.status == 'success', f"Expected status 'success', got '{plan.status}'. Message: {plan.message}"
    assert len(plan.actions) == 1, "Should be 1 action"
    assert plan.actions[0].new_path == expected_vid_new_path, f"Expected Vid: {expected_vid_new_path}\nActual Vid:   {plan.actions[0].new_path}"