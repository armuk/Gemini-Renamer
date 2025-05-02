# tests/test_main_processor.py

import pytest
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch, call, ANY
import argparse
import sys

# Import the class to test and supporting classes/exceptions
from rename_app.main_processor import MainProcessor
from rename_app.models import RenamePlan, RenameAction, MediaInfo # Import necessary models
from rename_app.exceptions import UserAbortError, RenamerError

# Define logger name consistent with the module
LOGGER_NAME = "rename_app"

# --- Fixtures ---

@pytest.fixture
def mock_args():
    """Provides a default mock Namespace object for args."""
    return argparse.Namespace(
        directory=Path("."),
        dry_run=True,
        use_metadata=True,
        interactive=False,
        backup_dir=None,
        stage_dir=None,
        use_trash=False,
        force=False, # Assuming a --force flag might exist
        # Add other relevant args from your actual argparse setup
        recursive=False,
        profile='default',
        video_formats=['.mp4', '.mkv'],
        exclude_patterns=[],
        log_file=None,
        log_level_console='INFO',
        # Config settings often overlap args, ensure cfg_helper provides them too
        rename_movies=True,
        rename_series=True,
        movie_format="{title} ({year})",
        series_format="{series_title} - S{season:02d}E{episode:02d} - {episode_title}",
        multi_ep_style="S01E01-E02",
        create_folders=False,
        folder_format_movie="{title} ({year})",
        folder_format_series="{series_title}/Season {season:02d}",
        conflict_resolution="skip",
        enable_undo=True,
        strip_tags=[],
        tmdb_language='en',
    )

@pytest.fixture
def mock_cfg_helper(mock_args):
    """Provides a mock ConfigHelper."""
    helper = MagicMock()
    # Configure helper to return values based on mock_args or defaults
    # The __call__ signature is helper(key, default_value=None, arg_value=None)
    def cfg_side_effect(key, default_value=None, arg_value=None):
        # Prioritize arg_value if explicitly passed (though less common in Processor)
        if arg_value is not None:
             # Simulate bool optional handling
             is_bool_optional = key in ['recursive', 'use_metadata', 'create_folders', 'enable_undo']
             if is_bool_optional and arg_value is None:
                 pass # Don't return None, fall back
             else:
                  return arg_value

        # Fallback to getattr on mock_args
        # Use a sensible default if arg doesn't exist on mock_args either
        config_default_map = {
             'video_formats': ['.mp4', '.mkv'],
             'enable_undo': True,
             'conflict_resolution': 'skip',
             # Add other config-specific defaults here if they differ from args defaults
        }
        return getattr(mock_args, key, config_default_map.get(key, default_value))

    helper.side_effect = cfg_side_effect # Mock the __call__ method

    # Also mock specific methods if needed (though __call__ is primary)
    helper.get_list.side_effect = lambda key, default_value=None: helper(key, default_value) or (default_value if isinstance(default_value, list) else [])
    helper.get_api_key.return_value = "mock_api_key" # Default mock key

    return helper

@pytest.fixture
def mock_undo_manager():
    """Provides a mock UndoManager."""
    manager = MagicMock()
    manager.is_enabled = True # Default to enabled
    # Mock methods if they were called directly by MainProcessor
    # manager.log_action = MagicMock() # Not called directly here
    return manager

@pytest.fixture
def mock_renamer_engine(mocker):
    """Provides a mock RenamerEngine instance."""
    with patch('rename_app.main_processor.RenamerEngine', autospec=True) as mock_class:
        instance = mock_class.return_value
        # Default mock behaviors
        instance.parse_filename.return_value = {'title': 'Mock Title', 'year': 2023, 'type': 'movie'}
        instance._determine_file_type.return_value = 'movie' # Default guess
        # --- FIX: Provide required arguments for default RenamePlan ---
        instance.plan_rename.return_value = RenamePlan(
            batch_id="mock_batch_default",        # Add dummy batch_id
            video_file=Path("dummy/default.mkv"), # Add dummy video_file Path
            status='skipped',
            message='Default mock plan'
        )
    yield instance # Yield the instance created by the mocked class

@pytest.fixture
def mock_metadata_fetcher(mocker):
    """Provides a mock MetadataFetcher instance, patching its instantiation."""
    with patch('rename_app.main_processor.MetadataFetcher', autospec=True) as mock_class:
        instance = mock_class.return_value
        # Default behavior: return None (no metadata found)
        instance.fetch_series_metadata.return_value = None
        instance.fetch_movie_metadata.return_value = None
        yield instance # Yield the instance

@pytest.fixture
def mock_scan_media_files(mocker):
    """Patches the scan_media_files utility function."""
    # Default: return an empty dict (no files found)
    return mocker.patch('rename_app.main_processor.scan_media_files', return_value={})

@pytest.fixture
def mock_perform_file_actions(mocker):
    """Patches the perform_file_actions function."""
    # Default: Simulate success with 1 action taken
    return mocker.patch('rename_app.main_processor.perform_file_actions',
                       return_value={'success': True, 'message': 'Mock action success', 'actions_taken': 1})

@pytest.fixture
def mock_input(mocker):
    """Patches the built-in input function."""
    # Default: Simulate user confirming 'y'
    return mocker.patch('builtins.input', return_value='y')

@pytest.fixture
def mock_print(mocker):
    """Patches the built-in print function."""
    return mocker.patch('builtins.print')

@pytest.fixture
def mock_tqdm(mocker):
    """Patches tqdm to return a mock object that is iterable and has tqdm methods."""
    # Create a mock instance that *acts like* a tqdm object
    mock_tqdm_instance = MagicMock()
    # Make it iterable - store items separately
    items_to_iterate = []
    mock_tqdm_instance.__iter__.return_value = iter(items_to_iterate)
    # Ensure methods like set_postfix_str exist (they will do nothing by default)
    mock_tqdm_instance.set_postfix_str = MagicMock()

    # Patch the tqdm class/function to return our prepared mock instance
    mock_tqdm_class_or_func = mocker.patch('rename_app.main_processor.tqdm', return_value=mock_tqdm_instance)

    # Also patch TQDM_AVAILABLE if needed
    mocker.patch('rename_app.main_processor.TQDM_AVAILABLE', True)

    # Return the main mock and the list to populate for iteration control
    return mock_tqdm_class_or_func, items_to_iterate, mock_tqdm_instance

@pytest.fixture
def processor(
    mock_args,
    mock_cfg_helper,
    mock_undo_manager,
    mock_renamer_engine,
    mock_metadata_fetcher
    ):
    """Instantiates MainProcessor with mocked dependencies needed at init."""
# 1. Instantiate the processor normally.
    #    It will initially create REAL engine/fetcher inside __init__.
    instance = MainProcessor(mock_args, mock_cfg_helper, mock_undo_manager)

    # 2. *** Replace the attributes with the mock instances ***
    instance.renamer = mock_renamer_engine
    # Only replace fetcher if it was supposed to be created
    if mock_args.use_metadata:
        instance.metadata_fetcher = mock_metadata_fetcher
    else:
        # Ensure it's None if metadata is off, overriding __init__
        instance.metadata_fetcher = None

    return instance # Return the processor instance with mocks injected
# --- Test Cases ---

# Test Initialization
def test_init_no_metadata(mock_args, mock_cfg_helper, mock_undo_manager):
    """Test fetcher is None if use_metadata is False."""
    mock_args.use_metadata = False
    # Manually configure cfg_helper for this specific scenario if needed
    mock_cfg_helper.side_effect = lambda key, default_value=None, arg_value=None: False if key == 'use_metadata' else getattr(mock_args, key, default_value)

    # Patch the constructor call *within* the test context is cleaner here
    with patch('rename_app.main_processor.MetadataFetcher') as mock_fetcher_class:
         processor = MainProcessor(mock_args, mock_cfg_helper, mock_undo_manager)
         assert processor.metadata_fetcher is None
         mock_fetcher_class.assert_not_called() # Verify constructor wasn't called

def test_init_with_metadata(mock_args, mock_cfg_helper, mock_undo_manager):
    """Test fetcher is initialized if use_metadata is True."""
    mock_args.use_metadata = True
    # Manually configure cfg_helper for this specific scenario
    mock_cfg_helper.side_effect = lambda key, default_value=None, arg_value=None: True if key == 'use_metadata' else getattr(mock_args, key, default_value)

    with patch('rename_app.main_processor.MetadataFetcher', autospec=True) as mock_fetcher_class:
        processor = MainProcessor(mock_args, mock_cfg_helper, mock_undo_manager)
        assert processor.metadata_fetcher is not None
        mock_fetcher_class.assert_called_once_with(mock_cfg_helper) # Verify constructor was called

# Test Directory Handling
def test_run_processing_target_dir_not_found(processor, mock_args, caplog, mock_scan_media_files):
    """Test exit if target directory doesn't exist."""
    mock_args.directory = Path("/non/existent/path")
    # Mock is_dir check on the specific path
    with patch.object(Path, 'is_dir', return_value=False) as mock_is_dir:
        with caplog.at_level(logging.CRITICAL):
            processor.run_processing()

        # Check if the specific path instance had is_dir called on it
        # This is tricky because resolve() is called first.
        # Let's just assert the log message.
        assert f"Target directory not found or is not a directory: {mock_args.directory.resolve()}" in caplog.text
        mock_scan_media_files.assert_not_called() # Scan should not happen


# Test Scanning
def test_run_processing_no_files_found(processor, mock_scan_media_files, caplog, mock_print):
    """Test exit if scan_media_files returns empty."""
    mock_scan_media_files.return_value = {} # Ensure it returns empty dict
    with caplog.at_level(logging.WARNING):
        processor.run_processing()

    mock_scan_media_files.assert_called_once()
    assert "No valid video files/batches found" in caplog.text
    # mock_print might be called with separators, check specific summary calls are absent
    # A better check might be to see if specific downstream mocks were called.
    assert call("Processing Summary:") not in mock_print.call_args_list


# --- Basic Dry Run Scenario ---
def test_run_processing_dry_run_success(
    processor,
    mock_args,
    mock_scan_media_files,
    mock_renamer_engine,
    mock_metadata_fetcher,
    mock_perform_file_actions,
    mock_print,
    mock_tqdm, # Use updated mock_tqdm
    tmp_path
):
    """Test a successful dry run with one batch."""
    mock_args.dry_run = True
    mock_args.use_metadata = True # Enable metadata for this test

    # --- FIX: Use tmp_path for target directory ---
    target_dir = tmp_path / "test_processing_dir" # Use a subdir within tmp_path
    target_dir.mkdir() # *** Create the directory ***
    mock_args.directory = target_dir # Set args to use this existing directory
    # --- End FIX ---

    # Use the created target_dir for paths
    mock_video_path = target_dir / "Movie Title (2021).mkv"
    mock_sub_path = target_dir / "Movie Title (2021).eng.srt"

    # --- FIX: Create the subtitle file for encoding detection ---
    mock_sub_path.touch()
    # --- End FIX ---

    # 1. Setup Scan Results
    scan_results = {
        "Movie Title (2021)": {
            "video": mock_video_path,
            "associated": [mock_sub_path],
            "common_dir": target_dir
        }
    }
    mock_scan_media_files.return_value = scan_results

    # 2. Setup TQDM Mock Iterator
    mock_tqdm_func, items_list, mock_tqdm_instance = mock_tqdm # Unpack instance too
    items_list.extend(list(scan_results.items())) # Populate the list for iteration

    # 3. Setup Renamer Engine Mocks for the loop
    parse_result = {'title': 'Movie Title', 'year': 2021}
    mock_renamer_engine.parse_filename.return_value = parse_result
    mock_renamer_engine._determine_file_type.return_value = 'movie'
    # Use the *mock_metadata_fetcher* instance provided by the fixture
    metadata_result = {'title': 'Correct Movie Title', 'year': 2021, 'tmdb_id': 123}
    mock_metadata_fetcher.fetch_movie_metadata.return_value = metadata_result
    # Plan result setup on *mock_renamer_engine*
    new_video_path = target_dir / "Correct Movie Title (2021).mkv"
    new_sub_path = target_dir / "Correct Movie Title (2021).eng.srt"
    mock_plan = RenamePlan(
        batch_id="test_batch_dry_run", video_file=mock_video_path, status='success',
        actions=[
            RenameAction(action_type='rename', original_path=mock_video_path, new_path=new_video_path),
            RenameAction(action_type='rename', original_path=mock_sub_path, new_path=new_sub_path)
        ]
    )
    mock_renamer_engine.plan_rename.return_value = mock_plan

    # Mock the _format_associated_name as it might be called by plan_rename
    # Need to configure this based on expected inputs/outputs if plan_rename mock isn't enough
    mock_renamer_engine._format_associated_name.return_value = new_sub_path.name

    # --- Remove Loop Entry Mock (less critical now) ---
    # loop_entry_mock = MagicMock()

    # 4. Run Processing

    def media_info_side_effect(*args, **kwargs):
        loop_entry_mock(*args, **kwargs) # Call the loop entry mock
        return MediaInfo(*args, **kwargs) # <<< ALWAYS return a real MediaInfo instance

    processor.run_processing()

    # 5. Assertions
    # Check dependencies were called
    # loop_entry_mock.assert_called_once() # Removed
    mock_scan_media_files.assert_called_once_with(target_dir.resolve(), processor.cfg)
    mock_tqdm_instance.set_postfix_str.assert_called_once_with(mock_video_path.name, refresh=True)

    # Check calls on the *mock_renamer_engine* instance
    mock_renamer_engine.parse_filename.assert_called_once_with(mock_video_path)
    mock_renamer_engine._determine_file_type.assert_called_once_with(parse_result)

    # Check call on the *mock_metadata_fetcher* instance
    mock_metadata_fetcher.fetch_movie_metadata.assert_called_once_with(
        parse_result.get('title'), parse_result.get('year')
    )

    # Check plan_rename call on *mock_renamer_engine* instance
    mock_renamer_engine.plan_rename.assert_called_once()
    call_args, call_kwargs = mock_renamer_engine.plan_rename.call_args
    assert call_args[0] == mock_video_path
    assert call_args[1] == [mock_sub_path]
    media_info_arg = call_args[2]
    assert isinstance(media_info_arg, MediaInfo)
    assert media_info_arg.original_path == mock_video_path
    # Check metadata was attached inside plan_rename call's argument
    assert media_info_arg.metadata == metadata_result
    # Check file actions NOT called
    mock_perform_file_actions.assert_not_called()

    # --- FIX: Assert the combined print call ---
    expected_dry_run_message = (
        f"DRY RUN: Would rename '{mock_video_path.name}' -> '{new_video_path}'\n"
        f"DRY RUN: Would rename '{mock_sub_path.name}' -> '{new_sub_path}'"
    )
    # Use assert_any_call as other prints occur (like separators and summary)
    mock_print.assert_any_call(expected_dry_run_message)
    # --- End FIX ---

    # Check output (use mock_print) - Verify key messages
    mock_print.assert_any_call("Processing Summary:")
    mock_print.assert_any_call("  Batches Scanned: 1")
    mock_print.assert_any_call("  Batches Successfully Processed: 1")
    mock_print.assert_any_call("  Batches Skipped: 0")
    mock_print.assert_any_call("  Batches with Errors: 0")
    mock_print.assert_any_call("DRY RUN COMPLETE. To apply changes, run again with --live")

# --- TODO: Add More Tests ---
# - Dry run with no actions (plan status != success)
# - Dry run with metadata disabled
# - Dry run with series
# - Live run confirmation: 'y', 'n', EOF
# - Live run success (check perform_file_actions called, undo message)
# - Live run skip (check perform_file_actions NOT called)
# - Live run error from perform_file_actions
# - Live run with staging/backup/trash
# - Interactive mode: 'y', 'n', 's', 'q', EOF
# - Errors during planning/fetching in main loop
# - Unhandled exceptions
# - Check tqdm calls (set_postfix_str)