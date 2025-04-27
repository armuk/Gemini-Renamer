# tests/conftest.py
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys
import argparse

# Ensure the app package is findable by pytest by adding the project root to the path
project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))

# --- Mock Config Fixture ---
# (Fixture as before)
@pytest.fixture
def mock_cfg_helper(mocker):
    mock_args = argparse.Namespace(profile='default')
    mock_config_manager = MagicMock()
    class MockConfigHelper:
        def __init__(self, manager, args): self.manager = manager; self.args = args; self.profile = getattr(args, 'profile', 'default') or 'default'
        def __call__(self, key, default_value=None, arg_value=None):
             cmd_line_val = getattr(self.args, key, None) if arg_value is None else arg_value
             # Basic mock return logic for tests
             if key in self.manager._mock_values: return self.manager._mock_values[key]
             # Add specific logic for boolean args if needed here or in manager mock
             return default_value
        def get_api_key(self, service_name): return self.manager._mock_apikeys.get(service_name)
        def get_list(self, key, default_value=None):
            val = self(key, default_value);
            if isinstance(val, str): return [i.strip() for i in val.split(',') if i.strip()]
            if isinstance(val, list): return val
            return default_value if isinstance(default_value, list) else []
    mock_config_manager._mock_values = {}
    mock_config_manager._mock_apikeys = {}
    helper = MockConfigHelper(mock_config_manager, mock_args)
    helper.manager = mock_config_manager
    return helper


# --- Test Files Fixture ---
@pytest.fixture
def test_files(tmp_path: Path):
    """Create some dummy files in a temporary directory for testing."""
    # Define stems accurately
    series_stem = "Series.Title.S01E01.720p.WEBRip.x264-Grp"
    movie_stem = "My Awesome Movie (2022) [1080p] {imdb-tt12345}" # Corrected year
    ambiguous_stem = "Ambiguous S02E05 File"
    multi_ep_stem = "multi.episode.s03e01-e03"
    proper_stem = "some.file.PROPER"
    repack_stem = "another.file.REPACK"
    no_pattern_stem = "File Without Pattern"
    nested_stem = "Nested.Show.S05E10"

    # Series files - ENSURE STEMS MATCH EXACTLY
    (tmp_path / f"{series_stem}.mkv").touch()
    (tmp_path / f"{series_stem}.eng.srt").touch() # SRT file
    (tmp_path / f"{series_stem}.nfo").touch()     # NFO file

    # Other files
    (tmp_path / f"{movie_stem}.mp4").touch()
    (tmp_path / f"{ambiguous_stem}.avi").touch()
    (tmp_path / f"{multi_ep_stem}.mkv").touch()
    (tmp_path / f"{multi_ep_stem}.forced.ger.sub").touch()
    (tmp_path / f"{proper_stem}.mkv").touch()
    (tmp_path / f"{repack_stem}.mkv").touch()
    (tmp_path / f"{no_pattern_stem}.mkv").touch()


    # Create a subdirectory for recursive tests
    sub_dir = tmp_path / "subdir"
    sub_dir.mkdir()
    (sub_dir / f"{nested_stem}.mkv").touch()

    return tmp_path

# --- Mock Guessit Fixture ---
# (Fixture as before)
@pytest.fixture
def mock_guessit(mocker):
    mock = mocker.patch('rename_app.utils.guessit') # Patch where used
    mock_engine = mocker.patch('rename_app.renamer_engine.guessit') # Patch where used
    return mock

# --- Mock API Client Fixtures ---
# (Fixtures as before)
@pytest.fixture
def mock_tmdb_client(mocker):
    mock = MagicMock()
    mock_search = MagicMock(); mock_search.search.return_value = []
    mock.Tv.return_value = mock_search
    mock.Movie.return_value = mock_search
    mocker.patch('rename_app.api_clients._tmdb_client', mock)
    return mock