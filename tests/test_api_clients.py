# tests/test_api_clients.py
import pytest
import sys
from unittest.mock import MagicMock, patch, call

# Import the module we are testing
import rename_app.api_clients as api_clients
# Import config_manager ONLY if needed for spec, avoid if possible
# from rename_app import config_manager

# --- Fixtures ---

@pytest.fixture
def mock_cfg_helper(mocker):
    """Provides a MagicMock for the cfg_helper dependency."""
    # Using duck-typing instead of spec avoids direct dependency on config_manager here
    mock = MagicMock()
    mock.get_api_key.return_value = None
    mock.side_effect = lambda key, default=None: {'tmdb_language': 'en'}.get(key, default)
    # Add configure method mock if cfg_helper() is used like that
    mock.__call__ = mock.side_effect
    return mock

@pytest.fixture
def mock_tmdb(mocker):
    """Mocks the tmdbv3api TMDb class and instance."""
    mock_instance = MagicMock(name="TMDbInstance")
    mock_class = mocker.patch('tmdbv3api.TMDb', return_value=mock_instance, create=True)
    return mock_class, mock_instance

@pytest.fixture
def mock_tvdb(mocker):
    """Mocks the tvdb_api Tvdb class and instance."""
    mock_instance = MagicMock(name="TvdbInstance")
    mock_class = mocker.patch('tvdb_api.Tvdb', return_value=mock_instance, create=True)
    return mock_class, mock_instance

@pytest.fixture(autouse=True)
def reset_api_clients_state():
    """Fixture to automatically reset the global state before each test."""
    api_clients._tmdb_client = None
    api_clients._tvdb_client = None
    api_clients._clients_initialized = False
    yield # Test runs here
    api_clients._tmdb_client = None
    api_clients._tvdb_client = None
    api_clients._clients_initialized = False

# --- Test Cases ---

def test_initialize_api_clients_success_both_keys(
    mocker, mock_cfg_helper, mock_tmdb, mock_tvdb # Removed caplog
):
    """Test successful initialization with both TMDB and TVDB keys."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch the logger
    mock_tmdb_class, mock_tmdb_instance = mock_tmdb
    mock_tvdb_class, mock_tvdb_instance = mock_tvdb

    mock_cfg_helper.get_api_key.side_effect = lambda k: {'tmdb': 'tmdb-key', 'tvdb': 'tvdb-key'}.get(k)
    mock_cfg_helper.side_effect = lambda key, default=None: {'tmdb_language': 'fr'}.get(key, default)
    mock_cfg_helper.__call__ = mock_cfg_helper.side_effect # Ensure direct call works

    result = api_clients.initialize_api_clients(mock_cfg_helper)

    assert result is True
    # ... other assertions ...

    # Verify logs using the mocked logger
    mock_log.info.assert_any_call("TMDB API Client initialized (Lang: fr).")
    mock_log.info.assert_any_call("TVDB API Client initialized (Lang: fr).")
    # Check warning was NOT called
    assert not any("No API keys loaded" in call_args[0][0] for call_args in mock_log.warning.call_args_list)

def test_initialize_api_clients_success_tmdb_only(
    mocker, mock_cfg_helper, mock_tmdb, mock_tvdb # Removed caplog
):
    """Test successful initialization with only TMDB key."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch the logger
    mock_tmdb_class, mock_tmdb_instance = mock_tmdb
    mock_tvdb_class, _ = mock_tvdb

    mock_cfg_helper.get_api_key.side_effect = lambda k: {'tmdb': 'tmdb-key'}.get(k)
    mock_cfg_helper.side_effect = lambda key, default=None: {'tmdb_language': 'en'}.get(key, default)
    mock_cfg_helper.__call__ = mock_cfg_helper.side_effect

    result = api_clients.initialize_api_clients(mock_cfg_helper)

    # ... other assertions ...
    mock_tvdb_class.assert_not_called()

    # Verify logs using the mocked logger
    mock_log.info.assert_called_once_with("TMDB API Client initialized (Lang: en).")
    mock_log.debug.assert_called_once_with("TVDB API Key not found.")
    assert not any("TVDB API Client initialized" in call_args[0][0] for call_args in mock_log.info.call_args_list)


def test_initialize_api_clients_success_tvdb_only(
    mocker, mock_cfg_helper, mock_tmdb, mock_tvdb # Removed caplog
):
    """Test successful initialization with only TVDB key."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch the logger
    mock_tmdb_class, _ = mock_tmdb
    mock_tvdb_class, mock_tvdb_instance = mock_tvdb

    mock_cfg_helper.get_api_key.side_effect = lambda k: {'tvdb': 'tvdb-key'}.get(k)
    mock_cfg_helper.side_effect = lambda key, default=None: {'tmdb_language': 'es'}.get(key, default)
    mock_cfg_helper.__call__ = mock_cfg_helper.side_effect

    result = api_clients.initialize_api_clients(mock_cfg_helper)

    # ... other assertions ...
    mock_tmdb_class.assert_not_called()

    # Verify logs using the mocked logger
    mock_log.info.assert_called_once_with("TVDB API Client initialized (Lang: es).")
    mock_log.debug.assert_called_once_with("TMDB API Key not found.")
    assert not any("TMDB API Client initialized" in call_args[0][0] for call_args in mock_log.info.call_args_list)


def test_initialize_api_clients_no_keys(
    mocker, mock_cfg_helper, mock_tmdb, mock_tvdb # Removed caplog
):
    """Test initialization when no API keys are found."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch the logger
    mock_tmdb_class, _ = mock_tmdb
    mock_tvdb_class, _ = mock_tvdb

    result = api_clients.initialize_api_clients(mock_cfg_helper)

    # ... other assertions ...
    mock_tmdb_class.assert_not_called()
    mock_tvdb_class.assert_not_called()

    # Verify logs using the mocked logger
    mock_log.debug.assert_any_call("TMDB API Key not found.")
    mock_log.debug.assert_any_call("TVDB API Key not found.")
    mock_log.warning.assert_called_once_with("No API keys loaded or clients initialized.")

def test_initialize_api_clients_tmdb_import_error(
    mocker, mock_cfg_helper, mock_tvdb # Removed caplog
):
    """Test initialization when tmdbv3api import fails."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch the logger
    mock_tvdb_class, mock_tvdb_instance = mock_tvdb
    mocker.patch('tmdbv3api.TMDb', side_effect=ImportError, create=True)
    mock_cfg_helper.get_api_key.side_effect = lambda k: {'tmdb': 'tmdb-key', 'tvdb': 'tvdb-key'}.get(k)
    mock_cfg_helper.side_effect = lambda key, default=None: {'tmdb_language': 'en'}.get(key, default)
    mock_cfg_helper.__call__ = mock_cfg_helper.side_effect

    result = api_clients.initialize_api_clients(mock_cfg_helper)

    # ... other assertions ...
    mock_tvdb_class.assert_called_once()

    # Verify logs using the mocked logger
    mock_log.warning.assert_called_once_with("TMDB requires 'tmdbv3api'.")
    mock_log.info.assert_called_once_with("TVDB API Client initialized (Lang: en).")

def test_initialize_api_clients_tvdb_import_error(
    mocker, mock_cfg_helper, mock_tmdb # Removed caplog
):
    """Test initialization when tvdb_api import fails."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch the logger
    mock_tmdb_class, mock_tmdb_instance = mock_tmdb
    mocker.patch('tvdb_api.Tvdb', side_effect=ImportError, create=True)
    mock_cfg_helper.get_api_key.side_effect = lambda k: {'tmdb': 'tmdb-key', 'tvdb': 'tvdb-key'}.get(k)
    mock_cfg_helper.side_effect = lambda key, default=None: {'tmdb_language': 'en'}.get(key, default)
    mock_cfg_helper.__call__ = mock_cfg_helper.side_effect

    result = api_clients.initialize_api_clients(mock_cfg_helper)

    # ... other assertions ...
    mock_tmdb_class.assert_called_once()

    # Verify logs using the mocked logger
    mock_log.warning.assert_called_once_with("TVDB requires 'tvdb_api'.")
    mock_log.info.assert_called_once_with("TMDB API Client initialized (Lang: en).")

def test_initialize_api_clients_tmdb_init_exception(
    mocker, mock_cfg_helper, mock_tvdb # Removed caplog
):
    """Test initialization when TMDb() constructor raises an exception."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch the logger
    mock_tvdb_class, mock_tvdb_instance = mock_tvdb
    mocker.patch('tmdbv3api.TMDb', side_effect=Exception("TMDB Init Failed"), create=True)
    mock_cfg_helper.get_api_key.side_effect = lambda k: {'tmdb': 'tmdb-key', 'tvdb': 'tvdb-key'}.get(k)
    mock_cfg_helper.side_effect = lambda key, default=None: {'tmdb_language': 'en'}.get(key, default)
    mock_cfg_helper.__call__ = mock_cfg_helper.side_effect

    result = api_clients.initialize_api_clients(mock_cfg_helper)

    # ... other assertions ...
    mock_tvdb_class.assert_called_once()

    # Verify logs using the mocked logger
    mock_log.error.assert_called_once_with("Failed to init TMDB Client: TMDB Init Failed")
    mock_log.info.assert_called_once_with("TVDB API Client initialized (Lang: en).")

def test_initialize_api_clients_tvdb_init_exception(
    mocker, mock_cfg_helper, mock_tmdb # Removed caplog
):
    """Test initialization when Tvdb() constructor raises an exception."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch the logger
    mock_tmdb_class, mock_tmdb_instance = mock_tmdb
    mocker.patch('tvdb_api.Tvdb', side_effect=Exception("TVDB Init Failed"), create=True)
    mock_cfg_helper.get_api_key.side_effect = lambda k: {'tmdb': 'tmdb-key', 'tvdb': 'tvdb-key'}.get(k)
    mock_cfg_helper.side_effect = lambda key, default=None: {'tmdb_language': 'en'}.get(key, default)
    mock_cfg_helper.__call__ = mock_cfg_helper.side_effect

    result = api_clients.initialize_api_clients(mock_cfg_helper)

    # ... other assertions ...
    mock_tmdb_class.assert_called_once()

    # Verify logs using the mocked logger
    mock_log.error.assert_called_once_with("Failed to init TVDB Client: TVDB Init Failed")
    mock_log.info.assert_called_once_with("TMDB API Client initialized (Lang: en).")

def test_initialize_api_clients_already_initialized(
    mocker, mock_cfg_helper, mock_tmdb # Removed caplog
):
    """Test that initialization is skipped if already done."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch the logger
    mock_tmdb_class, mock_tmdb_instance = mock_tmdb

    mock_cfg_helper.get_api_key.side_effect = lambda k: {'tmdb': 'tmdb-key'}.get(k)
    mock_cfg_helper.side_effect = lambda key, default=None: {'tmdb_language': 'en'}.get(key, default)
    mock_cfg_helper.__call__ = mock_cfg_helper.side_effect

    # First call - should initialize
    result1 = api_clients.initialize_api_clients(mock_cfg_helper)
    assert result1 is True
    assert api_clients._clients_initialized is True
    assert api_clients._tmdb_client is mock_tmdb_instance
    mock_tmdb_class.assert_called_once()
    mock_log.info.assert_called_once_with("TMDB API Client initialized (Lang: en).")
    mock_log.reset_mock() # Reset mock log calls before second call

    # Second call - should skip
    result2 = api_clients.initialize_api_clients(mock_cfg_helper)
    assert result2 is True
    assert api_clients._tmdb_client is mock_tmdb_instance
    mock_tmdb_class.assert_called_once() # Still only called once
    mock_log.debug.assert_called_once_with("API clients already initialized.")
    mock_log.info.assert_not_called() # Ensure init info log wasn't called again

# --- Getter Tests ---

def test_get_clients_before_init(mocker): # Removed caplog
    """Test getting clients before initialization."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch the logger
    assert not api_clients._clients_initialized

    tmdb = api_clients.get_tmdb_client()
    assert tmdb is None
    mock_log.warning.assert_any_call("API clients not initialized yet.")

    tvdb = api_clients.get_tvdb_client()
    assert tvdb is None
    mock_log.warning.assert_any_call("API clients not initialized yet.")
    assert mock_log.warning.call_count == 2


def test_get_clients_after_init_success(mocker, mock_cfg_helper, mock_tmdb, mock_tvdb):
    """Test getting clients after successful initialization."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch logger
    _, mock_tmdb_instance = mock_tmdb
    _, mock_tvdb_instance = mock_tvdb
    mock_cfg_helper.get_api_key.side_effect = lambda k: {'tmdb': 'tmdb-key', 'tvdb': 'tvdb-key'}.get(k)
    mock_cfg_helper.side_effect = lambda key, default=None: {'tmdb_language': 'en'}.get(key, default)
    mock_cfg_helper.__call__ = mock_cfg_helper.side_effect

    api_clients.initialize_api_clients(mock_cfg_helper) # Initialize

    assert api_clients.get_tmdb_client() is mock_tmdb_instance
    assert api_clients.get_tvdb_client() is mock_tvdb_instance
    # Ensure no warning logs from getters
    assert not any("API clients not initialized yet." in call_args[0][0] for call_args in mock_log.warning.call_args_list)


def test_get_clients_after_init_partial_fail(mocker, mock_cfg_helper):
    """Test getting clients when only one initialized successfully."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch logger
    # Mock TMDb to fail import
    mocker.patch('tmdbv3api.TMDb', side_effect=ImportError, create=True)
    # Mock TVDB to succeed
    mock_tvdb_instance = MagicMock(name="TvdbInstance")
    mocker.patch('tvdb_api.Tvdb', return_value=mock_tvdb_instance, create=True)

    mock_cfg_helper.get_api_key.side_effect = lambda k: {'tmdb': 'tmdb-key', 'tvdb': 'tvdb-key'}.get(k)
    mock_cfg_helper.side_effect = lambda key, default=None: {'tmdb_language': 'en'}.get(key, default)
    mock_cfg_helper.__call__ = mock_cfg_helper.side_effect

    api_clients.initialize_api_clients(mock_cfg_helper) # Initialize

    assert api_clients._clients_initialized is True
    assert api_clients.get_tmdb_client() is None # TMDb failed
    assert api_clients.get_tvdb_client() is mock_tvdb_instance # TVDB succeeded
    # Ensure no warning logs from getters
    assert not any("API clients not initialized yet." in call_args[0][0] for call_args in mock_log.warning.call_args_list)


def test_get_clients_after_init_no_keys(mocker, mock_cfg_helper):
    """Test getting clients when initialization ran but found no keys."""
    mock_log = mocker.patch('rename_app.api_clients.log') # Patch logger
    api_clients.initialize_api_clients(mock_cfg_helper) # Initialize (runs, finds nothing)

    assert api_clients._clients_initialized is True
    assert api_clients.get_tmdb_client() is None
    assert api_clients.get_tvdb_client() is None
    # Ensure no warning logs from getters
    assert not any("API clients not initialized yet." in call_args[0][0] for call_args in mock_log.warning.call_args_list)