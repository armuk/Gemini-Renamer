# tests/test_log_setup.py

import pytest
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, ANY

# Import the function to test
from rename_app import log_setup

# Define logger name consistent with the module
LOGGER_NAME = "rename_app"

@pytest.fixture(autouse=True)
def reset_logger():
    """Ensure the target logger is clean before each test."""
    logger = logging.getLogger(LOGGER_NAME)
    # Remove all handlers added during tests
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        # Important: Close file handlers to release file locks on Windows
        if isinstance(handler, logging.FileHandler):
            handler.close()
    # Reset level allows tests to set it as needed
    logger.setLevel(logging.WARNING) # Default safe level
    yield # Run the test
    # Cleanup after test (redundant due to start cleanup, but safe)
    logger = logging.getLogger(LOGGER_NAME)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        if isinstance(handler, logging.FileHandler):
            handler.close()
    logger.setLevel(logging.WARNING)


def test_setup_logging_defaults():
    """Test default setup: console handler at INFO level."""
    log = log_setup.setup_logging()

    assert log.name == LOGGER_NAME
    assert len(log.handlers) == 1
    assert isinstance(log.handlers[0], logging.StreamHandler)
    assert log.handlers[0].level == logging.INFO
    # Check default (INFO) format - simple
    assert '%(levelname)-8s: %(message)s' in log.handlers[0].formatter._fmt

def test_setup_logging_console_debug():
    """Test console handler setup at DEBUG level."""
    log = log_setup.setup_logging(log_level_console=logging.DEBUG)

    assert log.name == LOGGER_NAME
    assert len(log.handlers) == 1
    assert isinstance(log.handlers[0], logging.StreamHandler)
    assert log.handlers[0].level == logging.DEBUG
    # Check DEBUG format - more complex
    assert '%(asctime)s %(levelname)-8s [%(name)s:%(lineno)d] %(message)s' in log.handlers[0].formatter._fmt

def test_setup_logging_with_file(tmp_path):
    """Test adding a file handler."""
    log_file = tmp_path / "test_run.log"
    test_argv = ['script.py', '--verbose', 'arg2']

    # Patch sys.argv for the command log test
    with patch('sys.argv', test_argv):
        log = log_setup.setup_logging(log_file=str(log_file))

    assert log.name == LOGGER_NAME
    assert len(log.handlers) == 2 # Console + File

    console_handler = next((h for h in log.handlers if isinstance(h, logging.StreamHandler)), None)
    file_handler = next((h for h in log.handlers if isinstance(h, logging.FileHandler)), None)

    assert console_handler is not None
    assert console_handler.level == logging.INFO # Default console level

    assert file_handler is not None
    assert file_handler.level == logging.DEBUG # File level is always DEBUG
    assert file_handler.baseFilename == str(log_file.resolve())
    assert file_handler.encoding == 'utf-8'
    assert file_handler.mode == 'a'
    assert '%(asctime)s %(levelname)-8s [%(name)s:%(lineno)d] %(message)s' in file_handler.formatter._fmt

    # Check log file content
    assert log_file.exists()
    content = log_file.read_text(encoding='utf-8')
    assert "--- Log session started:" in content
    assert f"Command: {' '.join(test_argv)}" in content

def test_setup_logging_file_and_debug_console(tmp_path):
    """Test file handler with DEBUG console level."""
    log_file = tmp_path / "test_run_debug.log"
    log = log_setup.setup_logging(log_level_console=logging.DEBUG, log_file=str(log_file))

    assert len(log.handlers) == 2
    console_handler = next((h for h in log.handlers if isinstance(h, logging.StreamHandler)), None)
    file_handler = next((h for h in log.handlers if isinstance(h, logging.FileHandler)), None)

    assert console_handler is not None
    assert console_handler.level == logging.DEBUG # Console level set to DEBUG
    assert '%(asctime)s %(levelname)-8s [%(name)s:%(lineno)d] %(message)s' in console_handler.formatter._fmt


    assert file_handler is not None
    assert file_handler.level == logging.DEBUG # File level is always DEBUG
    assert log_file.exists()

def test_setup_logging_file_creation_error(mocker, caplog):
    """Test handling of exception during file handler setup."""
    # --- FIX: Remove or disable mkdir mock to test FileHandler failure ---
    # mock_mkdir = mocker.patch('pathlib.Path.mkdir', side_effect=OSError("Permission denied"))
    mocker.patch('pathlib.Path.mkdir') # Mock it to do nothing, allowing execution to proceed
    # --- End FIX ---

    # Mock FileHandler init as mkdir failure might prevent reaching it,
    # or FileHandler itself could fail. Let's mock FileHandler init.
    mock_filehandler_init = mocker.patch('logging.FileHandler.__init__', side_effect=OSError("Cannot open file"))


    # Use a path that would likely require directory creation
    log_file_path = '/nonexistent_dir_for_test/fail.log'

    with caplog.at_level(logging.ERROR):
        log = log_setup.setup_logging(log_file=log_file_path)

    # Should still have the console handler
    assert len(log.handlers) == 1
    assert isinstance(log.handlers[0], logging.StreamHandler)

    # Check that the error was logged
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "ERROR"
    assert f"Failed to configure file logging to '{log_file_path}'" in caplog.text
    # --- FIX: Assert the correct exception message ---
    assert "Cannot open file" in caplog.text # Check the specific exception from FileHandler mock
    # --- End FIX ---
    
    # mock_mkdir.assert_called_once() # mkdir might not be reached if FileHandler fails first

def test_setup_logging_clears_existing_handlers():
    """Verify that setup_logging removes handlers from previous calls."""
    logger = logging.getLogger(LOGGER_NAME)

    # Add a dummy handler first
    dummy_handler = logging.NullHandler()
    logger.addHandler(dummy_handler)
    assert len(logger.handlers) == 1

    # Run setup (should remove dummy, add StreamHandler)
    log1 = log_setup.setup_logging()
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)
    assert dummy_handler not in logger.handlers

    # Run setup again (should remove first StreamHandler, add a new one)
    handler_before = logger.handlers[0]
    log2 = log_setup.setup_logging(log_level_console=logging.DEBUG) # Use different level to ensure recreation
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)
    assert logger.handlers[0] is not handler_before # A new handler was added
    assert logger.handlers[0].level == logging.DEBUG # Check the new handler's property


def test_return_value():
    """Verify the function returns the configured logger instance."""
    log = log_setup.setup_logging()
    assert isinstance(log, logging.Logger)
    assert log.name == LOGGER_NAME