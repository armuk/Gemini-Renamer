import sys
import pytest
from rename_app import cli
from pathlib import Path

@pytest.fixture
def reset_argv():
    """Fixture to reset sys.argv after each test."""
    original_argv = sys.argv.copy()
    yield
    sys.argv = original_argv

def test_parse_arguments_minimal(mocker, reset_argv):
    """Test basic argument parsing for the rename command."""
    mocker.patch.object(sys, 'argv', ['rename_main.py', 'rename', 'some/path'])
    args = cli.parse_arguments()
    assert args.command == 'rename'
    assert args.directory == Path('some/path')
    assert args.live is False  # Default dry run

def test_parse_arguments_with_flags(mocker, reset_argv):
    """Test parsing rename with additional flags."""
    mocker.patch.object(sys, 'argv', [
        'rename_main.py', '--log-level', 'DEBUG', 'rename', 'another/path',
        '--live', '--recursive'
    ])
    args = cli.parse_arguments()
    assert args.command == 'rename'
    assert args.directory == Path('another/path')
    assert args.live is True
    assert args.recursive is True
    assert args.log_level == 'DEBUG'

def test_parse_arguments_undo_command(mocker, reset_argv):
    """Test parsing the undo command."""
    mocker.patch.object(sys, 'argv', ['rename_main.py', 'undo', 'batch123'])
    args = cli.parse_arguments()
    assert args.command == 'undo'
    assert args.batch_id == 'batch123'

def test_parse_arguments_missing_command(mocker, reset_argv):
    """Test missing required subcommand (rename, undo, config)."""
    mocker.patch.object(sys, 'argv', ['rename_main.py'])
    with pytest.raises(SystemExit):
        cli.parse_arguments()

def test_help_message(capsys, mocker, reset_argv):
    """Test that help message prints correctly."""
    mocker.patch.object(sys, 'argv', ['rename_main.py', '--help'])
    with pytest.raises(SystemExit):
        cli.parse_arguments()
    captured = capsys.readouterr()
    assert 'usage' in captured.out.lower()
    assert 'rename' in captured.out.lower()
    assert 'undo' in captured.out.lower()
