import logging
import sys
from pathlib import Path
from datetime import datetime, timezone

def setup_logging(log_level_console=logging.INFO, log_file=None):
    log = logging.getLogger("rename_app") # Get specific logger
    log.setLevel(logging.DEBUG)
    for handler in log.handlers[:]: log.removeHandler(handler) # Clear existing

    # Console
    log_fmt_console = '%(levelname)-8s: %(message)s'
    if log_level_console <= logging.DEBUG:
        log_fmt_console = '%(asctime)s %(levelname)-8s [%(name)s:%(lineno)d] %(message)s'

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(log_level_console)
    
    # --- Add this line ---
    print(f"DEBUG_CHECK: Console handler level set to: {logging.getLevelName(console_handler.level)}", file=sys.stderr)
    # --- End Add ---

    console_formatter = logging.Formatter(log_fmt_console, datefmt='%H:%M:%S')
    console_handler.setFormatter(console_formatter)
    log.addHandler(console_handler)

    # File
    if log_file:
        try:
            log_file_path = Path(log_file).resolve()
            log_file_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(name)s:%(lineno)d] %(message)s', datefmt='%Y-%m-%d %H:%M:%S%z')
            file_handler.setFormatter(file_formatter)
            log.addHandler(file_handler)
            log.info(f"--- Log session started: {datetime.now(timezone.utc).isoformat()} ---")
            log.info(f"Command: {' '.join(sys.argv)}")
        except Exception as e:
            log.error(f"Failed to configure file logging to '{log_file}': {e}")
    return log # Return the configured logger instance