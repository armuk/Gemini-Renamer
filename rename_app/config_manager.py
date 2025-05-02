# rename_app/config_manager.py

import os
import pytomlpp
import logging
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from .exceptions import ConfigError
import argparse

log = logging.getLogger(__name__) # Uses rename_app logger hierarchy
DEFAULT_CONFIG_FILENAME = "config.toml"

class ConfigManager:
    # (Same class structure as outlined in previous thought process)
    def __init__(self, config_path=None):
        self.config_path = self._resolve_config_path(config_path)
        self._config = self._load_config()
        self._api_keys = self._load_env_keys()
        log.debug(f"Config path resolved to: {self.config_path}")

    def _resolve_config_path(self, config_path):
        if config_path: return Path(config_path).resolve()
        # Search order: Current dir -> Script/Project dir -> User config?
        cwd_path = Path.cwd() / DEFAULT_CONFIG_FILENAME
        if cwd_path.is_file(): return cwd_path
        try:
             script_dir = Path(__file__).parent.parent.resolve() # Project root
             proj_path = script_dir / DEFAULT_CONFIG_FILENAME
             if proj_path.is_file(): return proj_path
        except NameError: # __file__ might not be defined if run interactively
             pass
        # Add user config dir search if needed
        return Path.cwd() / DEFAULT_CONFIG_FILENAME # Default to current dir if not found


    def _load_config(self):
        cfg = {'default': {}}
        if not self.config_path or not self.config_path.is_file():
            log.warning(f"Config file not found at '{self.config_path}'. Using defaults.")
            return cfg
        try:
            cfg = pytomlpp.load(self.config_path)
            log.info(f"Loaded configuration from '{self.config_path}'")
            if 'default' not in cfg: cfg['default'] = {}
            return cfg
        except Exception as e:
            raise ConfigError(f"Failed to load/parse config '{self.config_path}': {e}")

    def _load_env_keys(self):
        keys = {}
        env_path = None # Initialize env_path
        try:
            env_path = find_dotenv(usecwd=True)
            if env_path:
                log.debug(f"Loading environment variables from: {env_path}")
                load_dotenv(dotenv_path=env_path)
                # Load keys even if file wasn't necessarily loaded successfully by load_dotenv
                # os.getenv will return None if not set
            else:
                log.debug(".env file not found.")

        except Exception as e:
            log.warning(f"Error accessing or processing .env file: {e}")
        finally:
            # --- FIX: Add debug logging for key retrieval ---
            tmdb_key_env = os.getenv("TMDB_API_KEY")
            tvdb_key_env = os.getenv("TVDB_API_KEY")
            tmdb_lang_env = os.getenv("TMDB_LANGUAGE")
            log.debug(f"_load_env_keys: os.getenv('TMDB_API_KEY') = '{tmdb_key_env}'")
            log.debug(f"_load_env_keys: os.getenv('TVDB_API_KEY') = '{tvdb_key_env}'")
            log.debug(f"_load_env_keys: os.getenv('TMDB_LANGUAGE') = '{tmdb_lang_env}'")
            # --- End FIX ---

            keys['tmdb_api_key'] = tmdb_key_env
            keys['tvdb_api_key'] = tvdb_key_env
            keys['tmdb_language'] = tmdb_lang_env

            # Log only if any actual values were loaded from env
            if any(v for k, v in keys.items() if k.endswith('_api_key')): # Check only keys
                 if env_path: # Check if we actually found a file path
                     log.info("Loaded API keys/settings from .env file.")
                 else: # Keys might be set in system env vars without a file
                     log.info("Loaded API keys/settings from environment variables.")
            elif env_path: # File found but no keys in it or system env
                 log.debug(".env file found but no relevant keys set.")
            # else: # No file found and no relevant system env vars
                # log.debug("No relevant environment variables found.") # Already logged .env not found

        return keys

    def get_value(self, key, profile='default', command_line_value=None, default_value=None):
        # Command line has highest precedence
        if command_line_value is not None:
            if isinstance(command_line_value, bool) or command_line_value is not None:
                # Handle BooleanOptionalAction where None means "not set by user"
                if key == 'recursive' and command_line_value is None: pass # Special handling if needed
                elif key == 'use_metadata' and command_line_value is None: pass
                elif key == 'create_folders' and command_line_value is None: pass
                elif key == 'enable_undo' and command_line_value is None: pass
                else: return command_line_value

        # Environment variable (for specific keys like API language)
        if key == 'tmdb_language' and self._api_keys.get('tmdb_language'):
             return self._api_keys['tmdb_language']

        # Look in specified profile
        val = self._config.get(profile, {}).get(key)
        if val is not None: return val

        # Look in default section
        val = self._config.get('default', {}).get(key)
        if val is not None: return val

        # Return hardcoded default
        return default_value

    def get_api_key(self, service_name):
        """Gets API key specifically (checks env first)."""
        key_name = f"{service_name.lower()}_api_key"
        return self._api_keys.get(key_name)

    def get_profile_settings(self, profile='default'):
        settings = self._config.get('default', {}).copy()
        settings.update(self._config.get(profile, {}))
        return settings

# --- Config Helper Class ---
# This helper makes accessing config values cleaner elsewhere
class ConfigHelper:
    def __init__(self, config_manager: ConfigManager, args_ns: argparse.Namespace):
        self.manager = config_manager
        self.args = args_ns
        self.profile = getattr(args_ns, 'profile', 'default') or 'default'

    def __call__(self, key, default_value=None, arg_value=None):
         # Allow passing explicit arg_value to check, otherwise check args namespace
         cmd_line_val = getattr(self.args, key, None) if arg_value is None else arg_value
         return self.manager.get_value(key, self.profile, cmd_line_val, default_value)

    def get_api_key(self, service_name):
        return self.manager.get_api_key(service_name)

    def get_list(self, key, default_value=None):
        """Helper to ensure list values are handled correctly from config/args."""
        # Check if the value specifically came from args namespace
        cmd_line_val = getattr(self.args, key, None)
        val = self(key, default_value=default_value, arg_value=cmd_line_val) # Get value using normal precedence

        # --- FIX: Only split if the source was the command line value AND it's a string ---
        if cmd_line_val is not None and isinstance(val, str):
             # Only split comma-separated string if it came from command line
             return [item.strip() for item in val.split(',') if item.strip()]
        elif isinstance(val, list): # From config or correctly typed default
            return val
        else: # Default (which wasn't a list) or invalid type from config
            return default_value if isinstance(default_value, list) else []
        # --- End Fix ---