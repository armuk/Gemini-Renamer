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
        try:
            # find_dotenv searches cwd and parent dirs
            env_path = find_dotenv(usecwd=True) # Start search from cwd
            if env_path:
                log.debug(f"Loading environment variables from: {env_path}")
                load_dotenv(dotenv_path=env_path)
                keys['tmdb_api_key'] = os.getenv("TMDB_API_KEY")
                keys['tvdb_api_key'] = os.getenv("TVDB_API_KEY")
                keys['tmdb_language'] = os.getenv("TMDB_LANGUAGE") # Load language from env too
                if any(keys.values()):
                     log.info("Loaded API keys/settings from .env file.")
                else:
                     log.debug(".env file found but no relevant keys set.")
            else:
                log.debug(".env file not found.")
        except Exception as e:
            log.warning(f"Error loading .env file: {e}")
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
        val = self(key, default_value)
        if isinstance(val, str): # From command line
            return [item.strip() for item in val.split(',') if item.strip()]
        elif isinstance(val, list): # From config
            return val
        else: # Default or invalid
            return default_value if isinstance(default_value, list) else []