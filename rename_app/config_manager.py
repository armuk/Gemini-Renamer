# --- START OF FILE config_manager.py ---

# rename_app/config_manager.py

import os
import pytomlpp
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any


from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
try:
    import platformdirs
    PLATFORMDIRS_AVAILABLE = True
except ImportError:
    PLATFORMDIRS_AVAILABLE = False

from dotenv import load_dotenv, find_dotenv, dotenv_values, set_key, unset_key
from .exceptions import ConfigError
import argparse

log = logging.getLogger(__name__)
DEFAULT_CONFIG_FILENAME = "config.toml"
DEFAULT_DOTENV_FILENAME = ".env"

class BaseProfileSettings(BaseModel):
    recursive: Optional[bool] = None
    processing_mode: Optional[str] = None
    use_metadata: Optional[bool] = None
    series_format: Optional[str] = None
    movie_format: Optional[str] = None
    subtitle_format: Optional[str] = None
    video_extensions: Optional[List[str]] = None
    associated_extensions: Optional[List[str]] = None
    subtitle_extensions: Optional[List[str]] = None
    on_conflict: Optional[str] = None
    create_folders: Optional[bool] = None
    folder_format_series: Optional[str] = None
    folder_format_movie: Optional[str] = None
    enable_undo: Optional[bool] = None
    log_file: Optional[str] = None
    log_level: Optional[str] = None
    api_rate_limit_delay: Optional[float] = None
    scene_tags_in_filename: Optional[bool] = None
    scene_tags_to_preserve: Optional[List[str]] = None
    subtitle_encoding_detection: Optional[bool] = None
    api_retry_attempts: Optional[int] = Field(None, ge=0)
    api_retry_wait_seconds: Optional[float] = Field(None, ge=0.0)
    undo_db_path: Optional[str] = None
    undo_expire_days: Optional[int] = Field(None, ge=-1)
    undo_check_integrity: Optional[bool] = None
    undo_integrity_hash_bytes: Optional[int] = Field(None, ge=0)
    cache_enabled: Optional[bool] = None
    cache_directory: Optional[str] = None
    cache_expire_seconds: Optional[int] = Field(None, ge=0)
    scan_strategy: Optional[str] = Field(default='memory')
    extract_stream_info: Optional[bool] = Field (default=False)
    api_year_tolerance: Optional[int] = Field(default=1, ge=0)
    tmdb_match_strategy: Optional[str] = Field(default='first')
    tmdb_match_fuzzy_cutoff: Optional[int] = Field(default=70, ge=0, le=100)

    # --- NEW: Unknown File Handling ---
    unknown_file_handling: Optional[str] = Field(default='skip') # skip, guessit_only, move_to_unknown
    unknown_files_dir: Optional[str] = Field(default='_unknown_files_') # Relative to target dir or absolute
    # --- END NEW ---


    @field_validator('on_conflict', mode='before')
    @classmethod
    def check_on_conflict(cls, v):
        if v is not None and v.lower() not in ['skip', 'overwrite', 'suffix', 'fail']: # Make case-insensitive
            raise ValueError("on_conflict must be one of 'skip', 'overwrite', 'suffix', 'fail'")
        return v.lower() if v else None

    @field_validator('log_level', mode='before')
    @classmethod
    def check_log_level(cls, v):
        if v is not None and v.upper() not in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
            raise ValueError("log_level must be one of DEBUG, INFO, WARNING, ERROR")
        return v.upper() if v else None

    @field_validator('tmdb_match_strategy', mode='before')
    @classmethod
    def check_tmdb_strategy(cls, v):
        if v is not None and v.lower() not in ['first', 'fuzzy']:
            raise ValueError("tmdb_match_strategy must be 'first' or 'fuzzy'")
        return v.lower() if v else 'first'

    @field_validator('scan_strategy', mode='before')
    @classmethod
    def check_scan_strategy(cls, v):
        if v is not None and v.lower() not in ['memory', 'low_memory']:
            raise ValueError("scan_strategy must be 'memory' or 'low_memory'")
        return v.lower() if v else 'memory'

    @field_validator('extract_stream_info', mode='before')
    @classmethod
    def check_extract_stream_info(cls, v):
        if v is not None and not isinstance(v, bool):
            raise ValueError("extract_stream_info must be a boolean (true/false)")
        return v

    # --- NEW: Validator for unknown_file_handling ---
    @field_validator('unknown_file_handling', mode='before')
    @classmethod
    def check_unknown_file_handling(cls, v):
        if v is not None and v.lower() not in ['skip', 'guessit_only', 'move_to_unknown']:
            raise ValueError("unknown_file_handling must be one of 'skip', 'guessit_only', 'move_to_unknown'")
        return v.lower() if v else 'skip' # Default to 'skip'
    # --- END NEW ---

class DefaultSettings(BaseProfileSettings):
    pass

class RootConfigModel(BaseModel):
    default: DefaultSettings = Field(default_factory=DefaultSettings)
    model_config = {'extra': 'allow'}


class ConfigManager:
    def __init__(self, config_path=None):
        self.config_path = self._resolve_config_path(config_path)
        self._raw_toml_content_str: Optional[str] = None
        self._config = self._load_config()
        self._api_keys = self._load_env_keys()
        log.debug(f"Config path resolved to: {self.config_path}")

    def _resolve_config_path(self, config_path):
        if config_path:
            p = Path(config_path)
            if p.is_file():
                log.debug(f"Using explicit config path: {p.resolve()}")
                return p.resolve()
            else:
                raise ConfigError(f"Explicitly provided config file not found: {p}")
        cwd_path = Path.cwd() / DEFAULT_CONFIG_FILENAME
        if cwd_path.is_file():
            log.debug(f"Found config file in current directory: {cwd_path}")
            return cwd_path.resolve()
        user_config_path = None
        if PLATFORMDIRS_AVAILABLE:
            try:
                user_dir = Path(platformdirs.user_config_dir("rename_app", "rename_app_author", ensure_exists=True))
                user_config_path = user_dir / DEFAULT_CONFIG_FILENAME
                if user_config_path.is_file():
                    log.debug(f"Found config file in user config directory: {user_config_path}")
                    return user_config_path.resolve()
            except Exception as e:
                log.warning(f"Could not access or check user config directory via platformdirs: {e}")
        else:
            log.debug("'platformdirs' not installed, skipping user config directory check.")
        try:
             script_dir = Path(__file__).parent.parent.resolve()
             proj_path = script_dir / DEFAULT_CONFIG_FILENAME
             if proj_path.is_file():
                 log.debug(f"Found config file in project directory: {proj_path}")
                 return proj_path.resolve()
        except NameError:
             log.debug("__file__ not defined, skipping project directory check.")
        except Exception as e:
            log.warning(f"Error checking project directory for config: {e}")
        log.debug(f"No config file found in CWD, User Dir, or Project Dir. Defaulting to: {cwd_path}")
        return cwd_path.resolve()

    def _load_config(self):
        cfg_dict = {}
        if not self.config_path or not self.config_path.is_file():
            log.warning(f"Config file not found at '{self.config_path}'. Using empty config.")
            self._raw_toml_content_str = "# Config file not found or empty.\n"
            return RootConfigModel().model_dump()
        try:
            self._raw_toml_content_str = self.config_path.read_text(encoding='utf-8')
            cfg_dict = pytomlpp.loads(self._raw_toml_content_str)
            log.info(f"Loaded configuration from '{self.config_path}'")
            try:
                validated_config = RootConfigModel.model_validate(cfg_dict)
                log.debug("Config validation successful.")
                return validated_config.model_dump(exclude_unset=False)
            except ValidationError as e:
                error_details = e.errors()
                error_msgs = [f"  - Field `{' -> '.join(map(str, err['loc']))}`: {err['msg']}" for err in error_details]
                error_summary = f"Config file '{self.config_path}' validation failed:\n" + "\n".join(error_msgs)
                log.error(error_summary)
                raise ConfigError(error_summary) from e
        except pytomlpp.DecodeError as e:
            raise ConfigError(f"Failed to parse TOML config '{self.config_path}': {e}")
        except OSError as e:
            self._raw_toml_content_str = f"# Error reading config file: {e}\n"
            raise ConfigError(f"Failed to read config file '{self.config_path}': {e}")
        except Exception as e:
            self._raw_toml_content_str = f"# Unexpected error loading config: {e}\n"
            log.exception(f"Unexpected error loading/validating config '{self.config_path}': {e}")
            raise ConfigError(f"Unexpected error loading/validating config '{self.config_path}': {e}")

    def get_raw_toml_content(self) -> Optional[str]:
        return self._raw_toml_content_str

    def _load_env_keys(self):
        keys = {}
        env_path = None
        try:
            env_path = find_dotenv(usecwd=True)
            if env_path: log.debug(f"Loading environment variables from: {env_path}"); load_dotenv(dotenv_path=env_path)
            else: log.debug(".env file not found.")
        except Exception as e: log.warning(f"Error accessing or processing .env file: {e}")
        tmdb_key_env = os.getenv("TMDB_API_KEY")
        tvdb_key_env = os.getenv("TVDB_API_KEY")
        tmdb_lang_env = os.getenv("TMDB_LANGUAGE")
        keys['tmdb_api_key'] = tmdb_key_env
        keys['tvdb_api_key'] = tvdb_key_env
        keys['tmdb_language'] = tmdb_lang_env
        if any(v for k, v in keys.items() if k.endswith('_api_key')):
             log_msg = "Loaded API keys/settings from " + (".env file." if env_path else "environment variables.")
             log.info(log_msg)
        elif env_path: log.debug(".env file found but no relevant keys set.")
        return keys

    def get_value(self, key, profile='default', command_line_value=None, default_value=None):
        if command_line_value is not None:
            bool_optional_keys = {'recursive', 'use_metadata', 'create_folders', 'enable_undo',
                                  'scene_tags_in_filename', 'subtitle_encoding_detection'}
            if key in bool_optional_keys and command_line_value is None:
                pass
            else:
                return command_line_value
        if key == 'tmdb_language' and self._api_keys.get('tmdb_language'):
             return self._api_keys['tmdb_language']
        profile_settings = self._config.get(profile, {})
        if profile_settings is not None and key in profile_settings:
             val = profile_settings[key]
             if val is not None:
                 return val
        default_settings = self._config.get('default', {})
        if default_settings is not None and key in default_settings:
             val = default_settings[key]
             if val is not None:
                 return val
        return default_value

    def get_api_key(self, service_name):
        key_name = f"{service_name.lower()}_api_key"
        return self._api_keys.get(key_name)

    def get_profile_settings(self, profile='default'):
        settings = self._config.get('default', {}).copy()
        if profile != 'default' and profile in self._config:
            profile_data = self._config.get(profile, {})
            if isinstance(profile_data, dict):
                 settings.update(profile_data)
            else:
                 log.warning(f"Profile '{profile}' in config is not a dictionary. Skipping merge for this profile.")
        elif profile != 'default':
            log.debug(f"Profile '{profile}' not found in config. Using default settings only.")
        return settings

class ConfigHelper:
    def __init__(self, config_manager: ConfigManager, args_ns: argparse.Namespace):
        self.manager = config_manager
        self.args = args_ns
        self.profile = getattr(args_ns, 'profile', 'default') or 'default'

    def __call__(self, key, default_value=None, arg_value=None):
         cmd_line_val = getattr(self.args, key, None) if arg_value is None else arg_value
         return self.manager.get_value(key, self.profile, cmd_line_val, default_value)

    def get_api_key(self, service_name):
        return self.manager.get_api_key(service_name)

    def get_list(self, key, default_value=None):
        cmd_line_val = getattr(self.args, key, None)
        val = self(key, default_value=default_value, arg_value=cmd_line_val)
        if cmd_line_val is not None and isinstance(val, str):
             return [item.strip() for item in val.split(',') if item.strip()]
        elif isinstance(val, list):
            return val
        else:
            return default_value if isinstance(default_value, list) else []

def interactive_api_setup(dotenv_path_override: Optional[Path] = None) -> bool:
    # ... (interactive_api_setup unchanged) ...
    if dotenv_path_override:
        dotenv_path = dotenv_path_override.resolve()
    else:
        dotenv_path = Path.cwd() / DEFAULT_DOTENV_FILENAME
    log.info(f"Starting interactive API setup. Target .env file: {dotenv_path}")
    print(f"--- API Key Setup ---")
    print(f"This will guide you through setting up API keys in '{dotenv_path}'.")
    print("Press Enter to keep the current value (if any) or skip if not set.")
    try:
        current_values = {}
        if dotenv_path.exists() and dotenv_path.is_file():
            log.debug(f"Loading existing values from {dotenv_path}")
            current_values = dotenv_values(dotenv_path)
        else:
            log.debug(f".env file not found at {dotenv_path}. Will create a new one.")
        keys_to_set = {
            "TMDB_API_KEY": {"prompt": "Enter your TMDB API Key", "current": current_values.get("TMDB_API_KEY", "")},
            "TVDB_API_KEY": {"prompt": "Enter your TVDB API Key (V4)", "current": current_values.get("TVDB_API_KEY", "")},
            "TMDB_LANGUAGE": {"prompt": "Enter default TMDB language (e.g., en, de, fr)", "current": current_values.get("TMDB_LANGUAGE", "en"), "default": "en"}
        }
        updated_any = False
        for key, info in keys_to_set.items():
            prompt_text = f"{info['prompt']}"
            if info['current']:
                prompt_text += f" [current: {info['current']}]"
            elif info.get('default'):
                prompt_text += f" [default: {info.get('default')}]"
            prompt_text += ": "
            try:
                user_input = input(prompt_text).strip()
                if user_input:
                    set_key(dotenv_path, key, user_input, quote_mode="never")
                    log.info(f"Set {key} to '{user_input}' in {dotenv_path}")
                    print(f"  ✓ {key} set to: {user_input}")
                    updated_any = True
                elif not info['current'] and info.get('default') and key == "TMDB_LANGUAGE":
                    set_key(dotenv_path, key, info['default'], quote_mode="never")
                    log.info(f"Set {key} to default '{info['default']}' in {dotenv_path}")
                    print(f"  ✓ {key} set to default: {info['default']}")
                    updated_any = True
                elif not user_input and not info['current']:
                    if key in ["TMDB_API_KEY", "TVDB_API_KEY"] and dotenv_path.exists() and key in dotenv_values(dotenv_path):
                        unset_key(dotenv_path, key)
                        log.info(f"Removed empty {key} from {dotenv_path}")
                        print(f"  ✓ {key} removed (was empty).")
                        updated_any = True
                    else:
                        print(f"  - {key} skipped (no value provided).")
            except KeyboardInterrupt:
                print("\nSetup cancelled by user.")
                log.warning("API setup cancelled by user during input.")
                return False
            except Exception as e:
                log.error(f"Error during input for {key}: {e}", exc_info=True)
                print(f"  ✗ Error processing input for {key}. Skipping.")
        if updated_any:
            print(f"\nConfiguration saved to: {dotenv_path}")
        else:
            print("\nNo changes made to .env file.")
        print("--- Setup Complete ---")
        return True
    except IOError as e:
        log.error(f"IOError during API setup writing to {dotenv_path}: {e}", exc_info=True)
        print(f"\nError: Could not write to .env file at '{dotenv_path}'. Check permissions.")
        return False
    except Exception as e:
        log.exception(f"An unexpected error occurred during interactive API setup: {e}")
        print(f"\nAn unexpected error occurred: {e}")
        return False
# --- END OF FILE config_manager.py ---