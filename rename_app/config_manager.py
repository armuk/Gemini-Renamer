# --- START OF FILE config_manager.py ---

# rename_app/config_manager.py

import os
import pytomlpp
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any # Keep existing imports


# --- Pydantic and PlatformDirs Imports ---
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
try:
    import platformdirs
    PLATFORMDIRS_AVAILABLE = True
except ImportError:
    PLATFORMDIRS_AVAILABLE = False
# --- End Imports ---

from dotenv import load_dotenv, find_dotenv
from .exceptions import ConfigError
import argparse

log = logging.getLogger(__name__) # Uses rename_app logger hierarchy
DEFAULT_CONFIG_FILENAME = "config.toml"

# --- Pydantic Schema Definition ---

# Define settings common to default and profiles if applicable
# Or keep them separate for clarity
class BaseProfileSettings(BaseModel):
    # Define fields that can appear in ANY profile section (default or named)
    # Use Optional if they don't have to be present
    # Add defaults using Field(...) or standard assignment
    recursive: Optional[bool] = None
    processing_mode: Optional[str] = None # choices=['auto', 'series', 'movie']? Validation possible here too.
    use_metadata: Optional[bool] = None
    series_format: Optional[str] = None
    movie_format: Optional[str] = None
    subtitle_format: Optional[str] = None
    video_extensions: Optional[List[str]] = None
    associated_extensions: Optional[List[str]] = None # Added
    subtitle_extensions: Optional[List[str]] = None # Added
    on_conflict: Optional[str] = None # choices=['skip', 'overwrite', 'suffix', 'fail']
    create_folders: Optional[bool] = None
    folder_format_series: Optional[str] = None
    folder_format_movie: Optional[str] = None
    enable_undo: Optional[bool] = None
    log_file: Optional[str] = None
    log_level: Optional[str] = None # choices=['DEBUG', 'INFO', 'WARNING', 'ERROR']
    api_rate_limit_delay: Optional[float] = None
    scene_tags_in_filename: Optional[bool] = None
    scene_tags_to_preserve: Optional[List[str]] = None
    subtitle_encoding_detection: Optional[bool] = None
    api_retry_attempts: Optional[int] = Field(None, ge=0) # Example validation
    api_retry_wait_seconds: Optional[float] = Field(None, ge=0.0)
    undo_db_path: Optional[str] = None
    undo_expire_days: Optional[int] = Field(None, ge=-1) # Allow -1 maybe? Or 0? Let's stick to >= 0
    undo_check_integrity: Optional[bool] = None
    undo_integrity_hash_bytes: Optional[int] = Field(None, ge=0)
    cache_enabled: Optional[bool] = None
    cache_directory: Optional[str] = None
    cache_expire_seconds: Optional[int] = Field(None, ge=0)
    scan_strategy: Optional[str] = Field(default='memory') # 'memory' or 'low_memory'
    extract_stream_info: Optional[bool] = Field (default=False) # Default to False
    # Add any other profile-specific keys here
    # --- START ADDED FIELDS ---
    api_year_tolerance: Optional[int] = Field(default=1, ge=0) # Default to 1 year tolerance
    tmdb_match_strategy: Optional[str] = Field(default='first') # Default to first result
    tmdb_match_fuzzy_cutoff: Optional[int] = Field(default=70, ge=0, le=100) # Default score cutoff
    # --- END ADDED FIELDS ---    
    # Example: custom_profile_setting: Optional[str] = None

    # Example validator
    @field_validator('on_conflict', mode='before')
    @classmethod
    def check_on_conflict(cls, v):
        # ... (validator unchanged) ...
        if v is not None and v not in ['skip', 'overwrite', 'suffix', 'fail']:
            raise ValueError("on_conflict must be one of 'skip', 'overwrite', 'suffix', 'fail'")
        return v

    @field_validator('log_level', mode='before')
    @classmethod
    def check_log_level(cls, v):
        # ... (validator unchanged) ...
        if v is not None and v.upper() not in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
            raise ValueError("log_level must be one of DEBUG, INFO, WARNING, ERROR")
        return v.upper() if v else None

    # --- START ADDED VALIDATOR ---
    @field_validator('tmdb_match_strategy', mode='before')
    @classmethod
    def check_tmdb_strategy(cls, v):
        if v is not None and v.lower() not in ['first', 'fuzzy']:
            raise ValueError("tmdb_match_strategy must be 'first' or 'fuzzy'")
        return v.lower() if v else 'first' # Return lowercase or default
    # --- END ADDED VALIDATOR ---

    # Add validator for scan_strategy
    @field_validator('scan_strategy', mode='before')
    @classmethod
    def check_scan_strategy(cls, v):
        if v is not None and v.lower() not in ['memory', 'low_memory']:
            raise ValueError("scan_strategy must be 'memory' or 'low_memory'")
        return v.lower() if v else 'memory' # Default to memory

    # Example validator for new field (add constraints if needed)
    @field_validator('extract_stream_info', mode='before')
    @classmethod
    def check_extract_stream_info(cls, v):
        # Just checks type for now
        if v is not None and not isinstance(v, bool):
            raise ValueError("extract_stream_info must be a boolean (true/false)")
        return v

class DefaultSettings(BaseProfileSettings):
    # Add fields that are *only* expected or required in [default]
    # If all fields are optional or shared, this class might be empty
    # or merged into BaseProfileSettings
    pass

# Root model for the entire config.toml structure
class RootConfigModel(BaseModel):
    default: DefaultSettings = Field(default_factory=DefaultSettings)
    # Define known profile sections explicitly if needed
    # web: Optional[BaseProfileSettings] = None # Example specific profile
    # Or allow arbitrary profiles using a dict (less type safe for specific profile names)
    # profiles: Optional[Dict[str, BaseProfileSettings]] = None

    # Use model_extra = 'allow' if you want to allow undefined profile sections
    # without explicitly defining them here, though they won't be validated
    # by BaseProfileSettings unless accessed via a loop or specific check.
    # For now, let's assume only 'default' is strictly required.
    # Pydantic V2: Use model_config = ConfigDict(extra='allow')
    # class Config:
    #     extra = 'allow' # Pydantic V1 style
    model_config = {'extra': 'allow'} # Pydantic V2 style


# --- End Pydantic Schema ---


class ConfigManager:
    def __init__(self, config_path=None):
        self.config_path = self._resolve_config_path(config_path)
        self._config = self._load_config() # Loads and validates
        self._api_keys = self._load_env_keys()
        log.debug(f"Config path resolved to: {self.config_path}")

    def _resolve_config_path(self, config_path):
        # --- START MODIFICATION: Add User Config Dir ---
        if config_path:
            p = Path(config_path)
            if p.is_file():
                log.debug(f"Using explicit config path: {p.resolve()}")
                return p.resolve()
            else:
                # Raise error if explicit path provided but not found
                raise ConfigError(f"Explicitly provided config file not found: {p}")

        # Search Order: CWD -> User Config Dir -> Project Dir
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
             script_dir = Path(__file__).parent.parent.resolve() # Project root
             proj_path = script_dir / DEFAULT_CONFIG_FILENAME
             if proj_path.is_file():
                 log.debug(f"Found config file in project directory: {proj_path}")
                 return proj_path.resolve()
        except NameError: # __file__ might not be defined if run interactively
             log.debug("__file__ not defined, skipping project directory check.")
        except Exception as e:
            log.warning(f"Error checking project directory for config: {e}")


        # Default to current directory path (even if it doesn't exist)
        log.debug(f"No config file found in CWD, User Dir, or Project Dir. Defaulting to: {cwd_path}")
        return cwd_path.resolve()
        # --- END MODIFICATION ---


    def _load_config(self):
        # --- START MODIFICATION: Add Pydantic Validation ---
        cfg_dict = {}
        if not self.config_path or not self.config_path.is_file():
            log.warning(f"Config file not found at '{self.config_path}'. Using empty config.")
            # Return a structure Pydantic can validate with defaults
            return RootConfigModel().model_dump() # Use default empty model

        try:
            raw_toml_content = self.config_path.read_text(encoding='utf-8')
            cfg_dict = pytomlpp.loads(raw_toml_content)
            log.info(f"Loaded configuration from '{self.config_path}'")

            # Validate the loaded dictionary against the Pydantic model
            try:
                validated_config = RootConfigModel.model_validate(cfg_dict)
                # Return the validated data as a dictionary
                log.debug("Config validation successful.")
                # Use model_dump(mode='python') for richer python types if needed later
                return validated_config.model_dump(exclude_unset=False) # Include defaults
            except ValidationError as e:
                # Provide a more user-friendly error message
                error_details = e.errors()
                error_msgs = [f"  - Field `{' -> '.join(map(str, err['loc']))}`: {err['msg']}" for err in error_details]
                error_summary = f"Config file '{self.config_path}' validation failed:\n" + "\n".join(error_msgs)
                log.error(error_summary)
                raise ConfigError(error_summary) from e

        except pytomlpp.DecodeError as e:
            raise ConfigError(f"Failed to parse TOML config '{self.config_path}': {e}")
        except OSError as e:
            raise ConfigError(f"Failed to read config file '{self.config_path}': {e}")
        except Exception as e:
            # Catch potential Pydantic errors or other unexpected issues
            log.exception(f"Unexpected error loading/validating config '{self.config_path}': {e}")
            raise ConfigError(f"Unexpected error loading/validating config '{self.config_path}': {e}")
        # --- END MODIFICATION ---


    def _load_env_keys(self):
        # (Function largely unchanged, maybe add more env vars if needed)
        keys = {}
        env_path = None
        try:
            env_path = find_dotenv(usecwd=True)
            if env_path: log.debug(f"Loading environment variables from: {env_path}"); load_dotenv(dotenv_path=env_path)
            else: log.debug(".env file not found.")
        except Exception as e: log.warning(f"Error accessing or processing .env file: {e}")

        # Load specific keys
        tmdb_key_env = os.getenv("TMDB_API_KEY")
        tvdb_key_env = os.getenv("TVDB_API_KEY")
        tmdb_lang_env = os.getenv("TMDB_LANGUAGE")
        # Add more env vars as needed (e.g., CACHE_DIR?)
        # cache_dir_env = os.getenv("RENAME_APP_CACHE_DIR")

        log.debug(f"_load_env_keys: os.getenv('TMDB_API_KEY') = '{tmdb_key_env}'")
        log.debug(f"_load_env_keys: os.getenv('TVDB_API_KEY') = '{tvdb_key_env}'")
        log.debug(f"_load_env_keys: os.getenv('TMDB_LANGUAGE') = '{tmdb_lang_env}'")

        keys['tmdb_api_key'] = tmdb_key_env
        keys['tvdb_api_key'] = tvdb_key_env
        keys['tmdb_language'] = tmdb_lang_env
        # keys['cache_directory'] = cache_dir_env # Add if using env var for cache dir

        if any(v for k, v in keys.items() if k.endswith('_api_key')):
             log_msg = "Loaded API keys/settings from " + (".env file." if env_path else "environment variables.")
             log.info(log_msg)
        elif env_path: log.debug(".env file found but no relevant keys set.")

        return keys

    def get_value(self, key, profile='default', command_line_value=None, default_value=None):
        # Command line has highest precedence
        if command_line_value is not None:
            # Handle specific boolean optional actions where None means "not set by user"
            # These keys should match those in argparse using BooleanOptionalAction
            bool_optional_keys = {'recursive', 'use_metadata', 'create_folders', 'enable_undo',
                                  'scene_tags_in_filename', 'subtitle_encoding_detection'}
            if key in bool_optional_keys and command_line_value is None:
                pass # Explicitly bypass return for None value on these keys
            else:
                # Return any other non-None cmd line value (incl False for bools)
                return command_line_value

        # Environment variable (for specific keys)
        # Add other env var overrides here if needed (e.g., cache dir)
        if key == 'tmdb_language' and self._api_keys.get('tmdb_language'):
             return self._api_keys['tmdb_language']
        # if key == 'cache_directory' and self._api_keys.get('cache_directory'):
        #      return self._api_keys['cache_directory']

        # Look in specified profile section of validated config
        # Use .get() with a default empty dict to handle missing profiles gracefully
        profile_settings = self._config.get(profile, {})
        if profile_settings is not None and key in profile_settings:
             val = profile_settings[key]
             # Pydantic should handle defaults, but check for None just in case
             if val is not None:
                 return val

        # Look in default section of validated config
        default_settings = self._config.get('default', {})
        if default_settings is not None and key in default_settings:
             val = default_settings[key]
             if val is not None:
                 return val

        # Return hardcoded default (least precedence)
        return default_value

    def get_api_key(self, service_name):
        """Gets API key specifically (checks env first)."""
        key_name = f"{service_name.lower()}_api_key"
        return self._api_keys.get(key_name)

    def get_profile_settings(self, profile='default'):
        """Gets merged settings for a profile, layering profile over default."""
        # Pydantic validation ensures 'default' exists if config loaded
        settings = self._config.get('default', {}).copy()
        # Merge profile-specific settings over the defaults
        settings.update(self._config.get(profile, {}))
        return settings

# --- Config Helper Class ---
# (Class structure remains the same, benefits from validated config)
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
        """Helper to ensure list values are handled correctly from config/args."""
        cmd_line_val = getattr(self.args, key, None)
        val = self(key, default_value=default_value, arg_value=cmd_line_val)
        # Handle command line string splitting
        if cmd_line_val is not None and isinstance(val, str):
             return [item.strip() for item in val.split(',') if item.strip()]
        # Pydantic ensures lists from config are already lists
        elif isinstance(val, list):
            return val
        # Fallback to default if needed and ensure it's a list
        else:
            return default_value if isinstance(default_value, list) else []

# --- END OF FILE config_manager.py ---