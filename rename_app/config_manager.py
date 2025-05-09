# rename_app/config_manager.py

import os
import pytomlpp
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Set, Union

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
try:
    import platformdirs
    PLATFORMDIRS_AVAILABLE = True
except ImportError:
    PLATFORMDIRS_AVAILABLE = False

# --- RICH IMPORT FOR CONFIRMATION ---
import builtins
try:
    from rich.console import Console
    from rich.prompt import Confirm # For user confirmation
    RICH_AVAILABLE_FOR_CONFIRM = True
except ImportError:
    RICH_AVAILABLE_FOR_CONFIRM = False
    class Console: # Fallback
        def print(self, *args, **kwargs): builtins.print(*args, **kwargs)
    class Confirm: # Fallback
        @staticmethod
        def ask(prompt_text: str, default: bool = False) -> bool:
            response = builtins.input(f"{prompt_text} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
            if not response: return default
            return response == 'y'
# --- END RICH IMPORT ---


from dotenv import load_dotenv, find_dotenv, dotenv_values, set_key, unset_key
from .exceptions import ConfigError
import argparse

log = logging.getLogger(__name__)
DEFAULT_CONFIG_FILENAME = "config.toml"
DEFAULT_DOTENV_FILENAME = ".env"

class BaseProfileSettings(BaseModel):
    # Core Settings
    recursive: Optional[bool] = Field(default=False, description="Scan subdirectories.")
    processing_mode: Optional[str] = Field(default='auto', description="Processing mode: 'auto', 'series', 'movie'.")
    use_metadata: Optional[bool] = Field(default=True, description="Fetch metadata from APIs.")
    extract_stream_info: Optional[bool] = Field(default=False, description="Extract technical stream info (resolution, codecs).")
    preserve_mtime: Optional[bool] = Field(default=False, description="Preserve original file modification time.")
    ignore_dirs: Optional[List[str]] = Field(default_factory=list, description="List of exact directory names to ignore.") # e.g., ["@eaDir", ".recycle"]
    ignore_patterns: Optional[List[str]] = Field(
        default_factory=lambda: ['.*', '*.partial', 'Thumbs.db', '*[sS]ample*'], # More useful defaults
        description="List of glob patterns (e.g., '*.tmp', '.*') to ignore."
    )

    # Format Strings
    series_format: Optional[str] = Field(default="{show_title} ({show_year})/Season {season:02d}/S{season:02d}{ep_identifier} - {episode_title}", description="Filename format for series episodes.")
    movie_format: Optional[str] = Field(default="{movie_title} ({movie_year})/{movie_title} ({movie_year})", description="Filename format for movies.")
    subtitle_format: Optional[str] = Field(default="{stem}{lang_dot}{flags_dot}", description="Filename format for subtitles.")
    series_format_specials: Optional[str] = Field(default="{show_title} ({show_year})/Season 00/S00{ep_identifier} - {episode_title}", description="Filename format for series specials (Season 00).")
    folder_format_series: Optional[str] = Field(default="{show_title} ({show_year})/Season {season:02d}", description="Folder structure for series.")
    folder_format_movie: Optional[str] = Field(default="{movie_title} ({movie_year})", description="Folder structure for movies.")
    folder_format_specials: Optional[str] = Field(default="{show_title} ({show_year})/Season 00", description="Folder structure for series specials.")

    # File Handling & Extensions
    video_extensions: Optional[List[str]] = Field(default_factory=lambda: [".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm", ".mpg", ".mpeg", ".m4v", ".ts", ".m2ts"], description="List of video file extensions.")
    associated_extensions: Optional[List[str]] = Field(default_factory=lambda: [".nfo", ".txt", ".jpg", ".png", ".sfv"], description="List of associated file extensions (excluding subtitles).")
    subtitle_extensions: Optional[List[str]] = Field(default_factory=lambda: [".srt", ".sub", ".ass", ".ssa", ".vtt", ".idx"], description="List of subtitle file extensions.")
    on_conflict: Optional[str] = Field(default='skip', description="Action on filename conflict: 'skip', 'overwrite', 'suffix', 'fail'.")
    create_folders: Optional[bool] = Field(default=True, description="Create destination folders if they don't exist.")
    unknown_file_handling: Optional[str] = Field(default='skip', description="How to handle unknown files: 'skip', 'guessit_only', 'move_to_unknown'.")
    unknown_files_dir: Optional[str] = Field(default='_unknown_files_', description="Directory for 'move_to_unknown' files (relative to target or absolute).")
    scan_strategy: Optional[str] = Field(default='memory', description="Scanning strategy: 'memory', 'low_memory'.")

    # Scene Tags
    scene_tags_in_filename: Optional[bool] = Field(default=True, description="Include scene tags in the final filename.")
    scene_tags_to_preserve: Optional[List[str]] = Field(default_factory=lambda: ["PROPER", "REPACK", "READ.NFO", "INTERNAL", "LIMITED", "UNCUT", "UNRATED", "DIRECTORS.CUT", "EXTENDED", "REMASTERED", "COMPLETE"], description="List of scene tags to preserve if found.")

    # Subtitles
    subtitle_encoding_detection: Optional[bool] = Field(default=True, description="Attempt to detect subtitle encoding (requires 'chardet').")

    # API & Metadata Options
    api_rate_limit_delay: Optional[float] = Field(default=0.5, ge=0.0, description="Delay (seconds) between API calls.")
    api_retry_attempts: Optional[int] = Field(default=3, ge=0, description="Number of retry attempts for API calls.")
    api_retry_wait_seconds: Optional[float] = Field(default=2.0, ge=0.0, description="Wait time (seconds) between API retry attempts.")
    api_year_tolerance: Optional[int] = Field(default=1, ge=0, description="Year tolerance for matching API results.")
    tmdb_match_strategy: Optional[str] = Field(default='first', description="TMDB matching strategy: 'first', 'fuzzy'.")
    tmdb_match_fuzzy_cutoff: Optional[int] = Field(default=70, ge=0, le=100, description="Minimum score for 'fuzzy' TMDB match.")
    tmdb_first_result_min_score: Optional[int] = Field(default=65, ge=0, le=100, description="Minimum fuzzy score for a 'first' strategy TMDB match to be considered valid (requires 'thefuzz').")
    confirm_match_below: Optional[int] = Field(default=None, ge=0, le=100, description="Interactively confirm metadata match if score is below this value (0-100).")
    series_metadata_preference: Optional[List[str]] = Field(default=['tmdb', 'tvdb'], description="Preferred metadata source order for series.")

    # Caching Options
    cache_enabled: Optional[bool] = Field(default=True, description="Enable API response caching.")
    cache_directory: Optional[str] = Field(default=None, description="Custom cache directory (default: user cache dir).")
    cache_expire_seconds: Optional[int] = Field(default=604800, ge=0, description="Cache expiration time in seconds (default: 7 days).") # 7 days

    # Undo Options
    enable_undo: Optional[bool] = Field(default=True, description="Enable undo logging.")
    undo_db_path: Optional[str] = Field(default=None, description="Path to undo database file (default: in app dir).")
    undo_expire_days: Optional[int] = Field(default=30, ge=-1, description="Days to keep undo logs (-1 for forever, 0 for session only).")
    undo_check_integrity: Optional[bool] = Field(default=False, description="Verify file integrity before undoing (size, mtime).")
    undo_integrity_hash_bytes: Optional[int] = Field(default=0, ge=0, description="Bytes to hash for integrity check (0 to disable partial hash).")
    undo_integrity_hash_full: Optional[bool] = Field(default=False, description="Calculate full file hash for undo integrity check (SLOW, overrides hash_bytes).")

    # Logging Options
    log_file: Optional[str] = Field(default=None, description="Path to log file (e.g., rename_app.log).") # Default "rename_app.log" if enabled
    log_level: Optional[str] = Field(default='INFO', description="Logging level: DEBUG, INFO, WARNING, ERROR.")

    @field_validator('on_conflict', mode='before')
    @classmethod
    def check_on_conflict(cls, v):
        if v is not None and v.lower() not in ['skip', 'overwrite', 'suffix', 'fail']:
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

    @field_validator('unknown_file_handling', mode='before')
    @classmethod
    def check_unknown_file_handling(cls, v):
        if v is not None and v.lower() not in ['skip', 'guessit_only', 'move_to_unknown']:
            raise ValueError("unknown_file_handling must be one of 'skip', 'guessit_only', 'move_to_unknown'")
        return v.lower() if v else 'skip'

    @field_validator('series_metadata_preference', mode='before')
    @classmethod
    def check_series_metadata_preference(cls, v):
        if v is None: return ['tmdb', 'tvdb'] 
        if isinstance(v, str): v = [item.strip().lower() for item in v.split(',') if item.strip()]
        if not isinstance(v, list): raise ValueError("series_metadata_preference must be a list")
        if len(v) != 2: raise ValueError("series_metadata_preference must contain exactly two sources")
        sources = {s.lower() for s in v}; allowed = {'tmdb', 'tvdb'}
        if sources != allowed: raise ValueError(f"series_metadata_preference must be 'tmdb' and 'tvdb', got: {v}")
        return [s.lower() for s in v]

    @field_validator('preserve_mtime', mode='before')
    @classmethod
    def check_preserve_mtime(cls, v):
        if v is not None and not isinstance(v, bool): raise ValueError("preserve_mtime must be a boolean")
        return v

    @field_validator('undo_integrity_hash_full', mode='before')
    @classmethod
    def check_undo_integrity_hash_full(cls, v):
        if v is not None and not isinstance(v, bool): raise ValueError("undo_integrity_hash_full must be a boolean")
        return v

class DefaultSettings(BaseProfileSettings):
    pass

class RootConfigModel(BaseModel):
    default: DefaultSettings = Field(default_factory=DefaultSettings)
    model_config = {'extra': 'allow'} # Allow other profiles like [series_profile], [movie_profile]


def generate_default_toml_content() -> str:
    """Generates the content for a default config.toml file with comments."""
    default_settings = DefaultSettings()
    content_lines = ["# Gemini-Renamer Default Configuration File"]
    content_lines.append("# For more details on placeholders, see README.md or documentation.\n")
    
    sections: Dict[str, List[str]] = {
        "Core Settings": ['recursive', 'processing_mode', 'use_metadata', 'extract_stream_info', 'preserve_mtime', 'ignore_dirs', 'ignore_patterns'],
        "Format Strings": ['series_format', 'movie_format', 'subtitle_format', 'series_format_specials', 'folder_format_series', 'folder_format_movie', 'folder_format_specials'],
        "File Handling & Extensions": ['video_extensions', 'associated_extensions', 'subtitle_extensions', 'on_conflict', 'create_folders', 'unknown_file_handling', 'unknown_files_dir', 'scan_strategy'],
        "Scene Tags": ['scene_tags_in_filename', 'scene_tags_to_preserve'],
        "Subtitles": ['subtitle_encoding_detection'],
        "API & Metadata Options": ['api_rate_limit_delay', 'api_retry_attempts', 'api_retry_wait_seconds', 'api_year_tolerance', 'tmdb_match_strategy', 'tmdb_match_fuzzy_cutoff', 'tmdb_first_result_min_score', 'confirm_match_below', 'series_metadata_preference'],
        "Caching Options": ['cache_enabled', 'cache_directory', 'cache_expire_seconds'],
        "Undo Options": ['enable_undo', 'undo_db_path', 'undo_expire_days', 'undo_check_integrity', 'undo_integrity_hash_bytes', 'undo_integrity_hash_full'],
        "Logging Options": ['log_file', 'log_level'],
    }

    content_lines.append("[default]")
    for section_name, keys in sections.items():
        content_lines.append(f"\n  # --- {section_name} ---")
        for key in keys:
            field_info = BaseProfileSettings.model_fields.get(key)
            if field_info:
                default_value = getattr(default_settings, key)
                comment = field_info.description or ""
                if comment:
                    content_lines.append(f"  # {comment}")
                
                # Format default value for TOML
                if isinstance(default_value, str):
                    toml_value = f'"{default_value}"'
                elif isinstance(default_value, bool):
                    toml_value = str(default_value).lower()
                elif isinstance(default_value, list):
                    # Ensure strings in list are quoted
                    toml_value = "[" + ", ".join([f'"{str(i)}"' if isinstance(i, str) else str(i).lower() if isinstance(i, bool) else str(i) for i in default_value]) + "]"
                elif default_value is None: # Handle None for optional fields
                    toml_value = "# (not set, uses internal default or None)" 
                    content_lines.append(f"  # {key} = {toml_value}") # Comment out unset optionals
                    continue 
                else: # Numbers, etc.
                    toml_value = str(default_value)
                
                content_lines.append(f"  {key} = {toml_value}")
    
    content_lines.append("\n# You can create other profiles, e.g.:")
    content_lines.append("# [movie_profile]")
    content_lines.append("# movie_format = \"{movie_title} [{movie_year}] - {resolution}\"")
    content_lines.append("# use_metadata = true")

    return "\n".join(content_lines)


class ConfigManager:
    def __init__(self, config_path_override: Optional[Path] = None, interactive_fallback: bool = True):
        self.console = Console() # For prompting
        self.config_path = self._resolve_config_path(config_path_override)
        self._raw_toml_content_str: Optional[str] = None
        self._config = self._load_config(interactive_fallback=interactive_fallback) # Pass flag
        self._api_keys = self._load_env_keys()
        log.debug(f"Config path used: {self.config_path}")


    def _resolve_config_path(self, config_path_override: Optional[Path]):
        # (Resolution logic remains the same, but it will return the *intended* path)
        if config_path_override:
            p = Path(config_path_override)
            # If it's an existing file, great. If not, we assume the user wants to create it there if confirmed.
            log.debug(f"Using explicit config path target: {p.resolve()}")
            return p.resolve()
            
        # Standard search order
        cwd_path = Path.cwd() / DEFAULT_CONFIG_FILENAME
        if cwd_path.is_file():
            log.debug(f"Found config file in current directory: {cwd_path}")
            return cwd_path.resolve()
        
        user_config_path = None
        if PLATFORMDIRS_AVAILABLE:
            try:
                user_dir = Path(platformdirs.user_config_dir("rename_app", "rename_app_author", ensure_exists=False)) # Don't ensure_exists yet
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
        except NameError: log.debug("__file__ not defined, skipping project directory check.")
        except Exception as e: log.warning(f"Error checking project directory for config: {e}")
        
        # If not found, default to creating in CWD (if confirmed) or user_config_path if available
        if user_config_path:
            log.debug(f"No config file found. Preferred default creation location: {user_config_path}")
            return user_config_path
        
        log.debug(f"No config file found. Defaulting to CWD for potential creation: {cwd_path}")
        return cwd_path.resolve()


    def _create_default_config_interactively(self, target_path: Path) -> bool:
        """Asks user if they want to create a default config file and creates it."""
        self.console.print(f"[yellow]Configuration file not found at an expected location.[/yellow]")
        self.console.print(f"A default configuration file can be created at:")
        self.console.print(f"  [cyan]{target_path}[/cyan]")
        
        if Confirm.ask("Would you like to create a default configuration file now?", default=True):
            try:
                default_content = generate_default_toml_content()
                target_path.parent.mkdir(parents=True, exist_ok=True) # Ensure parent dir exists
                with open(target_path, "w", encoding="utf-8") as f:
                    f.write(default_content)
                self.console.print(f"[green]✓ Default configuration file created at: {target_path}[/green]")
                self.console.print(f"[bright_magenta]Please review and customize '{target_path}' as needed, especially API keys if not using an .env file.[/bright_magenta]")
                log.info(f"Default configuration file created at {target_path}")
                return True
            except IOError as e:
                self.console.print(f"[bold red]Error creating configuration file: {e}[/bold red]")
                log.error(f"Failed to write default config to {target_path}: {e}")
                return False
            except Exception as e:
                self.console.print(f"[bold red]An unexpected error occurred while creating the configuration file: {e}[/bold red]")
                log.exception(f"Unexpected error creating default config at {target_path}: {e}")
                return False
        else:
            self.console.print("[yellow]Skipping default configuration file creation. Using internal defaults.[/yellow]")
            log.info("User opted out of creating a default configuration file.")
            return False


    def _load_config(self, interactive_fallback: bool = True):
        cfg_dict = {}
        config_file_existed_initially = self.config_path.is_file()

        if not config_file_existed_initially and interactive_fallback:
            # Offer to create it
            if not self._create_default_config_interactively(self.config_path):
                # User declined or creation failed, proceed with empty/default config
                log.warning(f"Proceeding without a config file. Using internal defaults.")
                self._raw_toml_content_str = "# No configuration file present or created.\n"
                # Return Pydantic model defaults
                return RootConfigModel().model_dump(exclude_unset=False) # Use exclude_unset=False to get all defaults

        # If file now exists (either it was there or just created)
        if self.config_path.is_file():
            try:
                self._raw_toml_content_str = self.config_path.read_text(encoding='utf-8')
                cfg_dict = pytomlpp.loads(self._raw_toml_content_str)
                log.info(f"Loaded configuration from '{self.config_path}'")
            except pytomlpp.DecodeError as e:
                raise ConfigError(f"Failed to parse TOML config '{self.config_path}': {e}")
            except OSError as e:
                self._raw_toml_content_str = f"# Error reading config file: {e}\n"
                raise ConfigError(f"Failed to read config file '{self.config_path}': {e}")
        else: # File still doesn't exist (e.g., non-interactive mode, or creation failed and user still declined)
            log.warning(f"Config file not found at '{self.config_path}' and not created. Using internal defaults.")
            self._raw_toml_content_str = "# Config file not found or empty.\n"
            return RootConfigModel().model_dump(exclude_unset=False)

        try:
            validated_config = RootConfigModel.model_validate(cfg_dict)
            log.debug("Config validation successful.")
            return validated_config.model_dump(exclude_unset=False) 
        except ValidationError as e:
            error_details = e.errors()
            error_msgs = [f"  - Field `{' -> '.join(map(str, err['loc']))}`: {err['msg']}" for err in error_details]
            error_summary = f"Config file '{self.config_path}' validation failed:\n" + "\n".join(error_msgs)
            log.error(error_summary)
            # If the file was just created, a validation error is unlikely unless our default generator is wrong.
            # If it existed, it's a user error.
            if config_file_existed_initially:
                raise ConfigError(error_summary) from e
            else: # Newly created file failed validation (should be rare)
                log.critical(f"Newly created default config FAILED validation. This is an internal error. {error_summary}")
                self.console.print(f"[bold red]INTERNAL ERROR: The generated default configuration is invalid. Please report this.[/bold red]")
                self.console.print(error_summary)
                # Fallback to pure Pydantic defaults
                return RootConfigModel().model_dump(exclude_unset=False)
        except Exception as e:
            self._raw_toml_content_str = f"# Unexpected error loading config: {e}\n"
            log.exception(f"Unexpected error loading/validating config '{self.config_path}': {e}")
            raise ConfigError(f"Unexpected error loading/validating config '{self.config_path}': {e}")

    def get_raw_toml_content(self) -> Optional[str]:
        return self._raw_toml_content_str

    def _load_env_keys(self):
        # (Function unchanged)
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
        # (Function unchanged)
        if command_line_value is not None:
            bool_optional_keys = {
                'recursive', 'use_metadata', 'create_folders', 'enable_undo',
                'scene_tags_in_filename', 'subtitle_encoding_detection',
                'extract_stream_info', 'preserve_mtime',
                'undo_integrity_hash_full'
            }
            if key in bool_optional_keys and command_line_value is None:
                 pass 
            else:
                if key == 'series_metadata_preference' and isinstance(command_line_value, str):
                    validated_list = [item.strip().lower() for item in command_line_value.split(',') if item.strip()]
                    if len(validated_list) == 2 and set(validated_list) == {'tmdb', 'tvdb'}:
                        return validated_list
                    else:
                        log.warning(f"Invalid command-line value for {key}: '{command_line_value}'. Ignoring.")
                else:
                    return command_line_value
        if key == 'tmdb_language' and self._api_keys.get('tmdb_language'):
             return self._api_keys['tmdb_language']
        profile_settings = self._config.get(profile, {})
        if profile_settings is not None and key in profile_settings:
            val = profile_settings[key]
            if val is not None:
                if key == 'series_metadata_preference':
                    if isinstance(val, list) and len(val) == 2 and set(s.lower() for s in val) == {'tmdb', 'tvdb'}:
                        return [s.lower() for s in val]
                    else:
                        log.warning(f"Invalid config value for {key} in profile '{profile}'. Using default.")
                else: return val
        default_settings = self._config.get('default', {})
        if default_settings is not None and key in default_settings:
            val = default_settings[key]
            if val is not None:
                if key == 'series_metadata_preference':
                    if isinstance(val, list) and len(val) == 2 and set(s.lower() for s in val) == {'tmdb', 'tvdb'}:
                        return [s.lower() for s in val]
                    else:
                        log.warning(f"Invalid config value for {key} in profile 'default'. Using model default.")
                else: return val
        model_default = None
        if profile == 'default' and hasattr(DefaultSettings, 'model_fields') and key in DefaultSettings.model_fields:
            model_default = DefaultSettings.model_fields[key].default
        elif hasattr(BaseProfileSettings, 'model_fields') and key in BaseProfileSettings.model_fields:
            model_default = BaseProfileSettings.model_fields[key].default
        if model_default is not None:
            if key == 'series_metadata_preference' and isinstance(model_default, list): return model_default[:] 
            return model_default
        return default_value

    def get_api_key(self, service_name):
        # (Function unchanged)
        key_name = f"{service_name.lower()}_api_key"
        return self._api_keys.get(key_name)

    def get_profile_settings(self, profile='default'):
        # (Function unchanged)
        settings = self._config.get('default', {}).copy()
        if profile != 'default' and profile in self._config:
            profile_data = self._config.get(profile, {})
            if isinstance(profile_data, dict):
                 settings.update({k: v for k, v in profile_data.items() if v is not None})
            else:
                 log.warning(f"Profile '{profile}' in config is not a dictionary. Skipping merge for this profile.")
        elif profile != 'default':
            log.debug(f"Profile '{profile}' not found in config. Using default settings only.")
        return settings


class ConfigHelper:
    # (Class unchanged)
    def __init__(self, config_manager: ConfigManager, args_ns: argparse.Namespace):
        self.manager = config_manager
        self.args = args_ns
        self.profile = getattr(args_ns, 'profile', 'default') or 'default'
    def __call__(self, key, default_value=None, arg_value=None):
        if arg_value is not None: cmd_line_val = arg_value
        else: cmd_line_val = getattr(self.args, key, None)
        return self.manager.get_value(key, self.profile, cmd_line_val, default_value)
    def get_api_key(self, service_name): return self.manager.get_api_key(service_name)
    def get_list(self, key, default_value=None):
        cmd_line_val_str = getattr(self.args, key, None); cmd_line_list = None
        if isinstance(cmd_line_val_str, str):
            cmd_line_list = [item.strip() for item in cmd_line_val_str.split(',') if item.strip()]
        val = self(key, default_value=default_value, arg_value=cmd_line_list)
        if isinstance(val, list): return val
        elif isinstance(val, str): return [item.strip() for item in val.split(',') if item.strip()]
        else: return default_value if isinstance(default_value, list) else []

def interactive_api_setup(dotenv_path_override: Optional[Path] = None) -> bool:
    # (Function unchanged)
    if dotenv_path_override: dotenv_path = dotenv_path_override.resolve()
    else: dotenv_path = Path.cwd() / DEFAULT_DOTENV_FILENAME
    log.info(f"Starting interactive API setup. Target .env file: {dotenv_path}")
    print(f"--- API Key Setup ---")
    print(f"This will guide you through setting up API keys in '{dotenv_path}'.")
    print("Press Enter to keep the current value (if any) or skip if not set.")
    try:
        current_values = {}; console = Console()
        if dotenv_path.exists() and dotenv_path.is_file():
            log.debug(f"Loading existing values from {dotenv_path}"); current_values = dotenv_values(dotenv_path)
        else: log.debug(f".env file not found at {dotenv_path}. Will create a new one.")
        keys_to_set = {
            "TMDB_API_KEY": {"prompt": "Enter your TMDB API Key", "current": current_values.get("TMDB_API_KEY", "")},
            "TVDB_API_KEY": {"prompt": "Enter your TVDB API Key (V4)", "current": current_values.get("TVDB_API_KEY", "")},
            "TMDB_LANGUAGE": {"prompt": "Enter default TMDB language (e.g., en, de, fr)", "current": current_values.get("TMDB_LANGUAGE", "en"), "default": "en"}
        }
        updated_any = False
        for key, info in keys_to_set.items():
            prompt_text = f"{info['prompt']}"
            if info['current']: prompt_text += f" [current: {info['current']}]"
            elif info.get('default'): prompt_text += f" [default: {info.get('default')}]"
            prompt_text += ": "
            try:
                user_input = console.input(prompt_text).strip() # Use Rich console input
                if user_input:
                    set_key(dotenv_path, key, user_input, quote_mode="never")
                    log.info(f"Set {key} to '{user_input}' in {dotenv_path}"); console.print(f"  ✓ {key} set to: {user_input}"); updated_any = True
                elif not info['current'] and info.get('default') and key == "TMDB_LANGUAGE":
                    set_key(dotenv_path, key, info['default'], quote_mode="never")
                    log.info(f"Set {key} to default '{info['default']}' in {dotenv_path}"); console.print(f"  ✓ {key} set to default: {info['default']}"); updated_any = True
                elif not user_input and not info['current']:
                    if key in ["TMDB_API_KEY", "TVDB_API_KEY"] and dotenv_path.exists() and key in dotenv_values(dotenv_path):
                        unset_key(dotenv_path, key); log.info(f"Removed empty {key} from {dotenv_path}"); console.print(f"  ✓ {key} removed (was empty)."); updated_any = True
                    else: console.print(f"  - {key} skipped (no value provided).")
            except KeyboardInterrupt: console.print("\nSetup cancelled by user."); log.warning("API setup cancelled by user during input."); return False
            except Exception as e: log.error(f"Error during input for {key}: {e}", exc_info=True); console.print(f"  ✗ Error processing input for {key}. Skipping.")
        if updated_any: console.print(f"\nConfiguration saved to: {dotenv_path}")
        else: console.print("\nNo changes made to .env file.")
        console.print("--- Setup Complete ---"); return True
    except IOError as e: log.error(f"IOError during API setup writing to {dotenv_path}: {e}", exc_info=True); console.print(f"\nError: Could not write to .env file at '{dotenv_path}'. Check permissions."); return False
    except Exception as e: log.exception(f"An unexpected error occurred during interactive API setup: {e}"); console.print(f"\nAn unexpected error occurred: {e}"); return False