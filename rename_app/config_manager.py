# rename_app/config_manager.py

import os
import sys # For sys.stderr in critical prints
import builtins # For builtins.print
import pytomlpp
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Set, Union

from pydantic import BaseModel, Field, ValidationError, field_validator # model_validator not used here, but fine

try:
    import platformdirs
    PLATFORMDIRS_AVAILABLE = True # Export this for rename_main.py if needed there
except ImportError:
    PLATFORMDIRS_AVAILABLE = False
    platformdirs = None # Define for type checking if used directly below

# --- MODIFIED RICH IMPORT ---
from rename_app.ui_utils import ( # Import from the new ui_utils module
    ConsoleClass, ConfirmClass,
    RICH_AVAILABLE_UI as RICH_AVAILABLE_FOR_CONFIRM # Use the flag from ui_utils
)
# --- END MODIFIED RICH IMPORT ---


from dotenv import load_dotenv, find_dotenv, dotenv_values, set_key, unset_key
from .exceptions import ConfigError
import argparse # Keep for ConfigHelper type hint

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
    ignore_dirs: Optional[List[str]] = Field(default_factory=list, description="List of exact directory names to ignore.")
    ignore_patterns: Optional[List[str]] = Field(
        default_factory=lambda: ['.*', '*.partial', 'Thumbs.db', '*[sS]ample*'],
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
    temp_file_suffix_prefix: Optional[str] = Field(default=".renametmp_", description="Prefix for temporary filenames during transactional renames (e.g., '.tmp_', '_temp_'). Should include leading/trailing separators as desired.")


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
    cache_expire_seconds: Optional[int] = Field(default=604800, ge=0, description="Cache expiration time in seconds (default: 7 days).")

    # Undo Options
    enable_undo: Optional[bool] = Field(default=True, description="Enable undo logging.")
    undo_db_path: Optional[str] = Field(default=None, description="Path to undo database file (default: in app dir).")
    undo_expire_days: Optional[int] = Field(default=30, ge=-1, description="Days to keep undo logs (-1 for forever, 0 for session only).")
    undo_check_integrity: Optional[bool] = Field(default=False, description="Verify file integrity before undoing (size, mtime).")
    undo_integrity_hash_bytes: Optional[int] = Field(default=0, ge=0, description="Bytes to hash for integrity check (0 to disable partial hash).")
    undo_integrity_hash_full: Optional[bool] = Field(default=False, description="Calculate full file hash for undo integrity check (SLOW, overrides hash_bytes).")

    # Logging Options
    log_file: Optional[str] = Field(default=None, description="Path to log file (e.g., rename_app.log).")
    log_level: Optional[str] = Field(default='INFO', description="Logging level: DEBUG, INFO, WARNING, ERROR.")

    @field_validator('on_conflict', mode='before')
    @classmethod
    def check_on_conflict(cls, v: Any) -> Optional[str]:
        if v is not None and isinstance(v, str) and v.lower() not in ['skip', 'overwrite', 'suffix', 'fail']:
            raise ValueError("on_conflict must be one of 'skip', 'overwrite', 'suffix', 'fail'")
        return v.lower() if isinstance(v, str) else None

    @field_validator('log_level', mode='before')
    @classmethod
    def check_log_level(cls, v: Any) -> Optional[str]:
        if v is not None and isinstance(v, str) and v.upper() not in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
            raise ValueError("log_level must be one of DEBUG, INFO, WARNING, ERROR")
        return v.upper() if isinstance(v, str) else None

    @field_validator('tmdb_match_strategy', mode='before')
    @classmethod
    def check_tmdb_strategy(cls, v: Any) -> Optional[str]:
        if v is not None and isinstance(v, str) and v.lower() not in ['first', 'fuzzy']:
            raise ValueError("tmdb_match_strategy must be 'first' or 'fuzzy'")
        return v.lower() if isinstance(v, str) else 'first'

    @field_validator('scan_strategy', mode='before')
    @classmethod
    def check_scan_strategy(cls, v: Any) -> Optional[str]:
        if v is not None and isinstance(v, str) and v.lower() not in ['memory', 'low_memory']:
            raise ValueError("scan_strategy must be 'memory' or 'low_memory'")
        return v.lower() if isinstance(v, str) else 'memory'

    @field_validator('extract_stream_info', mode='before')
    @classmethod
    def check_extract_stream_info(cls, v: Any) -> Optional[bool]:
        if v is not None and not isinstance(v, bool):
            raise ValueError("extract_stream_info must be a boolean (true/false)")
        return v

    @field_validator('unknown_file_handling', mode='before')
    @classmethod
    def check_unknown_file_handling(cls, v: Any) -> Optional[str]:
        if v is not None and isinstance(v, str) and v.lower() not in ['skip', 'guessit_only', 'move_to_unknown']:
            raise ValueError("unknown_file_handling must be one of 'skip', 'guessit_only', 'move_to_unknown'")
        return v.lower() if isinstance(v, str) else 'skip'

    @field_validator('series_metadata_preference', mode='before')
    @classmethod
    def check_series_metadata_preference(cls, v: Any) -> List[str]:
        default_pref = ['tmdb', 'tvdb']
        if v is None: return default_pref
        
        val_list: List[str]
        if isinstance(v, str):
            val_list = [item.strip().lower() for item in v.split(',') if item.strip()]
        elif isinstance(v, list):
            val_list = [str(item).strip().lower() for item in v if str(item).strip()]
        else:
            raise ValueError("series_metadata_preference must be a list or comma-separated string")

        if not val_list:
            return default_pref

        if len(val_list) != 2:
            raise ValueError("series_metadata_preference must contain exactly two sources (e.g., 'tmdb,tvdb')")
        
        sources = {s.lower() for s in val_list}
        allowed = {'tmdb', 'tvdb'}
        if sources != allowed:
            raise ValueError(f"series_metadata_preference must be 'tmdb' and 'tvdb', got: {val_list}")
        return [s.lower() for s in val_list]

    @field_validator('preserve_mtime', mode='before')
    @classmethod
    def check_preserve_mtime(cls, v: Any) -> Optional[bool]:
        if v is not None and not isinstance(v, bool): raise ValueError("preserve_mtime must be a boolean")
        return v

    @field_validator('undo_integrity_hash_full', mode='before')
    @classmethod
    def check_undo_integrity_hash_full(cls, v: Any) -> Optional[bool]:
        if v is not None and not isinstance(v, bool): raise ValueError("undo_integrity_hash_full must be a boolean")
        return v
    
    @field_validator('temp_file_suffix_prefix', mode='before')
    @classmethod
    def check_temp_file_suffix_prefix(cls, v: Any) -> Optional[str]:
        if v is not None:
            if not isinstance(v, str):
                raise ValueError("temp_file_suffix_prefix must be a string.")
            if not v: # cannot be empty
                raise ValueError("temp_file_suffix_prefix cannot be empty.")
            # Basic check for problematic characters if you want, but usually not strictly needed for internal suffixes
            # For example, ensure it doesn't contain characters that would break path construction.
            # if any(char in v for char in ['/', '\\', ':', '*', '?', '"', '<', '>']):
            #     raise ValueError("temp_file_suffix_prefix contains invalid path characters.")
        return v


class DefaultSettings(BaseProfileSettings):
    pass

class RootConfigModel(BaseModel):
    default: DefaultSettings = Field(default_factory=DefaultSettings)
    model_config = {'extra': 'allow'}


def generate_default_toml_content() -> str:
    default_settings = DefaultSettings()
    content_lines = ["# Gemini-Renamer Default Configuration File"]
    content_lines.append("# For more details on placeholders, see README.md or documentation.\n")
    
    sections: Dict[str, List[str]] = {
        "Core Settings": ['recursive', 'processing_mode', 'use_metadata', 'extract_stream_info', 'preserve_mtime', 'ignore_dirs', 'ignore_patterns'],
        "Format Strings": ['series_format', 'movie_format', 'subtitle_format', 'series_format_specials', 'folder_format_series', 'folder_format_movie', 'folder_format_specials'],
        "File Handling & Extensions": ['video_extensions', 'associated_extensions', 'subtitle_extensions', 'on_conflict', 'create_folders', 'unknown_file_handling', 'unknown_files_dir', 'scan_strategy', 'temp_file_suffix_prefix'],
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
                
                toml_value_str: str
                if isinstance(default_value, str):
                    # Escape backslashes and double quotes in string values for TOML
                    escaped_default_value = default_value.replace('\\', '\\\\').replace('"', '\\"')
                    toml_value_str = f'"{escaped_default_value}"'
                elif isinstance(default_value, bool):
                    toml_value_str = str(default_value).lower()
                elif isinstance(default_value, list):
                    list_items_str = []
                    for item in default_value:
                        if isinstance(item, str): list_items_str.append(f'"{str(item)}"')
                        elif isinstance(item, bool): list_items_str.append(str(item).lower())
                        else: list_items_str.append(str(item))
                    toml_value_str = "[" + ", ".join(list_items_str) + "]"
                elif default_value is None: 
                    toml_value_str = "# (not set, uses internal default or None)"
                    content_lines.append(f"  # {key} = {toml_value_str}")
                    continue 
                else: 
                    toml_value_str = str(default_value)
                
                content_lines.append(f"  {key} = {toml_value_str}")
    
    content_lines.append("\n# You can create other profiles, e.g.:")
    content_lines.append("# [movie_profile]")
    content_lines.append("# movie_format = \"{movie_title} [{movie_year}] - {resolution}\"")
    content_lines.append("# use_metadata = true")

    return "\n".join(content_lines)


class ConfigManager:
    def __init__(self, config_path_override: Optional[Path] = None, interactive_fallback: bool = True, quiet_mode: bool = False):
        self.console = ConsoleClass(quiet=quiet_mode)
        self.quiet_mode = quiet_mode

        self.config_path = self._resolve_config_path(config_path_override)
        self._raw_toml_content_str: Optional[str] = None
        self._config = self._load_config(interactive_fallback=interactive_fallback)
        self._api_keys = self._load_env_keys()
        log.debug(f"Config path used: {self.config_path}")


    def _resolve_config_path(self, config_path_override: Optional[Path]) -> Path:
        if config_path_override:
            p = Path(config_path_override)
            log.debug(f"Using explicit config path target: {p.resolve()}")
            return p.resolve()
            
        cwd_path = Path.cwd() / DEFAULT_CONFIG_FILENAME
        if cwd_path.is_file():
            log.debug(f"Found config file in current directory: {cwd_path}")
            return cwd_path.resolve()
        
        user_config_path_obj: Optional[Path] = None
        if PLATFORMDIRS_AVAILABLE and platformdirs:
            try:
                user_dir_str = platformdirs.user_config_dir("rename_app", "rename_app_author", ensure_exists=False)
                user_config_path_obj = Path(user_dir_str) / DEFAULT_CONFIG_FILENAME
                if user_config_path_obj.is_file():
                    log.debug(f"Found config file in user config directory: {user_config_path_obj}")
                    return user_config_path_obj.resolve()
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
            log.debug("__file__ not defined, skipping project directory check based on script location.")
        except Exception as e: 
            log.warning(f"Error checking project directory for config: {e}")
        
        if user_config_path_obj:
            log.debug(f"No config file found. Preferred default creation location: {user_config_path_obj.resolve()}")
            return user_config_path_obj.resolve()
        
        log.debug(f"No config file found. Defaulting to CWD for potential creation: {cwd_path.resolve()}")
        return cwd_path.resolve()


    def _create_default_config_interactively(self, target_path: Path) -> bool:
        if self.quiet_mode:
            log.info("Quiet mode: Skipping interactive creation of default config file.")
            return False

        self.console.print(f"[yellow]Configuration file not found at an expected location.[/yellow]")
        self.console.print(f"A default configuration file can be created at:")
        self.console.print(f"  [cyan]{target_path}[/cyan]")
        
        try:
            if ConfirmClass.ask("Would you like to create a default configuration file now?", default=True):
                try:
                    default_content = generate_default_toml_content()
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(target_path, "w", encoding="utf-8") as f:
                        f.write(default_content)
                    self.console.print(f"[green]✓ Default configuration file created at: {target_path}[/green]")
                    self.console.print(f"[bright_magenta]Please review and customize '{target_path}' as needed, especially API keys if not using an .env file.[/bright_magenta]")
                    log.info(f"Default configuration file created at {target_path}")
                    return True
                except IOError as e_io:
                    self.console.print(f"[bold red]Error creating configuration file: {e_io}[/bold red]", file=sys.stderr)
                    log.error(f"Failed to write default config to {target_path}: {e_io}")
                    return False
                except Exception as e_create:
                    self.console.print(f"[bold red]An unexpected error occurred while creating the configuration file: {e_create}[/bold red]", file=sys.stderr)
                    log.exception(f"Unexpected error creating default config at {target_path}: {e_create}")
                    return False
            else:
                self.console.print("[yellow]Skipping default configuration file creation. Using internal defaults.[/yellow]")
                log.info("User opted out of creating a default configuration file.")
                return False
        except KeyboardInterrupt:
            self.console.print("\n[yellow]Config creation cancelled by user.[/yellow]", file=sys.stderr)
            log.warning("User cancelled config creation during interactive prompt.")
            return False


    def _load_config(self, interactive_fallback: bool = True) -> Dict[str, Any]:
        cfg_dict: Dict[str, Any] = {}
        config_file_existed_initially = self.config_path.is_file()

        if not config_file_existed_initially and interactive_fallback:
            if not self._create_default_config_interactively(self.config_path):
                log.warning(f"Proceeding without a config file. Using internal defaults.")
                self._raw_toml_content_str = "# No configuration file present or created.\n"
                return RootConfigModel().model_dump(exclude_unset=False, by_alias=False)

        if self.config_path.is_file():
            try:
                self._raw_toml_content_str = self.config_path.read_text(encoding='utf-8')
                if not self._raw_toml_content_str.strip():
                    log.warning(f"Config file '{self.config_path}' is empty. Using internal defaults.")
                    self._raw_toml_content_str = "# Config file was empty.\n"
                    return RootConfigModel().model_dump(exclude_unset=False, by_alias=False)
                cfg_dict = pytomlpp.loads(self._raw_toml_content_str)
                log.info(f"Loaded configuration from '{self.config_path}'")
            except pytomlpp.DecodeError as e_toml:
                raise ConfigError(f"Failed to parse TOML config '{self.config_path}': {e_toml}")
            except OSError as e_os:
                self._raw_toml_content_str = f"# Error reading config file: {e_os}\n"
                raise ConfigError(f"Failed to read config file '{self.config_path}': {e_os}")
        else:
            log.warning(f"Config file not found at '{self.config_path}' and not created (interactive_fallback={interactive_fallback}). Using internal defaults.")
            self._raw_toml_content_str = "# Config file not found or empty.\n"
            return RootConfigModel().model_dump(exclude_unset=False, by_alias=False)

        try:
            validated_config = RootConfigModel.model_validate(cfg_dict)
            log.debug("Config validation successful.")
            return validated_config.model_dump(exclude_unset=False, by_alias=False)
        except ValidationError as e_val:
            error_details = e_val.errors()
            error_msgs = [f"  - Field `{' -> '.join(map(str, err['loc']))}`: {err['msg']}" for err in error_details]
            error_summary = f"Config file '{self.config_path}' validation failed:\n" + "\n".join(error_msgs)
            log.error(error_summary)
            if config_file_existed_initially:
                raise ConfigError(error_summary) from e_val
            else:
                log.critical(f"Newly created default config FAILED validation. This is an internal error. {error_summary}")
                self.console.print(f"[bold red]INTERNAL ERROR: The generated default configuration is invalid. Please report this.[/bold red]", file=sys.stderr)
                self.console.print(error_summary, file=sys.stderr)
                return RootConfigModel().model_dump(exclude_unset=False, by_alias=False)
        except Exception as e_load_val:
            self._raw_toml_content_str = f"# Unexpected error loading config: {e_load_val}\n"
            log.exception(f"Unexpected error loading/validating config '{self.config_path}': {e_load_val}")
            raise ConfigError(f"Unexpected error loading/validating config '{self.config_path}': {e_load_val}")

    def get_raw_toml_content(self) -> Optional[str]:
        return self._raw_toml_content_str

    def _load_env_keys(self) -> Dict[str, Optional[str]]:
        keys: Dict[str, Optional[str]] = {}
        env_path: Union[str, Path, None] = None
        try:
            env_path = find_dotenv(usecwd=True)
            if env_path:
                log.debug(f"Loading environment variables from: {env_path}")
                load_dotenv(dotenv_path=env_path)
            else:
                log.debug(".env file not found by find_dotenv. Checking os.getenv directly.")
        except Exception as e:
            log.warning(f"Error accessing or processing .env file: {e}")

        keys['tmdb_api_key'] = os.getenv("TMDB_API_KEY")
        keys['tvdb_api_key'] = os.getenv("TVDB_API_KEY")
        keys['tmdb_language'] = os.getenv("TMDB_LANGUAGE")

        if any(v for k, v in keys.items() if k.endswith('_api_key')):
             log_msg_source = ".env file" if env_path and Path(env_path).exists() else "environment variables"
             log.info(f"Loaded API keys/settings from {log_msg_source}.")
        elif env_path and Path(env_path).exists():
            log.debug(f".env file found at {env_path} but no relevant API keys (TMDB_API_KEY, TVDB_API_KEY) were set within it.")
        else:
            log.debug("No .env file found and no relevant API keys set as environment variables.")
        return keys

    def get_value(self, key: str, profile: str = 'default', command_line_value: Any = None, default_value: Any = None) -> Any:
        if command_line_value is not None:
            if key == 'series_metadata_preference' and isinstance(command_line_value, str):
                try:
                    validated_list = BaseProfileSettings.model_fields['series_metadata_preference'].validate(command_line_value)
                    return validated_list
                except ValueError:
                    log.warning(f"Invalid command-line value for {key}: '{command_line_value}'. Ignoring.")
            else:
                return command_line_value

        if key == 'tmdb_language' and self._api_keys.get('tmdb_language'):
             return self._api_keys['tmdb_language']

        profile_settings_dict = self._config.get(profile, {})
        if isinstance(profile_settings_dict, dict) and key in profile_settings_dict:
            val_from_profile = profile_settings_dict[key]
            if val_from_profile is not None:
                if key == 'series_metadata_preference' and not (isinstance(val_from_profile, list) and len(val_from_profile) == 2 and set(s.lower() for s in val_from_profile) == {'tmdb', 'tvdb'}):
                    log.warning(f"Invalid config value for '{key}' in profile '{profile}'. Using default from model.")
                else:
                    return val_from_profile

        default_settings_dict = self._config.get('default', {})
        if isinstance(default_settings_dict, dict) and key in default_settings_dict:
            val_from_default_section = default_settings_dict[key]
            if val_from_default_section is not None:
                if key == 'series_metadata_preference' and not (isinstance(val_from_default_section, list) and len(val_from_default_section) == 2 and set(s.lower() for s in val_from_default_section) == {'tmdb', 'tvdb'}):
                    log.warning(f"Invalid config value for '{key}' in profile 'default'. Using default from model.")
                else:
                    return val_from_default_section
        
        # Fallback to Pydantic model's default if not found above
        if default_value is None and hasattr(BaseProfileSettings, 'model_fields') and key in BaseProfileSettings.model_fields:
            field_info = BaseProfileSettings.model_fields[key]
            if field_info.default_factory is not None:
                return field_info.default_factory()
            return field_info.default # This could be None itself, which is fine.
            
        return default_value

    def get_api_key(self, service_name: str) -> Optional[str]:
        key_name = f"{service_name.lower()}_api_key"
        return self._api_keys.get(key_name)

    def get_profile_settings(self, profile: str = 'default') -> Dict[str, Any]:
        base_defaults = DefaultSettings().model_dump(exclude_unset=False, by_alias=False)

        default_section_settings = self._config.get('default', {})
        if isinstance(default_section_settings, dict):
            for k, v in default_section_settings.items():
                if v is not None or k not in base_defaults: # Merge if value is set, or if it's a custom key
                    base_defaults[k] = v

        final_settings = base_defaults.copy()

        if profile != 'default' and profile in self._config:
            profile_specific_data = self._config.get(profile, {})
            if isinstance(profile_specific_data, dict):
                 for k, v in profile_specific_data.items():
                     if v is not None: # Only override if value is explicitly set in profile
                         final_settings[k] = v
            else:
                 log.warning(f"Profile '{profile}' in config is not a dictionary. Skipping merge for this profile.")
        elif profile != 'default':
            log.debug(f"Profile '{profile}' not found in config. Using effectively merged default settings.")
        return final_settings


class ConfigHelper:
    def __init__(self, config_manager: ConfigManager, args_ns: argparse.Namespace):
        self.manager = config_manager
        self.args = args_ns
        self.profile = getattr(args_ns, 'profile', 'default') or 'default'

    def __call__(self, key: str, default_value: Any = None, arg_value: Any = None) -> Any:
        cmd_line_val = arg_value if arg_value is not None else getattr(self.args, key, None)
        return self.manager.get_value(key, self.profile, cmd_line_val, default_value)

    def get_api_key(self, service_name: str) -> Optional[str]:
        return self.manager.get_api_key(service_name)

    def get_list(self, key: str, default_value: Optional[List[Any]] = None) -> List[Any]:
        cmd_line_val_str = getattr(self.args, key, None)
        cmd_line_list: Optional[List[str]] = None
        if isinstance(cmd_line_val_str, str):
            cmd_line_list = [item.strip() for item in cmd_line_val_str.split(',') if item.strip()]

        val = self.manager.get_value(key, self.profile, cmd_line_list, None)

        if isinstance(val, list):
            return val
        elif isinstance(val, str):
            return [item.strip() for item in val.split(',') if item.strip()]

        # If no value from config or CLI, use Pydantic model's default for lists if available
        if default_value is None and hasattr(BaseProfileSettings, 'model_fields') and key in BaseProfileSettings.model_fields:
            field_info = BaseProfileSettings.model_fields[key]
            if field_info.default_factory is not None:
                model_default_list = field_info.default_factory()
                if isinstance(model_default_list, list): return model_default_list
            elif isinstance(field_info.default, list):
                return field_info.default

        return default_value if isinstance(default_value, list) else []

def interactive_api_setup(dotenv_path_override: Optional[Path] = None, quiet_mode: bool = False) -> bool:
    # This function now uses ConsoleClass and ConfirmClass imported from ui_utils
    # which are already quiet-aware or have fallbacks.
    console = ConsoleClass(quiet=quiet_mode)

    if quiet_mode:
        builtins.print("ERROR: Interactive API setup cannot run in quiet mode (config_manager).", file=sys.stderr)
        return False

    resolved_dotenv_path: Path
    if dotenv_path_override:
        resolved_dotenv_path = dotenv_path_override.resolve()
    else:
        resolved_dotenv_path = Path.cwd() / DEFAULT_DOTENV_FILENAME

    log.info(f"Starting interactive API setup. Target .env file: {resolved_dotenv_path}")

    console.print(f"--- API Key Setup ---")
    console.print(f"This will guide you through setting up API keys in '{resolved_dotenv_path}'.")
    console.print("Press Enter to keep the current value (if any) or skip if not set.")

    try:
        current_values: Dict[str, Optional[str]] = {}
        if resolved_dotenv_path.exists() and resolved_dotenv_path.is_file():
            log.debug(f"Loading existing values from {resolved_dotenv_path}")
            current_values = dotenv_values(resolved_dotenv_path)
        else:
            log.debug(f".env file not found at {resolved_dotenv_path}. Will create a new one if keys are set.")

        keys_to_set = {
            "TMDB_API_KEY": {"prompt": "Enter your TMDB API Key", "current": current_values.get("TMDB_API_KEY", "")},
            "TVDB_API_KEY": {"prompt": "Enter your TVDB API Key (V4)", "current": current_values.get("TVDB_API_KEY", "")},
            "TMDB_LANGUAGE": {"prompt": "Enter default TMDB language (e.g., en, de, fr)", "current": current_values.get("TMDB_LANGUAGE", "en"), "default": "en"}
        }
        updated_any = False

        for key, info in keys_to_set.items():
            prompt_text = f"{info['prompt']}"
            current_val_display = info['current'] if info['current'] is not None else ""
            default_val_display = info.get('default', "")

            if current_val_display:
                prompt_text += f" [current: {current_val_display}]"
            elif default_val_display and key == "TMDB_LANGUAGE":
                prompt_text += f" [default: {default_val_display}]"
            prompt_text += ": "

            try:
                user_input = console.input(prompt_text).strip()

                if user_input:
                    set_key(resolved_dotenv_path, key, user_input, quote_mode="never")
                    log.info(f"Set {key} to '{user_input}' in {resolved_dotenv_path}")
                    console.print(f"  ✓ {key} set to: {user_input}")
                    updated_any = True
                elif not user_input and not current_val_display and default_val_display and key == "TMDB_LANGUAGE":
                    set_key(resolved_dotenv_path, key, default_val_display, quote_mode="never")
                    log.info(f"Set {key} to default '{default_val_display}' in {resolved_dotenv_path}")
                    console.print(f"  ✓ {key} set to default: {default_val_display}")
                    updated_any = True
                elif not user_input and current_val_display:
                    console.print(f"  - {key} kept as: {current_val_display}")
                elif not user_input and not current_val_display and not default_val_display:
                    if key in current_values and current_values[key] == "":
                         if resolved_dotenv_path.is_file():
                            unset_key(resolved_dotenv_path, key)
                            log.info(f"Removed empty {key} from {resolved_dotenv_path}")
                            console.print(f"  ✓ {key} removed (was empty).")
                            updated_any = True
                         else:
                            console.print(f"  - {key} skipped (no value provided).")
                    else:
                        console.print(f"  - {key} skipped (no value provided).")

            except KeyboardInterrupt:
                console.print("\nSetup cancelled by user.", file=sys.stderr)
                log.warning("API setup cancelled by user during input.")
                return False
            except Exception as e_input:
                log.error(f"Error during input for {key}: {e_input}", exc_info=True)
                console.print(f"  ✗ Error processing input for {key}. Skipping.", file=sys.stderr)

        if updated_any:
            console.print(f"\nConfiguration saved to: {resolved_dotenv_path}")
        else:
            console.print("\nNo changes made to .env file.")
        console.print("--- Setup Complete ---")
        return True
    except IOError as e_io:
        log.error(f"IOError during API setup writing to {resolved_dotenv_path}: {e_io}", exc_info=True)
        console.print(f"\nError: Could not write to .env file at '{resolved_dotenv_path}'. Check permissions.", file=sys.stderr)
        return False
    except Exception as e_main:
        log.exception(f"An unexpected error occurred during interactive API setup: {e_main}")
        console.print(f"\nAn unexpected error occurred: {e_main}", file=sys.stderr)
        return False