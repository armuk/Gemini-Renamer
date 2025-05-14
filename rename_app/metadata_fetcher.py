# rename_app/metadata_fetcher.py

import logging
import time
import asyncio
import builtins 
import sys
from functools import wraps, partial 
from pathlib import Path
from typing import Optional, Tuple, TYPE_CHECKING, Any, Iterable, Sequence, Dict, cast, List, Deque, Union, TypeAlias

from collections import deque 

from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_fixed, retry_if_exception

from .api_clients import get_tmdb_client, get_tvdb_client
from .exceptions import MetadataError
from .models import MediaMetadata
from .config_manager import ConfigHelper 

from rename_app.ui_utils import (
    ConsoleClass, ConfirmClass, 
    RichConsoleActual,
    RICH_AVAILABLE_UI as RICH_AVAILABLE
)

log = logging.getLogger(__name__)

try:
    import diskcache as actual_diskcache_module
    DISKCACHE_AVAILABLE = True
except ImportError:
    DISKCACHE_AVAILABLE = False
    actual_diskcache_module = None

if TYPE_CHECKING:
    try:
        from diskcache import Cache as _ActualDiskCacheClass
        DiskCacheType_Hint: TypeAlias = _ActualDiskCacheClass 
    except ImportError:
        DiskCacheType_Hint: TypeAlias = Any
else:
    DiskCacheType_Hint: TypeAlias = Any

DiskCacheType = DiskCacheType_Hint

try:
    import platformdirs
    PLATFORMDIRS_AVAILABLE = True
except ImportError:
    PLATFORMDIRS_AVAILABLE = False
    platformdirs = None

try:
    from thefuzz import process as fuzz_process
    from thefuzz import fuzz
    THEFUZZ_AVAILABLE = True
except ImportError:
    THEFUZZ_AVAILABLE = False
    fuzz = None
    fuzz_process = None 

try:
    import dateutil.parser
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False
    dateutil = None

try:
    import requests.exceptions as req_exceptions
except ImportError:
    class req_exceptions: #type: ignore
        ConnectionError=IOError
        Timeout=IOError
        RequestException=IOError
        HTTPError=type('HTTPError',(IOError,),{'response':type('MockResponse',(),{'status_code':0})()})

try:
    from tmdbv3api import Movie, TV, Season
    from tmdbv3api.exceptions import TMDbException
    from tmdbv3api.as_obj import AsObj
    TMDBV3API_AVAILABLE = True
except ImportError:
    TMDbException = type('TMDbException', (Exception,), {})
    AsObj = object
    class Movie: pass #type: ignore
    class TV: pass #type: ignore
    class Season: pass #type: ignore
    TMDBV3API_AVAILABLE = False

DIRECT_ID_MATCH_SCORE = 101.0 

class AsyncRateLimiter:
    def __init__(self, delay: float):
        self.delay = delay
        self.last_call = 0.0
        self._lock = asyncio.Lock()

    async def wait(self):
        if self.delay <= 0: return
        async with self._lock:
            now = time.monotonic()
            since_last = now - self.last_call
            if since_last < self.delay:
                wait_time = self.delay - since_last
                log.debug(f"Rate limiting: sleeping for {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
            self.last_call = time.monotonic()

def should_retry_api_error(exception: Exception) -> bool:
    if isinstance(exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)):
        log.debug(f"Retry check PASSED for Connection/Timeout Error: {type(exception).__name__}")
        return True
    if isinstance(exception, req_exceptions.HTTPError):
        status_code = getattr(getattr(exception, 'response', None), 'status_code', 0)
        if status_code == 429: log.warning(f"Retry check PASSED for HTTP 429 (Rate Limit)."); return True
        if 500 <= status_code <= 599: log.warning(f"Retry check PASSED for HTTP {status_code} (Server Error)."); return True
        if status_code == 401: log.error(f"Retry check FAILED for HTTP 401 (Unauthorized - Check API Key)."); return False
        if status_code == 403: log.error(f"Retry check FAILED for HTTP 403 (Forbidden - Check API Key/Permissions)."); return False
        if status_code == 404: log.debug(f"Retry check FAILED for HTTP 404 (Not Found)."); return False # Not found is not retryable by default
        log.debug(f"Retry check FAILED for other HTTP Status Code: {status_code}"); return False

    if TMDBV3API_AVAILABLE and isinstance(exception, TMDbException):
        msg_lower = str(exception).lower()
        status_code_from_tmdb_exception = 0
        if hasattr(exception, 'args') and exception.args and isinstance(exception.args[0], dict):
            status_code_from_tmdb_exception = exception.args[0].get('status_code', 0)

        if "invalid api key" in msg_lower or status_code_from_tmdb_exception == 7 or status_code_from_tmdb_exception == 401:
            log.error(f"Retry check FAILED for TMDbException (API Key/Auth Issue): {exception}"); return False
        if "resource not found" in msg_lower or status_code_from_tmdb_exception == 34 or status_code_from_tmdb_exception == 404:
            log.debug(f"Retry check FAILED for TMDbException (Not Found): {exception}"); return False # Not found is not retryable
        if status_code_from_tmdb_exception == 403:
             log.error(f"Retry check FAILED for TMDbException (Forbidden): {exception}"); return False
        # For other TMDbExceptions, assume they might be transient (e.g., server errors not mapped to HTTP status)
        log.warning(f"Retry check PASSED (tentative) for TMDbException: {exception} (status: {status_code_from_tmdb_exception})")
        return True # Retry other TMDb errors

    # TVDB Specific Error Handling for Retries
    # Check if the exception's context (if it exists) is from tvdb-v4-official
    is_tvdb_lib_error = False
    if exception.__context__ and "tvdb_v4_official" in str(type(exception.__context__)).lower():
        is_tvdb_lib_error = True
    
    # Also check common ValueError patterns from TVDB library if context isn't set (older versions might do this)
    if isinstance(exception, ValueError) or is_tvdb_lib_error:
        msg = str(exception).lower()
        if "not found" in msg or "no record" in msg or "invalid id" in msg or "404" in msg:
            log.debug(f"Retry check FAILED for TVDB Error (likely Not Found): {msg}"); return False
        if "unauthorized" in msg or "api key" in msg or "401" in msg:
            log.error(f"Retry check FAILED for TVDB Error (likely API Key/Auth Issue): {msg}"); return False
        if '500' in msg or '502' in msg or '503' in msg or '504' in msg or 'timeout' in msg:
            log.warning(f"Retry check PASSED for potential TVDB Server Error/Timeout: {type(exception).__name__}: {msg}")
            return True
    
    if isinstance(exception, (AttributeError, TypeError, UnboundLocalError)): 
         log.error(f"Retry check FAILED for Internal Programming Error ({type(exception).__name__}): {str(exception)}"); return False
    log.debug(f"Retry check FAILED by default for: {type(exception).__name__}: {str(exception)}"); return False


def find_best_match(title_to_find: str, api_results_tuple: Tuple[Dict, ...], result_key: str ='title', id_key: str ='id', score_cutoff: int = 70) -> Optional[Tuple[Dict, Optional[float]]]:
    if not api_results_tuple: return None
    first_result_dict = next(iter(api_results_tuple), None)

    if not THEFUZZ_AVAILABLE or not fuzz_process or not fuzz:
        log.debug("Fuzzy matching unavailable ('thefuzz' not installed). Returning first result.")
        return first_result_dict, None

    choices: Dict[Any, str] = {}
    log.debug(f"Attempting to build choices for fuzzy match '{title_to_find}'.")
    try:
        for r_dict in api_results_tuple:
            if not isinstance(r_dict, dict):
                log.warning(f"Skipping non-dict item in fuzzy match choices: {type(r_dict)}")
                continue
            current_id = r_dict.get(id_key)
            current_result_val = r_dict.get(result_key)
            if current_id is not None and current_result_val is not None:
                choices[current_id] = str(current_result_val)
            else:
                log.debug(f"Skipping item due to missing id ('{id_key}') or result ('{result_key}'): {r_dict}")
    except Exception as e_choices:
        log.error(f"Error creating choices dict for fuzzy matching: {e_choices}", exc_info=True)
        return first_result_dict, None

    if not choices:
        log.debug("No valid choices built for fuzzy matching. Returning first result.")
        return first_result_dict, None

    best_match_dict: Optional[Dict] = None; best_score: Optional[float] = None
    try:
        if not isinstance(title_to_find, str): title_to_find = str(title_to_find)
        str_choices = {k: str(v) for k,v in choices.items()}
        best_result_list = fuzz_process.extractBests(title_to_find, str_choices, score_cutoff=score_cutoff, limit=1)

        if best_result_list:
             matched_value, score_val, best_id = best_result_list[0]
             best_score = float(score_val)
             log.debug(f"Fuzzy match '{title_to_find}': Found '{matched_value}' (ID:{best_id}) score {best_score:.1f}")
             best_match_dict = next((r_dict for r_dict in api_results_tuple if isinstance(r_dict, dict) and str(r_dict.get(id_key)) == str(best_id)), None)
             if not best_match_dict:
                 log.error(f"Fuzzy match found ID {best_id} but couldn't find corresponding dict in original results.")
                 best_match_dict = first_result_dict; best_score = None
        else:
            log.warning(f"Fuzzy match failed for '{title_to_find}' (cutoff {score_cutoff}). Falling back to first result.")
            best_match_dict = first_result_dict; best_score = None
    except Exception as e_fuzz:
        log.error(f"Error during fuzzy matching process: {e_fuzz}", exc_info=True)
        best_match_dict = first_result_dict; best_score = None
    return best_match_dict, best_score

def get_external_ids(tmdb_obj: Optional[Any] = None, tvdb_obj: Optional[Any] = None) -> Dict[str, Any]:
    ids: Dict[str, Any] = {'imdb_id': None, 'tmdb_id': None, 'tvdb_id': None, 'collection_id': None, 'collection_name': None}
    if tmdb_obj:
        try:
            tmdb_id_val = None
            if isinstance(tmdb_obj, dict): tmdb_id_val = tmdb_obj.get('id')
            elif hasattr(tmdb_obj, 'id'): tmdb_id_val = getattr(tmdb_obj, 'id', None)
            if tmdb_id_val is not None: ids['tmdb_id'] = int(tmdb_id_val)
            ext_ids_data: Dict[str, Any] = {}
            if isinstance(tmdb_obj, dict): ext_ids_data = tmdb_obj.get('external_ids', {})
            elif hasattr(tmdb_obj, 'external_ids'):
                 ext_ids_attr = getattr(tmdb_obj, 'external_ids', None)
                 if isinstance(ext_ids_attr, dict): ext_ids_data = ext_ids_attr
                 elif callable(ext_ids_attr):
                     try: ext_ids_data = ext_ids_attr()
                     except Exception as e_call: log.debug(f"Error calling external_ids method on TMDB object: {e_call}")
            if not ext_ids_data: ext_ids_data = {}
            imdb_id_found = ext_ids_data.get('imdb_id'); tvdb_id_found = ext_ids_data.get('tvdb_id')
            if imdb_id_found: ids['imdb_id'] = str(imdb_id_found)
            if tvdb_id_found and ids.get('tvdb_id') is None:
                try: ids['tvdb_id'] = int(tvdb_id_found)
                except (ValueError, TypeError, Exception): log.warning(f"Could not convert TMDB-provided TVDB ID '{tvdb_id_found}' to int.")
            collection_info: Any = None
            if isinstance(tmdb_obj, dict): collection_info = tmdb_obj.get('belongs_to_collection')
            elif hasattr(tmdb_obj, 'belongs_to_collection'): collection_info = getattr(tmdb_obj, 'belongs_to_collection', None)
            if isinstance(collection_info, (dict, AsObj)):
                 col_id = None; col_name = None
                 if isinstance(collection_info, dict):
                     col_id = collection_info.get('id'); col_name = collection_info.get('name')
                 else: # AsObj
                     col_id = getattr(collection_info, 'id', None); col_name = getattr(collection_info, 'name', None)
                 if col_id:
                     try: ids['collection_id'] = int(col_id)
                     except (ValueError, TypeError, Exception): log.warning(f"Could not convert collection ID '{col_id}' to int.")
                 if col_name: ids['collection_name'] = str(col_name)
        except Exception as e_tmdb:
            log.warning(f"Unexpected error parsing TMDB IDs: {e_tmdb}", exc_info=True)
    if tvdb_obj and isinstance(tvdb_obj, dict):
        try:
            if ids.get('tvdb_id') is None:
                 tvdb_id_val = tvdb_obj.get('id')
                 if tvdb_id_val is not None:
                     try: ids['tvdb_id'] = int(tvdb_id_val)
                     except (ValueError, TypeError, Exception): log.warning(f"Could not convert TVDB-provided TVDB ID '{tvdb_id_val}' to int.")
            if ids.get('imdb_id') is None:
                remote_ids_list: List[Dict[str, Any]] = tvdb_obj.get('remoteIds', tvdb_obj.get('remote_ids', []))
                imdb_found_in_remote = False
                if remote_ids_list and isinstance(remote_ids_list, list):
                     for remote in remote_ids_list:
                         if isinstance(remote, dict) and remote.get('sourceName') == 'IMDB':
                              imdb_id_found = remote.get('id')
                              if imdb_id_found: ids['imdb_id'] = str(imdb_id_found); imdb_found_in_remote = True; break
                if not imdb_found_in_remote:
                     imdb_id_found = tvdb_obj.get('imdbId') or tvdb_obj.get('imdb_id')
                     if imdb_id_found: ids['imdb_id'] = str(imdb_id_found)
            if ids.get('tmdb_id') is None:
                 tmdb_id_found = tvdb_obj.get('tmdbId') or tvdb_obj.get('tmdb_id')
                 if tmdb_id_found:
                     try: ids['tmdb_id'] = int(tmdb_id_found)
                     except(ValueError, TypeError, Exception): log.warning(f"Could not convert TVDB-provided TMDB ID '{tmdb_id_found}' to int.")
        except Exception as e_tvdb_ids:
             log.warning(f"Error parsing external IDs from TVDB object: {e_tvdb_ids}", exc_info=True)
    return {k: v for k, v in ids.items() if v is not None}

def _tmdb_results_to_dicts(results_iterable: Optional[Iterable[Any]], result_type: str = 'movie') -> Tuple[Dict[str, Any], ...]:
    if not results_iterable: return tuple()
    dict_list: List[Dict[str, Any]] = []
    try:
        for item in results_iterable:
            if not item: continue
            item_dict: Dict[str, Any] = {}
            try:
                if isinstance(item, (dict, AsObj)):
                    item_dict['id'] = getattr(item, 'id', None) if isinstance(item, AsObj) else item.get('id')
                    if result_type == 'movie':
                        item_dict['title'] = getattr(item, 'title', None) if isinstance(item, AsObj) else item.get('title')
                        item_dict['release_date'] = getattr(item, 'release_date', None) if isinstance(item, AsObj) else item.get('release_date')
                    elif result_type == 'series':
                        item_dict['name'] = getattr(item, 'name', None) if isinstance(item, AsObj) else item.get('name')
                        item_dict['first_air_date'] = getattr(item, 'first_air_date', None) if isinstance(item, AsObj) else item.get('first_air_date')

                    if item_dict.get('id') is not None and (item_dict.get('title') is not None or item_dict.get('name') is not None):
                        dict_list.append(item_dict)
                    else:
                        log.debug(f"Skipping TMDB result due to missing id or title/name: {item_dict.get('id', 'N/A')}")
                else:
                    log.warning(f"Skipping unexpected item type in TMDB results: {type(item)}")
            except Exception as e_conv:
                log.error(f"Unexpected error converting TMDB result item to dict: {e_conv}. Item: {item}", exc_info=True)
    except TypeError: 
        log.warning(f"Cannot iterate over TMDB results object (type {type(results_iterable)}) for dict conversion.")
    except Exception as e_iter:
        log.error(f"Error iterating TMDB results during dict conversion: {e_iter}", exc_info=True)
    log.debug(f"Converted {len(dict_list)} TMDB {result_type} results to dicts for matching.")
    return tuple(dict_list)

class MetadataFetcher:
    from rename_app.config_manager import ConfigHelper 

    def __init__(self, cfg_helper: ConfigHelper, console: Optional[ConsoleClass] = None):
        self.cfg = cfg_helper 
        self.tmdb = get_tmdb_client()
        self.tvdb = get_tvdb_client()

        if console:
            self.console = console
        else:
            quiet_mode_fetcher = getattr(self.cfg.args, 'quiet', False) if hasattr(self.cfg, 'args') and self.cfg.args else False
            self.console = ConsoleClass(quiet=quiet_mode_fetcher)

        self.rate_limiter = AsyncRateLimiter(float(self.cfg('api_rate_limit_delay', 0.5)))
        self.year_tolerance = int(self.cfg('api_year_tolerance', 1))
        self.tmdb_strategy = str(self.cfg('tmdb_match_strategy', 'first'))
        self.tmdb_fuzzy_cutoff = int(self.cfg('tmdb_match_fuzzy_cutoff', 70))
        self.tmdb_first_result_min_score = int(self.cfg('tmdb_first_result_min_score', 65))
        self.movie_yearless_match_confidence = str(self.cfg('movie_yearless_match_confidence', 'medium'))
        log.debug(f"Fetcher Config: Year Tolerance={self.year_tolerance}, TMDB Strategy='{self.tmdb_strategy}', TMDB Fuzzy Cutoff={self.tmdb_fuzzy_cutoff}, TMDB First Result Min Score={self.tmdb_first_result_min_score}, Movie Yearless Confidence='{self.movie_yearless_match_confidence}'")

        self.cache: Optional[DiskCacheType] = None
        self.cache_enabled = bool(self.cfg('cache_enabled', True)) 
        self.cache_expire = int(self.cfg('cache_expire_seconds', 60 * 60 * 24 * 7))
        if self.cache_enabled:
            if DISKCACHE_AVAILABLE and actual_diskcache_module is not None: 
                cache_dir_config = self.cfg('cache_directory', None)
                cache_dir_path: Optional[Path] = None
                if cache_dir_config: cache_dir_path = Path(str(cache_dir_config)).resolve()
                elif PLATFORMDIRS_AVAILABLE and platformdirs is not None:
                    try: cache_dir_path = Path(platformdirs.user_cache_dir("rename_app", "rename_app_author"))
                    except Exception as e_pdirs: log.warning(f"Platformdirs failed to get cache dir: {e_pdirs}. Falling back.")
                if not cache_dir_path:
                    cache_dir_path = Path(__file__).parent.parent / ".rename_cache"; log.warning(f"Could not determine platform cache directory. Using fallback: {cache_dir_path}")
                 
                if cache_dir_path:
                    status_context: Any = None
                    is_quiet_for_status = getattr(self.cfg.args, 'quiet', False) if hasattr(self.cfg, 'args') and self.cfg.args else False
                    try:
                        if RICH_AVAILABLE and isinstance(self.console, RichConsoleActual) and hasattr(self.console, 'status') and not is_quiet_for_status:
                            status_context = self.console.status("[bold green]Initializing metadata cache...[/]", spinner="dots") 
                            if status_context: status_context.start() 
                        elif not is_quiet_for_status: 
                            self.console.print("Initializing metadata cache...")
                        cache_dir_path.mkdir(parents=True, exist_ok=True)
                        self.cache = actual_diskcache_module.Cache(str(cache_dir_path)) 
                        log.info(f"Persistent cache initialized at: {cache_dir_path} (Expiration: {self.cache_expire}s)")
                        if status_context and hasattr(status_context, 'stop'): status_context.stop() 
                        if not is_quiet_for_status: self.console.print("[green]✓ Metadata cache initialized.[/green]")
                    except Exception as e:
                        if status_context and hasattr(status_context, 'stop'): status_context.stop() 
                        builtins.print(f"Error initializing disk cache at '{cache_dir_path}': {e}. Disabling cache.", file=sys.stderr)
                        log.error(f"Failed to initialize disk cache at '{cache_dir_path}': {e}. Disabling cache."); self.cache = None; self.cache_enabled = False
                else:
                    log.error("Could not determine a valid cache directory. Persistent caching disabled."); self.cache_enabled = False
            else:
                log.warning("Persistent caching enabled, but 'diskcache' library not found. Caching disabled."); self.cache_enabled = False
        else:
            log.info("Persistent caching disabled by configuration.")

    def _get_year_from_date(self, date_str: Optional[str]) -> Optional[int]:
        if not date_str or not DATEUTIL_AVAILABLE or dateutil is None: return None
        try:
            if len(date_str) == 4 and date_str.isdigit(): return int(date_str)
            return dateutil.parser.parse(date_str).year
        except: return None

    async def _run_sync(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args, **kwargs)

    async def _get_cache(self, key: str) -> Optional[Any]:
        if not self.cache_enabled or not self.cache: return None
        _cache_miss = object()
        try:
            if self.cache is None: return None # Should not happen if cache_enabled is true
            cached_value = await self._run_sync(self.cache.get, key, default=_cache_miss)
            if cached_value is not _cache_miss:
                log.debug(f"Cache HIT for key: {key}")
                # Check structure based on expected return type (movie vs series)
                if isinstance(cached_value, tuple) and \
                   ( (len(cached_value) == 4 and isinstance(cached_value[1], dict)) or # Movie: data, ids, score, error_signal
                     (len(cached_value) == 5 and isinstance(cached_value[1], dict) and isinstance(cached_value[2], dict)) ): # Series: data, ep_map, ids, score, error_signal
                    return cached_value
                else:
                    log.warning(f"Cache data for {key} has unexpected structure. Ignoring cache. Data: {cached_value}")
                    await self._run_sync(self.cache.delete, key)
                    return None
            else:
                log.debug(f"Cache MISS for key: {key}"); return None
        except Exception as e:
            log.warning(f"Error getting from cache key '{key}': {e}", exc_info=True); return None

    async def _set_cache(self, key: str, value: Any):
        if not self.cache_enabled or not self.cache: return
        # Validate structure before caching
        if not isinstance(value, tuple) or not \
           ( (len(value) == 4 and isinstance(value[1], dict)) or \
             (len(value) == 5 and isinstance(value[1], dict) and isinstance(value[2], dict)) ):
             log.error(f"Attempted to cache value with incorrect structure for key {key}. Aborting cache set. Value: {value}")
             return
        try:
            if self.cache is None: return
            await self._run_sync(self.cache.set, key, value, expire=self.cache_expire)
            log.debug(f"Cache SET for key: {key}")
        except Exception as e:
            log.warning(f"Error setting cache key '{key}': {e}", exc_info=True)

    def _sync_tmdb_movie_fetch(self, sync_title: str, sync_year_guess: Optional[int], sync_lang: str, forced_tmdb_id: Optional[int] = None) -> Tuple[Optional[Any], Optional[Dict[str, Any]], Optional[float]]:
        log.debug(f"Executing TMDB Movie Fetch [sync thread] for: '{sync_title}' (year: {sync_year_guess}, lang: {sync_lang}, forced_id: {forced_tmdb_id})")

        if not self.tmdb or not TMDBV3API_AVAILABLE or not Movie:
            log.error("TMDB client/library/Movie class not available in _sync_tmdb_movie_fetch.")
            raise ConnectionError("TMDB client not configured in _sync_tmdb_movie_fetch") # Let Tenacity handle this

        search = Movie()
        movie_match_obj: Optional[Any] = None
        match_score: Optional[float] = None
        
        if forced_tmdb_id:
            log.info(f"Attempting direct TMDB movie fetch by ID: {forced_tmdb_id}")
            try:
                movie_match_obj = search.details(forced_tmdb_id, append_to_response="external_ids,belongs_to_collection")
                if movie_match_obj: 
                    log.debug(f"Direct TMDB movie fetch successful for ID {forced_tmdb_id}")
                    match_score = DIRECT_ID_MATCH_SCORE 
                else: # This case is unlikely if tmdbv3api raises TMDbException for 404
                    log.warning(f"TMDB movie details for ID {forced_tmdb_id} returned None but did not raise TMDbException.")
                    raise MetadataError(f"FORCED_TMDB_ID_NOT_FOUND::{forced_tmdb_id}") # Treat as not found
            except TMDbException as e_details_id:
                msg_lower = str(e_details_id).lower()
                is_not_found_error = "resource not found" in msg_lower or "could not be found" in msg_lower

                # Try to get status code from tmdbv3api exception structure if possible
                status_code = 0
                if hasattr(e_details_id, 'response') and hasattr(e_details_id.response, 'status_code'): # More direct if it exists
                    status_code = e_details_id.response.status_code
                elif hasattr(e_details_id, 'args') and e_details_id.args and isinstance(e_details_id.args[0], dict):
                    status_code = e_details_id.args[0].get('status_code', 0)
                
                if is_not_found_error or status_code == 34 or status_code == 404: 
                    log.warning(f"TMDB movie with forced ID {forced_tmdb_id} not found via API (msg: '{msg_lower}', status: {status_code}). Raising specific MetadataError.")
                    raise MetadataError(f"FORCED_TMDB_ID_NOT_FOUND::{forced_tmdb_id}") from e_details_id
                
                # If it's another type of TMDbException, let Tenacity handle retries if applicable
                log.error(f"TMDbException fetching TMDB movie by forced ID {forced_tmdb_id} (not a 'not found' error): {e_details_id}", exc_info=False); 
                raise 
            except Exception as e_details_id_unexp:
                log.error(f"Unexpected error fetching TMDB movie by forced ID {forced_tmdb_id}: {e_details_id_unexp}", exc_info=True); raise
        else: 
            # ... (existing search, filter, match logic from your complete file - this part was okay) ...
            # ... ensure it returns (None, None, None) if no search match ...
            results_obj: Optional[Iterable[Any]] = None
            try:
                results_obj = search.search(sync_title) 
                results_list = list(results_obj) if results_obj else []
                if not results_list:
                    log.warning(f"TMDB Search returned no results for movie '{sync_title}'.")
                    return None, None, None
            except TMDbException as e_search:
                 msg_lower = str(e_search).lower(); status_code = 0
                 if hasattr(e_search, 'args') and e_search.args and isinstance(e_search.args[0], dict): status_code = e_search.args[0].get('status_code',0)
                 if "resource not found" in msg_lower or status_code == 34 or status_code == 404 :
                     log.warning(f"TMDB Search resulted in 'Not Found' for movie '{sync_title}': {e_search}"); return None, None, None
                 log.error(f"TMDbException during TMDB movie search for '{sync_title}' [sync thread]: {e_search}", exc_info=False); raise
            except Exception as e_search:
                log.error(f"Unexpected error during TMDB movie search for '{sync_title}' [sync thread]: {e_search}", exc_info=True); raise

            processed_results_list = results_list
            is_yearless_match_attempt = sync_year_guess is None

            if sync_year_guess and processed_results_list:
                log.debug(f"Applying year filter ({sync_year_guess} +/- {self.year_tolerance}) to TMDB movie results.")
                filtered_list = []
                try:
                    for r_item in processed_results_list:
                        if not isinstance(r_item, (dict, AsObj)): continue
                        result_year_val = None
                        release_date_val = getattr(r_item, 'release_date', None) if isinstance(r_item, AsObj) else r_item.get('release_date')
                        if release_date_val: result_year_val = self._get_year_from_date(str(release_date_val))
                        if result_year_val is not None and abs(result_year_val - sync_year_guess) <= self.year_tolerance:
                            filtered_list.append(r_item)
                    if not filtered_list and processed_results_list: log.debug(f"Year filtering removed all TMDB movie results, using original list.")
                    else: processed_results_list = filtered_list
                    log.debug(f"Year filtering resulted in {len(processed_results_list)} TMDB movie results.")
                except Exception as e_filter: log.error(f"Error during TMDB movie year filtering: {e_filter}", exc_info=True)


            if processed_results_list:
                results_as_dicts_tuple = _tmdb_results_to_dicts(processed_results_list, result_type='movie')
                if results_as_dicts_tuple:
                    best_match_from_fuzzy_dict: Optional[Dict] = None; temp_score: Optional[float] = None
                    effective_fuzzy_cutoff = self.tmdb_fuzzy_cutoff
                    if is_yearless_match_attempt:
                        if self.movie_yearless_match_confidence == 'high': effective_fuzzy_cutoff = 90
                        elif self.movie_yearless_match_confidence == 'medium': effective_fuzzy_cutoff = 80
                        elif self.movie_yearless_match_confidence == 'low': effective_fuzzy_cutoff = self.tmdb_fuzzy_cutoff
                        log.debug(f"Yearless movie match: using effective fuzzy cutoff of {effective_fuzzy_cutoff} (strategy: {self.movie_yearless_match_confidence})")

                    if self.tmdb_strategy == 'fuzzy' and THEFUZZ_AVAILABLE and fuzz:
                        log.debug(f"Attempting TMDB movie fuzzy match (cutoff: {effective_fuzzy_cutoff}).")
                        match_tuple_res = find_best_match(sync_title, results_as_dicts_tuple, result_key='title', id_key='id', score_cutoff=effective_fuzzy_cutoff)
                        if match_tuple_res: best_match_from_fuzzy_dict, temp_score = match_tuple_res
                    
                    if not best_match_from_fuzzy_dict: 
                        log.debug("Using 'first' result strategy for TMDB movie (or fuzzy failed).")
                        first_raw_match_dict = next(iter(results_as_dicts_tuple), None)
                        if first_raw_match_dict:
                            if THEFUZZ_AVAILABLE and fuzz:
                                api_title_str = str(first_raw_match_dict.get('title', ''))
                                first_score_val = float(fuzz.ratio(str(sync_title).lower(), api_title_str.lower()))
                                effective_first_min_score = self.tmdb_first_result_min_score
                                if is_yearless_match_attempt:
                                    if self.movie_yearless_match_confidence == 'high': effective_first_min_score = 90
                                    elif self.movie_yearless_match_confidence == 'medium': effective_first_min_score = 80
                                    elif self.movie_yearless_match_confidence == 'low': effective_first_min_score = self.tmdb_first_result_min_score
                                
                                log.debug(f"  'first' strategy: Title='{api_title_str}', Score vs '{sync_title}' = {first_score_val:.1f} (Min required: {effective_first_min_score})")
                                if first_score_val >= effective_first_min_score:
                                    best_match_from_fuzzy_dict = first_raw_match_dict; temp_score = first_score_val
                                else: best_match_from_fuzzy_dict = None 
                            else: best_match_from_fuzzy_dict = first_raw_match_dict; temp_score = None

                    if best_match_from_fuzzy_dict:
                        matched_id_val = best_match_from_fuzzy_dict.get('id')
                        if matched_id_val is not None:
                            movie_match_obj = next((r_obj for r_obj in processed_results_list if isinstance(r_obj, (dict, AsObj)) and (getattr(r_obj, 'id', None) if isinstance(r_obj, AsObj) else r_obj.get('id')) == matched_id_val), None)
                            match_score = temp_score 
                        if not movie_match_obj: movie_match_obj = best_match_from_fuzzy_dict 
        
        if not movie_match_obj: return None, None, None
        if not isinstance(movie_match_obj, (dict, AsObj)): return None, None, None
        movie_id_val = getattr(movie_match_obj, 'id', None) if isinstance(movie_match_obj, AsObj) else movie_match_obj.get('id')
        if not movie_id_val: return None, None, None
        
        if not forced_tmdb_id and is_yearless_match_attempt and self.movie_yearless_match_confidence == 'confirm':
            match_score = -1.0 
            log.debug(f"TMDB matched movie '{getattr(movie_match_obj, 'title', movie_match_obj.get('title', 'N/A'))}' ID: {movie_id_val} (Score: marked for yearless confirm)")
        elif match_score != DIRECT_ID_MATCH_SCORE : 
            log.debug(f"TMDB matched movie '{getattr(movie_match_obj, 'title', movie_match_obj.get('title', 'N/A'))}' ID: {movie_id_val} (Score: {match_score if match_score is not None else 'N/A'})")

        final_movie_data_obj_details: Any = movie_match_obj
        if not forced_tmdb_id and movie_id_val: 
            try:
                details_obj = search.details(movie_id_val, append_to_response="external_ids,belongs_to_collection")
                if details_obj: final_movie_data_obj_details = details_obj
            except TMDbException as e_details_tmdb:
                 status_code_details = 0
                 if hasattr(e_details_tmdb, 'args') and e_details_tmdb.args and isinstance(e_details_tmdb.args[0], dict): status_code_details = e_details_tmdb.args[0].get('status_code',0)
                 if "resource not found" in str(e_details_tmdb).lower() or status_code_details == 34 or status_code_details == 404: log.warning(f"TMDB movie details for ID {movie_id_val} not found.") 
                 else: log.warning(f"TMDbException fetching movie details ID {movie_id_val}: {e_details_tmdb}");
            except Exception as e_details_unexp: log.warning(f"Unexpected error fetching movie details ID {movie_id_val}: {e_details_unexp}");

        ids_dict_final = get_external_ids(tmdb_obj=final_movie_data_obj_details)
        log.debug(f"_sync_tmdb_movie_fetch returning: data type={type(final_movie_data_obj_details)}, ids={ids_dict_final}, score={match_score}")
        return final_movie_data_obj_details, ids_dict_final, match_score

    def _sync_tmdb_series_fetch(self, sync_title: str, sync_season: int, sync_episodes: Tuple[int, ...], sync_year_guess: Optional[int], sync_lang: str, forced_tmdb_id: Optional[int] = None) -> Tuple[Optional[Any], Optional[Dict[int, Any]], Optional[Dict[str, Any]], Optional[float], Optional[str]]:
        log.debug(f"Executing TMDB Series Fetch [sync thread] for: '{sync_title}' S{sync_season} E{sync_episodes} (lang: {sync_lang}, year: {sync_year_guess}, forced_id: {forced_tmdb_id})")
        specific_error_signal: Optional[str] = None
        if not self.tmdb or not TMDBV3API_AVAILABLE or not TV or not Season:
            log.error("TMDB client/library/TV/Season class not available in _sync_tmdb_series_fetch.")
            return None, None, None, None, "CLIENT_UNAVAILABLE"

        search = TV()
        show_match_obj: Optional[Any] = None; match_score: Optional[float] = None

        if forced_tmdb_id:
            log.info(f"Attempting direct TMDB series fetch by ID: {forced_tmdb_id}")
            try:
                show_match_obj = search.details(forced_tmdb_id, append_to_response="external_ids")
                if show_match_obj: log.debug(f"Direct TMDB series fetch successful for ID {forced_tmdb_id}"); match_score = DIRECT_ID_MATCH_SCORE
            except TMDbException as e_details_id:
                msg_lower = str(e_details_id).lower(); status_code_from_exception = 0
                if hasattr(e_details_id, 'args') and e_details_id.args and isinstance(e_details_id.args[0], dict): status_code_from_exception = e_details_id.args[0].get('status_code', 0)
                if "resource not found" in msg_lower or status_code_from_exception == 34:
                    log.warning(f"TMDB series with forced ID {forced_tmdb_id} not found: {e_details_id}")
                    specific_error_signal = f"FORCED_TMDB_ID_NOT_FOUND::{forced_tmdb_id}"
                    return None, None, None, None, specific_error_signal
                log.error(f"TMDbException fetching TMDB series by forced ID {forced_tmdb_id}: {e_details_id}", exc_info=False); raise
            except Exception as e_details_id_unexp:
                log.error(f"Unexpected error fetching TMDB series by forced ID {forced_tmdb_id}: {e_details_id_unexp}", exc_info=True); raise
        else:
            results_obj: Optional[Iterable[Any]] = None
            try:
                results_obj = search.search(sync_title)
                results_list = list(results_obj) if results_obj else []
                if not results_list: log.warning(f"TMDB Search returned no results for series '{sync_title}'."); return None, None, None, None, "SEARCH_NO_RESULTS"
            except TMDbException as e_search: 
                 msg_lower = str(e_search).lower(); status_code = 0
                 if hasattr(e_search, 'args') and e_search.args and isinstance(e_search.args[0], dict): status_code = e_search.args[0].get('status_code',0)
                 if "resource not found" in msg_lower or status_code == 34 or status_code == 404 :
                     log.warning(f"TMDB Search resulted in 'Not Found' for series '{sync_title}': {e_search}"); return None, None, None, None, "SEARCH_NOT_FOUND"
                 log.error(f"TMDbException during TMDB series search for '{sync_title}' [sync thread]: {e_search}", exc_info=False); raise
            except Exception as e_search: log.error(f"Unexpected error during TMDB series search for '{sync_title}' [sync thread]: {e_search}", exc_info=True); raise

            processed_results_list = results_list
            if sync_year_guess and processed_results_list:
                log.debug(f"Applying year filter ({sync_year_guess} +/- {self.year_tolerance}) to TMDB series results.")
                filtered_list = []
                try:
                    for r_item in processed_results_list:
                        if not isinstance(r_item, (dict, AsObj)): continue
                        result_year_val = None
                        first_air_date_val = getattr(r_item, 'first_air_date', None) if isinstance(r_item, AsObj) else r_item.get('first_air_date')
                        if first_air_date_val: result_year_val = self._get_year_from_date(str(first_air_date_val))
                        if result_year_val is not None and abs(result_year_val - sync_year_guess) <= self.year_tolerance:
                            filtered_list.append(r_item)
                    if not filtered_list and processed_results_list: log.debug(f"Year filtering removed all TMDB series results, using original list.")
                    else: processed_results_list = filtered_list
                    log.debug(f"Year filtering resulted in {len(processed_results_list)} TMDB series results.")
                except Exception as e_filter: log.error(f"Error during TMDB series year filtering: {e_filter}", exc_info=True)

            if processed_results_list:
                results_as_dicts_tuple = _tmdb_results_to_dicts(processed_results_list, result_type='series')
                if results_as_dicts_tuple:
                    best_match_from_fuzzy_dict: Optional[Dict] = None; temp_score: Optional[float] = None
                    if self.tmdb_strategy == 'fuzzy' and THEFUZZ_AVAILABLE and fuzz:
                        log.debug(f"Attempting TMDB series fuzzy match (cutoff: {self.tmdb_fuzzy_cutoff}).")
                        match_tuple_res = find_best_match(sync_title, results_as_dicts_tuple, result_key='name', id_key='id', score_cutoff=self.tmdb_fuzzy_cutoff)
                        if match_tuple_res: best_match_from_fuzzy_dict, temp_score = match_tuple_res
                    if not best_match_from_fuzzy_dict:
                        log.debug("Using 'first' result strategy for TMDB series.")
                        first_raw_match_dict = next(iter(results_as_dicts_tuple), None)
                        if first_raw_match_dict:
                            if THEFUZZ_AVAILABLE and fuzz:
                                api_name_str = str(first_raw_match_dict.get('name', ''))
                                first_score_val = float(fuzz.ratio(str(sync_title).lower(), api_name_str.lower()))
                                log.debug(f"  'first' strategy: Series Name='{api_name_str}', Score vs '{sync_title}' = {first_score_val:.1f} (Min required: {self.tmdb_first_result_min_score})")
                                if first_score_val >= self.tmdb_first_result_min_score:
                                    best_match_from_fuzzy_dict = first_raw_match_dict; temp_score = first_score_val
                                else: best_match_from_fuzzy_dict = None 
                            else: best_match_from_fuzzy_dict = first_raw_match_dict; temp_score = None
                    if best_match_from_fuzzy_dict:
                        matched_id_val = best_match_from_fuzzy_dict.get('id')
                        if matched_id_val is not None:
                            show_match_obj = next((r_obj for r_obj in processed_results_list if isinstance(r_obj, (dict, AsObj)) and (getattr(r_obj, 'id', None) if isinstance(r_obj, AsObj) else r_obj.get('id')) == matched_id_val), None)
                            match_score = temp_score 
                        if not show_match_obj: show_match_obj = best_match_from_fuzzy_dict

        if not show_match_obj:
            log.warning(f"No suitable TMDB series match found for '{sync_title}' S{sync_season} (forced_id: {forced_tmdb_id})."); return None, None, None, None, "NO_FINAL_MATCH"
        if not isinstance(show_match_obj, (dict, AsObj)):
            log.error(f"Final TMDB series match is not a valid object/dict type: {type(show_match_obj)}. Skipping.")
            return None, None, None, None, "INVALID_MATCH_TYPE"

        show_id_val = getattr(show_match_obj, 'id', None) if isinstance(show_match_obj, AsObj) else show_match_obj.get('id')
        if not show_id_val:
            log.error(f"Final TMDB series match lacks 'id': {show_match_obj}"); return None, None, None, None, "MISSING_ID_IN_MATCH"
        
        if match_score != DIRECT_ID_MATCH_SCORE:
            log.debug(f"TMDB matched series '{getattr(show_match_obj, 'name', show_match_obj.get('name', 'N/A'))}' ID: {show_id_val} (Score: {match_score if match_score is not None else 'N/A'})")

        final_show_data_obj_details: Any = show_match_obj
        if not forced_tmdb_id and show_id_val: 
            try:
                details_obj = search.details(show_id_val, append_to_response="external_ids")
                if details_obj: final_show_data_obj_details = details_obj
            except TMDbException as e_details_tmdb: 
                 status_code_details = 0; 
                 if hasattr(e_details_tmdb, 'args') and e_details_tmdb.args and isinstance(e_details_tmdb.args[0], dict): status_code_details = e_details_tmdb.args[0].get('status_code',0)
                 if "resource not found" in str(e_details_tmdb).lower() or status_code_details == 34 or status_code_details == 404: 
                     log.warning(f"TMDB series details for ID {show_id_val} not found.")
                 else: log.warning(f"TMDbException fetching series details ID {show_id_val}: {e_details_tmdb}");
            except Exception as e_details_unexp: log.warning(f"Unexpected error fetching series details ID {show_id_val}: {e_details_unexp}");

        ep_data_map: Dict[int, Any] = {}
        if sync_episodes and sync_season is not None and show_id_val is not None: 
            try:
                log.debug(f"Fetching TMDB season {sync_season} details for show ID {show_id_val}")
                season_fetcher = Season()
                season_details_obj = season_fetcher.details(tv_id=show_id_val, season_num=sync_season)
                episodes_list_api = getattr(season_details_obj, 'episodes', []) if season_details_obj else []
                if episodes_list_api:
                    episodes_in_season_dict: Dict[int, Any] = {}
                    for api_ep_obj in episodes_list_api:
                        ep_num_api_val = getattr(api_ep_obj, 'episode_number', None)
                        if ep_num_api_val is not None:
                            try: episodes_in_season_dict[int(ep_num_api_val)] = api_ep_obj
                            except (ValueError, TypeError): pass
                    for ep_num_needed in sync_episodes:
                        episode_obj_found = episodes_in_season_dict.get(ep_num_needed)
                        if episode_obj_found: ep_data_map[ep_num_needed] = episode_obj_found
                        else: log.warning(f"TMDB S{sync_season} E{ep_num_needed} not found for '{getattr(final_show_data_obj_details, 'name', final_show_data_obj_details.get('name', 'N/A'))}'")
                else:
                    season_name_str = getattr(season_details_obj, 'name', f'S{sync_season}') if season_details_obj else f'S{sync_season}'
                    log.warning(f"TMDB season details '{season_name_str}' ID {show_id_val} lacks 'episodes' list or attribute.")
            except TMDbException as e_season_tmdb:
                status_code_season = 0
                if hasattr(e_season_tmdb, 'args') and e_season_tmdb.args and isinstance(e_season_tmdb.args[0], dict): status_code_season = e_season_tmdb.args[0].get('status_code',0)
                if "resource not found" in str(e_season_tmdb).lower() or status_code_season == 34 or status_code_season == 404: 
                    log.warning(f"TMDB season S{sync_season} for ID {show_id_val} not found.")
                else: log.warning(f"TMDbException getting season S{sync_season} ID {show_id_val}: {e_season_tmdb}")
            except Exception as e_season_unexp: log.warning(f"Unexpected error getting season S{sync_season} ID {show_id_val}: {e_season_unexp}")

        ids_final = get_external_ids(tmdb_obj=final_show_data_obj_details)
        log.debug(f"_sync_tmdb_series_fetch returning: data type={type(final_show_data_obj_details)}, ep_map keys={list(ep_data_map.keys())}, ids={ids_final}, score={match_score}")
        return final_show_data_obj_details, ep_data_map, ids_final, match_score, specific_error_signal

    def _sync_tvdb_series_fetch(self, sync_title: str, sync_season_num: int, sync_episodes: Tuple[int, ...], sync_tvdb_id_arg: Optional[int], sync_year_guess: Optional[int], sync_lang: str, forced_tvdb_id: Optional[int] = None) -> Tuple[Optional[Dict], Optional[Dict[int, Any]], Optional[Dict[str, Any]], Optional[float]]:
        log.debug(f"Executing TVDB Series Fetch [sync thread] for: '{sync_title}' S{sync_season_num} E{sync_episodes} (lang: {sync_lang}, year: {sync_year_guess}, tvdb_id_arg: {sync_tvdb_id_arg}, forced_id: {forced_tvdb_id})")
        
        if not self.tvdb:
            log.error("TVDB client not available in _sync_tvdb_series_fetch.")
            raise ConnectionError("TVDB client not configured in _sync_tvdb_series_fetch")

        show_data_dict: Optional[Dict] = None
        best_match_id_val: Optional[int] = forced_tvdb_id if forced_tvdb_id is not None else sync_tvdb_id_arg
        match_score_val: Optional[float] = None

        if forced_tvdb_id:
            log.info(f"Attempting direct TVDB series fetch by ID: {forced_tvdb_id}")
            match_score_val = DIRECT_ID_MATCH_SCORE # Assume direct match unless it fails
            try:
                show_data_dict = self.tvdb.get_series_extended(forced_tvdb_id) # type: ignore
                if not show_data_dict or not isinstance(show_data_dict, dict):
                    log.warning(f"TVDB series with forced ID {forced_tvdb_id} not found (get_series_extended returned non-dict or None).")
                    # This specific case might indicate the library didn't raise an exception for a 404,
                    # so we raise our specific error.
                    raise MetadataError(f"FORCED_TVDB_ID_NOT_FOUND::{forced_tvdb_id}")
                log.debug(f"Direct TVDB series fetch successful for ID {forced_tvdb_id}")
            except (ValueError, Exception) as e_details_id: # tvdb-v4-official can raise ValueError for "not found"
                msg = str(e_details_id).lower()
                # Check for common "not found" patterns from the TVDB library
                if ("not found" in msg or "no record" in msg or "invalid id" in msg or "404" in msg) or \
                   (isinstance(e_details_id, ValueError) and "id not found" in msg): # More specific for tvdb lib if it raises ValueError
                    log.warning(f"TVDB series with forced ID {forced_tvdb_id} not found via API: {e_details_id}")
                    raise MetadataError(f"FORCED_TVDB_ID_NOT_FOUND::{forced_tvdb_id}") from e_details_id
                # For other ValueErrors or Exceptions during forced ID lookup, re-raise them
                log.error(f"TVDB API Exception fetching series by forced ID {forced_tvdb_id} (not 'not found'): {e_details_id}", exc_info=False); 
                raise # Re-raise the original error for Tenacity to evaluate
        elif not best_match_id_val: # No forced ID and no pre-known ID, so search
            search_results_list: Optional[List[Dict]] = None
            try:
                log.debug(f"TVDB searching for: '{sync_title}' (Year guess: {sync_year_guess})")
                search_results_list = self.tvdb.search(sync_title) # type: ignore
                log.debug(f"TVDB search returned {len(search_results_list) if search_results_list else 0} results.")
                if not search_results_list:
                    log.warning(f"TVDB Search returned no results for series '{sync_title}'.")
                    return None, None, None, None # No match
            except (ValueError, Exception) as e_search_tvdb:
                 msg = str(e_search_tvdb).lower()
                 if ("not found" in msg or "no record" in msg or "invalid id" in msg or "404" in msg) or \
                    (isinstance(e_search_tvdb, ValueError) and "id not found" in msg):
                     log.warning(f"TVDB Search resulted in 'Not Found' for series '{sync_title}': {e_search_tvdb}")
                     return None, None, None, None # No match
                 log.warning(f"TVDB search failed for '{sync_title}': {type(e_search_tvdb).__name__}: {e_search_tvdb}", exc_info=False);
                 raise # Re-raise other search errors for Tenacity

            if search_results_list: # This block is only if not forced_tvdb_id and search was done
                if sync_year_guess: # Year filter
                    filtered_results_tvdb: List[Dict] = []
                    for r_dict_tvdb in search_results_list:
                        if not isinstance(r_dict_tvdb, dict): continue
                        result_year_str_tvdb = r_dict_tvdb.get('year')
                        if result_year_str_tvdb:
                            try:
                                result_year_int_tvdb = int(result_year_str_tvdb)
                                if abs(result_year_int_tvdb - sync_year_guess) <= self.year_tolerance:
                                    filtered_results_tvdb.append(r_dict_tvdb)
                            except (ValueError, TypeError): pass
                    if not filtered_results_tvdb and search_results_list: log.debug("TVDB year filtering removed all results, using original list.")
                    else: search_results_list = filtered_results_tvdb
                    log.debug(f"TVDB results after year filter: {len(search_results_list)}.")

                if search_results_list: # Fuzzy match
                    try:
                         tvdb_fuzzy_cutoff_val = int(self.cfg('tmdb_match_fuzzy_cutoff', 70)) 
                         best_match_tuple = find_best_match(sync_title, tuple(search_results_list), result_key='name', id_key='tvdb_id', score_cutoff=tvdb_fuzzy_cutoff_val)
                         if best_match_tuple:
                             best_match_dict_tvdb, score_tvdb = best_match_tuple
                             if best_match_dict_tvdb:
                                 matched_id_str_tvdb = best_match_dict_tvdb.get('tvdb_id')
                                 if matched_id_str_tvdb:
                                     try: best_match_id_val = int(matched_id_str_tvdb); match_score_val = score_tvdb
                                     except (ValueError, TypeError): log.warning(f"Could not convert matched TVDB ID '{matched_id_str_tvdb}' to int.")
                    except Exception as e_fuzz_tvdb:
                        log.error(f"Error during TVDB fuzzy match: {e_fuzz_tvdb}")
                        first_tvdb_res = next(iter(search_results_list), None)
                        if first_tvdb_res and isinstance(first_tvdb_res, dict):
                             first_id_str_tvdb = first_tvdb_res.get('tvdb_id')
                             if first_id_str_tvdb:
                                 try: best_match_id_val = int(first_id_str_tvdb)
                                 except (ValueError, TypeError): log.warning(f"Could not convert first result TVDB ID '{first_id_str_tvdb}' to int.")
            if not best_match_id_val: 
                log.warning(f"TVDB could not find suitable match ID for series '{sync_title}' after search.")
                return None, None, None, None
            
            if best_match_id_val and not show_data_dict: # If ID from search, fetch details
                try:
                    log.debug(f"TVDB fetching extended series data for ID found by search: {best_match_id_val}")
                    show_data_dict = self.tvdb.get_series_extended(best_match_id_val) # type: ignore
                    if not show_data_dict or not isinstance(show_data_dict, dict):
                        log.warning(f"TVDB get_series_extended for searched ID {best_match_id_val} returned invalid data: {type(show_data_dict)}")
                        return None, None, None, None
                except (ValueError, Exception) as e_fetch_tvdb_search: # Handle errors fetching details for searched ID
                    msg = str(e_fetch_tvdb_search).lower()
                    if ("not found" in msg or "no record" in msg or "invalid id" in msg or "404" in msg) or \
                       (isinstance(e_fetch_tvdb_search, ValueError) and "id not found" in msg):
                        log.warning(f"TVDB get_series_extended failed for searched ID {best_match_id_val}: Not Found. Error: {e_fetch_tvdb_search}")
                        return None, None, None, None
                    log.warning(f"TVDB get_series_extended failed for searched ID {best_match_id_val}: {type(e_fetch_tvdb_search).__name__}: {e_fetch_tvdb_search}", exc_info=False);
                    raise 
        
        if not show_data_dict: # Should be populated if forced ID or search ID was valid and details fetched
            log.warning(f"No TVDB show data obtained for '{sync_title}' (forced_id: {forced_tvdb_id})."); return None, None, None, None

        log.debug(f"TVDB successfully fetched/selected extended data for: {show_data_dict.get('name', 'N/A')} (Score: {match_score_val if match_score_val is not None else 'N/A'})")

        ep_data_map_tvdb: Dict[int, Any] = {}
        ids_dict_tvdb: Dict[str, Any] = {}
        current_show_id_for_episodes = int(show_data_dict['id']) if show_data_dict and show_data_dict.get('id') else best_match_id_val

        if show_data_dict and current_show_id_for_episodes is not None:
            # ... (TVDB episode fetching and ID extraction as before) ...
            try:
                log.debug(f"TVDB fetching ALL episodes for show ID {current_show_id_for_episodes} (pagination may occur)")
                all_episodes_list_tvdb: List[Dict] = []
                page_num = 0
                while True: 
                    episodes_page_data_dict = self.tvdb.get_series_episodes(current_show_id_for_episodes, page=page_num, lang=sync_lang)
                    if episodes_page_data_dict and isinstance(episodes_page_data_dict.get('episodes'), list):
                        page_episodes_list = episodes_page_data_dict['episodes']
                        all_episodes_list_tvdb.extend(page_episodes_list)
                        log.debug(f"  Fetched page {page_num}, {len(page_episodes_list)} TVDB episodes.")
                        links_dict = self.tvdb.get_req_links()
                        if links_dict and links_dict.get('next'):
                             page_num += 1; log.debug(f"  Found 'next' link for TVDB episodes, fetching page {page_num}...")
                        else:
                            log.debug("  No 'next' link found for TVDB episodes or links structure unexpected. Assuming end of pages."); break
                    else: 
                        if episodes_page_data_dict and episodes_page_data_dict.get('status') == 'success' and not episodes_page_data_dict.get('episodes'):
                            log.debug(f"TVDB episodes page {page_num} for show ID {current_show_id_for_episodes} is empty, assuming end.")
                        else:
                            log.warning(f"TVDB episodes data invalid or missing 'episodes' key for page {page_num}, show ID {current_show_id_for_episodes}. Stopping pagination.")
                        break
                log.debug(f"Total TVDB episodes fetched for show ID {current_show_id_for_episodes}: {len(all_episodes_list_tvdb)}")

                episodes_in_season_dict_tvdb: Dict[int, Dict] = {}
                for ep_dict_item in all_episodes_list_tvdb:
                    if isinstance(ep_dict_item, dict):
                        api_season_num_val = ep_dict_item.get('seasonNumber'); api_ep_num_val = ep_dict_item.get('number')
                        if api_season_num_val is not None and api_ep_num_val is not None:
                            try:
                                if int(api_season_num_val) == int(sync_season_num): episodes_in_season_dict_tvdb[int(api_ep_num_val)] = ep_dict_item
                            except (ValueError, TypeError): log.warning(f"Could not parse season/episode number from TVDB episode dict: {ep_dict_item}")

                episode_iterator_tvdb = sync_episodes if sync_episodes else []
                for ep_num_val_tvdb in episode_iterator_tvdb:
                    episode_details_dict = episodes_in_season_dict_tvdb.get(ep_num_val_tvdb)
                    if episode_details_dict: ep_data_map_tvdb[ep_num_val_tvdb] = episode_details_dict
                    else: log.warning(f"TVDB S{sync_season_num} E{ep_num_val_tvdb} not found in fetched episodes for '{show_data_dict.get('name')}'")
            except (ValueError, Exception) as e_ep_fetch_tvdb:
                 msg = str(e_ep_fetch_tvdb).lower()
                 if "not found" in msg or "404" in msg: log.warning(f"TVDB episodes fetch failed for ID {current_show_id_for_episodes}: Not Found.")
                 else: log.warning(f"TVDB error fetching/processing episode data for S{sync_season_num}, ID {current_show_id_for_episodes}: {e_ep_fetch_tvdb}", exc_info=False)
            
            try: ids_dict_tvdb = get_external_ids(tvdb_obj=show_data_dict)
            except Exception as e_ids_tvdb: log.warning(f"Error extracting external IDs from TVDB data: {e_ids_tvdb}", exc_info=True)

        log.debug(f"_sync_tvdb_series_fetch returning: data type={type(show_data_dict)}, ep_map keys={list(ep_data_map_tvdb.keys())}, ids={ids_dict_tvdb}, score={match_score_val}")
        # Returns 4 items now, specific_error_signal is handled by raising MetadataError directly
        return show_data_dict, ep_data_map_tvdb, ids_dict_tvdb, match_score_val

    async def _do_fetch_tmdb_movie(self, title_arg: str, year_arg: Optional[int], lang: str ='en', force_tmdb_id_arg: Optional[int] = None) -> Tuple[Optional[Any], Optional[Dict[str, Any]], Optional[float]]:
        max_attempts = max(1, int(self.cfg('api_retry_attempts', 3)))
        wait_seconds = float(self.cfg('api_retry_wait_seconds', 2.0))
        async_retryer = AsyncRetrying( stop=stop_after_attempt(max_attempts), wait=wait_fixed(wait_seconds), retry=retry_if_exception(should_retry_api_error), reraise=True )
        
        data_obj, ids_dict, score = None, None, None
        try:
            log.debug(f"Attempting TMDB movie fetch for '{title_arg}' ({year_arg}, id:{force_tmdb_id_arg}) with tenacity.")
            # _sync_tmdb_movie_fetch now returns 3 items
            data_obj, ids_dict, score = await async_retryer( 
                self._run_sync, self._sync_tmdb_movie_fetch, str(title_arg), year_arg, str(lang), force_tmdb_id_arg 
            )
            
            if data_obj is None and ids_dict is None and score is None : 
                log.info(f"TMDB movie '{title_arg}' ({year_arg}, id:{force_tmdb_id_arg}) no match found after search/filtering.")
                raise MetadataError(f"No TMDB match found for movie '{title_arg}' (ID: {force_tmdb_id_arg if force_tmdb_id_arg else 'N/A - search failed'}).")
            
            return data_obj, ids_dict, score
        except MetadataError as me: # This catches "FORCED_TMDB_ID_NOT_FOUND" OR "No TMDB match found..."
            # Refine message specifically for "FORCED_ID_NOT_FOUND"
            if "FORCED_TMDB_ID_NOT_FOUND::" in str(me):
                try:
                    forced_id = str(me).split("::")[1]
                    log.warning(f"TMDB movie: Forced ID {forced_id} not found.")
                    raise MetadataError(f"Provided TMDB ID '{forced_id}' was not found.") from me
                except IndexError: # Fallback if parsing signal fails
                    log.error(f"Error parsing FORCED_ID signal: {me}")
                    raise # Re-raise original MetadataError
            else: # Other MetadataErrors (like no search match)
                log.error(f"MetadataError during TMDB movie fetch for '{title_arg}': {me}") 
                raise 
        except RetryError as e: 
            last_exception = e.last_attempt.exception() if e.last_attempt else e
            error_context = f"Movie: '{title_arg}' (ID: {force_tmdb_id_arg or 'N/A'})"; 
            
            # Check if the underlying cause of RetryError was a "forced ID not found"
            if force_tmdb_id_arg and isinstance(last_exception, MetadataError) and f"FORCED_TMDB_ID_NOT_FOUND::{force_tmdb_id_arg}" in str(last_exception):
                final_error_msg = f"Provided TMDB ID '{force_tmdb_id_arg}' was not found (after retries for other initial errors)."
            elif force_tmdb_id_arg and isinstance(last_exception, TMDbException): # Check original TMDbException too
                msg_lower = str(last_exception).lower(); status_code = 0
                if hasattr(last_exception, 'args') and last_exception.args and isinstance(last_exception.args[0], dict): status_code = last_exception.args[0].get('status_code',0)
                if "resource not found" in msg_lower or status_code == 34:
                    final_error_msg = f"Provided TMDB ID '{force_tmdb_id_arg}' was not found (after retries for other initial errors)."
                else:
                    final_error_msg = f"Failed to fetch TMDB metadata ({error_context}) after {max_attempts} attempts."
                    if isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network connection."
                    elif isinstance(last_exception, req_exceptions.HTTPError) and 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely a temporary TMDB server issue."
            else: # General retry failure
                final_error_msg = f"Failed to fetch TMDB metadata ({error_context}) after {max_attempts} attempts."
                if isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network connection."
                elif isinstance(last_exception, req_exceptions.HTTPError) and 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely a temporary TMDB server issue."

            log.error(f"All {max_attempts} retry attempts failed for TMDB movie '{title_arg}'. Last error: {type(last_exception).__name__}: {last_exception}")
            raise MetadataError(final_error_msg) from last_exception
        except Exception as e: 
            log.error(f"Non-retryable or unexpected error during TMDB movie fetch for '{title_arg}': {type(e).__name__}: {e}", exc_info=True)
            error_context = f"Movie: '{title_arg}' (ID: {force_tmdb_id_arg or 'N/A'})"; user_facing_error: Optional[str] = None
            if isinstance(e, (TMDbException, req_exceptions.HTTPError)):
                msg_lower = str(e).lower(); status_code = 0
                if isinstance(e, req_exceptions.HTTPError): status_code = getattr(getattr(e, 'response', None), 'status_code', 0)
                elif hasattr(e, 'args') and e.args and isinstance(e.args[0], dict): status_code = e.args[0].get('status_code', 0)

                if "invalid api key" in msg_lower or status_code == 7 or status_code == 401: 
                    user_facing_error = f"Invalid TMDB API Key or Authentication Failed ({error_context}). Please check your key."
                elif status_code == 403: user_facing_error = f"TMDB API request forbidden ({error_context}). Check API key permissions."
            
            final_error_msg = user_facing_error or f"Unrecoverable error fetching TMDB metadata ({error_context}). Details: {type(e).__name__}"
            raise MetadataError(final_error_msg) from e

    async def _do_fetch_tmdb_series(self, title_arg: str, season_arg: int, episodes_arg: Tuple[int, ...], year_guess_arg: Optional[int] = None, lang: str ='en', force_tmdb_id_arg: Optional[int] = None) -> Tuple[Optional[Any], Optional[Dict[int, Any]], Optional[Dict[str, Any]], Optional[float]]:
        max_attempts = max(1, int(self.cfg('api_retry_attempts', 3)))
        wait_seconds = float(self.cfg('api_retry_wait_seconds', 2.0))
        async_retryer = AsyncRetrying(stop=stop_after_attempt(max_attempts), wait=wait_fixed(wait_seconds), retry=retry_if_exception(should_retry_api_error), reraise=True)
        data_obj, ep_map, ids_dict, score, specific_error = None, None, None, None, None
        try:
            log.debug(f"Attempting TMDB series fetch for '{title_arg}' S{season_arg} (id:{force_tmdb_id_arg}) with tenacity.")
            data_obj, ep_map, ids_dict, score, specific_error = await async_retryer(
                self._run_sync, self._sync_tmdb_series_fetch, str(title_arg), int(season_arg), tuple(episodes_arg), year_guess_arg, str(lang), force_tmdb_id_arg
            )
            if specific_error:
                if "FORCED_TMDB_ID_NOT_FOUND" in specific_error:
                    forced_id = specific_error.split("::")[1]
                    raise MetadataError(f"Provided TMDB Series ID '{forced_id}' was not found.")
                else: raise MetadataError(f"A specific error occurred during TMDB series fetch: {specific_error}")

            if data_obj is None:
                log.info(f"TMDB series '{title_arg}' S{season_arg} (id:{force_tmdb_id_arg}) not found or no match.")
                raise MetadataError(f"No TMDB match found for series '{title_arg}' S{season_arg} (ID: {force_tmdb_id_arg}).")
            return data_obj, ep_map, ids_dict, score
        except MetadataError as me: log.error(f"MetadataError during TMDB series fetch for '{title_arg}' S{season_arg}: {me}"); raise
        except RetryError as e: 
            last_exception = e.last_attempt.exception() if e.last_attempt else e; log.error(f"All {max_attempts} retry attempts failed for TMDB series '{title_arg}' S{season_arg}. Last error: {type(last_exception).__name__}: {last_exception}")
            error_context = f"Series: '{title_arg}' S{season_arg} (ID: {force_tmdb_id_arg or 'N/A'})"; final_error_msg = f"Failed to fetch TMDB metadata ({error_context}) after {max_attempts} attempts."
            if isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network."
            elif isinstance(last_exception, req_exceptions.HTTPError) and 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely TMDB server issue."
            raise MetadataError(final_error_msg) from last_exception
        except Exception as e: 
            log.error(f"Non-retryable or unexpected error during TMDB series fetch for '{title_arg}' S{season_arg}: {type(e).__name__}: {e}", exc_info=True)
            error_context = f"Series: '{title_arg}' S{season_arg} (ID: {force_tmdb_id_arg or 'N/A'})"; user_facing_error: Optional[str] = None
            if isinstance(e, (TMDbException, req_exceptions.HTTPError)):
                msg_lower = str(e).lower(); status_code = 0
                if isinstance(e, req_exceptions.HTTPError): status_code = getattr(getattr(e, 'response', None), 'status_code', 0)
                elif hasattr(e, 'args') and e.args and isinstance(e.args[0], dict): status_code = e.args[0].get('status_code',0)
                if "invalid api key" in msg_lower or status_code == 7 or status_code == 401: user_facing_error = f"Invalid TMDB API Key ({error_context})."
                elif status_code == 403: user_facing_error = f"TMDB API request forbidden ({error_context})."
            final_error_msg = user_facing_error or f"Unrecoverable error fetching TMDB metadata ({error_context}). Details: {type(e).__name__}"
            raise MetadataError(final_error_msg) from e

    async def _do_fetch_tvdb_series(self, title_arg: str, season_num_arg: int, episodes_arg: Tuple[int, ...], tvdb_id_arg: Optional[int] = None, year_guess_arg: Optional[int] = None, lang: str = 'en', force_tvdb_id_arg: Optional[int] = None) -> Tuple[Optional[Dict], Optional[Dict[int, Any]], Optional[Dict[str, Any]], Optional[float]]:
        max_attempts = max(1, int(self.cfg('api_retry_attempts', 3)))
        wait_seconds = float(self.cfg('api_retry_wait_seconds', 2.0))
        async_retryer = AsyncRetrying(stop=stop_after_attempt(max_attempts), wait=wait_fixed(wait_seconds), retry=retry_if_exception(should_retry_api_error), reraise=True)
        
        data_obj, ep_map, ids_dict, score = None, None, None, None
        try:
            log.debug(f"Attempting TVDB series fetch for '{title_arg}' S{season_num_arg} (id_arg:{tvdb_id_arg}, force_id:{force_tvdb_id_arg}) with tenacity.")
            # _sync_tvdb_series_fetch now returns 4 items. It raises MetadataError for FORCED_ID_NOT_FOUND.
            data_obj, ep_map, ids_dict, score = await async_retryer(
                self._run_sync, self._sync_tvdb_series_fetch, str(title_arg), int(season_num_arg), tuple(episodes_arg), tvdb_id_arg, year_guess_arg, str(lang), force_tvdb_id_arg 
            )

            if data_obj is None: # This means search found nothing or an unexpected issue not caught as specific error
                log.info(f"TVDB series '{title_arg}' S{season_num_arg} (id_arg:{tvdb_id_arg}, force_id:{force_tvdb_id_arg}) no match found.")
                raise MetadataError(f"No TVDB match found for series '{title_arg}' S{season_num_arg} (ID: {force_tvdb_id_arg or tvdb_id_arg}).")
            return data_obj, ep_map, ids_dict, score
        except MetadataError as me: # This will catch "FORCED_TVDB_ID_NOT_FOUND" or "No TVDB match found..."
            # Refine message if it's the internal signal
            if "FORCED_TVDB_ID_NOT_FOUND::" in str(me):
                try:
                    forced_id = str(me).split("::")[1]
                    log.warning(f"TVDB series: Forced ID {forced_id} not found.")
                    raise MetadataError(f"Provided TVDB ID '{forced_id}' was not found.") from me
                except IndexError: pass # Fall through to re-raise original 'me'
            log.error(f"MetadataError during TVDB series fetch for '{title_arg}' S{season_num_arg}: {me}"); 
            raise # Re-raise the (potentially refined) MetadataError
        except RetryError as e: 
            last_exception = e.last_attempt.exception() if e.last_attempt else e
            error_context = f"TVDB Series: '{title_arg}' S{season_num_arg} (ID: {force_tvdb_id_arg or tvdb_id_arg or 'N/A'})"
            final_error_msg = f"Failed to fetch TVDB metadata ({error_context}) after {max_attempts} attempts."

            # Check if the underlying cause of RetryError was a "forced ID not found"
            if force_tvdb_id_arg and isinstance(last_exception, MetadataError) and f"FORCED_TVDB_ID_NOT_FOUND::{force_tvdb_id_arg}" in str(last_exception):
                final_error_msg = f"Provided TVDB ID '{force_tvdb_id_arg}' was not found (after retries)."
            elif force_tvdb_id_arg and isinstance(last_exception, ValueError) and ("not found" in str(last_exception).lower() or "id not found" in str(last_exception).lower()):
                 final_error_msg = f"Provided TVDB ID '{force_tvdb_id_arg}' was not found (after retries)."
            elif isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network."
            elif isinstance(last_exception, req_exceptions.HTTPError) and 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely TVDB server issue."
            elif isinstance(last_exception, ValueError) and ("unauthorized" in str(last_exception).lower() or "api key" in str(last_exception).lower()): final_error_msg = f"TVDB Error ({error_context}): {last_exception}"
            
            log.error(f"All {max_attempts} retry attempts failed for TVDB series '{title_arg}' S{season_num_arg}. Last error: {type(last_exception).__name__}: {last_exception}")
            raise MetadataError(final_error_msg) from last_exception
        except Exception as e: 
            log.error(f"Non-retryable or unexpected error during TVDB series fetch for '{title_arg}' S{season_num_arg}: {type(e).__name__}: {e}", exc_info=True)
            error_context = f"TVDB Series: '{title_arg}' S{season_num_arg} (ID: {force_tvdb_id_arg or tvdb_id_arg or 'N/A'})"; user_facing_error: Optional[str] = None
            msg_lower = str(e).lower()
            if "unauthorized" in msg_lower or "api key" in msg_lower or ("response" in msg_lower and "401" in msg_lower): user_facing_error = f"Invalid TVDB API Key or Unauthorized ({error_context})."
            elif isinstance(e, req_exceptions.HTTPError) and getattr(getattr(e, 'response', None), 'status_code', 0) == 403: user_facing_error = f"TVDB API request forbidden ({error_context})."
            final_error_msg = user_facing_error or f"Unrecoverable error fetching TVDB metadata ({error_context}). Details: {type(e).__name__}"
            raise MetadataError(final_error_msg) from e

    async def fetch_movie_metadata(self, movie_title_guess: str, year_guess: Optional[int] = None, force_tmdb_id: Optional[int] = None) -> MediaMetadata:
        # ... (as previously provided, the error handling in its try-except MetadataError will catch the specific one)
        log.debug(f"Fetching movie metadata (async) for: '{movie_title_guess}' (Year guess: {year_guess}, Force TMDB ID: {force_tmdb_id})")
        final_meta = MediaMetadata(is_movie=True)
        tmdb_movie_data: Optional[Any] = None; tmdb_ids: Optional[Dict[str, Any]] = None; tmdb_score: Optional[float] = None
        lang = str(self.cfg('tmdb_language', 'en')) 
        
        cache_key_base = f"movie::{lang}"
        if force_tmdb_id: cache_key = f"{cache_key_base}::id_{force_tmdb_id}"
        else: cache_key = f"{cache_key_base}::{movie_title_guess}_{year_guess}"

        fetch_error_message: Optional[str] = None
        
        cached_data = await self._get_cache(cache_key) # Cache now stores 3 items for movie (data, ids, score)
        if cached_data and len(cached_data) == 3:
            tmdb_movie_data, tmdb_ids, tmdb_score_cached = cached_data
            tmdb_score = tmdb_score_cached if tmdb_score_cached is not None else (DIRECT_ID_MATCH_SCORE if force_tmdb_id else None)
            log.debug(f"Using cached data for movie: '{movie_title_guess}' (ID: {force_tmdb_id}, Score: {tmdb_score})")
        else:
            if cached_data: log.warning(f"Invalid movie cache structure for {cache_key}, re-fetching.")
            if not self.tmdb:
                fetch_error_message = "TMDB client not available."
            else:
                try:
                    await self.rate_limiter.wait()
                    tmdb_movie_data, tmdb_ids, tmdb_score = await self._do_fetch_tmdb_movie(
                        movie_title_guess, year_guess, lang, force_tmdb_id_arg=force_tmdb_id
                    )
                    if tmdb_movie_data is not None: 
                         await self._set_cache(cache_key, (tmdb_movie_data, tmdb_ids, tmdb_score)) # Cache 3 items
                except MetadataError as me: 
                    log.error(f"TMDB movie fetch failed for '{movie_title_guess}' (ID: {force_tmdb_id}): {me}")
                    fetch_error_message = str(me) 
                    tmdb_movie_data, tmdb_ids, tmdb_score = None, None, None 
                except Exception as e: 
                     log.error(f"Unexpected error during TMDB movie fetch for '{movie_title_guess}': {type(e).__name__}: {e}", exc_info=True)
                     fetch_error_message = f"Unexpected error fetching TMDB movie: {type(e).__name__}"; tmdb_movie_data, tmdb_ids, tmdb_score = None, None, None

            if tmdb_movie_data is None and not fetch_error_message: 
                 fetch_error_message = f"No TMDB match for movie '{movie_title_guess}' (ID: {force_tmdb_id})."
        
        if tmdb_movie_data: 
            try:
                final_meta.source_api = "tmdb"
                final_meta.match_confidence = tmdb_score 
                title_val = getattr(tmdb_movie_data, 'title', None) if isinstance(tmdb_movie_data, AsObj) else tmdb_movie_data.get('title')
                release_date_val = getattr(tmdb_movie_data, 'release_date', None) if isinstance(tmdb_movie_data, AsObj) else tmdb_movie_data.get('release_date')
                final_meta.movie_title = str(title_val) if title_val else None
                final_meta.release_date = str(release_date_val) if release_date_val else None
                final_meta.movie_year = self._get_year_from_date(final_meta.release_date) 
                if isinstance(tmdb_ids, dict):
                     final_meta.ids = tmdb_ids
                     final_meta.collection_name = str(tmdb_ids.get('collection_name')) if tmdb_ids.get('collection_name') else None
                     final_meta.collection_id = int(tmdb_ids['collection_id']) if tmdb_ids.get('collection_id') is not None else None
                else: final_meta.ids = {}
                log.debug(f"Successfully populated final_meta from TMDB for '{movie_title_guess}'. Score: {final_meta.match_confidence}")
            except Exception as e_populate:
                log.error(f"Error populating final_meta for '{movie_title_guess}' from TMDB data: {e_populate}", exc_info=True);
                final_meta.source_api = None 
                fetch_error_message = fetch_error_message or f"Error processing TMDB data: {type(e_populate).__name__}"

        if not final_meta.source_api: 
             log.warning(f"Metadata fetch or population ultimately failed for movie: '{movie_title_guess}' (Year guess: {year_guess}, ID: {force_tmdb_id})")
             if not final_meta.movie_title: final_meta.movie_title = movie_title_guess 
             if not final_meta.movie_year: final_meta.movie_year = year_guess
             final_error = fetch_error_message or f"Failed to obtain valid metadata for movie '{movie_title_guess}'."
             raise MetadataError(final_error)

        log.debug(f"fetch_movie_metadata returning for '{movie_title_guess}': Source='{final_meta.source_api}', Title='{final_meta.movie_title}', Year={final_meta.movie_year}, Score={final_meta.match_confidence}")
        return final_meta

    async def fetch_series_metadata(self, show_title_guess: str, season_num: int, episode_num_list: Tuple[int, ...], year_guess: Optional[int] = None, force_tmdb_id: Optional[int] = None, force_tvdb_id: Optional[int] = None) -> MediaMetadata:
        # ... (This method needs similar robust handling of the specific MetadataErrors from its _do_fetch... calls)
        log.debug(f"Fetching series metadata (async) for: '{show_title_guess}' S{season_num}E{episode_num_list} (Year: {year_guess}, Force TMDB ID: {force_tmdb_id}, Force TVDB ID: {force_tvdb_id})")
        final_meta = MediaMetadata(is_series=True)
        lang = str(self.cfg('tmdb_language', 'en'))
        episode_num_tuple = tuple(sorted(list(set(episode_num_list))))
        
        source_preference: List[str] = self.cfg.get_list('series_metadata_preference', ['tmdb', 'tvdb'])
        source_queue: Deque[str] = deque()

        if force_tmdb_id: source_queue.append('tmdb')
        elif force_tvdb_id: source_queue.append('tvdb')
        else: source_queue.extend(source_preference)
            
        results_by_source: Dict[str, Dict[str, Any]] = {
            'tmdb': {'data': None, 'ep_map': None, 'ids': None, 'score': None, 'error': None},
            'tvdb': {'data': None, 'ep_map': None, 'ids': None, 'score': None, 'error': None}
        }
        primary_source: Optional[str] = None; merged_ids: Dict[str, Any] = {}; last_error_message_from_sources: Optional[str] = None

        while source_queue:
            source = source_queue.popleft()
            log.debug(f"Attempting fetch from source: {source.upper()}")
            
            current_force_id_val = None
            if source == 'tmdb' and force_tmdb_id: current_force_id_val = force_tmdb_id
            elif source == 'tvdb' and force_tvdb_id: current_force_id_val = force_tvdb_id

            cache_key_base = f"series::{lang}::S{season_num}E{episode_num_tuple}"
            if current_force_id_val: cache_key = f"{cache_key_base}::{source}_id_{current_force_id_val}"
            else: cache_key = f"{cache_key_base}::{show_title_guess}_{year_guess}::{source}"
            
            source_data, source_ep_map, source_ids, source_score = None, None, None, None
            source_error: Optional[str] = None

            cached_data = await self._get_cache(cache_key) # Expects 4 items (data, ep_map, ids, score) from _sync_... via _do_fetch...
            if cached_data and len(cached_data) == 4: # Simplified cache structure check for series
                source_data, source_ep_map, source_ids, source_score = cached_data
                if current_force_id_val and source_score != DIRECT_ID_MATCH_SCORE: source_score = DIRECT_ID_MATCH_SCORE
                log.debug(f"Using cached {source.upper()} data for series: '{show_title_guess}' (ID: {current_force_id_val}, Score: {source_score})")
            else:
                if cached_data: log.warning(f"Invalid series cache structure for {cache_key}, re-fetching.")
                try:
                    await self.rate_limiter.wait()
                    if source == 'tmdb' and self.tmdb:
                        source_data, source_ep_map, source_ids, source_score = await self._do_fetch_tmdb_series(
                            show_title_guess, season_num, episode_num_tuple, year_guess, lang, force_tmdb_id_arg=current_force_id_val
                        )
                    elif source == 'tvdb' and self.tvdb:
                         tvdb_id_from_tmdb_source = results_by_source.get('tmdb', {}).get('ids', {}).get('tvdb_id') if not current_force_id_val else None
                         effective_tvdb_id_arg = current_force_id_val if current_force_id_val else tvdb_id_from_tmdb_source
                         source_data, source_ep_map, source_ids, source_score = await self._do_fetch_tvdb_series(
                             title_arg=show_title_guess, season_num_arg=season_num, episodes_arg=episode_num_tuple,
                             tvdb_id_arg=effective_tvdb_id_arg, 
                             year_guess_arg=year_guess, lang=lang,
                             force_tvdb_id_arg=current_force_id_val
                         )
                    else: source_error = f"{source.upper()} client not available."
                    
                    if source_data is not None: # Cache only if data was fetched
                        await self._set_cache(cache_key, (source_data, source_ep_map or {}, source_ids or {}, source_score)) # Cache 4 items
                except MetadataError as me_fetch: source_error = str(me_fetch)
                except Exception as e_fetch_unexp: source_error = f"Unexpected {source.upper()} error: {type(e_fetch_unexp).__name__}"
            
            results_by_source[source]['data'] = source_data; results_by_source[source]['ep_map'] = source_ep_map or {}
            results_by_source[source]['ids'] = source_ids or {}; results_by_source[source]['score'] = source_score
            results_by_source[source]['error'] = source_error
            if source_error: last_error_message_from_sources = source_error

            source_has_show_data = results_by_source[source]['data'] is not None
            source_has_all_episodes = not episode_num_tuple or all(ep_num in (results_by_source[source]['ep_map'] or {}) for ep_num in episode_num_tuple)
            
            if source_has_show_data and source_has_all_episodes:
                primary_source = source; break 
            elif source_has_show_data: log.warning(f"{source.upper()} found show data but missing episodes. Check next (unless ID forced).")
            else: log.info(f"{source.upper()} did not find show data. Error: {source_error or 'N/A'}. Check next (unless ID forced).")
            if current_force_id_val: break 

        if not primary_source and not (force_tmdb_id or force_tvdb_id) : 
            for source_pref in source_preference: 
                 if results_by_source[source_pref]['data'] is not None: 
                     primary_source = source_pref; break
        
        if primary_source: 
            primary_ids_dict = results_by_source[primary_source].get('ids') or {}; merged_ids.update(primary_ids_dict)
            for other_src in source_preference:
                if other_src != primary_source and results_by_source[other_src]['ids']:
                    for k, v_other in results_by_source[other_src]['ids'].items():
                        if v_other is not None and merged_ids.get(k) is None: merged_ids[k] = v_other
            
            final_meta.source_api = primary_source
            primary_show_data_obj = results_by_source[primary_source]['data']
            primary_ep_map_dict = results_by_source[primary_source]['ep_map']
            final_meta.match_confidence = results_by_source[primary_source]['score']
            final_meta.ids = merged_ids
            try:
                show_title_api_val = getattr(primary_show_data_obj, 'name', None) if isinstance(primary_show_data_obj, AsObj) else primary_show_data_obj.get('name')
                final_meta.show_title = str(show_title_api_val) if show_title_api_val else None
                show_air_date_val: Optional[str] = None
                if isinstance(primary_show_data_obj, AsObj): show_air_date_val = getattr(primary_show_data_obj, 'first_air_date', None)
                elif isinstance(primary_show_data_obj, dict): show_air_date_val = primary_show_data_obj.get('firstAired') or primary_show_data_obj.get('first_air_date')
                
                final_meta.show_year = self._get_year_from_date(show_air_date_val)
                final_meta.season = season_num; final_meta.episode_list = list(episode_num_tuple)

                if episode_num_tuple and primary_ep_map_dict:
                    for ep_num_val in episode_num_tuple:
                        ep_details_obj = primary_ep_map_dict.get(ep_num_val)
                        if ep_details_obj:
                            ep_title_val: Optional[str] = None; air_date_val: Optional[str] = None
                            if isinstance(ep_details_obj, AsObj): 
                                ep_title_val = getattr(ep_details_obj, 'name', None); air_date_val = getattr(ep_details_obj, 'air_date', None)
                            elif isinstance(ep_details_obj, dict): 
                                ep_title_val = ep_details_obj.get('name') or ep_details_obj.get('episodeName'); air_date_val = ep_details_obj.get('aired') or ep_details_obj.get('airDate') 
                            if ep_title_val: final_meta.episode_titles[ep_num_val] = str(ep_title_val)
                            if air_date_val: final_meta.air_dates[ep_num_val] = str(air_date_val)
                        else: log.debug(f"Episode S{season_num}E{ep_num_val} not found in primary source map ({primary_source}).")
            except Exception as e_populate_series:
                 log.error(f"Error populating final_meta for series '{show_title_guess}' from {primary_source}: {e_populate_series}", exc_info=True);
                 final_meta.source_api = None 
                 last_error_message_from_sources = last_error_message_from_sources or f"Error processing {primary_source} data: {type(e_populate_series).__name__}"
        else: 
            log.warning(f"Could not determine a primary metadata source for series: '{show_title_guess}' S{season_num}E{episode_num_tuple}.")
            if not last_error_message_from_sources: 
                error_messages = [f"{s_name.upper()} Error: {results_by_source[s_name]['error']}" for s_name in source_preference if results_by_source[s_name]['error']]
                last_error_message_from_sources = " | ".join(error_messages) if error_messages else "Metadata fetch failed from all sources (no specific errors, but no data found)."

        if not final_meta.source_api: 
             log.warning(f"Metadata fetch/population ultimately failed for series: '{show_title_guess}'. Reason: {last_error_message_from_sources or 'Unknown reason'}")
             final_meta.show_title = final_meta.show_title or show_title_guess
             final_meta.show_year = final_meta.show_year or year_guess
             final_error_msg = last_error_message_from_sources or "Unknown metadata fetch error"
             if force_tmdb_id and f"Provided TMDB Series ID '{force_tmdb_id}' was not found" in final_error_msg: pass # Already specific
             elif force_tvdb_id and f"Provided TVDB Series ID '{force_tvdb_id}' was not found" in final_error_msg: pass # Already specific
             raise MetadataError(final_error_msg)
        elif not final_meta.show_title: final_meta.show_title = show_title_guess
        
        log.debug(f"fetch_series_metadata returning for '{show_title_guess}': Source='{final_meta.source_api}', Title='{final_meta.show_title}', Score={final_meta.match_confidence}")
        return final_meta

    async def search_tmdb_movies_interactive(self, title_query: str, limit: int = 5) -> List[Dict[str, Any]]:
        # ... (Unchanged from previous full version)
        if not self.tmdb or not TMDBV3API_AVAILABLE or not Movie:
            log.error("TMDB client/Movie class not available for interactive search.")
            return []
        log.debug(f"Interactive TMDB movie search for: '{title_query}'")
        search_api = Movie()
        try:
            await self.rate_limiter.wait()
            results_iterable = await self._run_sync(search_api.search, title_query)
            
            formatted_results: List[Dict[str, Any]] = []
            if results_iterable:
                for item in results_iterable:
                    if len(formatted_results) >= limit: break
                    if isinstance(item, (dict, AsObj)):
                        item_id = getattr(item, 'id', None) if isinstance(item, AsObj) else item.get('id')
                        item_title = getattr(item, 'title', None) if isinstance(item, AsObj) else item.get('title')
                        item_date = getattr(item, 'release_date', None) if isinstance(item, AsObj) else item.get('release_date')
                        item_year = self._get_year_from_date(str(item_date)) if item_date else None
                        if item_id and item_title:
                            display_text = f"{item_title} ({item_year})" if item_year else str(item_title)
                            formatted_results.append({'id': item_id, 'text': display_text, 'title': item_title, 'year': item_year})
            return formatted_results
        except TMDbException as e_search:
            log.error(f"TMDbException during interactive TMDB movie search for '{title_query}': {e_search}", exc_info=True)
            self.console.print(f"[red]Error searching TMDB movies: {e_search}[/red]", file=sys.stderr)
            return []
        except Exception as e:
            log.error(f"Error during interactive TMDB movie search for '{title_query}': {e}", exc_info=True)
            self.console.print(f"[red]Unexpected error searching TMDB movies: {e}[/red]", file=sys.stderr)
            return []

    async def search_tvdb_series_interactive(self, title_query: str, limit: int = 5) -> List[Dict[str, Any]]:
        if not self.tvdb:
            log.error("TVDB client not available for interactive search.")
            return []
        log.debug(f"Interactive TVDB series search for: '{title_query}'")
        try:
            await self.rate_limiter.wait()
            search_results_list: Optional[List[Dict]] = await self._run_sync(self.tvdb.search, title_query) 
            
            formatted_results: List[Dict[str, Any]] = []
            if search_results_list:
                for item_dict in search_results_list:
                    if len(formatted_results) >= limit: break
                    if isinstance(item_dict, dict):
                        item_id_str = item_dict.get('tvdb_id') 
                        item_name = item_dict.get('name')
                        item_year_str = item_dict.get('year') 
                        item_id = None
                        if item_id_str:
                            try: item_id = int(item_id_str)
                            except ValueError: log.warning(f"Could not convert TVDB ID '{item_id_str}' to int.")
                        
                        if item_id and item_name:
                            display_text = f"{item_name} ({item_year_str})" if item_year_str else str(item_name)
                            formatted_results.append({'id': item_id, 'text': display_text, 'title': item_name, 'year': item_year_str}) 
            return formatted_results
        except Exception as e: 
            log.error(f"Error during interactive TVDB series search for '{title_query}': {e}", exc_info=True)
            self.console.print(f"[red]Error searching TVDB series: {e}[/red]", file=sys.stderr)
            return []

    async def search_tmdb_series_interactive(self, title_query: str, limit: int = 5) -> List[Dict[str, Any]]:
        if not self.tmdb or not TMDBV3API_AVAILABLE or not TV:
            log.error("TMDB client/TV class not available for interactive series search.")
            return []
        log.debug(f"Interactive TMDB series search for: '{title_query}'")
        search_api = TV()
        try:
            await self.rate_limiter.wait()
            results_iterable = await self._run_sync(search_api.search, title_query)
            
            formatted_results: List[Dict[str, Any]] = []
            if results_iterable:
                for item in results_iterable:
                    if len(formatted_results) >= limit: break
                    if isinstance(item, (dict, AsObj)):
                        item_id = getattr(item, 'id', None) if isinstance(item, AsObj) else item.get('id')
                        item_name = getattr(item, 'name', None) if isinstance(item, AsObj) else item.get('name')
                        item_date = getattr(item, 'first_air_date', None) if isinstance(item, AsObj) else item.get('first_air_date')
                        item_year = self._get_year_from_date(str(item_date)) if item_date else None
                        if item_id and item_name:
                            display_text = f"{item_name} ({item_year})" if item_year else str(item_name)
                            formatted_results.append({'id': item_id, 'text': display_text, 'title': item_name, 'year': item_year})
            return formatted_results
        except TMDbException as e_search:
            log.error(f"TMDbException during interactive TMDB series search for '{title_query}': {e_search}", exc_info=True)
            self.console.print(f"[red]Error searching TMDB series: {e_search}[/red]", file=sys.stderr)
            return []
        except Exception as e:
            log.error(f"Error during interactive TMDB series search for '{title_query}': {e}", exc_info=True)
            self.console.print(f"[red]Unexpected error searching TMDB series: {e}[/red]", file=sys.stderr)
            return []