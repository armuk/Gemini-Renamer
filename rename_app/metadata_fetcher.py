# --- START OF FILE metadata_fetcher.py ---

# rename_app/metadata_fetcher.py (Asynchronous Version)

import logging
import time
import asyncio # Import asyncio
from functools import wraps # For potential future decorators
from pathlib import Path
from typing import Optional, Tuple, TYPE_CHECKING, Any, Iterable, Sequence, Dict
from .api_clients import get_tmdb_client, get_tvdb_client
from .exceptions import MetadataError
from .models import MediaMetadata

log = logging.getLogger(__name__) # Define logger early

# --- Imports (DiskCache, PlatformDirs, thefuzz, tenacity, dateutil) ---
# Tenacity itself is not strictly needed for manual retry, but keep RetryError
try: import diskcache; DISKCACHE_AVAILABLE = True
except ImportError: DISKCACHE_AVAILABLE = False
try: import platformdirs; PLATFORMDIRS_AVAILABLE = True
except ImportError: PLATFORMDIRS_AVAILABLE = False
try: from thefuzz import process as fuzz_process; THEFUZZ_AVAILABLE = True
except ImportError: THEFUZZ_AVAILABLE = False
# Keep RetryError for potential future use or context, though not used in manual loop
try: from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, RetryError, AsyncRetrying; TENACITY_AVAILABLE = True
except ImportError: TENACITY_AVAILABLE = False; RetryError = Exception; AsyncRetrying = None
try: import dateutil.parser; DATEUTIL_AVAILABLE = True
except ImportError: DATEUTIL_AVAILABLE = False

# Import requests exceptions for retry logic
try:
    import requests.exceptions as req_exceptions
except ImportError:
    class req_exceptions: # Define fallback class
        ConnectionError = type('ConnectionError', (IOError,), {})
        Timeout = type('Timeout', (IOError,), {})
        RequestException = type('RequestException', (IOError,), {})
        HTTPError = type('HTTPError', (RequestException,), {'response': type('MockResponse', (), {'status_code': 0})()})

# Import TMDB exceptions if they exist and are useful for status codes
try:
    from tmdbv3api.exceptions import TMDbException
except ImportError:
    TMDbException = type('TMDbException', (Exception,), {}) # Fallback

# --- START TVDB Exception Handling ---
_TvdbBaseException = type('TvdbBaseException', (Exception,), {})
TvdbNotFoundError = type('TvdbNotFoundError', (_TvdbBaseException,), {})
TvdbApiException = type('TvdbApiException', (_TvdbBaseException,), {})
try:
    # --- FIX 1: Correct TVDB Exception Import ---
    from tvdb_v4_official import NotFoundError as TvdbNotFoundError, TvdbApiException
    log.debug("Successfully imported real TVDB API exceptions.")
except ImportError:
    # This block is now safe because 'log' is defined above
    log.warning("Could not import exceptions from 'tvdb-v4-official'. Using fallback exception types.")
    # Fallback types are already defined above, so just pass
    pass
# --- END TVDB Exception Handling ---

if TYPE_CHECKING: # For static analysis / linters
    try:
        # --- FIX 1: Correct TVDB Exception Import ---
        from tvdb_v4_official import NotFoundError as TvdbNotFoundError, TvdbApiException
    except ImportError:
        pass

# Import AsObj explicitly for type checking
try: from tmdbv3api.as_obj import AsObj
except ImportError: AsObj = None # Fallback

# --- Async Rate Limiter ---
class AsyncRateLimiter:
    """Simple asyncio rate limiter."""
    def __init__(self, delay: float):
        self.delay = delay
        self.last_call = 0
        self._lock = asyncio.Lock() # Lock for managing last_call update

    async def wait(self):
        if self.delay <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            since_last = now - self.last_call
            if since_last < self.delay:
                wait_time = self.delay - since_last
                log.debug(f"Rate limiting: sleeping for {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
            self.last_call = time.monotonic() # Update after potential sleep


# --- Enhanced Retry Logic Predicate (remains mostly the same) ---
def should_retry_api_error(exception):
    """Predicate for tenacity retry decorator (sync or async)."""
    # Connection-related errors are always retryable
    if isinstance(exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)):
        log.debug(f"Retry triggered for Connection/Timeout Error: {type(exception).__name__}")
        return True

    # Check for HTTP errors specifically
    if isinstance(exception, req_exceptions.HTTPError):
        status_code = getattr(getattr(exception, 'response', None), 'status_code', 0)
        if status_code == 429:
            log.warning(f"Retry triggered for HTTP 429 Too Many Requests.")
            return True
        if 500 <= status_code <= 599:
            log.warning(f"Retry triggered for HTTP {status_code} Server Error.")
            return True
        log.debug(f"Not retrying for HTTP Status Code: {status_code}")
        return False

    # Check for TVDB specific API exceptions
    if isinstance(exception, TvdbApiException):
        status_code = getattr(exception, 'status_code', None)
        if status_code == 429:
             log.warning("Retry triggered for TVDB API 429.")
             return True
        if status_code and 500 <= status_code <= 599:
            log.warning(f"Retry triggered for TVDB API Server Error {status_code}.")
            return True
        if isinstance(exception, TvdbNotFoundError):
            log.debug("Not retrying for TvdbNotFoundError.")
            return False
        log.debug(f"Not retrying for TVDB API Exception: {exception} (Status: {status_code})")
        return False

    # Check for TMDB specific exceptions (less likely needed if requests covers it)
    if isinstance(exception, TMDbException):
        log.debug(f"Not retrying for TMDbException: {exception}")
        return False

    # Don't retry unknown exceptions wrapped by the executor unless explicitly needed
    # Important: tenacity's default retry_if_exception doesn't cover exceptions
    # wrapped by executors directly. We check specific types known to be potentially
    # raised by the underlying *synchronous* libraries.
    log.debug(f"Not retrying for generic exception type: {type(exception).__name__}")
    return False

# --- FIX: Make setup_async_retry_decorator a FACTORY ---
# def setup_async_retry_decorator():
#     def decorator(func):
#         if not TENACITY_AVAILABLE or not AsyncRetrying:
#             log.debug(f"Tenacity (or async support) not installed, retry disabled for {func.__name__}.")
#             return func
#         @wraps(func)
#         async def wrapper(*args, **kwargs):
#             if not args:
#                 log.error(f"Cannot apply retry to {func.__name__}: missing 'self' argument.")
#                 return await func(*args, **kwargs)
#             instance = args[0]
#             cfg_helper = getattr(instance, 'cfg', None)
#             if not cfg_helper:
#                  log.error(f"Cannot apply retry to {func.__name__}: 'self.cfg' not found on instance.")
#                  return await func(*args, **kwargs) # Execute without retry if config missing

#             attempts_cfg = cfg_helper('api_retry_attempts', 3)
#             wait_sec_cfg = cfg_helper('api_retry_wait_seconds', 2)
#             attempts = max(1, attempts_cfg if attempts_cfg is not None else 3)
#             wait_sec = max(0, wait_sec_cfg if wait_sec_cfg is not None else 2)

#             log.debug(f"Applying retry to {func.__name__}: Attempts={attempts}, Wait={wait_sec}s")

#             retryer = AsyncRetrying(
#                 stop=stop_after_attempt(attempts),
#                 wait=wait_fixed(wait_sec),
#                 retry=retry_if_exception_type(( # List specific exceptions known from sync libs
#                      req_exceptions.ConnectionError, req_exceptions.Timeout,
#                      req_exceptions.HTTPError, TvdbApiException, TMDbException,
#                 )),
#                 retry_error_callback=lambda retry_state: should_retry_api_error(retry_state.outcome.exception()),
#                 reraise=True
#             )
#             return await retryer(func, *args, **kwargs)
#         return wrapper
#     return decorator

# --- Fuzzy Matching ---
def find_best_match(title_to_find, api_results_tuple, result_key='title', id_key='id', score_cutoff=70):
    """
    Finds the best match for a title within a tuple of API result dictionaries using fuzzy matching.

    Args:
        title_to_find: The string title to search for.
        api_results_tuple: A tuple containing dictionaries, where each dictionary
                           represents an API result (e.g., a movie or series).
        result_key: The key in the result dictionaries that holds the title string.
        id_key: The key in the result dictionaries that holds the unique identifier.
        score_cutoff: The minimum score (0-100) for a fuzzy match to be considered valid.

    Returns:
        The dictionary from api_results_tuple that is the best match, or None if no
        suitable match is found above the score_cutoff or if an error occurs.
    """
    # Check the input tuple FIRST
    if not api_results_tuple:
        log.debug(f"Fuzzy match input 'api_results_tuple' is empty.")
        return None

    # Check the type of the original input tuple (important!)
    if not isinstance(api_results_tuple, tuple):
         log.debug(f"Fuzzy match input 'api_results_tuple' is not a tuple: {type(api_results_tuple)}")
         # Decide how to handle this - maybe try converting or just return None
         # For safety, let's return None if not a tuple
         return None

    # --- Assign api_results AFTER the checks ---
    api_results = api_results_tuple # Now api_results is guaranteed to be a non-empty tuple

    if not THEFUZZ_AVAILABLE:
        log.debug("thefuzz library not available, returning first result.")
        # Use assigned api_results here. Since tuple is not empty, api_results[0] is safe.
        return api_results[0]

    # Rest of the function uses api_results (which is now a non-empty tuple)
    choices = {}
    log.debug(f"Attempting to build choices for fuzzy match '{title_to_find}'. Input assumed dicts.")
    try:
        for r in api_results: # Iterate over the tuple
            # Safely check if 'r' is a dictionary before accessing keys
            if not isinstance(r, dict):
                log.debug(f"  -> Skipped non-dict item during choice building: {r}")
                continue
            current_id = r.get(id_key)
            current_result = r.get(result_key)
            # Ensure both ID and the result string exist before adding
            if current_id is not None and current_result is not None:
                 choices[current_id] = str(current_result) # Store ID -> Title string
                 log.debug(f"  -> Added choice: ID={current_id}, Value='{str(current_result)}'")
            else:
                 log.debug(f"  -> Skipped item (missing ID '{id_key}' or Result '{result_key}'): {r}")
    except Exception as e_choices:
        log.error(f"Error creating choices dict for fuzzy matching '{title_to_find}': {e_choices}", exc_info=True)
        return None # Return None if choice building fails

    if not choices:
        log.debug(f"No valid choices found for fuzzy matching '{title_to_find}'.")
        return None

    log.debug(f"Fuzzy matching choices for '{title_to_find}': {choices}")
    best = None
    try:
        # Ensure title_to_find is a string
        if not isinstance(title_to_find, str):
            title_to_find = str(title_to_find)

        # Ensure values in choices are strings for fuzzy matching
        processed_choices = {k: str(v) for k, v in choices.items()}

        # Use thefuzz process.extractOne
        best = fuzz_process.extractOne(title_to_find, processed_choices, score_cutoff=score_cutoff)
    except Exception as e_fuzz:
        log.error(f"Error during fuzz_process.extractOne for '{title_to_find}': {e_fuzz}", exc_info=True)
        return None # Return None if fuzzy matching itself fails

    if best:
        # extractOne returns (value, score, key) -> (matched_title_str, score, best_id)
        matched_value, score, best_id = best
        log.debug(f"Fuzzy match '{title_to_find}': '{matched_value}' (ID:{best_id}) score {score}")

        # Iterate over api_results (the original tuple) again to find the full dict
        for r_dict in api_results:
            # Ensure item is a dict and ID matches (converting to string for comparison robustness)
            if isinstance(r_dict, dict) and str(r_dict.get(id_key)) == str(best_id):
                log.debug(f"Returning matched dict: {r_dict}")
                return r_dict # Return the original dictionary object

        # This part should ideally not be reached if best_id came from choices derived from api_results
        log.error(f"Fuzzy match found ID {best_id} but couldn't find corresponding dict in original results.")
        return None # Return None if the matched dict isn't found back in the tuple (unexpected)
    else:
        # No match found above the cutoff score
        log.warning(f"Fuzzy match failed for '{title_to_find}' (cutoff {score_cutoff})")
        return None

    # This part should not be reachable with the return statements above.
    # log.error(f"Reached end of find_best_match unexpectedly for '{title_to_find}'");
    # return None

# --- External ID Helper (remains synchronous) ---
def get_external_ids(tmdb_obj=None, tvdb_obj=None):
    """
    Extracts external IDs (IMDb, TMDB, TVDB) and TMDB collection info.
    """
    # (Function unchanged)
    ids = {'imdb_id': None, 'tmdb_id': None, 'tvdb_id': None}
    try: # TMDB
        if tmdb_obj:
            tmdb_id_val = getattr(tmdb_obj, 'id', None);
            if tmdb_id_val is None and isinstance(tmdb_obj, dict): tmdb_id_val = tmdb_obj.get('id')
            if tmdb_id_val is not None: ids['tmdb_id'] = tmdb_id_val
            ext_ids_data = {};
            if isinstance(tmdb_obj, dict): ext_ids_data = tmdb_obj.get('external_ids', {})
            else:
                ext_ids_attr = getattr(tmdb_obj, 'external_ids', None)
                if isinstance(ext_ids_attr, dict): ext_ids_data = ext_ids_attr
                elif callable(ext_ids_attr):
                    try: ext_ids_data = ext_ids_attr()
                    except Exception as e_call: log.debug(f"Error calling external_ids method on TMDB object: {e_call}")
            imdb_id_found = ext_ids_data.get('imdb_id'); tvdb_id_found = ext_ids_data.get('tvdb_id')
            if imdb_id_found: ids['imdb_id'] = str(imdb_id_found)
            if tvdb_id_found and ids.get('tvdb_id') is None:
                try: ids['tvdb_id'] = int(tvdb_id_found)
                except (ValueError, TypeError): log.warning(f"Could not convert TMDB-provided TVDB ID '{tvdb_id_found}' to int.")

            # --- NEW: Extract Collection Info ---
            collection_info = None
            # Check if it's an attribute (common for AsObj from details)
            collection_attr = getattr(tmdb_obj, 'belongs_to_collection', None)
            if isinstance(collection_attr, (dict, AsObj)): # Handle AsObj or dict directly
                collection_info = collection_attr
            elif isinstance(tmdb_obj, dict): # Check if it's in the base dict
                collection_info = tmdb_obj.get('belongs_to_collection')

            if collection_info:
                col_id = getattr(collection_info, 'id', None) if not isinstance(collection_info, dict) else collection_info.get('id')
                col_name = getattr(collection_info, 'name', None) if not isinstance(collection_info, dict) else collection_info.get('name')
                if col_id:
                    try: ids['collection_id'] = int(col_id)
                    except (ValueError, TypeError): log.warning(f"Could not convert collection ID '{col_id}' to int.")
                if col_name: ids['collection_name'] = str(col_name)
            # --- END NEW ---

    except AttributeError as e_tmdb: log.debug(f"AttributeError parsing TMDB external IDs: {e_tmdb}")
    except Exception as e_tmdb_other: log.warning(f"Unexpected error parsing TMDB external IDs: {e_tmdb_other}", exc_info=True)

    # --- TVDB Processing (Unchanged regarding Collection) ---    
    try: # TVDB
        if isinstance(tvdb_obj, dict):
             if ids.get('tvdb_id') is None:
                 tvdb_id_val = tvdb_obj.get('id');
                 if tvdb_id_val is not None:
                    try: ids['tvdb_id'] = int(tvdb_id_val)
                    except (ValueError, TypeError): log.warning(f"Could not convert TVDB-provided TVDB ID '{tvdb_id_val}' to int.")
             remote_ids = tvdb_obj.get('remoteIds', tvdb_obj.get('remote_ids', [])); imdb_found_in_remote = False
             if remote_ids and isinstance(remote_ids, list):
                 for remote in remote_ids:
                     if isinstance(remote, dict) and remote.get('sourceName') == 'IMDB':
                          imdb_id_found = remote.get('id');
                          if imdb_id_found:
                               if ids.get('imdb_id') is None: ids['imdb_id'] = str(imdb_id_found); imdb_found_in_remote = True; break
             if not imdb_found_in_remote and ids.get('imdb_id') is None:
                 imdb_id_found = tvdb_obj.get('imdbId') or tvdb_obj.get('imdb_id')
                 if imdb_id_found: ids['imdb_id'] = str(imdb_id_found)
             if ids.get('tmdb_id') is None:
                 tmdb_id_found = tvdb_obj.get('tmdbId') or tvdb_obj.get('tmdb_id')
                 if tmdb_id_found:
                     try: ids['tmdb_id'] = int(tmdb_id_found)
                     except(ValueError, TypeError): log.warning(f"Could not convert TVDB-provided TMDB ID '{tmdb_id_found}' to int.")
    except Exception as e_tvdb_ids: log.warning(f"Error parsing external IDs from TVDB object: {e_tvdb_ids}", exc_info=True)
    return {k: v for k, v in ids.items() if v is not None}

# --- TMDB AsObj to Dict Helper (remains synchronous) ---
def _tmdb_results_to_dicts(results_iterable: Optional[Iterable[Any]], result_type: str = 'movie') -> Tuple[Dict[str, Any], ...]:
    # (Function unchanged)
    if not results_iterable: return tuple()
    dict_list = []
    try:
        for item in results_iterable:
            if not item: continue; item_dict = {}
            try:
                item_dict['id'] = getattr(item, 'id', None)
                if result_type == 'movie': item_dict['title'] = getattr(item, 'title', None); item_dict['release_date'] = getattr(item, 'release_date', None)
                elif result_type == 'series': item_dict['name'] = getattr(item, 'name', None); item_dict['first_air_date'] = getattr(item, 'first_air_date', None)
                if item_dict.get('id') is not None and (item_dict.get('title') is not None or item_dict.get('name') is not None): dict_list.append(item_dict)
                else: log.debug(f"Skipping TMDB result due to missing id or title/name: {getattr(item, 'id', 'N/A')}")
            except AttributeError as e_attr: log.warning(f"AttributeError converting TMDB result item to dict: {e_attr}. Item: {item}")
            except Exception as e_conv: log.error(f"Unexpected error converting TMDB result item to dict: {e_conv}. Item: {item}", exc_info=True)
    except TypeError: log.warning(f"Cannot iterate over TMDB results object (type {type(results_iterable)}) for dict conversion.")
    except Exception as e_iter: log.error(f"Error iterating TMDB results during dict conversion: {e_iter}", exc_info=True)
    log.debug(f"Converted {len(dict_list)} TMDB {result_type} results to dicts for matching.")
    return tuple(dict_list)
    pass


# --- Metadata Fetcher Class ---
class MetadataFetcher:
    def __init__(self, cfg_helper):
        self.cfg = cfg_helper
        self.tmdb = get_tmdb_client()
        self.tvdb = get_tvdb_client()
        self.rate_limiter = AsyncRateLimiter(self.cfg('api_rate_limit_delay', 0.5))
        self.year_tolerance = self.cfg('api_year_tolerance', 1)
        self.tmdb_strategy = self.cfg('tmdb_match_strategy', 'first')
        self.tmdb_fuzzy_cutoff = self.cfg('tmdb_match_fuzzy_cutoff', 70)
        log.debug(f"Fetcher Config: Year Tolerance={self.year_tolerance}, TMDB Strategy='{self.tmdb_strategy}', TMDB Fuzzy Cutoff={self.tmdb_fuzzy_cutoff}")
        self.cache = None
        self.cache_enabled = self.cfg('cache_enabled', True)
        self.cache_expire = self.cfg('cache_expire_seconds', 60 * 60 * 24 * 7)
        # --- Cache setup logic (unchanged) ---
        if self.cache_enabled:
             if DISKCACHE_AVAILABLE:
                 cache_dir_config = self.cfg('cache_directory', None); cache_dir_path = None
                 if cache_dir_config: cache_dir_path = Path(cache_dir_config).resolve()
                 elif PLATFORMDIRS_AVAILABLE: cache_dir_path = Path(platformdirs.user_cache_dir("rename_app", "rename_app_author"))
                 else: cache_dir_path = Path(__file__).parent.parent / ".rename_cache"; log.warning(f"'platformdirs' not found. Using fallback cache directory: {cache_dir_path}")
                 if cache_dir_path:
                     try: cache_dir_path.mkdir(parents=True, exist_ok=True); self.cache = diskcache.Cache(str(cache_dir_path)); log.info(f"Persistent cache initialized at: {cache_dir_path} (Expiration: {self.cache_expire}s)")
                     except Exception as e: log.error(f"Failed to initialize disk cache at '{cache_dir_path}': {e}. Disabling cache."); self.cache = None; self.cache_enabled = False
                 else: log.error("Could not determine a valid cache directory. Persistent caching disabled."); self.cache_enabled = False
             else: log.warning("Persistent caching enabled, but 'diskcache' library not found. Caching disabled."); self.cache_enabled = False
        else: log.info("Persistent caching disabled by configuration.")
        # --- Decorator application REMOVED from __init__ ---

    def _get_year_from_date(self, date_str):
        # (Function unchanged)
        if not date_str or not DATEUTIL_AVAILABLE: return None
        try: return dateutil.parser.parse(date_str).year
        except (ValueError, TypeError): return None
        pass

    # --- Helper to run sync code in executor ---
    async def _run_sync(self, func, *args, **kwargs):
        """Runs a synchronous function in the default executor."""
        loop = asyncio.get_running_loop()
        # Use functools.partial to package the function and its arguments
        # This is generally safer than passing args directly to run_in_executor
        from functools import partial
        func_call = partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, func_call)
        pass

    # --- Cache Helper Methods ---
    async def _get_cache(self, key):
        """Async helper to get from diskcache."""
        if not self.cache_enabled or not self.cache:
            return None
        # diskcache uses 'None' as a standard default, but let's use a unique object
        # to differentiate between a cached None and a cache miss.
        _cache_miss = object()
        try:
            # Wrap the synchronous cache call
            # loop = asyncio.get_running_loop() # Can get loop inside _run_sync
            cached_value = await self._run_sync(self.cache.get, key, default=_cache_miss)
            if cached_value is not _cache_miss:
                log.debug(f"Cache HIT for key: {key}")
                return cached_value
            else:
                log.debug(f"Cache MISS for key: {key}")
                return None
        except Exception as e:
            log.warning(f"Error getting from cache key '{key}': {e}", exc_info=True)
            return None # Treat cache error as a miss

    async def _set_cache(self, key, value):
        """Async helper to set value in diskcache."""
        if not self.cache_enabled or not self.cache:
            return
        try:
            # Wrap the synchronous cache call
            # loop = asyncio.get_running_loop() # Can get loop inside _run_sync
            await self._run_sync(self.cache.set, key, value, expire=self.cache_expire)
            log.debug(f"Cache SET for key: {key}")
        except Exception as e:
            log.warning(f"Error setting cache key '{key}': {e}", exc_info=True)


    # --- Core Fetching Logic (Now Async) ---

    # Make the main fetch methods async
    async def fetch_series_metadata(self, show_title_guess, season_num, episode_num_list, year_guess=None):
        # (Orchestration logic unchanged - calls await _do_fetch... and handles cache/fallback)
        log.debug(f"Fetching series metadata (async) for: '{show_title_guess}' S{season_num}E{episode_num_list} (Year guess: {year_guess})")
        final_meta = MediaMetadata(is_series=True)
        tmdb_show_data, tmdb_ep_map, tmdb_ids = None, None, None
        tvdb_show_data, tvdb_ep_map, tvdb_ids = None, None, None
        lang = self.cfg('tmdb_language', 'en')
        episode_num_tuple = tuple(sorted(episode_num_list)) if isinstance(episode_num_list, (list, tuple)) else tuple()
        cache_key_base = f"series::{show_title_guess}_{season_num}_{episode_num_tuple}_{year_guess}_{lang}"

        # TMDB Attempt
        if self.tmdb:
            tmdb_cache_key = cache_key_base + "::tmdb"
            cached_tmdb = await self._get_cache(tmdb_cache_key)
            if cached_tmdb:
                tmdb_show_data, tmdb_ep_map, tmdb_ids = cached_tmdb
            else:
                try:
                    await self.rate_limiter.wait()
                    tmdb_show_data, tmdb_ep_map, tmdb_ids = await self._do_fetch_tmdb_series(
                        show_title_guess, season_num, episode_num_tuple, year_guess, lang
                    )
                    await self._set_cache(tmdb_cache_key, (tmdb_show_data, tmdb_ep_map, tmdb_ids))
                except Exception as e: # Catch errors from _do_fetch (including final retry failure)
                     log.error(f"TMDB series fetch ultimately failed for '{show_title_guess}': {type(e).__name__}: {e}", exc_info=True)
                     tmdb_show_data, tmdb_ep_map, tmdb_ids = None, None, None
        else: log.warning("TMDB client not available, skipping TMDB series fetch.")

        # TVDB Fallback
        needs_tvdb_fallback = (not tmdb_show_data) or (tmdb_show_data and not tmdb_ep_map)
        if needs_tvdb_fallback and self.tvdb:
            tvdb_cache_key = cache_key_base + "::tvdb"
            cached_tvdb = await self._get_cache(tvdb_cache_key)
            if cached_tvdb:
                 tvdb_show_data, tvdb_ep_map, tvdb_ids = cached_tvdb
            else:
                log.debug("Attempting TVDB fallback (async)...")
                tvdb_id_from_tmdb = tmdb_ids.get('tvdb_id') if tmdb_ids else None
                try:
                    await self.rate_limiter.wait()
                    tvdb_show_data, tvdb_ep_map, tvdb_ids = await self._do_fetch_tvdb_series(
                        show_title_guess, season_num, episode_num_tuple,
                        tvdb_id=tvdb_id_from_tmdb, year_guess=year_guess, lang=lang
                    )
                    await self._set_cache(tvdb_cache_key, (tvdb_show_data, tvdb_ep_map, tvdb_ids))
                except Exception as e: # Catch errors from _do_fetch (including final retry failure)
                     log.error(f"TVDB series fetch ultimately failed for '{show_title_guess}': {type(e).__name__}: {e}", exc_info=True)
                     tvdb_show_data, tvdb_ep_map, tvdb_ids = None, None, None
        elif needs_tvdb_fallback: log.warning("TVDB client not available, skipping TVDB series fallback.")

        # --- Combine Results (Logic remains the same, no async needed here) ---
        final_meta.source_api = "tmdb" if tmdb_show_data else ("tvdb" if tvdb_show_data else None)
        primary_show_data = tmdb_show_data if tmdb_show_data else tvdb_show_data
        primary_ep_map = tmdb_ep_map if tmdb_ep_map else tvdb_ep_map
        show_title_api = None

        if primary_show_data:
            show_title_api = getattr(primary_show_data, 'name', None)
            if show_title_api is None and isinstance(primary_show_data, dict):
                show_title_api = primary_show_data.get('name')
        final_meta.show_title = show_title_api or show_title_guess

        show_air_date = None
        if primary_show_data:
             show_air_date = getattr(primary_show_data, 'first_air_date', None)
             if show_air_date is None and isinstance(primary_show_data, dict):
                 show_air_date = primary_show_data.get('first_air_date') or primary_show_data.get('firstAired')

        final_meta.show_year = self._get_year_from_date(show_air_date)
        final_meta.ids = {**(tvdb_ids or {}), **(tmdb_ids or {})}
        final_meta.season = season_num
        final_meta.episode_list = list(episode_num_tuple) # Use original list if needed

        if primary_ep_map:
            for ep_num in episode_num_list: # Use original list for iteration order
                ep_details = primary_ep_map.get(ep_num)
                if ep_details:
                    ep_title = getattr(ep_details, 'name', None)
                    if not ep_title and isinstance(ep_details, dict): ep_title = ep_details.get('name')
                    air_date = getattr(ep_details, 'air_date', None)
                    if not air_date and isinstance(ep_details, dict): air_date = ep_details.get('aired')
                    if ep_title: final_meta.episode_titles[ep_num] = ep_title
                    if air_date: final_meta.air_dates[ep_num] = air_date
                else:
                    log.debug(f"Episode S{season_num}E{ep_num} not found in the chosen API results map.")

        if not final_meta.source_api and not final_meta.episode_titles:
             log.warning(f"Metadata fetch failed completely for series: '{show_title_guess}' S{season_num}E{final_meta.episode_list}")
        return final_meta

    # Decorator applied directly using the factory pattern for deferred config access
    # @setup_async_retry_decorator()
# In class MetadataFetcher:

    async def _do_fetch_tmdb_series(self, title, season, episodes, year_guess=None, lang='en'):
        """
        Core logic for TMDB series/episode fetching (runs sync code in executor).
        Includes MANUAL retry logic around the executor call.
        Returns a tuple: (show_data_object, episode_data_map, external_ids_dict) or (None, None, None).
        """
        if not self.tmdb:
            log.warning("TMDB client not available in _do_fetch_tmdb_series.")
            return None, None, None

        # Import necessary TMDB classes locally for the sync function
        # These are only needed within the thread executing the sync code
        from tmdbv3api import TV, Season
        from tmdbv3api.exceptions import TMDbException # Import from correct submodule

        # --- Define the synchronous function to run in the executor ---
        def _sync_tmdb_series_fetch():
            log.debug(f"Executing TMDB Series Fetch [sync thread] for: '{title}' S{season} E{episodes} (lang: {lang}, year: {year_guess}, strategy: {self.tmdb_strategy}, tolerance: {self.year_tolerance})")

            # 1. Search for the series
            try:
                search = TV()
                # Ensure language is set on the client if needed (though usually global)
                # self.tmdb.language = lang # Re-setting here might cause issues if client is shared
                results_obj = search.search(title) # Blocking Call 1
                log.debug(f"TMDB raw series search results [sync thread] for '{title}': Count={len(results_obj) if results_obj else 0}")
            except Exception as e_search:
                log.error(f"TMDB search failed unexpectedly for '{title}' [sync thread]: {e_search}", exc_info=True)
                # Re-raise the specific exception to be potentially caught by retry logic if retryable
                raise e_search

            show_match = None
            processed_results = results_obj

            # 2. Apply Year Filter First (Synchronous)
            if year_guess and processed_results:
                log.debug(f"Applying year filter ({year_guess} +/- {self.year_tolerance}) to TMDB series results [sync thread].")
                filtered_list = []
                try:
                    for r in processed_results:
                        result_year = None
                        first_air_date = getattr(r, 'first_air_date', None)
                        if first_air_date and isinstance(first_air_date, str) and len(first_air_date) >= 4:
                            try:
                                result_year = int(first_air_date.split('-')[0])
                            except (ValueError, IndexError, TypeError):
                                log.warning(f"Could not parse year from TMDB first_air_date: '{first_air_date}' [sync thread]")
                                pass # Keep result_year as None
                        if result_year is not None and abs(result_year - year_guess) <= self.year_tolerance:
                            log.debug(f"  -> Year filter PASSED for '{getattr(r, 'name', 'N/A')}' ({result_year}) [sync thread]")
                            filtered_list.append(r)
                        else:
                            log.debug(f"  -> Year filter FAILED for '{getattr(r, 'name', 'N/A')}' ({result_year or 'N/A'}) [sync thread]")

                    # Update processed_results: keep original if filter removes all, unless original was empty
                    if filtered_list or not processed_results: # If filter yielded results OR original was empty anyway
                         processed_results = filtered_list
                         log.debug(f"Year filtering resulted in {len(processed_results)} TMDB results [sync thread].")
                    else: # Filter removed all, but original had results, keep original
                         log.debug(f"Year filtering removed all TMDB results, keeping original {len(processed_results)} for matching [sync thread].")

                except TypeError:
                    log.warning(f"Could not iterate TMDB series results (type {type(processed_results)}) for filtering [sync thread].")
                except Exception as e_filter:
                    log.error(f"Error during TMDB series year filtering [sync thread]: {e_filter}", exc_info=True)
                    processed_results = None # Indicate filter failure

            # 3. Select Match based on Strategy (Synchronous)
            if processed_results:
                if self.tmdb_strategy == 'fuzzy' and THEFUZZ_AVAILABLE:
                    log.debug(f"Attempting TMDB fuzzy match (cutoff: {self.tmdb_fuzzy_cutoff}) [sync thread].")
                    results_as_dicts = _tmdb_results_to_dicts(processed_results, result_type='series')
                    if results_as_dicts:
                        matched_dict = find_best_match(title, results_as_dicts, result_key='name', id_key='id', score_cutoff=self.tmdb_fuzzy_cutoff)
                        if matched_dict:
                            matched_id = matched_dict.get('id')
                            log.debug(f"Fuzzy match found ID: {matched_id}. Finding original AsObj... [sync thread]")
                            try:
                                # Find the original AsObj object corresponding to the matched ID
                                temp_show_match = next((r for r in processed_results if getattr(r, 'id', None) == matched_id), None)
                            except Exception as e_find:
                                log.error(f"Error finding AsObj after fuzzy match [sync thread]: {e_find}", exc_info=True)
                                temp_show_match = None

                            if temp_show_match:
                                log.debug(f"Found original AsObj via fuzzy match: {getattr(temp_show_match, 'name', 'N/A')} [sync thread]")
                                show_match = temp_show_match
                            else:
                                log.error(f"Fuzzy match dict found, but could not find corresponding AsObj with ID {matched_id} in original results [sync thread].")
                        else:
                            log.debug("Fuzzy matching did not find a suitable TMDB series match [sync thread].")
                    else:
                        log.warning("Could not convert TMDB results to dicts for fuzzy matching [sync thread].")

                # Fallback to 'first' strategy if fuzzy failed or wasn't used
                if not show_match:
                    if self.tmdb_strategy == 'fuzzy':
                        log.warning("Fuzzy match failed or unavailable, falling back to 'first' result strategy [sync thread].")
                    log.debug("Using 'first' result strategy for TMDB series [sync thread].")
                    try:
                        show_match = next(iter(processed_results), None)
                    except Exception as e_first:
                        log.error(f"Error getting first TMDB series result [sync thread]: {e_first}", exc_info=True)
                    if show_match:
                        log.debug(f"Selected first TMDB series result: {getattr(show_match, 'name', 'N/A')} [sync thread]")
                    else:
                        log.debug("No first result found after filtering [sync thread].")

            # 4. Check if a match was found
            if not show_match:
                log.debug(f"No suitable TMDB show match found for '{title}' [sync thread].")
                return None, None, None # Return tuple indicating no match

            show_id = getattr(show_match, 'id', None)
            if not show_id:
                log.error(f"TMDB show match lacks 'id' [sync thread]: {show_match}")
                return None, None, None # Return tuple indicating error

            log.debug(f"TMDB matched show '{getattr(show_match, 'name', 'N/A')}' ID: {show_id} [sync thread]")

            # Initialize variables for details
            show_details = None
            combined_show_data = {}
            ep_data = {}
            ids = {}

            # 5. Fetch Show Details
            try:
                # search object is already instantiated from step 1
                show_details = search.details(show_id) # Blocking Call 2
                log.debug(f"Fetched TMDB show details ID {show_id}: {type(show_details)} [sync thread]")
            except Exception as e_details:
                log.error(f"Failed to fetch TMDB show details ID {show_id} [sync thread]: {e_details}", exc_info=True)
                # Can proceed without details, using search result as primary_obj

            primary_obj = show_details if show_details else show_match

            # 6. Combine Show Data for External IDs
            if primary_obj:
                try:
                    # Attempt to get full data dict if available (e.g., from AsObj._data)
                    if hasattr(primary_obj, '_data') and isinstance(primary_obj._data, dict):
                        combined_show_data = primary_obj._data.copy()
                        log.debug("Created combined_show_data from _data attribute [sync thread].")
                    else:
                        # Fallback: Manually create dict from known attributes
                        combined_show_data = {
                            'id': show_id,
                            'name': getattr(primary_obj, 'name', None),
                            'first_air_date': getattr(primary_obj, 'first_air_date', None),
                            'overview': getattr(primary_obj, 'overview', None),
                            'genres': getattr(primary_obj, 'genres', None), # Example
                            # Add other relevant fields if needed
                        }
                        log.debug(f"Created combined_show_data from attributes of {type(primary_obj)} [sync thread].")
                except Exception as e_comb:
                    log.error(f"Error creating combined_show_data from primary_obj [sync thread]: {e_comb}")

            # Ensure basic info is present if combination failed
            if 'id' not in combined_show_data and show_id: combined_show_data['id'] = show_id
            if 'name' not in combined_show_data: combined_show_data['name'] = getattr(show_match, 'name', None) # Fallback to search result name
            if 'first_air_date' not in combined_show_data: combined_show_data['first_air_date'] = getattr(show_match, 'first_air_date', None)

            # 7. Fetch External IDs
            ext_ids_data = {}
            fetched_ext_ids = False
            # Prefer fetching from details if available and callable
            if show_details and hasattr(show_details, 'external_ids') and callable(show_details.external_ids):
                 try:
                     ext_ids_data = show_details.external_ids() # Blocking Call 3a (potential)
                     fetched_ext_ids = True
                     log.debug("Fetched external IDs from details object [sync thread].")
                 except Exception as e_call_ext:
                     log.debug(f"Ignoring error calling external_ids method on details [sync thread]: {e_call_ext}")
            # Fallback to search object's external_ids method
            if not fetched_ext_ids:
                try:
                    # search object is still in scope from step 1
                    ext_ids_data = search.external_ids(show_id) # Blocking Call 3b
                    log.debug("Fetched external IDs using search.external_ids(show_id) [sync thread].")
                except Exception as e_ext:
                    log.warning(f"Failed to fetch TMDB external IDs using show ID {show_id} [sync thread]: {e_ext}")

            combined_show_data['external_ids'] = ext_ids_data
            log.debug(f"TMDB combined_show_data prepared [sync thread]: Keys={list(combined_show_data.keys())}")

            # 8. Fetch Season/Episode Details
            # Check if episode data is needed (episodes tuple is not empty)
            if episodes:
                try:
                    log.debug(f"TMDB: Attempting to fetch season {season} details for show ID {show_id} [sync thread]")
                    season_fetcher = Season()
                    season_details = season_fetcher.details(tv_id=show_id, season_num=season) # Blocking Call 4
                    log.debug(f"TMDB: Fetched season_details object type: {type(season_details)} [sync thread]")

                    if hasattr(season_details, 'episodes'):
                        log.debug(f"TMDB: Found 'episodes' attribute. Count: {len(season_details.episodes) if hasattr(season_details.episodes, '__len__') else 'N/A'} [sync thread]")
                        episodes_in_season = {}
                        # Build a map of {episode_number: episode_object}
                        for api_ep in season_details.episodes:
                            ep_num_api = getattr(api_ep, 'episode_number', None)
                            ep_name_api = getattr(api_ep, 'name', 'N/A')
                            log.debug(f"  -> TMDB API Ep Raw: Number={ep_num_api}, Name='{ep_name_api}' [sync thread]")
                            if ep_num_api is not None:
                                try:
                                    episodes_in_season[int(ep_num_api)] = api_ep
                                except (ValueError, TypeError):
                                    log.warning(f"  -> TMDB API Ep Skipped: Could not convert episode number '{ep_num_api}' to int [sync thread].")
                            else:
                                log.warning("  -> TMDB API Ep Skipped: Missing 'episode_number' attribute [sync thread].")

                        log.debug(f"TMDB: Built episodes_in_season map with keys: {list(episodes_in_season.keys())} [sync thread]")
                        # Iterate through the requested episode numbers (passed as 'episodes' tuple)
                        log.debug(f"TMDB: Checking for requested episode numbers: {episodes} [sync thread]")
                        for ep_num_needed in episodes:
                            log.debug(f"  -> TMDB: Looking for episode number: {ep_num_needed} in map keys [sync thread].")
                            episode_obj = episodes_in_season.get(ep_num_needed)
                            if episode_obj:
                                ep_data[ep_num_needed] = episode_obj # Store the found episode object
                                log.debug(f"  -> TMDB: Found match for E{ep_num_needed}. Storing object [sync thread].")
                            else:
                                log.debug(f"  -> TMDB: E{ep_num_needed} not found in episodes_in_season map [sync thread].")
                    else:
                        log.warning(f"TMDB season details ID {show_id} S{season} lacks 'episodes' [sync thread].")
                except Exception as e_season:
                    # Log the error but allow processing to continue without episode data
                    log.warning(f"TMDB error getting/processing season {season} ID {show_id} [sync thread]: {type(e_season).__name__}: {e_season}", exc_info=True)
            else:
                log.debug("No specific episodes requested, skipping season details fetch [sync thread].")


            # 9. Extract final IDs using the helper
            ids = get_external_ids(tmdb_obj=combined_show_data)
            # Return the most detailed object we got (details or the initial match)
            final_show_data_obj = show_details if show_details else show_match

            log.debug(f"_sync_tmdb_series_fetch returning: data type={type(final_show_data_obj)}, ep_map keys={list(ep_data.keys())}, ids={ids}")
            return final_show_data_obj, ep_data, ids
        # --- End of _sync_tmdb_series_fetch function ---

        # --- Manual Retry Logic ---
        attempts_cfg = self.cfg('api_retry_attempts', 3)
        wait_sec_cfg = self.cfg('api_retry_wait_seconds', 2)
        max_attempts = max(1, attempts_cfg if attempts_cfg is not None else 3)
        wait_seconds = max(0, wait_sec_cfg if wait_sec_cfg is not None else 2)
        last_exception = None

        for attempt in range(max_attempts):
            try:
                log.debug(f"Attempt {attempt + 1}/{max_attempts} for TMDB series: '{title}' S{season}")
                # Run the synchronous function in an executor
                result = await self._run_sync(_sync_tmdb_series_fetch)
                # Ensure we always return a 3-tuple, even if sync function returned None early
                if result is None:
                    return None, None, None
                # Result should be (data, ep_map, ids)
                return result
            except Exception as e:
                last_exception = e
                log.warning(f"Attempt {attempt + 1} failed for TMDB series '{title}' S{season}: {type(e).__name__}: {e}")
                # Check if this exception type should be retried
                if should_retry_api_error(e):
                    if attempt < max_attempts - 1:
                        log.info(f"Retrying TMDB series fetch in {wait_seconds}s...")
                        await asyncio.sleep(wait_seconds)
                        continue # Go to the next attempt in the loop
                    else:
                        log.error(f"All {max_attempts} retry attempts failed for TMDB series '{title}' S{season}.")
                        break # Exit loop after final attempt failure
                else:
                    log.error(f"Non-retryable error occurred for TMDB series '{title}' S{season}. Not retrying.")
                    break # Exit loop for non-retryable errors

        # If the loop finished without returning successfully (due to errors)
        log.error(f"Failed to fetch TMDB series metadata for '{title}' S{season} after {max_attempts} attempts.", exc_info=last_exception)
        return None, None, None # Indicate failure
    
    # Decorator applied directly using the factory pattern for deferred config access
    # @setup_async_retry_decorator()
    async def _do_fetch_tvdb_series(self, title: str, season_num: int, episodes: tuple, tvdb_id: int = None, year_guess: int = None, lang: str = 'en'):
        """
        Core logic for TVDB v4 series fetching (runs sync code in executor).
        Retries are handled by the decorator applied above.
        Returns a tuple: (show_data_dict, episode_data_map, external_ids_dict) or (None, None, None).
        """
        if not self.tvdb:
            log.warning("TVDB client not available in _do_fetch_tvdb_series.")
            return None, None, None

        # tvdb_v4_official library errors are already imported at the top level
        # from tvdb_v4_official.errors import NotFoundError as TvdbNotFoundError, TvdbApiException

        # --- Define the synchronous function to run in the executor ---
        def _sync_tvdb_series_fetch():
            log.debug(f"Executing TVDB Series Fetch [sync thread] for: '{title}' S{season_num} E{episodes} (lang: {lang}, year: {year_guess}, id: {tvdb_id}, tolerance: {self.year_tolerance})")
            show_data = None # Will store the final series data dictionary
            best_match_id = tvdb_id
            search_results = None

            # --- 1. Search if ID not provided ---
            if not best_match_id:
                try:
                    log.debug(f"TVDB searching for: '{title}' (Year guess: {year_guess}) [sync thread]")
                    # Assuming self.tvdb.search returns a list of dicts or raises error
                    search_results = self.tvdb.search(title) # Blocking Call 1 (Search)
                    log.debug(f"TVDB search returned {len(search_results) if search_results else 0} results [sync thread].")
                except TvdbNotFoundError:
                    log.debug(f"TVDB search returned NotFoundError for title '{title}' [sync thread].")
                    search_results = [] # Treat as no results found
                except Exception as e_search:
                    # Catch potential TvdbApiException or other requests/connection errors
                    log.warning(f"TVDB search failed unexpectedly for '{title}' [sync thread]: {type(e_search).__name__}: {e_search}", exc_info=True)
                    # Re-raise exception so tenacity decorator can handle retries if applicable
                    raise e_search

                # --- Process Search Results (only if search didn't fail critically) ---
                if search_results is not None: # Check if search completed (even if empty list)

                    # --- 2. Apply Year Filter ---
                    if search_results and year_guess:
                        log.debug(f"Applying year filter ({year_guess} +/- {self.year_tolerance}) to TVDB search results [sync thread].")
                        filtered_results = []
                        for r in search_results:
                            # tvdb-v4 returns year as string in search results
                            result_year_str = r.get('year')
                            if result_year_str:
                                try:
                                    result_year = int(result_year_str)
                                    if abs(result_year - year_guess) <= self.year_tolerance:
                                        log.debug(f"  -> TVDB Year filter PASSED for '{r.get('name', 'N/A')}' ({result_year}) [sync thread]")
                                        filtered_results.append(r)
                                    else:
                                        log.debug(f"  -> TVDB Year filter FAILED for '{r.get('name', 'N/A')}' ({result_year}) [sync thread]")
                                except (ValueError, TypeError):
                                    log.warning(f"Could not parse TVDB year '{result_year_str}' for filtering [sync thread]")
                                    pass # Ignore conversion errors for this item

                        # Update search_results: keep original if filter removes all, unless original was empty
                        if filtered_results or not search_results:
                             search_results = filtered_results
                             log.debug(f"TVDB results after year filter: {len(search_results)} [sync thread].")
                        else:
                             log.debug(f"TVDB search year filter removed all results, keeping original {len(search_results)} for matching [sync thread].")


                    # --- 3. Apply Fuzzy Match (if we still have results) ---
                    if search_results: # Check if list is not empty
                         try:
                             log.debug(f"Attempting TVDB fuzzy match (cutoff: 70) [sync thread].")
                             # tvdb_v4 search results are already dicts
                             search_results_tuple = tuple(search_results)
                             # Use the correct id key from tvdb_v4 search results (likely 'tvdb_id' or 'id')
                             # Inspect a sample result if unsure. Let's assume 'tvdb_id' for now.
                             match = find_best_match(title, search_results_tuple, result_key='name', id_key='tvdb_id', score_cutoff=70)

                             if match:
                                 # Extract the ID from the matched dictionary
                                 matched_id_val = match.get('tvdb_id')
                                 if matched_id_val is not None:
                                     try:
                                         best_match_id = int(matched_id_val) # Ensure it's an int
                                         log.debug(f"TVDB name search found match: {match.get('name', 'N/A')} (ID: {best_match_id}) [sync thread]")
                                     except (ValueError, TypeError):
                                         log.error(f"TVDB fuzzy match found non-integer ID: {matched_id_val} [sync thread]")
                                 else:
                                     log.error(f"TVDB fuzzy match result missing 'tvdb_id': {match} [sync thread]")
                             else:
                                 log.warning(f"TVDB search yielded results, but no good fuzzy match for '{title}' (after filter) [sync thread].")
                         except Exception as e_match_tvdb:
                             log.error(f"Error during find_best_match for TVDB results of '{title}' [sync thread]: {e_match_tvdb}", exc_info=True)
                             # best_match_id remains None or the original tvdb_id if error occurs here
                    else:
                        log.debug(f"TVDB search returned no results for '{title}' (after filter) [sync thread].")
                # else: search failed, best_match_id remains None or original tvdb_id

            # --- 4. Fetch Extended Data using best_match_id (if found/provided) ---
            if best_match_id:
                try:
                    log.debug(f"TVDB fetching extended series data for ID: {best_match_id} [sync thread]")
                    # Use the correct method name based on tvdb_v4_official library
                    # Assuming it returns a dictionary
                    show_data = self.tvdb.get_series_extended(best_match_id) # Blocking Call 2 (Get Extended)

                    if not show_data or not isinstance(show_data, dict):
                        log.warning(f"TVDB get_series_extended for ID {best_match_id} returned invalid data (type: {type(show_data)}) [sync thread].")
                        return None, None, None # Return None tuple if fetch returned nothing valid

                    log.debug(f"TVDB successfully fetched extended data for: {show_data.get('name', 'N/A')} [sync thread]")

                except TvdbNotFoundError:
                    log.warning(f"TVDB get_series_extended failed for ID {best_match_id}: Not Found [sync thread].")
                    return None, None, None # Not found is not a retryable error here
                except Exception as e_fetch:
                    log.warning(f"TVDB get_series_extended failed unexpectedly for ID {best_match_id} [sync thread]: {type(e_fetch).__name__}: {e_fetch}", exc_info=True)
                    # Re-raise exception so tenacity can handle retries if applicable
                    raise e_fetch
            else:
                # This covers cases where: tvdb_id was not passed AND search failed OR yielded no match
                log.warning(f"TVDB: Could not find suitable series match ID for '{title}' [sync thread].")
                return None, None, None # No match found, return None tuple

            # --- 5. Episode Extraction ---
            ep_data = {} # Stores {ep_num: episode_dict}
            ids = {}     # Stores external IDs

            if show_data: # Ensure we have valid show data before processing
                try:
                    log.debug(f"TVDB extracting episode data for S{season_num} from fetched series data [sync thread]")
                    target_season_data = None
                    # tvdb_v4_official usually returns season data within the extended series record
                    all_season_data = show_data.get('seasons', [])
                    if not isinstance(all_season_data, list):
                        log.warning(f"TVDB 'seasons' data is not a list: {type(all_season_data)} [sync thread]")
                        all_season_data = []

                    # Find the correct season dictionary
                    for season_info in all_season_data:
                         if not isinstance(season_info, dict): continue
                         season_num_from_api = season_info.get('number')
                         # tvdb_v4_official uses 'type': {'id': 1, 'name': 'Official', 'type': 'official'}
                         is_official = season_info.get('type', {}).get('type') == 'official'
                         log.debug(f"  Checking season: Number={season_num_from_api}, Type={season_info.get('type', {}).get('type')}, IsOfficial={is_official} [sync thread]")

                         if season_num_from_api is not None:
                            try:
                                # Ensure comparison is between integers
                                if int(season_num_from_api) == int(season_num) and is_official:
                                    target_season_data = season_info
                                    log.debug(f"  Found matching official season {season_num} data. [sync thread]")
                                    break
                            except (ValueError, TypeError):
                                log.warning(f"  Could not compare season number {season_num_from_api} [sync thread]")
                                continue # Skip this season if number isn't valid

                    # Process episodes if the target season was found
                    if target_season_data:
                         # Episodes are usually nested within the season data in v4 extended response
                         all_episode_data = target_season_data.get('episodes', [])
                         if not isinstance(all_episode_data, list):
                             log.warning(f"TVDB S{season_num} 'episodes' data is not a list: {type(all_episode_data)} [sync thread]")
                             all_episode_data = []

                         log.debug(f"Found {len(all_episode_data)} episodes in season {season_num} data [sync thread].")
                         episodes_in_season = {} # Map {ep_num_int: episode_dict}
                         # Build the map for quick lookup
                         for ep in all_episode_data:
                             # Check episode structure from tvdb_v4_official (likely dicts)
                             if isinstance(ep, dict) and ep.get('number') is not None:
                                 try:
                                     ep_num_int = int(ep['number'])
                                     episodes_in_season[ep_num_int] = ep
                                 except (ValueError, TypeError):
                                     log.warning(f"Could not parse episode number {ep.get('number')} [sync thread]")
                                     pass # Skip episode if number is invalid

                         # Extract the requested episodes
                         episode_iterator = episodes if episodes else [] # Use the passed tuple
                         log.debug(f"Looking for episode numbers: {episode_iterator} in fetched season data [sync thread].")
                         for ep_num in episode_iterator:
                             episode_details = episodes_in_season.get(ep_num)
                             if episode_details:
                                 ep_data[ep_num] = episode_details # Store the episode dictionary
                                 log.debug(f"TVDB extracted S{season_num}E{ep_num}: {episode_details.get('name')} [sync thread]")
                             else:
                                 log.debug(f"TVDB episode S{season_num}E{ep_num} not found in season map [sync thread].")
                    else:
                        log.warning(f"TVDB season {season_num} not found or not 'official' type for '{show_data.get('name')}' [sync thread].")

                except Exception as e_ep_extract:
                    log.warning(f"TVDB error processing episode data for '{show_data.get('name')}' [sync thread]: {e_ep_extract}", exc_info=True)

                # 6. Extract external IDs after successfully getting show_data
                try:
                    ids = get_external_ids(tvdb_obj=show_data)
                except Exception as e_ids:
                     log.warning(f"Error extracting external IDs from TVDB data [sync thread]: {e_ids}", exc_info=True)

            # 7. Return results
            log.debug(f"_sync_tvdb_series_fetch returning: data type={type(show_data)}, ep_map keys={list(ep_data.keys())}, ids={ids}")
            # Return the show data dict, the map of {ep_num: ep_dict}, and the external IDs dict
            return show_data, ep_data, ids
        # --- End of _sync_tvdb_series_fetch function ---

        # Run the synchronous function in an executor
        try:
            result = await self._run_sync(_sync_tvdb_series_fetch)
             # Ensure we always return a 3-tuple, even if sync function returned None early
            if result is None:
                return None, None, None
            return result # Should be (show_data, ep_data, ids)
        except Exception as e:
            # Handle potential errors from the executor itself or re-raised tenacity errors
            log.error(f"Error executing sync TVDB fetch for '{title}' S{season_num} E{episodes}: {e}", exc_info=True)
            raise # Re-raise exception to be caught by fetch_series_metadata

    # Make the main fetch method async
    async def fetch_movie_metadata(self, movie_title_guess, year_guess=None):
        """
        Fetches movie metadata asynchronously, checking cache first.
        """
        log.debug(f"Fetching movie metadata (async) for: '{movie_title_guess}' (Year guess: {year_guess})")
        final_meta = MediaMetadata(is_movie=True)
        tmdb_movie_data = None # Holds the detailed movie object (AsObj or dict)
        tmdb_ids = None        # Holds the dictionary of external IDs,  tmdb_ids now includes collection info

        lang = self.cfg('tmdb_language', 'en')
        # Create a cache key based on input parameters
        cache_key = f"movie::{movie_title_guess}_{year_guess}_{lang}"

        # 1. Check Cache
        cached_data = await self._get_cache(cache_key)
        if cached_data:
            tmdb_movie_data, tmdb_ids = cached_data
            log.debug(f"Using cached data for movie: '{movie_title_guess}'")
        else:
            # 2. Fetch from API (if not cached)
            if not self.tmdb:
                log.warning("TMDB client not available, skipping TMDB movie fetch.")
            else:
                try:
                    await self.rate_limiter.wait() # Apply rate limiting before the call
                    # Call the potentially retry-decorated async method
                    # _do_fetch_tmdb_movie handles running sync code in executor
                    tmdb_movie_data, tmdb_ids = await self._do_fetch_tmdb_movie(
                        movie_title_guess, year_guess, lang
                    )
                    # Store result in cache (even if None, indicates attempted fetch)
                    await self._set_cache(cache_key, (tmdb_movie_data, tmdb_ids))

                except RetryError as e:
                     log.error(f"TMDB movie fetch ultimately failed after retries for '{movie_title_guess}': {e}")
                     tmdb_movie_data, tmdb_ids = None, None # Ensure reset on final failure
                except Exception as e:
                     log.error(f"Unexpected error during TMDB movie fetch for '{movie_title_guess}': {type(e).__name__}: {e}", exc_info=True)
                     tmdb_movie_data, tmdb_ids = None, None # Ensure reset on other errors

            log.debug(f"fetch_movie_metadata (async): API fetch attempt returned: data type={type(tmdb_movie_data)}, ids type={type(tmdb_ids)}")
            # Ensure reset if fetch returned (None, None) without error explicitly
            if tmdb_movie_data is None and tmdb_ids is None:
                 log.debug("TMDB fetch returned (None, None) without error, variables reset.")
                 tmdb_movie_data, tmdb_ids = None, None


        # 3. Combine results into final MediaMetadata object
        if tmdb_movie_data:
            try:
                final_meta.source_api = "tmdb"
                # ... (extract title, release_date, movie_year as before) ...
                title_val = getattr(tmdb_movie_data, 'title', None)
                if title_val is None and isinstance(tmdb_movie_data, dict): title_val = tmdb_movie_data.get('title')
                final_meta.movie_title = title_val or movie_title_guess

                release_date_val = getattr(tmdb_movie_data, 'release_date', None)
                if release_date_val is None and isinstance(tmdb_movie_data, dict): release_date_val = tmdb_movie_data.get('release_date')
                final_meta.release_date = release_date_val
                final_meta.movie_year = self._get_year_from_date(final_meta.release_date) or year_guess

                # --- Assign IDs AND Collection info ---
                if isinstance(tmdb_ids, dict):
                     final_meta.ids = tmdb_ids # Assign the whole dict
                     # Extract collection info from the ids dict into the model fields
                     final_meta.collection_name = tmdb_ids.get('collection_name')
                     final_meta.collection_id = tmdb_ids.get('collection_id')
                     log.debug(f"Extracted collection: Name='{final_meta.collection_name}', ID={final_meta.collection_id}")
                else:
                     final_meta.ids = {} # Ensure it's an empty dict if fetch failed

                log.debug(f"fetch_movie_metadata: Successfully populated final_meta from TMDB.")

            except Exception as e_populate:
                 log.error(f"Error populating final_meta from tmdb_movie_data: {e_populate}", exc_info=True)
                 # Reset on failure
                 final_meta = MediaMetadata(is_movie=True) # Re-init blank
                 final_meta.movie_title = movie_title_guess
                 final_meta.movie_year = year_guess
        else:
           # Fallback logic
            log.warning(f"Metadata fetch ultimately failed for movie: '{movie_title_guess}' ({year_guess})")
            final_meta.movie_title = movie_title_guess
            final_meta.movie_year = year_guess

        log.debug(f"fetch_movie_metadata final result: Source='{final_meta.source_api}', Title='{final_meta.movie_title}', Year={final_meta.movie_year}, IDs={final_meta.ids}, Collection={final_meta.collection_name}")
        return final_meta
    
    # NOTE: Decorator applied dynamically in __init__ like:
    # self._do_fetch_tmdb_movie = setup_async_retry_decorator()(self._do_fetch_tmdb_movie)
# In class MetadataFetcher:

    # Decorator applied directly using the factory pattern for deferred config access
    # @setup_async_retry_decorator()
    async def _do_fetch_tmdb_movie(self, title, year, lang='en'):
        """
        Core logic for TMDB movie fetching (runs sync code in executor).
        Retries are handled by the decorator applied above.
        Returns a tuple: (movie_data_object, external_ids_dict) or (None, None).
        """
        if not self.tmdb:
            log.warning("TMDB client not available in _do_fetch_tmdb_movie.")
            return None, None

        # Import necessary TMDB classes locally for the sync function
        from tmdbv3api import Movie
        from tmdbv3api.exceptions import TMDbException # Import from correct submodule

        # --- Define the synchronous function to run in the executor ---
        def _sync_tmdb_movie_fetch():
            log.debug(f"Executing TMDB Movie Fetch [sync thread] for: '{title}' (lang: {lang}, year: {year}, strategy: {self.tmdb_strategy}, tolerance: {self.year_tolerance})")

            # 1. Search for the movie
            try:
                search = Movie()
                # Ensure language is set on the client before searching if specified
                # Note: tmdbv3api typically uses client-level language setting
                # self.tmdb.language = lang # This should already be set during client init
                results_obj = search.search(title) # Blocking Call 1
                log.debug(f"TMDB raw movie search results [sync thread] for '{title}': Count={len(results_obj) if results_obj else 0}")
            except Exception as e_search:
                log.error(f"TMDB movie search failed unexpectedly for '{title}' [sync thread]: {e_search}", exc_info=True)
                # Re-raise the specific exception for tenacity
                raise e_search

            movie_match = None
            processed_results = results_obj

            # 2. Apply Year Filter First (Synchronous)
            if year and processed_results:
                log.debug(f"Applying year filter ({year} +/- {self.year_tolerance}) to TMDB movie results [sync thread].")
                filtered_list = []
                try:
                    for r in processed_results:
                        release_year = None
                        release_date_val = getattr(r, 'release_date', None)
                        # Check if release_date_val is a non-empty string before splitting
                        if release_date_val and isinstance(release_date_val, str) and len(release_date_val) >= 4:
                            try:
                                release_year = int(release_date_val.split('-')[0])
                            except (ValueError, IndexError, TypeError):
                                log.warning(f"Could not parse year from TMDB release_date: '{release_date_val}' [sync thread]")
                                pass # Keep release_year as None
                        if release_year is not None and abs(release_year - year) <= self.year_tolerance:
                             log.debug(f"  -> Year filter PASSED for '{getattr(r, 'title', 'N/A')}' ({release_year}) [sync thread]")
                             filtered_list.append(r)
                        else:
                             log.debug(f"  -> Year filter FAILED for '{getattr(r, 'title', 'N/A')}' ({release_year or 'N/A'}) [sync thread]")

                    # Update processed_results: keep original if filter removes all, unless original was empty
                    if filtered_list or not processed_results:
                         processed_results = filtered_list
                         log.debug(f"Year filtering resulted in {len(processed_results)} TMDB movie results [sync thread].")
                    else:
                         log.debug(f"Year filtering removed all TMDB movie results, keeping original {len(processed_results)} for matching [sync thread].")

                except TypeError:
                    log.warning(f"Could not iterate over TMDB movie results object (type {type(processed_results)}) for filtering [sync thread].")
                except Exception as e_filter:
                    log.error(f"Error during TMDB movie year filtering [sync thread]: {e_filter}", exc_info=True)
                    processed_results = None # Indicate filter failure

            # 3. Select Match based on Strategy (Synchronous)
            if processed_results:
                if self.tmdb_strategy == 'fuzzy' and THEFUZZ_AVAILABLE:
                    log.debug(f"Attempting TMDB movie fuzzy match (cutoff: {self.tmdb_fuzzy_cutoff}) [sync thread].")
                    results_as_dicts = _tmdb_results_to_dicts(processed_results, result_type='movie')
                    if results_as_dicts:
                        matched_dict = find_best_match(title, results_as_dicts, result_key='title', id_key='id', score_cutoff=self.tmdb_fuzzy_cutoff)
                        if matched_dict:
                            matched_id = matched_dict.get('id')
                            log.debug(f"Fuzzy match found ID: {matched_id}. Finding original AsObj... [sync thread]")
                            try:
                                temp_movie_match = next((r for r in processed_results if getattr(r, 'id', None) == matched_id), None)
                            except Exception as e_find:
                                log.error(f"Error finding movie AsObj after fuzzy match [sync thread]: {e_find}", exc_info=True)
                                temp_movie_match = None

                            if temp_movie_match:
                                log.debug(f"Found original movie AsObj via fuzzy match: {getattr(temp_movie_match, 'title', 'N/A')} [sync thread]")
                                movie_match = temp_movie_match
                            else:
                                log.error(f"Fuzzy match dict found, but could not find corresponding movie AsObj with ID {matched_id} [sync thread].")
                        else:
                            log.debug("Fuzzy matching did not find a suitable TMDB movie match [sync thread].")
                    else:
                        log.warning("Could not convert TMDB movie results to dicts for fuzzy matching [sync thread].")

                # Fallback to 'first' strategy if fuzzy failed or wasn't used
                if not movie_match:
                    if self.tmdb_strategy == 'fuzzy':
                        log.warning("Fuzzy match failed or unavailable for movie, falling back to 'first' [sync thread].")
                    log.debug("Using 'first' result strategy for TMDB movie [sync thread].")
                    try:
                        movie_match = next(iter(processed_results), None)
                    except Exception as e_first:
                        log.error(f"Error getting first TMDB movie result [sync thread]: {e_first}", exc_info=True)
                    if movie_match:
                        log.debug(f"Selected first TMDB movie result: {getattr(movie_match, 'title', 'N/A')} [sync thread]")
                    else:
                        log.debug("No first movie result found after filtering [sync thread].")

            # 4. Check if a match was found
            if not movie_match:
                log.debug(f"No suitable TMDB movie match found for '{title}' [sync thread].")
                return None, None # Return tuple indicating no match

            movie_id = getattr(movie_match, 'id', None)
            if not movie_id:
                log.error(f"TMDB movie match lacks 'id' [sync thread]: {movie_match}")
                return None, None # Return tuple indicating error

            log.debug(f"TMDB matched movie '{getattr(movie_match, 'title', 'N/A')}' ID: {movie_id} [sync thread]")

            # Initialize variables for details
            movie_details = None
            combined_data_for_ids = {}
            ids = {}

            # 5. Fetch Movie Details
            try:
                movie_details = search.details(movie_id) # Blocking Call 2
                log.debug(f"Fetched TMDB movie details ID {movie_id}: {type(movie_details)} [sync thread]")
            except Exception as e_details:
                log.error(f"Failed to fetch TMDB movie details ID {movie_id} [sync thread]: {e_details}", exc_info=True)
                # Can proceed without details, using search result as final_data_obj

            final_data_obj = movie_details if movie_details else movie_match

            # 6. Combine Data for External ID Fetching
            if final_data_obj:
                 try:
                     # Attempt to get full data dict if available
                     if hasattr(final_data_obj, '_data') and isinstance(final_data_obj._data, dict):
                         combined_data_for_ids = final_data_obj._data.copy()
                         log.debug("Created combined_data_for_ids from _data attribute [sync thread].")
                     else:
                         # Fallback: Manually create dict
                         combined_data_for_ids = {
                             'id': movie_id,
                             'title': getattr(final_data_obj, 'title', None),
                             'release_date': getattr(final_data_obj, 'release_date', None)
                             # Add other fields needed by get_external_ids if any
                         }
                         log.debug(f"Created combined_data_for_ids from attributes of {type(final_data_obj)} [sync thread].")

                     # 7. Fetch External IDs (needs a dict or object with 'external_ids' or method)
                     ext_ids_data = {}
                     try:
                         # Prefer fetching from details if available and callable
                         # Note: Movie details object might have external_ids as attribute, not method
                         if movie_details:
                            ext_ids_method = getattr(movie_details, 'external_ids', None)
                            if callable(ext_ids_method):
                                 ext_ids_data = ext_ids_method() # Blocking Call 3a (Method)
                            elif isinstance(ext_ids_method, dict): # Or check if it's already a dict
                                 ext_ids_data = ext_ids_method
                            else:
                                 # Fallback to search object's external_ids method if details didn't have it
                                 ext_ids_data = search.external_ids(movie_id) # Blocking Call 3b
                         else:
                             # Fallback if no details were fetched
                             ext_ids_data = search.external_ids(movie_id) # Blocking Call 3b

                     except Exception as e_ext:
                         log.warning(f"Failed to fetch TMDB external IDs movie ID {movie_id} [sync thread]: {e_ext}")

                     # Add fetched IDs to the combined data dict for the helper function
                     combined_data_for_ids['external_ids'] = ext_ids_data

                 except Exception as e_comb:
                     log.error(f"Error creating combined_data_for_ids from final_data_obj [sync thread]: {e_comb}")

            # 8. Extract final IDs using the helper function
            ids = get_external_ids(tmdb_obj=combined_data_for_ids)

            log.debug(f"_sync_tmdb_movie_fetch returning: data type={type(final_data_obj)}, ids={ids}")
            # Return the most detailed object (details or match) and the extracted IDs
            return final_data_obj, ids
        # --- End of _sync_tmdb_movie_fetch function ---

        # Run the synchronous function in an executor
        try:
            result = await self._run_sync(_sync_tmdb_movie_fetch)
            # Ensure we return a 2-tuple, even if sync returned None
            if result is None:
                return None, None
            # Result should be (final_data_obj, ids)
            return result
        except Exception as e:
            # Handle potential errors from the executor itself or re-raised tenacity errors
            log.error(f"Error executing sync TMDB movie fetch for '{title}' ({year}): {e}", exc_info=True)
            raise # Re-raise exception to be caught by fetch_movie_metadata

# --- END OF FILE metadata_fetcher.py ---