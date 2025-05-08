# --- START OF FILE metadata_fetcher.py ---

import logging
import time
import asyncio
from functools import wraps, partial
from pathlib import Path
from typing import Optional, Tuple, TYPE_CHECKING, Any, Iterable, Sequence, Dict, cast, List

# --- API Client Imports ---
from .api_clients import get_tmdb_client, get_tvdb_client

# --- Local Imports ---
from .exceptions import MetadataError
from .models import MediaMetadata # Ensure MediaMetadata is imported

log = logging.getLogger(__name__)

# --- Optional Dependency Imports & Flags ---
try:
    import diskcache
    DISKCACHE_AVAILABLE = True
except ImportError:
    DISKCACHE_AVAILABLE = False
    diskcache = None # Define as None if not available

try:
    import platformdirs
    PLATFORMDIRS_AVAILABLE = True
except ImportError:
    PLATFORMDIRS_AVAILABLE = False

try:
    from thefuzz import process as fuzz_process
    THEFUZZ_AVAILABLE = True
except ImportError:
    THEFUZZ_AVAILABLE = False

try:
    from tenacity import RetryError, AsyncRetrying, stop_after_attempt, wait_fixed, retry_if_exception # Consider using tenacity if desired
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False
    RetryError = Exception # Define fallback

try:
    import dateutil.parser
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False

try:
    import requests.exceptions as req_exceptions
except ImportError:
    # Define dummy exceptions if requests is not installed
    class req_exceptions:
        ConnectionError=IOError
        Timeout=IOError
        RequestException=IOError
        HTTPError=type('HTTPError',(IOError,),{'response':type('MockResponse',(),{'status_code':0})()})

try:
    from tmdbv3api import Movie, TV, Season
    from tmdbv3api.exceptions import TMDbException
    from tmdbv3api.as_obj import AsObj # Import AsObj for type checking
    TMDBV3API_AVAILABLE = True
except ImportError:
    TMDbException = type('TMDbException', (Exception,), {}) # Define dummy exception
    AsObj = object # Define fallback type
    # Define dummy classes if tmdbv3api is missing to avoid NameErrors later
    class Movie: pass
    class TV: pass
    class Season: pass
    TMDBV3API_AVAILABLE = False


# --- Helper Classes/Functions ---

class AsyncRateLimiter:
    """Simple async rate limiter."""
    def __init__(self, delay: float):
        self.delay = delay
        self.last_call = 0
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
    """Determines if an API error is potentially temporary and worth retrying."""
    if isinstance(exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)):
        log.debug(f"Retry check PASSED for Connection/Timeout Error: {type(exception).__name__}")
        return True
    if isinstance(exception, req_exceptions.HTTPError):
        status_code = getattr(getattr(exception, 'response', None), 'status_code', 0)
        if status_code == 429: log.warning(f"Retry check PASSED for HTTP 429 (Rate Limit)."); return True
        if 500 <= status_code <= 599: log.warning(f"Retry check PASSED for HTTP {status_code} (Server Error)."); return True
        # Non-retryable HTTP errors
        if status_code == 401: log.error(f"Retry check FAILED for HTTP 401 (Unauthorized - Check API Key)."); return False
        if status_code == 403: log.error(f"Retry check FAILED for HTTP 403 (Forbidden - Check API Key/Permissions)."); return False
        if status_code == 404: log.debug(f"Retry check FAILED for HTTP 404 (Not Found)."); return False
        log.debug(f"Retry check FAILED for other HTTP Status Code: {status_code}"); return False

    if TMDBV3API_AVAILABLE and isinstance(exception, TMDbException):
        msg_lower = str(exception).lower()
        if "invalid api key" in msg_lower or "authentication failed" in msg_lower:
            log.error(f"Retry check FAILED for TMDbException (API Key Issue): {exception}"); return False
        if "resource not found" in msg_lower or "could not be found" in msg_lower:
            log.debug(f"Retry check FAILED for TMDbException (Not Found): {exception}"); return False
        log.warning(f"Retry check PASSED (tentative) for TMDbException: {exception}")
        return True # Let's retry other TMDb errors for now

    # Check for TVDB specific errors (based on common patterns in tvdb-v4-official exceptions)
    if isinstance(exception, (ValueError, Exception)): # Catch broader errors that might originate from TVDB client
        msg = str(exception).lower()
        if "failed to get" in msg and ("not found" in msg or "no record" in msg or "invalid id" in msg or "404" in msg):
            log.debug(f"Retry check FAILED for TVDB (likely Not Found): {msg}"); return False
        if "unauthorized" in msg or "api key" in msg or "401" in msg:
            log.error(f"Retry check FAILED for TVDB (likely API Key/Auth Issue): {msg}"); return False
        # Check for common server error codes or timeouts in the message
        if '500' in msg or '502' in msg or '503' in msg or '504' in msg or 'timeout' in msg:
            log.warning(f"Retry check PASSED for potential TVDB Server Error/Timeout: {type(exception).__name__}: {msg}")
            return True
        # Internal errors are unlikely to be resolved by retrying
        if isinstance(exception, (AttributeError, TypeError, UnboundLocalError)):
             log.error(f"Retry check FAILED for Internal Error ({type(exception).__name__}): {msg}"); return False

        log.debug(f"Retry check FAILED for generic ValueError/Exception (TVDB?): {type(exception).__name__}: {msg}"); return False

    log.debug(f"Retry check FAILED by default for: {type(exception).__name__}"); return False

def find_best_match(title_to_find: str, api_results_tuple: Tuple[Dict, ...], result_key: str ='title', id_key: str ='id', score_cutoff: int = 70) -> Optional[Tuple[Dict, Optional[float]]]:
    """Finds the best fuzzy match from a tuple of API result dictionaries."""
    if not api_results_tuple: return None
    first_result_dict = next(iter(api_results_tuple), None) # Get first item as fallback

    if not THEFUZZ_AVAILABLE:
        log.debug("Fuzzy matching unavailable ('thefuzz' not installed). Returning first result.")
        return first_result_dict, None # Return the first dict and None score

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
                choices[current_id] = str(current_result_val) # Ensure value is string
            else:
                log.debug(f"Skipping item due to missing id ('{id_key}') or result ('{result_key}'): {r_dict}")
    except Exception as e_choices:
        log.error(f"Error creating choices dict for fuzzy matching: {e_choices}", exc_info=True)
        return first_result_dict, None # Fallback on error

    if not choices:
        log.debug("No valid choices built for fuzzy matching. Returning first result.")
        return first_result_dict, None

    best_match_dict = None; best_score = None
    try:
        if not isinstance(title_to_find, str): title_to_find = str(title_to_find) # Ensure input is string
        # extractBests expects dict of {key: value_string}
        best_result_list = fuzz_process.extractBests(title_to_find, choices, score_cutoff=score_cutoff, limit=1)

        if best_result_list:
             # Result is [(value_string, score, key)]
             matched_value, score, best_id = best_result_list[0]
             best_score = float(score)
             log.debug(f"Fuzzy match '{title_to_find}': Found '{matched_value}' (ID:{best_id}) score {best_score:.1f}")
             # Find the original dictionary corresponding to the best_id
             best_match_dict = next((r_dict for r_dict in api_results_tuple if isinstance(r_dict, dict) and str(r_dict.get(id_key)) == str(best_id)), None)
             if not best_match_dict:
                 log.error(f"Fuzzy match found ID {best_id} but couldn't find corresponding dict in original results.")
                 best_match_dict = first_result_dict # Fallback
                 best_score = None # Reset score as it's not the fuzzy match
        else:
            log.warning(f"Fuzzy match failed for '{title_to_find}' (cutoff {score_cutoff}). Falling back to first result.")
            best_match_dict = first_result_dict
            best_score = None

    except Exception as e_fuzz:
        log.error(f"Error during fuzzy matching process: {e_fuzz}", exc_info=True)
        best_match_dict = first_result_dict # Fallback on error
        best_score = None

    return best_match_dict, best_score

def get_external_ids(tmdb_obj: Optional[Any] = None, tvdb_obj: Optional[Any] = None) -> Dict[str, Any]:
    """Extracts common external IDs (IMDb, TMDB, TVDB, Collection) from API objects/dicts."""
    ids: Dict[str, Any] = {'imdb_id': None, 'tmdb_id': None, 'tvdb_id': None, 'collection_id': None, 'collection_name': None}

    # --- TMDB Parsing ---
    if tmdb_obj:
        try:
            tmdb_id_val = None
            if isinstance(tmdb_obj, dict): tmdb_id_val = tmdb_obj.get('id')
            elif hasattr(tmdb_obj, 'id'): tmdb_id_val = getattr(tmdb_obj, 'id', None)

            if tmdb_id_val is not None: ids['tmdb_id'] = int(tmdb_id_val) # Ensure int

            # Get external_ids dictionary (handle direct dict or AsObj)
            ext_ids_data = {}
            if isinstance(tmdb_obj, dict): ext_ids_data = tmdb_obj.get('external_ids', {})
            elif hasattr(tmdb_obj, 'external_ids'):
                 ext_ids_attr = getattr(tmdb_obj, 'external_ids', None)
                 if isinstance(ext_ids_attr, dict): ext_ids_data = ext_ids_attr
                 # Handle case where external_ids is a callable method (less common now but safe)
                 elif callable(ext_ids_attr):
                     try: ext_ids_data = ext_ids_attr()
                     except Exception as e_call: log.debug(f"Error calling external_ids method on TMDB object: {e_call}")
            if not ext_ids_data: ext_ids_data = {} # Ensure it's a dict

            imdb_id_found = ext_ids_data.get('imdb_id')
            tvdb_id_found = ext_ids_data.get('tvdb_id')

            if imdb_id_found: ids['imdb_id'] = str(imdb_id_found)
            if tvdb_id_found and ids.get('tvdb_id') is None: # Prioritize TVDB source if available later
                try: ids['tvdb_id'] = int(tvdb_id_found)
                except (ValueError, TypeError, Exception): log.warning(f"Could not convert TMDB-provided TVDB ID '{tvdb_id_found}' to int.")

            # Get collection info (handle direct dict or AsObj)
            collection_info = None
            if isinstance(tmdb_obj, dict): collection_info = tmdb_obj.get('belongs_to_collection')
            elif hasattr(tmdb_obj, 'belongs_to_collection'): collection_info = getattr(tmdb_obj, 'belongs_to_collection', None)

            if isinstance(collection_info, (dict, AsObj)): # Check if it's a dict-like structure
                 col_id = None; col_name = None
                 if isinstance(collection_info, dict):
                     col_id = collection_info.get('id'); col_name = collection_info.get('name')
                 else: # Assuming AsObj
                     col_id = getattr(collection_info, 'id', None); col_name = getattr(collection_info, 'name', None)

                 if col_id:
                     try: ids['collection_id'] = int(col_id)
                     except (ValueError, TypeError, Exception): log.warning(f"Could not convert collection ID '{col_id}' to int.")
                 if col_name: ids['collection_name'] = str(col_name)

        except Exception as e_tmdb:
            log.warning(f"Unexpected error parsing TMDB IDs: {e_tmdb}", exc_info=True)

    # --- TVDB Parsing ---
    if tvdb_obj and isinstance(tvdb_obj, dict): # tvdb-v4-official primarily returns dicts
        try:
            if ids.get('tvdb_id') is None:
                 tvdb_id_val = tvdb_obj.get('id')
                 if tvdb_id_val is not None:
                     try: ids['tvdb_id'] = int(tvdb_id_val)
                     except (ValueError, TypeError, Exception): log.warning(f"Could not convert TVDB-provided TVDB ID '{tvdb_id_val}' to int.")

            # Prioritize remoteIds if present
            remote_ids = tvdb_obj.get('remoteIds', tvdb_obj.get('remote_ids', [])) # Allow both camelCase and snake_case
            imdb_found_in_remote = False
            if remote_ids and isinstance(remote_ids, list):
                 for remote in remote_ids:
                     if isinstance(remote, dict) and remote.get('sourceName') == 'IMDB':
                          imdb_id_found = remote.get('id')
                          if imdb_id_found:
                              # Only set if primary IMDb ID is missing
                              if ids.get('imdb_id') is None: ids['imdb_id'] = str(imdb_id_found)
                              imdb_found_in_remote = True; break # Found the preferred source

            # Fallback to top-level imdbId/imdb_id if not in remoteIds
            if not imdb_found_in_remote and ids.get('imdb_id') is None:
                 imdb_id_found = tvdb_obj.get('imdbId') or tvdb_obj.get('imdb_id')
                 if imdb_id_found: ids['imdb_id'] = str(imdb_id_found)

            # Look for TMDB ID if not already found
            if ids.get('tmdb_id') is None:
                 tmdb_id_found = tvdb_obj.get('tmdbId') or tvdb_obj.get('tmdb_id')
                 if tmdb_id_found:
                     try: ids['tmdb_id'] = int(tmdb_id_found)
                     except(ValueError, TypeError, Exception): log.warning(f"Could not convert TVDB-provided TMDB ID '{tmdb_id_found}' to int.")

        except Exception as e_tvdb_ids:
             log.warning(f"Error parsing external IDs from TVDB object: {e_tvdb_ids}", exc_info=True)

    # Clean up None values before returning
    return {k: v for k, v in ids.items() if v is not None}

def _tmdb_results_to_dicts(results_iterable: Optional[Iterable[Any]], result_type: str = 'movie') -> Tuple[Dict[str, Any], ...]:
    """Converts TMDB API AsObj results to simple dicts for matching."""
    if not results_iterable: return tuple()
    dict_list = []
    try:
        for item in results_iterable:
            if not item: continue
            item_dict = {} # Initialize dict inside the loop
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


# --- Main Fetcher Class ---

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
        self.cache_expire = self.cfg('cache_expire_seconds', 60 * 60 * 24 * 7) # Default 1 week
        if self.cache_enabled:
             if DISKCACHE_AVAILABLE:
                 cache_dir_config = self.cfg('cache_directory', None); cache_dir_path = None
                 if cache_dir_config: cache_dir_path = Path(cache_dir_config).resolve()
                 elif PLATFORMDIRS_AVAILABLE:
                     try: cache_dir_path = Path(platformdirs.user_cache_dir("rename_app", "rename_app_author"))
                     except Exception as e_pdirs: log.warning(f"Platformdirs failed to get cache dir: {e_pdirs}. Falling back.")
                 if not cache_dir_path:
                    cache_dir_path = Path(__file__).parent.parent / ".rename_cache"; log.warning(f"Could not determine platform cache directory. Using fallback: {cache_dir_path}")

                 if cache_dir_path:
                     try:
                         cache_dir_path.mkdir(parents=True, exist_ok=True)
                         self.cache = diskcache.Cache(str(cache_dir_path))
                         log.info(f"Persistent cache initialized at: {cache_dir_path} (Expiration: {self.cache_expire}s)")
                     except Exception as e:
                         log.error(f"Failed to initialize disk cache at '{cache_dir_path}': {e}. Disabling cache."); self.cache = None; self.cache_enabled = False
                 else: # Should not happen with fallback, but safety check
                     log.error("Could not determine a valid cache directory. Persistent caching disabled."); self.cache_enabled = False
             else:
                 log.warning("Persistent caching enabled, but 'diskcache' library not found. Caching disabled."); self.cache_enabled = False
        else:
            log.info("Persistent caching disabled by configuration.")

    def _get_year_from_date(self, date_str: Optional[str]) -> Optional[int]:
        """Safely extracts year from a date string."""
        if not date_str or not DATEUTIL_AVAILABLE: return None
        try:
            # Handle potential partial dates like 'YYYY' or 'YYYY-MM'
            if len(date_str) == 4 and date_str.isdigit(): return int(date_str)
            return dateutil.parser.parse(date_str).year
        except (ValueError, TypeError, OverflowError):
            log.debug(f"Could not parse year from date string: '{date_str}'")
            return None

    async def _run_sync(self, func, *args, **kwargs):
        """Runs a synchronous function in an executor."""
        loop = asyncio.get_running_loop()
        # Pass args and kwargs directly to the function within the executor
        return await loop.run_in_executor(None, func, *args, **kwargs)

    async def _get_cache(self, key: str) -> Optional[Any]:
        """Asynchronously gets data from the cache."""
        if not self.cache_enabled or not self.cache: return None
        _cache_miss = object()
        try:
            # Use _run_sync to run the synchronous diskcache get method
            cached_value = await self._run_sync(self.cache.get, key, default=_cache_miss)
            if cached_value is not _cache_miss:
                log.debug(f"Cache HIT for key: {key}")
                # Basic structure check (can be enhanced if needed)
                if isinstance(cached_value, tuple) and len(cached_value) >= 3:
                    return cached_value
                else:
                    log.warning(f"Cache data for {key} has unexpected structure. Ignoring cache.")
                    await self._run_sync(self.cache.delete, key) # Delete invalid entry
                    return None
            else:
                log.debug(f"Cache MISS for key: {key}");
                return None
        except Exception as e:
            log.warning(f"Error getting from cache key '{key}': {e}", exc_info=True)
            return None

    async def _set_cache(self, key: str, value: Any):
        """Asynchronously sets data in the cache."""
        if not self.cache_enabled or not self.cache: return
        # Basic structure check before caching
        if not isinstance(value, tuple) or len(value) < 3:
             log.error(f"Attempted to cache value with incorrect structure for key {key}. Aborting cache set.")
             return
        try:
            # Use _run_sync to run the synchronous diskcache set method
            await self._run_sync(self.cache.set, key, value, expire=self.cache_expire)
            log.debug(f"Cache SET for key: {key}")
        except Exception as e:
            log.warning(f"Error setting cache key '{key}': {e}", exc_info=True)

    # --- Synchronous Fetch Methods ---

    def _sync_tmdb_movie_fetch(self, sync_title, sync_year_guess, sync_lang):
        """Synchronous part of TMDB movie fetching."""
        log.debug(f"Executing TMDB Movie Fetch [sync thread] for: '{sync_title}' (year: {sync_year_guess}, lang: {sync_lang}, ...)")
        if not self.tmdb or not TMDBV3API_AVAILABLE:
            log.error("TMDB client/library not available in _sync_tmdb_movie_fetch [sync thread].")
            return None, None, None

        search = Movie(); results_obj = None; processed_results = None; movie_match = None; match_score = None
        try:
            if not isinstance(sync_title, str): sync_title = str(sync_title)
            # --- FIX: Remove year= keyword argument ---
            # results_obj = search.search(sync_title, year=search_year) # Network call - OLD
            results_obj = search.search(sync_title) # Network call - NEW
            # --- END FIX ---
            log.debug(f"TMDB raw movie search results [sync thread] for '{sync_title}': Count={len(results_obj) if hasattr(results_obj, '__len__') else 'N/A'}")
            if not results_obj:
                log.warning(f"TMDB Search returned no results for movie '{sync_title}'.")
                return None, None, None
            processed_results = results_obj
        except TMDbException as e_search:
             msg_lower = str(e_search).lower()
             if "resource not found" in msg_lower or "could not be found" in msg_lower or e_search.status_code == 404:
                 log.warning(f"TMDB Search resulted in 'Not Found' for movie '{sync_title}': {e_search}"); return None, None, None
             log.error(f"TMDbException during TMDB movie search for '{sync_title}' [sync thread]: {e_search}", exc_info=True); raise e_search
        except Exception as e_search:
            log.error(f"Unexpected error during TMDB movie search for '{sync_title}' [sync thread]: {e_search}", exc_info=True); raise e_search

        # Year Filtering (Now more important since search doesn't filter by year)
        if sync_year_guess and processed_results:
            log.debug(f"Applying year filter ({sync_year_guess} +/- {self.year_tolerance}) to TMDB movie results [sync thread].")
            filtered_list = []
            try:
                for r in processed_results:
                    if not isinstance(r, (dict, AsObj)): continue
                    result_year = None; release_date = getattr(r, 'release_date', None) if isinstance(r, AsObj) else r.get('release_date')
                    if release_date: result_year = self._get_year_from_date(str(release_date))

                    if result_year is not None and abs(result_year - sync_year_guess) <= self.year_tolerance:
                        log.debug(f"  -> Year filter PASSED for '{getattr(r, 'title', r.get('title', 'N/A'))}' ({result_year}) [sync thread]")
                        filtered_list.append(r)
                    else: log.debug(f"  -> Year filter FAILED for '{getattr(r, 'title', r.get('title', 'N/A'))}' ({result_year or 'N/A'}) [sync thread]")
                if not filtered_list and processed_results: log.debug(f"Year filtering removed all TMDB movie results, keeping original.")
                else: processed_results = filtered_list
                log.debug(f"Year filtering resulted in {len(processed_results)} TMDB movie results.")
            except Exception as e_filter: log.error(f"Error during TMDB movie year filtering: {e_filter}", exc_info=True); # Continue with original results

        # Matching Strategy
        if processed_results:
            results_as_dicts = _tmdb_results_to_dicts(processed_results, result_type='movie')
            if results_as_dicts:
                best_match_dict, score = None, None
                if self.tmdb_strategy == 'fuzzy' and THEFUZZ_AVAILABLE:
                    log.debug(f"Attempting TMDB movie fuzzy match (cutoff: {self.tmdb_fuzzy_cutoff}) [sync thread].")
                    best_match_dict, score = find_best_match(sync_title, results_as_dicts, result_key='title', id_key='id', score_cutoff=self.tmdb_fuzzy_cutoff)

                if not best_match_dict:
                    log.debug("Using 'first' result strategy for TMDB movie [sync thread].")
                    best_match_dict = next(iter(results_as_dicts), None)
                    score = None

                if best_match_dict:
                    matched_id = best_match_dict.get('id')
                    if matched_id:
                        # Find original object corresponding to matched dict
                        try: movie_match = next((r for r in processed_results if isinstance(r, (dict, AsObj)) and (getattr(r, 'id', None) if isinstance(r, AsObj) else r.get('id')) == matched_id), None)
                        except StopIteration: movie_match = None
                        match_score = score
                    if not movie_match:
                        log.warning(f"Could not find original object for matched movie dict ID {matched_id}. Using dict.")
                        movie_match = best_match_dict # Fallback to the dict itself

        # --- Final Checks, Details Fetch ---
        if not movie_match:
            log.warning(f"No suitable TMDB movie match found for '{sync_title}' (after filtering/matching)."); return None, None, None

        if not isinstance(movie_match, (dict, AsObj)):
            log.error(f"Final TMDB movie match for '{sync_title}' is not a valid object/dict type: {type(movie_match)} ({movie_match}). Skipping.")
            return None, None, None

        movie_id = getattr(movie_match, 'id', None) if isinstance(movie_match, AsObj) else movie_match.get('id')
        if not movie_id:
            log.error(f"Final TMDB movie match lacks 'id' or ID is None: {movie_match}"); return None, None, None

        log.debug(f"TMDB matched movie '{getattr(movie_match, 'title', movie_match.get('title', 'N/A'))}' ID: {movie_id} [sync thread] (Score: {match_score if match_score is not None else 'N/A'})")

        # Fetch Movie Details (includes external IDs, collection)
        movie_details = None; final_movie_data_obj = movie_match
        try:
            movie_details = search.details(movie_id, append_to_response="external_ids") # Network call
            if movie_details: final_movie_data_obj = movie_details
        except TMDbException as e_details:
             if "resource not found" in str(e_details).lower() or e_details.status_code == 404: log.warning(f"TMDB movie details for ID {movie_id} not found.")
             else: log.error(f"TMDbException fetching movie details ID {movie_id}: {e_details}"); raise e_details # Re-raise for retry logic
        except Exception as e_details: log.error(f"Unexpected error fetching movie details ID {movie_id}: {e_details}"); raise e_details

        # --- ID Extraction ---
        ids_dict = get_external_ids(tmdb_obj=final_movie_data_obj)

        log.debug(f"_sync_tmdb_movie_fetch returning: data type={type(final_movie_data_obj)}, ids={ids_dict}, score={match_score}")
        return final_movie_data_obj, ids_dict, match_score
    # --- END _sync_tmdb_movie_fetch ---

    def _sync_tmdb_series_fetch(self, sync_title, sync_season, sync_episodes, sync_year_guess, sync_lang):
        """Synchronous part of TMDB series fetching."""
        log.debug(f"Executing TMDB Series Fetch [sync thread] for: '{sync_title}' S{sync_season} E{sync_episodes} (lang: {sync_lang}, year: {sync_year_guess}, ...)")
        if not self.tmdb or not TMDBV3API_AVAILABLE:
            log.error("TMDB client/library not available in _sync_tmdb_series_fetch [sync thread].")
            return None, None, None, None

        search = TV(); results_obj = None; processed_results = None; show_match = None; match_score = None
        try:
            if not isinstance(sync_title, str): sync_title = str(sync_title)
            results_obj = search.search(sync_title) # Network call
            log.debug(f"TMDB raw series search results [sync thread] for '{sync_title}': Count={len(results_obj) if hasattr(results_obj, '__len__') else 'N/A'}")
            if not results_obj:
                log.warning(f"TMDB Search returned no results for series '{sync_title}'.")
                return None, None, None, None
            processed_results = results_obj
        except TMDbException as e_search:
             msg_lower = str(e_search).lower()
             if "resource not found" in msg_lower or "could not be found" in msg_lower or e_search.status_code == 404:
                 log.warning(f"TMDB Search resulted in 'Not Found' for series '{sync_title}': {e_search}"); return None, None, None, None
             log.error(f"TMDbException during TMDB series search for '{sync_title}' [sync thread]: {e_search}", exc_info=True); raise e_search
        except Exception as e_search:
            log.error(f"Unexpected error during TMDB series search for '{sync_title}' [sync thread]: {e_search}", exc_info=True); raise e_search

        # Year Filtering
        if sync_year_guess and processed_results:
            log.debug(f"Applying year filter ({sync_year_guess} +/- {self.year_tolerance}) to TMDB series results [sync thread].")
            filtered_list = []
            try:
                for r in processed_results:
                    if not isinstance(r, (dict, AsObj)): continue
                    result_year = None; first_air_date = getattr(r, 'first_air_date', None) if isinstance(r, AsObj) else r.get('first_air_date')
                    if first_air_date: result_year = self._get_year_from_date(str(first_air_date))

                    if result_year is not None and abs(result_year - sync_year_guess) <= self.year_tolerance:
                        log.debug(f"  -> Year filter PASSED for '{getattr(r, 'name', r.get('name', 'N/A'))}' ({result_year}) [sync thread]")
                        filtered_list.append(r)
                    else: log.debug(f"  -> Year filter FAILED for '{getattr(r, 'name', r.get('name', 'N/A'))}' ({result_year or 'N/A'}) [sync thread]")
                if not filtered_list and processed_results: log.debug(f"Year filtering removed all TMDB series results, keeping original.")
                else: processed_results = filtered_list
                log.debug(f"Year filtering resulted in {len(processed_results)} TMDB series results.")
            except Exception as e_filter: log.error(f"Error during TMDB series year filtering: {e_filter}", exc_info=True); # Continue with original results

        # Matching Strategy
        if processed_results:
            results_as_dicts = _tmdb_results_to_dicts(processed_results, result_type='series')
            if results_as_dicts:
                best_match_dict, score = None, None
                if self.tmdb_strategy == 'fuzzy' and THEFUZZ_AVAILABLE:
                    log.debug(f"Attempting TMDB series fuzzy match (cutoff: {self.tmdb_fuzzy_cutoff}) [sync thread].")
                    best_match_dict, score = find_best_match(sync_title, results_as_dicts, result_key='name', id_key='id', score_cutoff=self.tmdb_fuzzy_cutoff)

                if not best_match_dict:
                    log.debug("Using 'first' result strategy for TMDB series [sync thread].")
                    best_match_dict = next(iter(results_as_dicts), None)
                    score = None

                if best_match_dict:
                    matched_id = best_match_dict.get('id')
                    if matched_id:
                        try: show_match = next((r for r in processed_results if isinstance(r, (dict, AsObj)) and (getattr(r, 'id', None) if isinstance(r, AsObj) else r.get('id')) == matched_id), None)
                        except StopIteration: show_match = None
                        match_score = score
                    if not show_match:
                        log.warning(f"Could not find original object for matched series dict ID {matched_id}. Using dict.")
                        show_match = best_match_dict

        # --- Final Checks, Details & Episode Fetch ---
        if not show_match:
            log.warning(f"No suitable TMDB series match found for '{sync_title}' S{sync_season} (after filtering/matching)."); return None, None, None, None

        if not isinstance(show_match, (dict, AsObj)):
            log.error(f"Final TMDB series match for '{sync_title}' is not a valid object/dict type: {type(show_match)} ({show_match}). Skipping.")
            return None, None, None, None

        show_id = getattr(show_match, 'id', None) if isinstance(show_match, AsObj) else show_match.get('id')
        if not show_id:
            log.error(f"Final TMDB series match lacks 'id' or ID is None: {show_match}"); return None, None, None, None

        log.debug(f"TMDB matched series '{getattr(show_match, 'name', show_match.get('name', 'N/A'))}' ID: {show_id} [sync thread] (Score: {match_score if match_score is not None else 'N/A'})")

        # Fetch Show Details
        show_details = None; final_show_data_obj = show_match
        try:
            # Append external_ids to get IMDb/TVDB IDs in the same request
            show_details = search.details(show_id, append_to_response="external_ids") # Network call
            if show_details: final_show_data_obj = show_details
        except TMDbException as e_details:
             if "resource not found" in str(e_details).lower() or e_details.status_code == 404: log.warning(f"TMDB series details for ID {show_id} not found.")
             else: log.error(f"TMDbException fetching series details ID {show_id}: {e_details}"); raise e_details
        except Exception as e_details: log.error(f"Unexpected error fetching series details ID {show_id}: {e_details}"); raise e_details

        # Fetch Episode Details
        ep_data: Dict[int, Any] = {}
        if sync_episodes: # Only fetch if episodes were requested
            try:
                log.debug(f"Fetching TMDB season {sync_season} details for show ID {show_id}")
                season_fetcher = Season(); season_details = season_fetcher.details(tv_id=show_id, season_num=sync_season) # Network call

                # Safely access episodes using getattr
                episodes_list = getattr(season_details, 'episodes', [])

                if episodes_list: # Check if the list (potentially empty) exists
                    episodes_in_season = {}
                    for api_ep in episodes_list:
                        ep_num_api = getattr(api_ep, 'episode_number', None)
                        if ep_num_api is not None:
                            try: episodes_in_season[int(ep_num_api)] = api_ep
                            except (ValueError, TypeError): pass
                    for ep_num_needed in sync_episodes:
                        episode_obj = episodes_in_season.get(ep_num_needed)
                        if episode_obj: ep_data[ep_num_needed] = episode_obj # Store the AsObj/dict
                        else: log.warning(f"TMDB S{sync_season} E{ep_num_needed} not found for '{getattr(final_show_data_obj, 'name', final_show_data_obj.get('name', 'N/A'))}'")
                else:
                    season_name = getattr(season_details, 'name', f'S{sync_season}')
                    log.warning(f"TMDB season details '{season_name}' ID {show_id} lacks 'episodes' list or attribute.")
            except TMDbException as e_season:
                if "resource not found" in str(e_season).lower() or e_season.status_code == 404: log.warning(f"TMDB season S{sync_season} for ID {show_id} not found.")
                else: log.warning(f"TMDbException getting season S{sync_season} ID {show_id}: {e_season}") # Don't raise, fallback might work
            except Exception as e_season: log.warning(f"Unexpected error getting season S{sync_season} ID {show_id}: {e_season}")

        # --- ID Extraction ---
        ids = get_external_ids(tmdb_obj=final_show_data_obj)

        log.debug(f"_sync_tmdb_series_fetch returning: data type={type(final_show_data_obj)}, ep_map keys={list(ep_data.keys())}, ids={ids}, score={match_score}")
        return final_show_data_obj, ep_data, ids, match_score
    # --- END _sync_tmdb_series_fetch ---

    def _sync_tvdb_series_fetch(self, sync_title, sync_season_num, sync_episodes, sync_tvdb_id, sync_year_guess, sync_lang):
        """Synchronous part of TVDB series fetching."""
        # ... (This method remains the same as provided in the previous response) ...
        log.debug(f"Executing TVDB Series Fetch [sync thread] for: '{sync_title}' S{sync_season_num} E{sync_episodes} (lang: {sync_lang}, year: {sync_year_guess}, id: {sync_tvdb_id}, ...)")
        if not self.tvdb:
            log.error("TVDB client not available in _sync_tvdb_series_fetch [sync thread].")
            return None, None, None, None

        show_data: Optional[Dict] = None; best_match_id = sync_tvdb_id; search_results: Optional[List[Dict]] = None; match_score = None

        # --- Search/ID Fetching Logic ---
        if not best_match_id:
            try:
                log.debug(f"TVDB searching for: '{sync_title}' (Year guess: {sync_year_guess}) [sync thread]")
                search_results = self.tvdb.search(sync_title) # Network call
                log.debug(f"TVDB search returned {len(search_results) if search_results else 0} results [sync thread].")
                if not search_results:
                    log.warning(f"TVDB Search returned no results for series '{sync_title}'."); return None, None, None, None
            except (ValueError, Exception) as e_search:
                 msg = str(e_search).lower()
                 if "failed to get" in msg and ("not found" in msg or "no record" in msg or "invalid id" in msg or "404" in msg):
                     log.warning(f"TVDB Search resulted in 'Not Found' for series '{sync_title}': {e_search} [sync thread]."); return None, None, None, None
                 log.warning(f"TVDB search failed for '{sync_title}': {type(e_search).__name__}: {e_search} [sync thread]", exc_info=False);
                 raise e_search

            if search_results:
                # Year Filtering
                if sync_year_guess:
                    filtered_results = []
                    for r in search_results:
                        if not isinstance(r, dict): continue # Skip non-dicts
                        result_year_str = r.get('year')
                        if result_year_str:
                            try:
                                result_year = int(result_year_str)
                                if abs(result_year - sync_year_guess) <= self.year_tolerance: filtered_results.append(r)
                            except (ValueError, TypeError): pass
                    if not filtered_results and search_results: log.debug("TVDB year filtering removed all results, keeping original.")
                    else: search_results = filtered_results
                    log.debug(f"TVDB results after year filter: {len(search_results)}.")

                # Matching logic
                if search_results:
                    try:
                         best_match_dict, score = find_best_match(sync_title, tuple(search_results), result_key='name', id_key='tvdb_id', score_cutoff=70)
                         if best_match_dict:
                             matched_id_val = best_match_dict.get('tvdb_id');
                             if matched_id_val:
                                 try: best_match_id = int(matched_id_val); match_score = score
                                 except (ValueError, TypeError): log.warning(f"Could not convert matched TVDB ID '{matched_id_val}' to int.")
                    except Exception as e_fuzz:
                        log.error(f"Error during TVDB fuzzy match: {e_fuzz}")
                        first = next(iter(search_results), None)
                        if first and isinstance(first, dict):
                             first_id = first.get('tvdb_id')
                             if first_id:
                                 try: best_match_id = int(first_id)
                                 except (ValueError, TypeError): log.warning(f"Could not convert first result TVDB ID '{first_id}' to int.")

            if not best_match_id: log.warning(f"TVDB could not find suitable match ID for series '{sync_title}' after search."); return None, None, None, None


        # --- Fetch Extended Show Data ---
        if best_match_id:
            try:
                log.debug(f"TVDB fetching extended series data for ID: {best_match_id} [sync thread]")
                show_data = self.tvdb.get_series_extended(best_match_id) # Network call
                if not show_data or not isinstance(show_data, dict):
                    log.warning(f"TVDB get_series_extended for ID {best_match_id} returned invalid data: {type(show_data)}"); return None, None, None, None
                log.debug(f"TVDB successfully fetched extended data for: {show_data.get('name', 'N/A')} (Score: {match_score if match_score is not None else 'N/A'})")
            except (ValueError, Exception) as e_fetch:
                 msg = str(e_fetch).lower()
                 if "failed to get" in msg and ("not found" in msg or "no record" in msg or "invalid id" in msg or "404" in msg):
                     log.warning(f"TVDB get_series_extended failed for ID {best_match_id}: Not Found. Error: {e_fetch}"); return None, None, None, None
                 log.warning(f"TVDB get_series_extended failed for ID {best_match_id}: {type(e_fetch).__name__}: {e_fetch}", exc_info=False);
                 raise e_fetch
        else:
            log.error(f"Internal logic error: best_match_id became None before fetching extended data for '{sync_title}'.")
            return None, None, None, None

        # --- Extract Episode Data and IDs ---
        ep_data: Dict[int, Any] = {}; ids: Dict[str, Any] = {}
        if show_data:
            try:
                log.debug(f"TVDB fetching ALL episodes for show ID {best_match_id} (pagination may occur) [sync thread]")
                all_episodes_list = []
                page = 0
                while True: # Loop for pagination
                    episodes_page_data = self.tvdb.get_series_episodes(best_match_id, page=page, lang=sync_lang) # Network call per page
                    if episodes_page_data and isinstance(episodes_page_data.get('episodes'), list):
                        page_episodes = episodes_page_data['episodes']
                        all_episodes_list.extend(page_episodes)
                        log.debug(f"  Fetched page {page}, {len(page_episodes)} episodes.")
                        # Check pagination links (adapt based on actual library behavior)
                        links = self.tvdb.get_req_links() # Get links from the last request
                        if links and links.get('next'):
                             page += 1 # Increment page number for the next request
                             log.debug(f"  Found 'next' link, fetching page {page}...")
                             # Optional: Add a small sleep here if hitting rate limits between pages
                             # time.sleep(0.1)
                        else:
                            log.debug("  No 'next' link found or links structure unexpected. Assuming end of pages.")
                            break # Exit pagination loop
                    else:
                        log.warning(f"TVDB episodes data invalid or missing 'episodes' key for page {page}, show ID {best_match_id}. Stopping pagination.")
                        break # Exit pagination loop on error or bad data

                log.debug(f"Total episodes fetched for show ID {best_match_id}: {len(all_episodes_list)}")

                # Filter fetched episodes for the desired season and episode numbers
                episodes_in_season: Dict[int, Dict] = {}
                for ep_dict in all_episodes_list:
                    if isinstance(ep_dict, dict):
                        api_season_num = ep_dict.get('seasonNumber')
                        api_ep_num = ep_dict.get('number')
                        # Check if season matches and episode number is valid
                        if api_season_num is not None and api_ep_num is not None:
                            try:
                                if int(api_season_num) == int(sync_season_num):
                                    episodes_in_season[int(api_ep_num)] = ep_dict
                            except (ValueError, TypeError):
                                log.warning(f"Could not parse season/episode number from TVDB episode dict: {ep_dict}")

                # Populate ep_data with the matched episodes
                episode_iterator = sync_episodes if sync_episodes else []
                for ep_num in episode_iterator:
                    episode_details = episodes_in_season.get(ep_num)
                    if episode_details: ep_data[ep_num] = episode_details
                    else: log.warning(f"TVDB S{sync_season_num} E{ep_num} not found in fetched episodes for '{show_data.get('name')}'")

            except (ValueError, Exception) as e_ep_fetch:
                 msg = str(e_ep_fetch).lower()
                 if "not found" in msg or "404" in msg: log.warning(f"TVDB episodes fetch failed for ID {best_match_id}: Not Found.")
                 else: log.warning(f"TVDB error fetching/processing episode data for S{sync_season_num}, ID {best_match_id}: {e_ep_fetch}", exc_info=False)

            # Extract IDs
            try: ids = get_external_ids(tvdb_obj=show_data)
            except Exception as e_ids: log.warning(f"Error extracting external IDs from TVDB data: {e_ids}", exc_info=True)

        log.debug(f"_sync_tvdb_series_fetch returning: data type={type(show_data)}, ep_map keys={list(ep_data.keys())}, ids={ids}, score={match_score}")
        return show_data, ep_data, ids, match_score
    # --- END _sync_tvdb_series_fetch ---

    # --- Async Fetch Methods ---

    async def _do_fetch_tmdb_movie(self, title_arg, year_arg, lang='en'):
        """Async wrapper for TMDB movie fetch with retries."""
        attempts_cfg = self.cfg('api_retry_attempts', 3); wait_sec_cfg = self.cfg('api_retry_wait_seconds', 2)
        max_attempts = max(1, attempts_cfg if attempts_cfg is not None else 3); wait_seconds = max(0, wait_sec_cfg if wait_sec_cfg is not None else 2)
        last_exception = None

        for attempt in range(max_attempts):
            try:
                log.debug(f"Attempt {attempt + 1}/{max_attempts} for TMDB movie: '{title_arg}' ({year_arg})")
                # Call the CORRECT instance method via _run_sync
                data_obj, ids_dict, score = await self._run_sync(self._sync_tmdb_movie_fetch, title_arg, year_arg, lang)

                if data_obj is None:
                    log.info(f"TMDB movie '{title_arg}' ({year_arg}) not found or no match after filtering.")
                    return None, None, None # Return None if not found
                return data_obj, ids_dict, score
            except Exception as e:
                last_exception = e
                user_facing_error = None; should_stop_retries = False
                error_context = f"Movie: '{title_arg}'"

                # --- Consolidated Error Handling & Retry Logic ---
                if isinstance(e, (TMDbException, req_exceptions.HTTPError)):
                    msg_lower = str(e).lower(); status_code = 0
                    if isinstance(e, req_exceptions.HTTPError): status_code = getattr(getattr(e, 'response', None), 'status_code', 0)

                    if "invalid api key" in msg_lower or status_code == 401 or "authentication failed" in msg_lower:
                        user_facing_error = f"Invalid TMDB API Key or Authentication Failed ({error_context}). Please check your key."
                        log.error(f"{user_facing_error} Details: {e}"); should_stop_retries = True
                    elif status_code == 403:
                        user_facing_error = f"TMDB API request forbidden ({error_context}). Check API key permissions."
                        log.error(f"{user_facing_error} Details: {e}"); should_stop_retries = True
                    elif "resource not found" in msg_lower or status_code == 404:
                        log.warning(f"TMDB resource not found {error_context}. Error: {e}")
                        return None, None, None # Definitive not found, stop retries
                    else: log.warning(f"Attempt {attempt + 1} TMDB API error ({error_context}): {type(e).__name__}: {e}")

                elif isinstance(e, (req_exceptions.ConnectionError, req_exceptions.Timeout)):
                     log.warning(f"Attempt {attempt + 1} failed ({error_context}): Network connection error: {type(e).__name__}: {e}")
                else:
                     if isinstance(e, AttributeError) and '_sync_tmdb_movie_fetch' in str(e):
                         log.critical(f"INTERNAL ERROR: AttributeError persists after fix for _sync_tmdb_movie_fetch? Error: {e}", exc_info=True)
                         user_facing_error = f"Internal attribute error fetching TMDB metadata ({error_context})."
                         should_stop_retries = True
                     # Check for the TypeError from incorrect search arguments
                     elif isinstance(e, TypeError) and "got an unexpected keyword argument 'year'" in str(e):
                          log.critical(f"INTERNAL ERROR: Incorrect keyword argument 'year' passed to Movie.search. Error: {e}", exc_info=True)
                          user_facing_error = f"Internal error calling TMDB movie search function ({error_context})."
                          should_stop_retries = True
                     else:
                         log.warning(f"Attempt {attempt + 1} failed ({error_context}): Unexpected error: {type(e).__name__}: {e}", exc_info=True)

                if should_stop_retries:
                    raise MetadataError(user_facing_error or f"Unrecoverable error fetching TMDB metadata ({error_context}).") from e

                if not should_retry_api_error(e):
                    log.error(f"Non-retryable error occurred ({error_context}): {type(e).__name__}")
                    user_facing_error = user_facing_error or f"Non-retryable error fetching TMDB metadata ({error_context}). Details: {e}"
                    raise MetadataError(user_facing_error) from e

                # Retry logic
                if attempt < max_attempts - 1:
                    log.info(f"Retrying TMDB movie fetch for '{title_arg}' in {wait_seconds}s... ({attempt+1}/{max_attempts})")
                    await asyncio.sleep(wait_seconds)
                else:
                    log.error(f"All {max_attempts} retry attempts failed for TMDB movie '{title_arg}'. Last error: {last_exception}")
                    final_error_msg = f"Failed to fetch TMDB metadata ({error_context}) after {max_attempts} attempts."
                    # Add context based on last error type
                    if isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network connection."
                    elif isinstance(last_exception, req_exceptions.HTTPError) and 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely a temporary TMDB server issue."
                    elif isinstance(last_exception, TypeError) and "unexpected keyword argument 'year'" in str(last_exception): final_error_msg += " Internal library call error."
                    raise MetadataError(final_error_msg) from last_exception

        # Fallback return if loop somehow completes without success or specific error
        return None, None, None

    async def _do_fetch_tmdb_series(self, title_arg, season_arg, episodes_arg, year_guess_arg=None, lang='en'):
        """Async wrapper for TMDB series fetch with retries."""
        # ... (This method remains the same as provided in the previous response) ...
        attempts_cfg = self.cfg('api_retry_attempts', 3); wait_sec_cfg = self.cfg('api_retry_wait_seconds', 2)
        max_attempts = max(1, attempts_cfg if attempts_cfg is not None else 3); wait_seconds = max(0, wait_sec_cfg if wait_sec_cfg is not None else 2)
        last_exception = None

        for attempt in range(max_attempts):
            try:
                log.debug(f"Attempt {attempt + 1}/{max_attempts} for TMDB series: '{title_arg}' S{season_arg}")
                # Call the instance method via _run_sync
                show_obj, ep_map, ids_dict, score = await self._run_sync(self._sync_tmdb_series_fetch, title_arg, season_arg, episodes_arg, year_guess_arg, lang)

                if show_obj is None:
                    log.info(f"TMDB series '{title_arg}' S{season_arg} not found or no match.")
                    return None, None, None, None # Return None if not found
                return show_obj, ep_map, ids_dict, score
            except Exception as e:
                last_exception = e; user_facing_error = None; should_stop_retries = False
                error_context = f"Series: '{title_arg}' S{season_arg}"

                if isinstance(e, (TMDbException, req_exceptions.HTTPError)):
                    msg_lower = str(e).lower(); status_code = 0
                    if isinstance(e, req_exceptions.HTTPError): status_code = getattr(getattr(e, 'response', None), 'status_code', 0)
                    if "invalid api key" in msg_lower or status_code == 401 or "authentication failed" in msg_lower:
                        user_facing_error = f"Invalid TMDB API Key or Authentication Failed ({error_context}). Please check your key."
                        log.error(f"{user_facing_error} Details: {e}"); should_stop_retries = True
                    elif status_code == 403:
                        user_facing_error = f"TMDB API request forbidden ({error_context}). Check API key permissions."
                        log.error(f"{user_facing_error} Details: {e}"); should_stop_retries = True
                    elif "resource not found" in msg_lower or status_code == 404:
                        log.warning(f"TMDB resource not found ({error_context}). Error: {e}")
                        return None, None, None, None # Definitive not found
                    else: log.warning(f"Attempt {attempt + 1} TMDB API error ({error_context}): {type(e).__name__}: {e}")
                elif isinstance(e, (req_exceptions.ConnectionError, req_exceptions.Timeout)):
                     log.warning(f"Attempt {attempt + 1} failed ({error_context}): Network connection error: {type(e).__name__}: {e}")
                else:
                    log.warning(f"Attempt {attempt + 1} failed ({error_context}): Unexpected error: {type(e).__name__}: {e}", exc_info=True)
                    if isinstance(e, TypeError) and "_sync_tmdb_series_fetch() missing" in str(e):
                        log.critical(f"INTERNAL ERROR: _sync_tmdb_series_fetch called incorrectly? Error: {e}")
                        user_facing_error = f"Internal error calling sync TMDB function ({error_context})."
                        should_stop_retries = True

                if should_stop_retries: raise MetadataError(user_facing_error or f"Unrecoverable error fetching TMDB metadata ({error_context}).") from e
                if not should_retry_api_error(e):
                    log.error(f"Non-retryable error occurred ({error_context}).")
                    user_facing_error = user_facing_error or f"Non-retryable error fetching TMDB metadata ({error_context}). Details: {e}"
                    raise MetadataError(user_facing_error) from e

                if attempt < max_attempts - 1:
                    log.info(f"Retrying TMDB series fetch for '{title_arg}' S{season_arg} in {wait_seconds}s... ({attempt+1}/{max_attempts})")
                    await asyncio.sleep(wait_seconds)
                else:
                    log.error(f"All {max_attempts} retry attempts failed for TMDB series '{title_arg}' S{season_arg}. Last error: {last_exception}")
                    final_error_msg = f"Failed to fetch TMDB metadata ({error_context}) after {max_attempts} attempts."
                    if isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network connection."
                    elif isinstance(last_exception, req_exceptions.HTTPError) and 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely a temporary TMDB server issue."
                    raise MetadataError(final_error_msg) from last_exception
        return None, None, None, None # Fallback

    async def _do_fetch_tvdb_series(self, title_arg: str, season_num_arg: int, episodes_arg: tuple, tvdb_id_arg: Optional[int] = None, year_guess_arg: Optional[int] = None, lang: str = 'en'):
        """Async wrapper for TVDB series fetch with retries."""
        # ... (This method remains the same as provided in the previous response) ...
        attempts_cfg = self.cfg('api_retry_attempts', 3); wait_sec_cfg = self.cfg('api_retry_wait_seconds', 2)
        max_attempts = max(1, attempts_cfg if attempts_cfg is not None else 3); wait_seconds = max(0, wait_sec_cfg if wait_sec_cfg is not None else 2)
        last_exception = None

        for attempt in range(max_attempts):
            try:
                log.debug(f"Attempt {attempt + 1}/{max_attempts} for TVDB series: '{title_arg}' S{season_num_arg}")
                 # Call the instance method via _run_sync
                show_dict, ep_map, ids_dict, score = await self._run_sync(self._sync_tvdb_series_fetch, title_arg, season_num_arg, episodes_arg, tvdb_id_arg, year_guess_arg, lang)

                if show_dict is None:
                    log.info(f"TVDB series '{title_arg}' S{season_num_arg} not found or no match.")
                    return None, None, None, None # Return None if not found
                return show_dict, ep_map, ids_dict, score
            except Exception as e:
                last_exception = e; user_facing_error = None; should_stop_retries = False
                error_context = f"TVDB Series: '{title_arg}' S{season_num_arg}"
                msg_lower = str(e).lower()

                if "unauthorized" in msg_lower or "api key" in msg_lower or ("response" in msg_lower and "401" in msg_lower):
                     user_facing_error = f"Invalid TVDB API Key or Unauthorized ({error_context}). Please check your key."
                     log.error(f"{user_facing_error} Details: {e}"); should_stop_retries = True
                elif "failed to get" in msg_lower and ("not found" in msg_lower or "no record" in msg_lower or "404" in msg_lower):
                     log.warning(f"TVDB resource not found ({error_context}). Error: {e}")
                     return None, None, None, None # Definitive not found
                elif isinstance(e, (req_exceptions.ConnectionError, req_exceptions.Timeout)):
                     log.warning(f"Attempt {attempt + 1} failed ({error_context}): Network connection error to TVDB: {type(e).__name__}: {e}")
                elif isinstance(e, req_exceptions.HTTPError):
                     status_code = getattr(getattr(e, 'response', None), 'status_code', 0)
                     if status_code == 403:
                         user_facing_error = f"TVDB API request forbidden ({error_context}). Check API key permissions."
                         log.error(f"{user_facing_error} Details: {e}"); should_stop_retries = True
                     elif 500 <= status_code <= 599:
                         log.warning(f"Attempt {attempt+1} TVDB API server error ({status_code}) ({error_context}): {e}")
                     else: log.warning(f"Attempt {attempt + 1} TVDB API HTTP error ({status_code}) ({error_context}): {type(e).__name__}: {e}")
                else:
                    log.warning(f"Attempt {attempt + 1} failed ({error_context}): Unexpected TVDB error: {type(e).__name__}: {e}", exc_info=True)
                    if isinstance(e, TypeError) and "_sync_tvdb_series_fetch() missing" in str(e):
                        log.critical(f"INTERNAL ERROR: _sync_tvdb_series_fetch called incorrectly? Error: {e}")
                        user_facing_error = f"Internal error calling sync TVDB function ({error_context})."
                        should_stop_retries = True

                if should_stop_retries: raise MetadataError(user_facing_error or f"Unrecoverable error fetching TVDB metadata ({error_context}).") from e
                if not should_retry_api_error(e):
                    log.error(f"Non-retryable error occurred ({error_context}).")
                    user_facing_error = user_facing_error or f"Non-retryable error fetching TVDB metadata ({error_context}). Details: {e}"
                    raise MetadataError(user_facing_error) from e

                if attempt < max_attempts - 1:
                    log.info(f"Retrying TVDB series fetch for '{title_arg}' S{season_num_arg} in {wait_seconds}s... ({attempt+1}/{max_attempts})")
                    await asyncio.sleep(wait_seconds)
                else:
                    log.error(f"All {max_attempts} retry attempts failed for TVDB series '{title_arg}' S{season_num_arg}. Last error: {last_exception}")
                    final_error_msg = f"Failed to fetch TVDB metadata ({error_context}) after {max_attempts} attempts."
                    # Add context
                    if isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network connection."
                    elif isinstance(last_exception, req_exceptions.HTTPError) and 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely a temporary TVDB server issue."
                    elif isinstance(last_exception, ValueError) and "not found" in str(last_exception).lower(): final_error_msg = f"TVDB resource not found ({error_context}) after {max_attempts} attempts."

                    raise MetadataError(final_error_msg) from last_exception
        return None, None, None, None # Fallback

    # --- Public Fetch Methods ---

    async def fetch_movie_metadata(self, movie_title_guess: str, year_guess: Optional[int] = None) -> MediaMetadata:
        """Fetches movie metadata, prioritizing TMDB."""
        # ... (This method remains the same as provided in the previous response) ...
        log.debug(f"Fetching movie metadata (async) for: '{movie_title_guess}' (Year guess: {year_guess})")
        final_meta = MediaMetadata(is_movie=True)
        tmdb_movie_data: Optional[Any] = None
        tmdb_ids: Optional[Dict[str, Any]] = None
        tmdb_score: Optional[float] = None
        lang = self.cfg('tmdb_language', 'en')
        cache_key = f"movie::{movie_title_guess}_{year_guess}_{lang}"
        fetch_error_message = None # Store error message from fetch attempt

        cached_data = await self._get_cache(cache_key)
        if cached_data:
            tmdb_movie_data, tmdb_ids, tmdb_score = cached_data
            log.debug(f"Using cached data for movie: '{movie_title_guess}' (Score: {tmdb_score})")
        else:
            if not self.tmdb: log.warning("TMDB client not available, skipping TMDB movie fetch.")
            else:
                try:
                    await self.rate_limiter.wait()
                    tmdb_movie_data, tmdb_ids, tmdb_score = await self._do_fetch_tmdb_movie(movie_title_guess, year_guess, lang)
                    # Only cache if data was successfully retrieved
                    if tmdb_movie_data is not None:
                         await self._set_cache(cache_key, (tmdb_movie_data, tmdb_ids, tmdb_score))
                except MetadataError as me:
                    log.error(f"TMDB movie fetch failed: {me}")
                    fetch_error_message = str(me) # Store specific error
                    tmdb_movie_data, tmdb_ids, tmdb_score = None, None, None
                except Exception as e:
                     log.error(f"Unexpected error during TMDB movie fetch for '{movie_title_guess}': {type(e).__name__}: {e}", exc_info=True)
                     fetch_error_message = f"Unexpected error fetching TMDB movie: {e}" # Store generic error
                     tmdb_movie_data, tmdb_ids, tmdb_score = None, None, None

            # Check if fetch failed and no specific error message was already set
            if tmdb_movie_data is None and not fetch_error_message:
                 log.debug(f"TMDB fetch for '{movie_title_guess}' returned no movie data object (no specific error).")

        # Populate final_meta if data was found
        if tmdb_movie_data:
            try:
                final_meta.source_api = "tmdb"
                final_meta.match_confidence = tmdb_score
                title_val = getattr(tmdb_movie_data, 'title', None) if isinstance(tmdb_movie_data, AsObj) else tmdb_movie_data.get('title')
                release_date_val = getattr(tmdb_movie_data, 'release_date', None) if isinstance(tmdb_movie_data, AsObj) else tmdb_movie_data.get('release_date')

                final_meta.movie_title = title_val
                final_meta.release_date = str(release_date_val) if release_date_val else None
                final_meta.movie_year = self._get_year_from_date(final_meta.release_date)

                if isinstance(tmdb_ids, dict):
                     final_meta.ids = tmdb_ids
                     # Populate collection info directly from IDs dict if present
                     final_meta.collection_name = tmdb_ids.get('collection_name')
                     final_meta.collection_id = tmdb_ids.get('collection_id')
                else: final_meta.ids = {}

                log.debug(f"Successfully populated final_meta from TMDB for '{movie_title_guess}'. Score: {tmdb_score}")
            except Exception as e_populate:
                log.error(f"Error populating final_meta for '{movie_title_guess}' from TMDB data: {e_populate}", exc_info=True);
                final_meta.source_api = None # Mark as failed if population error occurs
                fetch_error_message = fetch_error_message or f"Error processing TMDB data: {e_populate}" # Store population error

        # If still no source API, it means fetch or population failed
        if not final_meta.source_api:
             log.warning(f"Metadata fetch or population ultimately failed for movie: '{movie_title_guess}' (Year guess: {year_guess})")
             # Set fallbacks
             if not final_meta.movie_title: final_meta.movie_title = movie_title_guess
             if not final_meta.movie_year: final_meta.movie_year = year_guess
             # Raise the error to the caller (main_processor)
             if fetch_error_message:
                 raise MetadataError(fetch_error_message)
             else:
                 # Raise a generic error if no specific message was caught
                 raise MetadataError(f"Failed to obtain valid metadata for movie '{movie_title_guess}'.")

        log.debug(f"fetch_movie_metadata returning final result for '{movie_title_guess}': Source='{final_meta.source_api}', Title='{final_meta.movie_title}', Year={final_meta.movie_year}, IDs={final_meta.ids}, Collection={final_meta.collection_name}, Score={final_meta.match_confidence}")
        return final_meta

    async def fetch_series_metadata(self, show_title_guess: str, season_num: int, episode_num_list: Tuple[int, ...], year_guess: Optional[int] = None) -> MediaMetadata:
        """Fetches series metadata, trying TMDB first then TVDB if needed."""
        # ... (This method remains the same as provided in the previous response) ...
        log.debug(f"Fetching series metadata (async) for: '{show_title_guess}' S{season_num}E{episode_num_list} (Year guess: {year_guess})")
        final_meta = MediaMetadata(is_series=True)
        tmdb_show_data, tmdb_ep_map, tmdb_ids, tmdb_score = None, None, None, None
        tvdb_show_data, tvdb_ep_map, tvdb_ids, tvdb_score = None, None, None, None
        tmdb_error_message = None
        tvdb_error_message = None

        lang = self.cfg('tmdb_language', 'en')
        episode_num_tuple = tuple(sorted(list(set(episode_num_list)))) # Ensure unique, sorted tuple
        cache_key_base = f"series::{show_title_guess}_{season_num}_{episode_num_tuple}_{year_guess}_{lang}"

        # --- Attempt TMDB ---
        if self.tmdb:
            tmdb_cache_key = cache_key_base + "::tmdb"
            cached_tmdb = await self._get_cache(tmdb_cache_key)
            if cached_tmdb:
                try:
                    tmdb_show_data, cached_tmdb_ids, tmdb_score = cached_tmdb
                    if cached_tmdb_ids and isinstance(cached_tmdb_ids, dict) and '_ep_map' in cached_tmdb_ids:
                        tmdb_ep_map = cached_tmdb_ids.pop('_ep_map') # Extract ep_map
                        tmdb_ids = cached_tmdb_ids # Remaining are IDs
                    else: # Handle older cache format or missing ep_map
                        tmdb_ids = cached_tmdb_ids if isinstance(cached_tmdb_ids, dict) else {}
                        tmdb_ep_map = {}
                    log.debug(f"Using cached TMDB data for series: '{show_title_guess}' S{season_num} (Score: {tmdb_score})")
                except (TypeError, ValueError, IndexError) as e_cache:
                    log.warning(f"Error unpacking TMDB cache data for {tmdb_cache_key}, ignoring cache: {e_cache}")
                    cached_tmdb = None # Invalidate cache on error
                    tmdb_show_data, tmdb_ep_map, tmdb_ids, tmdb_score = None, None, None, None
            else: # Not cached, fetch from API
                try:
                    await self.rate_limiter.wait()
                    tmdb_show_data, tmdb_ep_map, tmdb_ids, tmdb_score = await self._do_fetch_tmdb_series(
                        show_title_guess, season_num, episode_num_tuple, year_guess, lang
                    )
                    # Only cache if fetch was successful (show data exists)
                    if tmdb_show_data is not None:
                        cacheable_tmdb_ids = tmdb_ids or {}
                        # Store ep_map within the IDs dict for caching
                        cacheable_tmdb_ids['_ep_map'] = tmdb_ep_map or {}
                        await self._set_cache(tmdb_cache_key, (tmdb_show_data, cacheable_tmdb_ids, tmdb_score))
                        # Remove ep_map from tmdb_ids *after* caching
                        if '_ep_map' in cacheable_tmdb_ids:
                            tmdb_ep_map = cacheable_tmdb_ids.pop('_ep_map')
                            tmdb_ids = cacheable_tmdb_ids # Update tmdb_ids to exclude the map

                except MetadataError as me:
                    log.error(f"TMDB series fetch failed: {me}"); tmdb_error_message = str(me)
                    tmdb_show_data, tmdb_ep_map, tmdb_ids, tmdb_score = None, None, None, None
                except Exception as e:
                    log.error(f"Unexpected error during TMDB series fetch for '{show_title_guess}' S{season_num}: {type(e).__name__}: {e}", exc_info=True); tmdb_error_message = f"Unexpected TMDB error: {e}"
                    tmdb_show_data, tmdb_ep_map, tmdb_ids, tmdb_score = None, None, None, None
        else:
            log.warning("TMDB client not available, skipping TMDB series fetch.")

        # --- Determine if TVDB Fallback is Needed ---
        tmdb_ep_map = tmdb_ep_map or {} # Ensure it's a dict
        tmdb_has_all_requested_eps = not episode_num_tuple or all(ep_num in tmdb_ep_map for ep_num in episode_num_tuple)
        needs_tvdb_fallback = (not tmdb_show_data) or (episode_num_tuple and not tmdb_has_all_requested_eps)

        # --- Attempt TVDB (if needed and available) ---
        if needs_tvdb_fallback and self.tvdb:
            tvdb_cache_key = cache_key_base + "::tvdb"
            cached_tvdb = await self._get_cache(tvdb_cache_key)
            if cached_tvdb:
                try:
                    tvdb_show_data, cached_tvdb_ids, tvdb_score = cached_tvdb
                    if cached_tvdb_ids and isinstance(cached_tvdb_ids, dict) and '_ep_map' in cached_tvdb_ids:
                        tvdb_ep_map = cached_tvdb_ids.pop('_ep_map') # Extract ep_map
                        tvdb_ids = cached_tvdb_ids # Remaining are IDs
                    else: # Handle older cache format or missing ep_map
                        tvdb_ids = cached_tvdb_ids if isinstance(cached_tvdb_ids, dict) else {}
                        tvdb_ep_map = {}
                    log.debug(f"Using cached TVDB data for series: '{show_title_guess}' S{season_num} (Score: {tvdb_score})")
                except (TypeError, ValueError, IndexError) as e_cache:
                    log.warning(f"Error unpacking TVDB cache data for {tvdb_cache_key}, ignoring cache: {e_cache}")
                    cached_tvdb = None # Invalidate cache on error
                    tvdb_show_data, tvdb_ep_map, tvdb_ids, tvdb_score = None, None, None, None
            else: # Not cached, fetch from API
                log.debug(f"Attempting TVDB fallback for '{show_title_guess}' S{season_num} (async)...")
                tvdb_id_from_tmdb = tmdb_ids.get('tvdb_id') if tmdb_ids else None
                try:
                    await self.rate_limiter.wait()
                    tvdb_show_data, tvdb_ep_map, tvdb_ids, tvdb_score = await self._do_fetch_tvdb_series(
                        title_arg=show_title_guess, season_num_arg=season_num, episodes_arg=episode_num_tuple,
                        tvdb_id_arg=tvdb_id_from_tmdb, year_guess_arg=year_guess, lang=lang
                    )
                     # Only cache if fetch was successful
                    if tvdb_show_data is not None:
                        cacheable_tvdb_ids = tvdb_ids or {}
                        cacheable_tvdb_ids['_ep_map'] = tvdb_ep_map or {}
                        await self._set_cache(tvdb_cache_key, (tvdb_show_data, cacheable_tvdb_ids, tvdb_score))
                        # Remove ep_map from tvdb_ids *after* caching
                        if '_ep_map' in cacheable_tvdb_ids:
                             tvdb_ep_map = cacheable_tvdb_ids.pop('_ep_map')
                             tvdb_ids = cacheable_tvdb_ids

                except MetadataError as me:
                    log.error(f"TVDB series fetch failed: {me}"); tvdb_error_message = str(me)
                    tvdb_show_data, tvdb_ep_map, tvdb_ids, tvdb_score = None, None, None, None
                except Exception as e:
                    log.error(f"Unexpected error during TVDB series fetch for '{show_title_guess}' S{season_num}: {type(e).__name__}: {e}", exc_info=True); tvdb_error_message = f"Unexpected TVDB error: {e}"
                    tvdb_show_data, tvdb_ep_map, tvdb_ids, tvdb_score = None, None, None, None
        elif needs_tvdb_fallback:
            log.warning(f"TVDB client not available or fallback not triggered for '{show_title_guess}' S{season_num}, skipping TVDB series attempt.")

        # --- Select Primary Source and Merge Data ---
        final_meta.source_api = None; primary_show_data = None; primary_ep_map = None; merged_ids = {}; primary_score = None; final_error_message = None

        # Prioritize TMDB if it has the show and all requested episodes
        if tmdb_show_data and tmdb_has_all_requested_eps:
            log.debug(f"Using TMDB as primary source for '{show_title_guess}' S{season_num}.")
            final_meta.source_api = "tmdb"
            primary_show_data = tmdb_show_data
            primary_ep_map = tmdb_ep_map
            primary_score = tmdb_score
            if tmdb_ids: merged_ids.update(tmdb_ids)
            # Merge missing IDs from TVDB if available
            if tvdb_ids:
                 for k, v in tvdb_ids.items():
                     if v is not None and k not in merged_ids: merged_ids[k] = v
        # Fallback to TVDB if it was fetched successfully
        elif tvdb_show_data:
            log.debug(f"Using TVDB as primary source (fallback) for '{show_title_guess}' S{season_num}.")
            final_meta.source_api = "tvdb"
            primary_show_data = tvdb_show_data
            primary_ep_map = tvdb_ep_map
            primary_score = tvdb_score
            # Merge all IDs, prioritizing TVDB's values if both sources provided the same key
            if tmdb_ids: merged_ids.update(tmdb_ids)
            if tvdb_ids: merged_ids.update(tvdb_ids) # TVDB overwrites common keys if present
        # If TVDB failed but TMDB found the show (even if missing episodes), use TMDB
        elif tmdb_show_data:
            log.debug(f"Using TMDB (potentially incomplete episodes) as source for '{show_title_guess}' S{season_num}.")
            final_meta.source_api = "tmdb"
            primary_show_data = tmdb_show_data
            primary_ep_map = tmdb_ep_map
            primary_score = tmdb_score
            if tmdb_ids: merged_ids.update(tmdb_ids)
            # No TVDB data to merge here

        # --- Populate Final Metadata Object ---
        final_meta.ids = merged_ids # Use the merged IDs
        final_meta.match_confidence = primary_score

        if primary_show_data:
            try: # Wrap population in try-except
                # Handle AsObj or dict
                show_title_api = getattr(primary_show_data, 'name', None) if isinstance(primary_show_data, AsObj) else primary_show_data.get('name')
                final_meta.show_title = show_title_api

                # Get air date (handle different possible keys)
                show_air_date = None
                if isinstance(primary_show_data, AsObj): show_air_date = getattr(primary_show_data, 'first_air_date', None)
                elif isinstance(primary_show_data, dict): show_air_date = primary_show_data.get('firstAired') or primary_show_data.get('first_air_date')
                final_meta.show_year = self._get_year_from_date(str(show_air_date) if show_air_date else None)

                final_meta.season = season_num
                final_meta.episode_list = list(episode_num_tuple)

                # Populate episode details from the chosen primary source's map
                primary_ep_map = primary_ep_map or {} # Ensure dict
                if episode_num_tuple:
                    for ep_num in episode_num_tuple:
                        ep_details = primary_ep_map.get(ep_num)
                        if ep_details:
                            # Handle AsObj or dict for episode details
                            ep_title = None; air_date = None
                            if isinstance(ep_details, AsObj):
                                ep_title = getattr(ep_details, 'name', None)
                                air_date = getattr(ep_details, 'air_date', None)
                            elif isinstance(ep_details, dict):
                                ep_title = ep_details.get('name') or ep_details.get('episodeName')
                                air_date = ep_details.get('air_date') or ep_details.get('aired') # TVDB uses 'aired'

                            if ep_title: final_meta.episode_titles[ep_num] = str(ep_title)
                            if air_date: final_meta.air_dates[ep_num] = str(air_date)
                        else:
                            log.debug(f"Episode S{season_num}E{ep_num} not found in selected API map for '{show_title_guess}'.")

            except Exception as e_populate:
                 log.error(f"Error populating final_meta for series '{show_title_guess}': {e_populate}", exc_info=True);
                 final_meta.source_api = None # Mark as failed
                 # Preserve original fetch error message if it exists
                 final_error_message = tmdb_error_message or tvdb_error_message or f"Error processing API data: {e_populate}"

        # --- Handle Final Failure State ---
        if not final_meta.source_api:
             if not final_error_message: # Combine API fetch errors if population was ok but sources failed
                 if tmdb_error_message and tvdb_error_message: final_error_message = f"TMDB Error: {tmdb_error_message} | TVDB Error: {tvdb_error_message}"
                 elif tmdb_error_message: final_error_message = f"TMDB Error: {tmdb_error_message}"
                 elif tvdb_error_message: final_error_message = f"TVDB Error: {tvdb_error_message}"
                 else: final_error_message = "Metadata fetch failed from all sources or data invalid."
             log.warning(f"Metadata fetch/population failed for series: '{show_title_guess}' S{season_num}E{episode_num_tuple}. Reason: {final_error_message}")
             # Set fallbacks
             if not final_meta.show_title: final_meta.show_title = show_title_guess
             if not final_meta.show_year: final_meta.show_year = year_guess
             # Raise the consolidated error message
             raise MetadataError(final_error_message)
        elif not final_meta.show_title:
             final_meta.show_title = show_title_guess # Ensure title exists even if API returned None/empty

        log.debug(f"fetch_series_metadata returning final result for '{show_title_guess}': Source='{final_meta.source_api}', Title='{final_meta.show_title}', Year={final_meta.show_year}, S={final_meta.season}, EPs={final_meta.episode_list}, EpTitles={len(final_meta.episode_titles)}, Score={final_meta.match_confidence}")
        return final_meta

# --- END OF FILE metadata_fetcher.py ---