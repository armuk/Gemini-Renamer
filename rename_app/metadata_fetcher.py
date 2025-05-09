# --- START OF FILE metadata_fetcher.py ---

import logging
import time
import asyncio
from functools import wraps, partial
from pathlib import Path
from typing import Optional, Tuple, TYPE_CHECKING, Any, Iterable, Sequence, Dict, cast, List, Deque, Union
from collections import deque

# --- Tenacity Import ---
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_fixed, retry_if_exception

# --- API Client Imports ---
from .api_clients import get_tmdb_client, get_tvdb_client

# --- Local Imports ---
from .exceptions import MetadataError
from .models import MediaMetadata, MediaInfo 

log = logging.getLogger(__name__)

# --- Optional Dependency Imports & Flags ---
try:
    import diskcache
    DISKCACHE_AVAILABLE = True
except ImportError:
    DISKCACHE_AVAILABLE = False
    diskcache = None 

try:
    import platformdirs
    PLATFORMDIRS_AVAILABLE = True
except ImportError:
    PLATFORMDIRS_AVAILABLE = False

try:
    from thefuzz import process as fuzz_process
    from thefuzz import fuzz # Import fuzz for direct ratio calculation
    THEFUZZ_AVAILABLE = True
except ImportError:
    THEFUZZ_AVAILABLE = False
    fuzz = None # Define if not available

try:
    import dateutil.parser
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False

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


# --- Helper Classes/Functions ---

class AsyncRateLimiter:
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
    if isinstance(exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)):
        log.debug(f"Retry check PASSED for Connection/Timeout Error: {type(exception).__name__}")
        return True
    if isinstance(exception, req_exceptions.HTTPError):
        status_code = getattr(getattr(exception, 'response', None), 'status_code', 0)
        if status_code == 429: log.warning(f"Retry check PASSED for HTTP 429 (Rate Limit)."); return True
        if 500 <= status_code <= 599: log.warning(f"Retry check PASSED for HTTP {status_code} (Server Error)."); return True
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
        return True 

    if isinstance(exception, (ValueError, Exception)): 
        msg = str(exception).lower()
        if "failed to get" in msg and ("not found" in msg or "no record" in msg or "invalid id" in msg or "404" in msg):
            log.debug(f"Retry check FAILED for TVDB (likely Not Found): {msg}"); return False
        if "unauthorized" in msg or "api key" in msg or "401" in msg:
            log.error(f"Retry check FAILED for TVDB (likely API Key/Auth Issue): {msg}"); return False
        if '500' in msg or '502' in msg or '503' in msg or '504' in msg or 'timeout' in msg:
            log.warning(f"Retry check PASSED for potential TVDB Server Error/Timeout: {type(exception).__name__}: {msg}")
            return True
        if isinstance(exception, (AttributeError, TypeError, UnboundLocalError)):
             log.error(f"Retry check FAILED for Internal Error ({type(exception).__name__}): {msg}"); return False
        log.debug(f"Retry check FAILED for generic ValueError/Exception (TVDB?): {type(exception).__name__}: {msg}"); return False
    log.debug(f"Retry check FAILED by default for: {type(exception).__name__}"); return False

def find_best_match(title_to_find: str, api_results_tuple: Tuple[Dict, ...], result_key: str ='title', id_key: str ='id', score_cutoff: int = 70) -> Optional[Tuple[Dict, Optional[float]]]:
    if not api_results_tuple: return None
    first_result_dict = next(iter(api_results_tuple), None) 

    if not THEFUZZ_AVAILABLE:
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

    best_match_dict = None; best_score = None
    try:
        if not isinstance(title_to_find, str): title_to_find = str(title_to_find) 
        best_result_list = fuzz_process.extractBests(title_to_find, choices, score_cutoff=score_cutoff, limit=1)

        if best_result_list:
             matched_value, score, best_id = best_result_list[0]
             best_score = float(score)
             log.debug(f"Fuzzy match '{title_to_find}': Found '{matched_value}' (ID:{best_id}) score {best_score:.1f}")
             best_match_dict = next((r_dict for r_dict in api_results_tuple if isinstance(r_dict, dict) and str(r_dict.get(id_key)) == str(best_id)), None)
             if not best_match_dict:
                 log.error(f"Fuzzy match found ID {best_id} but couldn't find corresponding dict in original results.")
                 best_match_dict = first_result_dict 
                 best_score = None 
        else:
            log.warning(f"Fuzzy match failed for '{title_to_find}' (cutoff {score_cutoff}). Falling back to first result.")
            best_match_dict = first_result_dict
            best_score = None
    except Exception as e_fuzz:
        log.error(f"Error during fuzzy matching process: {e_fuzz}", exc_info=True)
        best_match_dict = first_result_dict 
        best_score = None
    return best_match_dict, best_score

def get_external_ids(tmdb_obj: Optional[Any] = None, tvdb_obj: Optional[Any] = None) -> Dict[str, Any]:
    ids: Dict[str, Any] = {'imdb_id': None, 'tmdb_id': None, 'tvdb_id': None, 'collection_id': None, 'collection_name': None}
    if tmdb_obj:
        try:
            tmdb_id_val = None
            if isinstance(tmdb_obj, dict): tmdb_id_val = tmdb_obj.get('id')
            elif hasattr(tmdb_obj, 'id'): tmdb_id_val = getattr(tmdb_obj, 'id', None)
            if tmdb_id_val is not None: ids['tmdb_id'] = int(tmdb_id_val) 
            ext_ids_data = {}
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
            collection_info = None
            if isinstance(tmdb_obj, dict): collection_info = tmdb_obj.get('belongs_to_collection')
            elif hasattr(tmdb_obj, 'belongs_to_collection'): collection_info = getattr(tmdb_obj, 'belongs_to_collection', None)
            if isinstance(collection_info, (dict, AsObj)): 
                 col_id = None; col_name = None
                 if isinstance(collection_info, dict):
                     col_id = collection_info.get('id'); col_name = collection_info.get('name')
                 else: 
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
            remote_ids = tvdb_obj.get('remoteIds', tvdb_obj.get('remote_ids', [])) 
            imdb_found_in_remote = False
            if remote_ids and isinstance(remote_ids, list):
                 for remote in remote_ids:
                     if isinstance(remote, dict) and remote.get('sourceName') == 'IMDB':
                          imdb_id_found = remote.get('id')
                          if imdb_id_found:
                              if ids.get('imdb_id') is None: ids['imdb_id'] = str(imdb_id_found)
                              imdb_found_in_remote = True; break 
            if not imdb_found_in_remote and ids.get('imdb_id') is None:
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
    dict_list = []
    try:
        for item in results_iterable:
            if not item: continue
            item_dict = {} 
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
    def __init__(self, cfg_helper):
        self.cfg = cfg_helper
        self.tmdb = get_tmdb_client()
        self.tvdb = get_tvdb_client()
        self.rate_limiter = AsyncRateLimiter(self.cfg('api_rate_limit_delay', 0.5))
        self.year_tolerance = self.cfg('api_year_tolerance', 1)
        self.tmdb_strategy = self.cfg('tmdb_match_strategy', 'first')
        self.tmdb_fuzzy_cutoff = self.cfg('tmdb_match_fuzzy_cutoff', 70)
        # --- ADDED: Get the new config option ---
        self.tmdb_first_result_min_score = self.cfg('tmdb_first_result_min_score', 65) # Default 65
        log.debug(f"Fetcher Config: Year Tolerance={self.year_tolerance}, TMDB Strategy='{self.tmdb_strategy}', TMDB Fuzzy Cutoff={self.tmdb_fuzzy_cutoff}, TMDB First Result Min Score={self.tmdb_first_result_min_score}")
        # --- END ADDED ---

        self.cache = None
        self.cache_enabled = self.cfg('cache_enabled', True)
        self.cache_expire = self.cfg('cache_expire_seconds', 60 * 60 * 24 * 7) 
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
                 else: 
                     log.error("Could not determine a valid cache directory. Persistent caching disabled."); self.cache_enabled = False
             else:
                 log.warning("Persistent caching enabled, but 'diskcache' library not found. Caching disabled."); self.cache_enabled = False
        else:
            log.info("Persistent caching disabled by configuration.")

    def _get_year_from_date(self, date_str: Optional[str]) -> Optional[int]:
        if not date_str or not DATEUTIL_AVAILABLE: return None
        try:
            if len(date_str) == 4 and date_str.isdigit(): return int(date_str)
            return dateutil.parser.parse(date_str).year
        except (ValueError, TypeError, OverflowError):
            log.debug(f"Could not parse year from date string: '{date_str}'")
            return None

    async def _run_sync(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args, **kwargs)

    async def _get_cache(self, key: str) -> Optional[Any]:
        if not self.cache_enabled or not self.cache: return None
        _cache_miss = object()
        try:
            cached_value = await self._run_sync(self.cache.get, key, default=_cache_miss)
            if cached_value is not _cache_miss:
                log.debug(f"Cache HIT for key: {key}")
                if isinstance(cached_value, tuple) and len(cached_value) >= 3:
                    return cached_value
                else:
                    log.warning(f"Cache data for {key} has unexpected structure. Ignoring cache.")
                    await self._run_sync(self.cache.delete, key) 
                    return None
            else:
                log.debug(f"Cache MISS for key: {key}");
                return None
        except Exception as e:
            log.warning(f"Error getting from cache key '{key}': {e}", exc_info=True)
            return None

    async def _set_cache(self, key: str, value: Any):
        if not self.cache_enabled or not self.cache: return
        if not isinstance(value, tuple) or len(value) < 3:
             log.error(f"Attempted to cache value with incorrect structure for key {key}. Aborting cache set.")
             return
        try:
            await self._run_sync(self.cache.set, key, value, expire=self.cache_expire)
            log.debug(f"Cache SET for key: {key}")
        except Exception as e:
            log.warning(f"Error setting cache key '{key}': {e}", exc_info=True)

    def _sync_tmdb_movie_fetch(self, sync_title, sync_year_guess, sync_lang):
        log.debug(f"Executing TMDB Movie Fetch [sync thread] for: '{sync_title}' (year: {sync_year_guess}, lang: {sync_lang}, ...)")
        if not self.tmdb or not TMDBV3API_AVAILABLE:
            log.error("TMDB client/library not available in _sync_tmdb_movie_fetch [sync thread].")
            return None, None, None

        search = Movie(); results_obj = None; processed_results = None; movie_match = None; match_score = None
        try:
            if not isinstance(sync_title, str): sync_title = str(sync_title)
            results_obj = search.search(sync_title) 
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

        if sync_year_guess and processed_results:
            log.debug(f"Applying year filter ({sync_year_guess} +/- {self.year_tolerance}) to TMDB movie results [sync thread].")
            filtered_list = []
            try:
                for r in processed_results:
                    if not isinstance(r, (dict, AsObj)): continue
                    result_year = None; release_date = getattr(r, 'release_date', None) if isinstance(r, AsObj) else r.get('release_date')
                    if release_date: result_year = self._get_year_from_date(str(release_date))
                    if result_year is not None and abs(result_year - sync_year_guess) <= self.year_tolerance:
                        filtered_list.append(r)
                    else: log.debug(f"  -> Year filter FAILED for '{getattr(r, 'title', r.get('title', 'N/A'))}' ({result_year or 'N/A'}) [sync thread]")
                if not filtered_list and processed_results: log.debug(f"Year filtering removed all TMDB movie results, keeping original.")
                else: processed_results = filtered_list
                log.debug(f"Year filtering resulted in {len(processed_results)} TMDB movie results.")
            except Exception as e_filter: log.error(f"Error during TMDB movie year filtering: {e_filter}", exc_info=True); 

        if processed_results:
            results_as_dicts = _tmdb_results_to_dicts(processed_results, result_type='movie')
            if results_as_dicts:
                best_match_dict, score = None, None
                if self.tmdb_strategy == 'fuzzy' and THEFUZZ_AVAILABLE:
                    log.debug(f"Attempting TMDB movie fuzzy match (cutoff: {self.tmdb_fuzzy_cutoff}) [sync thread].")
                    best_match_dict, score = find_best_match(sync_title, results_as_dicts, result_key='title', id_key='id', score_cutoff=self.tmdb_fuzzy_cutoff)
                
                # --- MODIFIED: Stricter 'first' result check ---
                if not best_match_dict: # Includes case where fuzzy match failed or wasn't attempted
                    log.debug("Using 'first' result strategy for TMDB movie [sync thread].")
                    first_raw_dict = next(iter(results_as_dicts), None)
                    if first_raw_dict:
                        if THEFUZZ_AVAILABLE and fuzz: # Check if fuzz is available for direct ratio
                            api_title = first_raw_dict.get('title', '')
                            # Use fuzz.ratio for a simple direct comparison if THEFUZZ_AVAILABLE
                            first_score_val = fuzz.ratio(str(sync_title).lower(), str(api_title).lower())
                            log.debug(f"  'first' strategy: Title='{api_title}', Score vs '{sync_title}' = {first_score_val:.1f} (Min required: {self.tmdb_first_result_min_score})")
                            if first_score_val >= self.tmdb_first_result_min_score:
                                best_match_dict = first_raw_dict
                                score = first_score_val # Use this calculated score
                            else:
                                log.warning(f"  'first' strategy: Match '{api_title}' score {first_score_val:.1f} is below threshold {self.tmdb_first_result_min_score}. Discarding match.")
                                best_match_dict = None # Discard
                        else: # thefuzz not available, accept first result without score check
                            best_match_dict = first_raw_dict
                            score = None 
                # --- END MODIFIED ---

                if best_match_dict:
                    matched_id = best_match_dict.get('id')
                    if matched_id:
                        try: movie_match = next((r for r in processed_results if isinstance(r, (dict, AsObj)) and (getattr(r, 'id', None) if isinstance(r, AsObj) else r.get('id')) == matched_id), None)
                        except StopIteration: movie_match = None
                        match_score = score 
                    if not movie_match:
                        log.warning(f"Could not find original object for matched movie dict ID {matched_id}. Using dict.")
                        movie_match = best_match_dict 

        if not movie_match:
            log.warning(f"No suitable TMDB movie match found for '{sync_title}' (after filtering/matching)."); return None, None, None
        if not isinstance(movie_match, (dict, AsObj)):
            log.error(f"Final TMDB movie match for '{sync_title}' is not a valid object/dict type: {type(movie_match)} ({movie_match}). Skipping.")
            return None, None, None
        movie_id = getattr(movie_match, 'id', None) if isinstance(movie_match, AsObj) else movie_match.get('id')
        if not movie_id:
            log.error(f"Final TMDB movie match lacks 'id' or ID is None: {movie_match}"); return None, None, None
        log.debug(f"TMDB matched movie '{getattr(movie_match, 'title', movie_match.get('title', 'N/A'))}' ID: {movie_id} [sync thread] (Score: {match_score if match_score is not None else 'N/A'})")
        movie_details = None; final_movie_data_obj = movie_match
        try:
            movie_details = search.details(movie_id, append_to_response="external_ids") 
            if movie_details: final_movie_data_obj = movie_details
        except TMDbException as e_details:
             if "resource not found" in str(e_details).lower() or e_details.status_code == 404: log.warning(f"TMDB movie details for ID {movie_id} not found.")
             else: log.error(f"TMDbException fetching movie details ID {movie_id}: {e_details}"); raise e_details 
        except Exception as e_details: log.error(f"Unexpected error fetching movie details ID {movie_id}: {e_details}"); raise e_details
        ids_dict = get_external_ids(tmdb_obj=final_movie_data_obj)
        log.debug(f"_sync_tmdb_movie_fetch returning: data type={type(final_movie_data_obj)}, ids={ids_dict}, score={match_score}")
        return final_movie_data_obj, ids_dict, match_score

    def _sync_tmdb_series_fetch(self, sync_title, sync_season, sync_episodes, sync_year_guess, sync_lang):
        log.debug(f"Executing TMDB Series Fetch [sync thread] for: '{sync_title}' S{sync_season} E{sync_episodes} (lang: {sync_lang}, year: {sync_year_guess}, ...)")
        if not self.tmdb or not TMDBV3API_AVAILABLE:
            log.error("TMDB client/library not available in _sync_tmdb_series_fetch [sync thread].")
            return None, None, None, None

        search = TV(); results_obj = None; processed_results = None; show_match = None; match_score = None
        try:
            if not isinstance(sync_title, str): sync_title = str(sync_title)
            results_obj = search.search(sync_title) 
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

        if sync_year_guess and processed_results:
            log.debug(f"Applying year filter ({sync_year_guess} +/- {self.year_tolerance}) to TMDB series results [sync thread].")
            filtered_list = []
            try:
                for r in processed_results:
                    if not isinstance(r, (dict, AsObj)): continue
                    result_year = None; first_air_date = getattr(r, 'first_air_date', None) if isinstance(r, AsObj) else r.get('first_air_date')
                    if first_air_date: result_year = self._get_year_from_date(str(first_air_date))
                    if result_year is not None and abs(result_year - sync_year_guess) <= self.year_tolerance:
                        filtered_list.append(r)
                    else: log.debug(f"  -> Year filter FAILED for '{getattr(r, 'name', r.get('name', 'N/A'))}' ({result_year or 'N/A'}) [sync thread]")
                if not filtered_list and processed_results: log.debug(f"Year filtering removed all TMDB series results, keeping original.")
                else: processed_results = filtered_list
                log.debug(f"Year filtering resulted in {len(processed_results)} TMDB series results.")
            except Exception as e_filter: log.error(f"Error during TMDB series year filtering: {e_filter}", exc_info=True); 

        if processed_results:
            results_as_dicts = _tmdb_results_to_dicts(processed_results, result_type='series')
            if results_as_dicts:
                best_match_dict, score = None, None
                if self.tmdb_strategy == 'fuzzy' and THEFUZZ_AVAILABLE:
                    log.debug(f"Attempting TMDB series fuzzy match (cutoff: {self.tmdb_fuzzy_cutoff}) [sync thread].")
                    best_match_dict, score = find_best_match(sync_title, results_as_dicts, result_key='name', id_key='id', score_cutoff=self.tmdb_fuzzy_cutoff)
                
                # --- MODIFIED: Stricter 'first' result check ---
                if not best_match_dict: # Includes case where fuzzy match failed or wasn't attempted
                    log.debug("Using 'first' result strategy for TMDB series [sync thread].")
                    first_raw_dict = next(iter(results_as_dicts), None)
                    if first_raw_dict:
                        if THEFUZZ_AVAILABLE and fuzz:
                            api_name = first_raw_dict.get('name', '')
                            first_score_val = fuzz.ratio(str(sync_title).lower(), str(api_name).lower())
                            log.debug(f"  'first' strategy: Series Name='{api_name}', Score vs '{sync_title}' = {first_score_val:.1f} (Min required: {self.tmdb_first_result_min_score})")
                            if first_score_val >= self.tmdb_first_result_min_score:
                                best_match_dict = first_raw_dict
                                score = first_score_val
                            else:
                                log.warning(f"  'first' strategy: Match '{api_name}' score {first_score_val:.1f} is below threshold {self.tmdb_first_result_min_score}. Discarding match.")
                                best_match_dict = None
                        else: # thefuzz not available
                            best_match_dict = first_raw_dict
                            score = None
                # --- END MODIFIED ---

                if best_match_dict:
                    matched_id = best_match_dict.get('id')
                    if matched_id:
                        try: show_match = next((r for r in processed_results if isinstance(r, (dict, AsObj)) and (getattr(r, 'id', None) if isinstance(r, AsObj) else r.get('id')) == matched_id), None)
                        except StopIteration: show_match = None
                        match_score = score 
                    if not show_match:
                        log.warning(f"Could not find original object for matched series dict ID {matched_id}. Using dict.")
                        show_match = best_match_dict

        if not show_match:
            log.warning(f"No suitable TMDB series match found for '{sync_title}' S{sync_season} (after filtering/matching)."); return None, None, None, None
        if not isinstance(show_match, (dict, AsObj)):
            log.error(f"Final TMDB series match for '{sync_title}' is not a valid object/dict type: {type(show_match)} ({show_match}). Skipping.")
            return None, None, None, None
        show_id = getattr(show_match, 'id', None) if isinstance(show_match, AsObj) else show_match.get('id')
        if not show_id:
            log.error(f"Final TMDB series match lacks 'id' or ID is None: {show_match}"); return None, None, None, None
        log.debug(f"TMDB matched series '{getattr(show_match, 'name', show_match.get('name', 'N/A'))}' ID: {show_id} [sync thread] (Score: {match_score if match_score is not None else 'N/A'})")
        show_details = None; final_show_data_obj = show_match
        try:
            show_details = search.details(show_id, append_to_response="external_ids") 
            if show_details: final_show_data_obj = show_details
        except TMDbException as e_details:
             if "resource not found" in str(e_details).lower() or e_details.status_code == 404: log.warning(f"TMDB series details for ID {show_id} not found.")
             else: log.error(f"TMDbException fetching series details ID {show_id}: {e_details}"); raise e_details
        except Exception as e_details: log.error(f"Unexpected error fetching series details ID {show_id}: {e_details}"); raise e_details
        ep_data: Dict[int, Any] = {}
        if sync_episodes: 
            try:
                log.debug(f"Fetching TMDB season {sync_season} details for show ID {show_id}")
                season_fetcher = Season(); season_details = season_fetcher.details(tv_id=show_id, season_num=sync_season) 
                episodes_list = getattr(season_details, 'episodes', [])
                if episodes_list: 
                    episodes_in_season = {}
                    for api_ep in episodes_list:
                        ep_num_api = getattr(api_ep, 'episode_number', None)
                        if ep_num_api is not None:
                            try: episodes_in_season[int(ep_num_api)] = api_ep
                            except (ValueError, TypeError): pass
                    for ep_num_needed in sync_episodes:
                        episode_obj = episodes_in_season.get(ep_num_needed)
                        if episode_obj: ep_data[ep_num_needed] = episode_obj 
                        else: log.warning(f"TMDB S{sync_season} E{ep_num_needed} not found for '{getattr(final_show_data_obj, 'name', final_show_data_obj.get('name', 'N/A'))}'")
                else:
                    season_name = getattr(season_details, 'name', f'S{sync_season}')
                    log.warning(f"TMDB season details '{season_name}' ID {show_id} lacks 'episodes' list or attribute.")
            except TMDbException as e_season:
                if "resource not found" in str(e_season).lower() or e_season.status_code == 404: log.warning(f"TMDB season S{sync_season} for ID {show_id} not found.")
                else: log.warning(f"TMDbException getting season S{sync_season} ID {show_id}: {e_season}") 
            except Exception as e_season: log.warning(f"Unexpected error getting season S{sync_season} ID {show_id}: {e_season}")
        ids = get_external_ids(tmdb_obj=final_show_data_obj)
        log.debug(f"_sync_tmdb_series_fetch returning: data type={type(final_show_data_obj)}, ep_map keys={list(ep_data.keys())}, ids={ids}, score={match_score}")
        return final_show_data_obj, ep_data, ids, match_score

    def _sync_tvdb_series_fetch(self, sync_title, sync_season_num, sync_episodes, sync_tvdb_id, sync_year_guess, sync_lang):
        # (This method remains unchanged for this specific bug fix, but ensure it also respects some form of confidence if it uses fuzzy matching internally)
        log.debug(f"Executing TVDB Series Fetch [sync thread] for: '{sync_title}' S{sync_season_num} E{sync_episodes} (lang: {sync_lang}, year: {sync_year_guess}, id: {sync_tvdb_id}, ...)")
        if not self.tvdb:
            log.error("TVDB client not available in _sync_tvdb_series_fetch [sync thread].")
            return None, None, None, None
        show_data: Optional[Dict] = None; best_match_id = sync_tvdb_id; search_results: Optional[List[Dict]] = None; match_score = None
        if not best_match_id:
            try:
                log.debug(f"TVDB searching for: '{sync_title}' (Year guess: {sync_year_guess}) [sync thread]")
                search_results = self.tvdb.search(sync_title) 
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
                if sync_year_guess:
                    filtered_results = []
                    for r in search_results:
                        if not isinstance(r, dict): continue 
                        result_year_str = r.get('year')
                        if result_year_str:
                            try:
                                result_year = int(result_year_str)
                                if abs(result_year - sync_year_guess) <= self.year_tolerance: filtered_results.append(r)
                            except (ValueError, TypeError): pass
                    if not filtered_results and search_results: log.debug("TVDB year filtering removed all results, keeping original.")
                    else: search_results = filtered_results
                    log.debug(f"TVDB results after year filter: {len(search_results)}.")
                if search_results:
                    try:
                         # Use a reasonable cutoff for TVDB fuzzy matching
                         tvdb_fuzzy_cutoff = self.cfg('tmdb_match_fuzzy_cutoff', 70) # Reuse for consistency or add tvdb_fuzzy_cutoff
                         best_match_dict, score = find_best_match(sync_title, tuple(search_results), result_key='name', id_key='tvdb_id', score_cutoff=tvdb_fuzzy_cutoff)
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
        if best_match_id:
            try:
                log.debug(f"TVDB fetching extended series data for ID: {best_match_id} [sync thread]")
                show_data = self.tvdb.get_series_extended(best_match_id) 
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
        ep_data: Dict[int, Any] = {}; ids: Dict[str, Any] = {}
        if show_data:
            try:
                log.debug(f"TVDB fetching ALL episodes for show ID {best_match_id} (pagination may occur) [sync thread]")
                all_episodes_list = []
                page = 0
                while True: 
                    episodes_page_data = self.tvdb.get_series_episodes(best_match_id, page=page, lang=sync_lang) 
                    if episodes_page_data and isinstance(episodes_page_data.get('episodes'), list):
                        page_episodes = episodes_page_data['episodes']
                        all_episodes_list.extend(page_episodes)
                        log.debug(f"  Fetched page {page}, {len(page_episodes)} episodes.")
                        links = self.tvdb.get_req_links() 
                        if links and links.get('next'):
                             page += 1 
                             log.debug(f"  Found 'next' link, fetching page {page}...")
                        else:
                            log.debug("  No 'next' link found or links structure unexpected. Assuming end of pages.")
                            break 
                    else:
                        log.warning(f"TVDB episodes data invalid or missing 'episodes' key for page {page}, show ID {best_match_id}. Stopping pagination.")
                        break 
                log.debug(f"Total episodes fetched for show ID {best_match_id}: {len(all_episodes_list)}")
                episodes_in_season: Dict[int, Dict] = {}
                for ep_dict in all_episodes_list:
                    if isinstance(ep_dict, dict):
                        api_season_num = ep_dict.get('seasonNumber')
                        api_ep_num = ep_dict.get('number')
                        if api_season_num is not None and api_ep_num is not None:
                            try:
                                if int(api_season_num) == int(sync_season_num):
                                    episodes_in_season[int(api_ep_num)] = ep_dict
                            except (ValueError, TypeError):
                                log.warning(f"Could not parse season/episode number from TVDB episode dict: {ep_dict}")
                episode_iterator = sync_episodes if sync_episodes else []
                for ep_num in episode_iterator:
                    episode_details = episodes_in_season.get(ep_num)
                    if episode_details: ep_data[ep_num] = episode_details
                    else: log.warning(f"TVDB S{sync_season_num} E{ep_num} not found in fetched episodes for '{show_data.get('name')}'")
            except (ValueError, Exception) as e_ep_fetch:
                 msg = str(e_ep_fetch).lower()
                 if "not found" in msg or "404" in msg: log.warning(f"TVDB episodes fetch failed for ID {best_match_id}: Not Found.")
                 else: log.warning(f"TVDB error fetching/processing episode data for S{sync_season_num}, ID {best_match_id}: {e_ep_fetch}", exc_info=False)
            try: ids = get_external_ids(tvdb_obj=show_data)
            except Exception as e_ids: log.warning(f"Error extracting external IDs from TVDB data: {e_ids}", exc_info=True)
        log.debug(f"_sync_tvdb_series_fetch returning: data type={type(show_data)}, ep_map keys={list(ep_data.keys())}, ids={ids}, score={match_score}")
        return show_data, ep_data, ids, match_score

    async def _do_fetch_tmdb_movie(self, title_arg, year_arg, lang='en'):
        max_attempts_cfg = self.cfg('api_retry_attempts', 3)
        wait_sec_cfg = self.cfg('api_retry_wait_seconds', 2)
        max_attempts = max(1, int(max_attempts_cfg)) if max_attempts_cfg is not None else 3
        wait_seconds = float(wait_sec_cfg) if wait_sec_cfg is not None else 2.0
        async_retryer = AsyncRetrying(
            stop=stop_after_attempt(max_attempts),
            wait=wait_fixed(wait_seconds),
            retry=retry_if_exception(should_retry_api_error), 
            reraise=True
        )
        try:
            log.debug(f"Attempting TMDB movie fetch for '{title_arg}' ({year_arg}) with tenacity.")
            data_obj, ids_dict, score = await async_retryer( #type: ignore
                self._run_sync, self._sync_tmdb_movie_fetch, title_arg, year_arg, lang
            )
            if data_obj is None and ids_dict is None and score is None:
                log.info(f"TMDB movie '{title_arg}' ({year_arg}) not found or no match (returned None from sync).")
                return None, None, None 
            return data_obj, ids_dict, score
        except RetryError as e:
            last_exception = e.last_attempt.exception()
            log.error(f"All {max_attempts} retry attempts failed for TMDB movie '{title_arg}'. Last error: {type(last_exception).__name__}: {last_exception}")
            error_context = f"Movie: '{title_arg}'"; final_error_msg = f"Failed to fetch TMDB metadata ({error_context}) after {max_attempts} attempts."
            if isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network connection."
            elif isinstance(last_exception, req_exceptions.HTTPError) and \
                 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely a temporary TMDB server issue."
            raise MetadataError(final_error_msg) from last_exception
        except Exception as e:
            log.error(f"Non-retryable or unexpected error during TMDB movie fetch for '{title_arg}': {type(e).__name__}: {e}", exc_info=True)
            error_context = f"Movie: '{title_arg}'"; user_facing_error = None
            if isinstance(e, (TMDbException, req_exceptions.HTTPError)):
                msg_lower = str(e).lower(); status_code = 0
                if isinstance(e, req_exceptions.HTTPError): status_code = getattr(getattr(e, 'response', None), 'status_code', 0)
                if "invalid api key" in msg_lower or status_code == 401 or "authentication failed" in msg_lower:
                    user_facing_error = f"Invalid TMDB API Key or Authentication Failed ({error_context}). Please check your key."
                elif status_code == 403:
                    user_facing_error = f"TMDB API request forbidden ({error_context}). Check API key permissions."
                elif "resource not found" in msg_lower or status_code == 404: 
                    log.warning(f"TMDB resource not found for {error_context} (unexpected non-retry path). Error: {e}")
                    return None, None, None
            final_error_msg = user_facing_error or f"Unrecoverable error fetching TMDB metadata ({error_context}). Details: {e}"
            raise MetadataError(final_error_msg) from e

    async def _do_fetch_tmdb_series(self, title_arg, season_arg, episodes_arg, year_guess_arg=None, lang='en'):
        max_attempts_cfg = self.cfg('api_retry_attempts', 3)
        wait_sec_cfg = self.cfg('api_retry_wait_seconds', 2)
        max_attempts = max(1, int(max_attempts_cfg)) if max_attempts_cfg is not None else 3
        wait_seconds = float(wait_sec_cfg) if wait_sec_cfg is not None else 2.0
        async_retryer = AsyncRetrying(
            stop=stop_after_attempt(max_attempts),
            wait=wait_fixed(wait_seconds),
            retry=retry_if_exception(should_retry_api_error), 
            reraise=True
        )
        try:
            log.debug(f"Attempting TMDB series fetch for '{title_arg}' S{season_arg} with tenacity.")
            show_obj, ep_map, ids_dict, score = await async_retryer( #type: ignore
                self._run_sync, self._sync_tmdb_series_fetch, title_arg, season_arg, episodes_arg, year_guess_arg, lang
            )
            if show_obj is None:
                log.info(f"TMDB series '{title_arg}' S{season_arg} not found or no match.")
                return None, None, None, None
            return show_obj, ep_map, ids_dict, score
        except RetryError as e:
            last_exception = e.last_attempt.exception()
            log.error(f"All {max_attempts} retry attempts failed for TMDB series '{title_arg}' S{season_arg}. Last error: {type(last_exception).__name__}: {last_exception}")
            error_context = f"Series: '{title_arg}' S{season_arg}"; final_error_msg = f"Failed to fetch TMDB metadata ({error_context}) after {max_attempts} attempts."
            if isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network connection."
            elif isinstance(last_exception, req_exceptions.HTTPError) and \
                 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely a temporary TMDB server issue."
            raise MetadataError(final_error_msg) from last_exception
        except Exception as e:
            log.error(f"Non-retryable or unexpected error during TMDB series fetch for '{title_arg}' S{season_arg}: {type(e).__name__}: {e}", exc_info=True)
            error_context = f"Series: '{title_arg}' S{season_arg}"; user_facing_error = None
            if isinstance(e, (TMDbException, req_exceptions.HTTPError)):
                msg_lower = str(e).lower(); status_code = 0
                if isinstance(e, req_exceptions.HTTPError): status_code = getattr(getattr(e, 'response', None), 'status_code', 0)
                if "invalid api key" in msg_lower or status_code == 401 or "authentication failed" in msg_lower:
                    user_facing_error = f"Invalid TMDB API Key ({error_context})."
                elif status_code == 403: user_facing_error = f"TMDB API request forbidden ({error_context})."
                elif "resource not found" in msg_lower or status_code == 404:
                    log.warning(f"TMDB resource not found ({error_context}) (unexpected non-retry path)."); return None, None, None, None
            final_error_msg = user_facing_error or f"Unrecoverable error fetching TMDB metadata ({error_context}). Details: {e}"
            raise MetadataError(final_error_msg) from e

    async def _do_fetch_tvdb_series(self, title_arg: str, season_num_arg: int, episodes_arg: tuple, tvdb_id_arg: Optional[int] = None, year_guess_arg: Optional[int] = None, lang: str = 'en'):
        max_attempts_cfg = self.cfg('api_retry_attempts', 3)
        wait_sec_cfg = self.cfg('api_retry_wait_seconds', 2)
        max_attempts = max(1, int(max_attempts_cfg)) if max_attempts_cfg is not None else 3
        wait_seconds = float(wait_sec_cfg) if wait_sec_cfg is not None else 2.0
        async_retryer = AsyncRetrying(
            stop=stop_after_attempt(max_attempts),
            wait=wait_fixed(wait_seconds),
            retry=retry_if_exception(should_retry_api_error), 
            reraise=True
        )
        try:
            log.debug(f"Attempting TVDB series fetch for '{title_arg}' S{season_num_arg} with tenacity.")
            show_dict, ep_map, ids_dict, score = await async_retryer( #type: ignore
                self._run_sync, self._sync_tvdb_series_fetch, title_arg, season_num_arg, episodes_arg, tvdb_id_arg, year_guess_arg, lang
            )
            if show_dict is None:
                log.info(f"TVDB series '{title_arg}' S{season_num_arg} not found or no match.")
                return None, None, None, None
            return show_dict, ep_map, ids_dict, score
        except RetryError as e:
            last_exception = e.last_attempt.exception()
            log.error(f"All {max_attempts} retry attempts failed for TVDB series '{title_arg}' S{season_num_arg}. Last error: {type(last_exception).__name__}: {last_exception}")
            error_context = f"TVDB Series: '{title_arg}' S{season_num_arg}"; final_error_msg = f"Failed to fetch TVDB metadata ({error_context}) after {max_attempts} attempts."
            if isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network connection."
            elif isinstance(last_exception, req_exceptions.HTTPError) and \
                 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely a temporary TVDB server issue."
            elif isinstance(last_exception, ValueError) and ("not found" in str(last_exception).lower() or "unauthorized" in str(last_exception).lower()):
                 final_error_msg = f"TVDB Error ({error_context}): {last_exception}" 
            raise MetadataError(final_error_msg) from last_exception
        except Exception as e:
            log.error(f"Non-retryable or unexpected error during TVDB series fetch for '{title_arg}' S{season_num_arg}: {type(e).__name__}: {e}", exc_info=True)
            error_context = f"TVDB Series: '{title_arg}' S{season_num_arg}"; user_facing_error = None
            msg_lower = str(e).lower()
            if "unauthorized" in msg_lower or "api key" in msg_lower or ("response" in msg_lower and "401" in msg_lower):
                 user_facing_error = f"Invalid TVDB API Key or Unauthorized ({error_context})."
            elif isinstance(e, req_exceptions.HTTPError) and getattr(getattr(e, 'response', None), 'status_code', 0) == 403:
                 user_facing_error = f"TVDB API request forbidden ({error_context})."
            elif "failed to get" in msg_lower and ("not found" in msg_lower or "no record" in msg_lower or "404" in msg_lower):
                 log.warning(f"TVDB resource not found ({error_context}) (unexpected non-retry path)."); return None, None, None, None
            final_error_msg = user_facing_error or f"Unrecoverable error fetching TVDB metadata ({error_context}). Details: {e}"
            raise MetadataError(final_error_msg) from e

    async def fetch_movie_metadata(self, movie_title_guess: str, year_guess: Optional[int] = None) -> MediaMetadata:
        log.debug(f"Fetching movie metadata (async) for: '{movie_title_guess}' (Year guess: {year_guess})")
        final_meta = MediaMetadata(is_movie=True)
        tmdb_movie_data: Optional[Any] = None; tmdb_ids: Optional[Dict[str, Any]] = None; tmdb_score: Optional[float] = None
        lang = self.cfg('tmdb_language', 'en')
        cache_key = f"movie::{movie_title_guess}_{year_guess}_{lang}"
        fetch_error_message = None 
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
                    if tmdb_movie_data is not None:
                         await self._set_cache(cache_key, (tmdb_movie_data, tmdb_ids, tmdb_score))
                except MetadataError as me:
                    log.error(f"TMDB movie fetch failed: {me}"); fetch_error_message = str(me) 
                    tmdb_movie_data, tmdb_ids, tmdb_score = None, None, None
                except Exception as e:
                     log.error(f"Unexpected error during TMDB movie fetch for '{movie_title_guess}': {type(e).__name__}: {e}", exc_info=True)
                     fetch_error_message = f"Unexpected error fetching TMDB movie: {e}" 
                     tmdb_movie_data, tmdb_ids, tmdb_score = None, None, None
            if tmdb_movie_data is None and not fetch_error_message:
                 log.debug(f"TMDB fetch for '{movie_title_guess}' returned no movie data object (no specific error).")
        if tmdb_movie_data:
            try:
                final_meta.source_api = "tmdb"; final_meta.match_confidence = tmdb_score
                title_val = getattr(tmdb_movie_data, 'title', None) if isinstance(tmdb_movie_data, AsObj) else tmdb_movie_data.get('title')
                release_date_val = getattr(tmdb_movie_data, 'release_date', None) if isinstance(tmdb_movie_data, AsObj) else tmdb_movie_data.get('release_date')
                final_meta.movie_title = title_val
                final_meta.release_date = str(release_date_val) if release_date_val else None
                final_meta.movie_year = self._get_year_from_date(final_meta.release_date)
                if isinstance(tmdb_ids, dict):
                     final_meta.ids = tmdb_ids
                     final_meta.collection_name = tmdb_ids.get('collection_name')
                     final_meta.collection_id = tmdb_ids.get('collection_id')
                else: final_meta.ids = {}
                log.debug(f"Successfully populated final_meta from TMDB for '{movie_title_guess}'. Score: {tmdb_score}")
            except Exception as e_populate:
                log.error(f"Error populating final_meta for '{movie_title_guess}' from TMDB data: {e_populate}", exc_info=True);
                final_meta.source_api = None 
                fetch_error_message = fetch_error_message or f"Error processing TMDB data: {e_populate}" 
        if not final_meta.source_api:
             log.warning(f"Metadata fetch or population ultimately failed for movie: '{movie_title_guess}' (Year guess: {year_guess})")
             if not final_meta.movie_title: final_meta.movie_title = movie_title_guess
             if not final_meta.movie_year: final_meta.movie_year = year_guess
             if fetch_error_message: raise MetadataError(fetch_error_message)
             else: raise MetadataError(f"Failed to obtain valid metadata for movie '{movie_title_guess}'.")
        log.debug(f"fetch_movie_metadata returning final result for '{movie_title_guess}': Source='{final_meta.source_api}', Title='{final_meta.movie_title}', Year={final_meta.movie_year}, IDs={final_meta.ids}, Collection={final_meta.collection_name}, Score={final_meta.match_confidence}")
        return final_meta

    async def fetch_series_metadata(self, show_title_guess: str, season_num: int, episode_num_list: Tuple[int, ...], year_guess: Optional[int] = None) -> MediaMetadata:
        log.debug(f"Fetching series metadata (async) for: '{show_title_guess}' S{season_num}E{episode_num_list} (Year guess: {year_guess})")
        final_meta = MediaMetadata(is_series=True)
        lang = self.cfg('tmdb_language', 'en')
        episode_num_tuple = tuple(sorted(list(set(episode_num_list)))) 
        source_preference: List[str] = self.cfg('series_metadata_preference', ['tmdb', 'tvdb'])
        log.debug(f"Series metadata source preference order: {source_preference}")
        source_queue: Deque[str] = deque(source_preference)
        results_by_source: Dict[str, Dict[str, Any]] = {
            'tmdb': {'data': None, 'ep_map': None, 'ids': None, 'score': None, 'error': None},
            'tvdb': {'data': None, 'ep_map': None, 'ids': None, 'score': None, 'error': None}
        }
        primary_source: Optional[str] = None; merged_ids: Dict[str, Any] = {}; final_error_message: Optional[str] = None

        while source_queue:
            source = source_queue.popleft()
            log.debug(f"Attempting fetch from source: {source.upper()}")
            cache_key = f"series::{show_title_guess}_{season_num}_{episode_num_tuple}_{year_guess}_{lang}::{source}"
            source_data, source_ep_map, source_ids, source_score = None, None, None, None
            cached_data = await self._get_cache(cache_key)
            if cached_data:
                try:
                    source_data, cached_ids, source_score = cached_data
                    if cached_ids and isinstance(cached_ids, dict) and '_ep_map' in cached_ids:
                        source_ep_map = cached_ids.pop('_ep_map', {})
                        source_ids = cached_ids
                    else:
                        source_ids = cached_ids if isinstance(cached_ids, dict) else {}
                        source_ep_map = {}
                    log.debug(f"Using cached {source.upper()} data for series: '{show_title_guess}' S{season_num} (Score: {source_score})")
                except (TypeError, ValueError, IndexError) as e_cache:
                    log.warning(f"Error unpacking {source.upper()} cache data for {cache_key}, ignoring cache: {e_cache}")
                    cached_data = None; source_data, source_ep_map, source_ids, source_score = None, None, None, None
            if not cached_data:
                try:
                    await self.rate_limiter.wait()
                    if source == 'tmdb' and self.tmdb:
                        source_data, source_ep_map, source_ids, source_score = await self._do_fetch_tmdb_series(
                            show_title_guess, season_num, episode_num_tuple, year_guess, lang
                        )
                    elif source == 'tvdb' and self.tvdb:
                         tmdb_result_dict = results_by_source.get('tmdb', {})
                         tmdb_ids_dict = tmdb_result_dict.get('ids') if tmdb_result_dict else {}
                         tvdb_id_from_other_source = tmdb_ids_dict.get('tvdb_id') if tmdb_ids_dict else None
                         source_data, source_ep_map, source_ids, source_score = await self._do_fetch_tvdb_series(
                             title_arg=show_title_guess, season_num_arg=season_num, episodes_arg=episode_num_tuple,
                             tvdb_id_arg=tvdb_id_from_other_source, year_guess_arg=year_guess, lang=lang
                         )
                    else:
                        log.warning(f"{source.upper()} client not available, skipping fetch.")
                        results_by_source[source]['error'] = f"{source.upper()} client not available."
                        continue
                    if source_data is not None:
                        cacheable_ids = source_ids or {}
                        cacheable_ids['_ep_map'] = source_ep_map or {}
                        await self._set_cache(cache_key, (source_data, cacheable_ids, source_score))
                        if '_ep_map' in cacheable_ids:
                            source_ep_map = cacheable_ids.pop('_ep_map')
                            source_ids = cacheable_ids
                except MetadataError as me:
                    log.error(f"{source.upper()} series fetch failed: {me}"); results_by_source[source]['error'] = str(me)
                    source_data, source_ep_map, source_ids, source_score = None, None, None, None
                except Exception as e:
                    log.error(f"Unexpected error during {source.upper()} series fetch for '{show_title_guess}' S{season_num}: {type(e).__name__}: {e}", exc_info=True); results_by_source[source]['error'] = f"Unexpected {source.upper()} error: {e}"
                    source_data, source_ep_map, source_ids, source_score = None, None, None, None
            results_by_source[source]['data'] = source_data
            results_by_source[source]['ep_map'] = source_ep_map or {}
            results_by_source[source]['ids'] = source_ids or {}
            results_by_source[source]['score'] = source_score
            source_has_show = results_by_source[source]['data'] is not None
            source_has_all_eps = not episode_num_tuple or all(ep_num in results_by_source[source]['ep_map'] for ep_num in episode_num_tuple)
            if source_has_show and source_has_all_eps:
                log.info(f"Using {source.upper()} as primary source (found show and all requested episodes).")
                primary_source = source; break
            elif source_has_show: log.warning(f"{source.upper()} found show data but is missing some requested episodes for S{season_num}. Will check next preferred source if available.")
            else: log.info(f"{source.upper()} did not find show data. Will check next preferred source if available.")
        if not primary_source:
            for source in source_preference:
                 if results_by_source[source]['data'] is not None:
                     log.warning(f"Using {source.upper()} as primary source (found show data, but possibly missing episodes as preferred source failed/was incomplete).")
                     primary_source = source; break
        if primary_source:
            primary_ids = results_by_source[primary_source].get('ids') or {}; merged_ids.update(primary_ids)
        for source in source_preference:
            if source != primary_source:
                other_ids = results_by_source[source].get('ids') or {}
                for k, v in other_ids.items():
                     if v is not None and k not in merged_ids: merged_ids[k] = v
        if primary_source:
            final_meta.source_api = primary_source
            primary_show_data = results_by_source[primary_source]['data']
            primary_ep_map = results_by_source[primary_source]['ep_map']
            final_meta.match_confidence = results_by_source[primary_source]['score']
            final_meta.ids = merged_ids
            try: 
                show_title_api = getattr(primary_show_data, 'name', None) if isinstance(primary_show_data, AsObj) else primary_show_data.get('name')
                final_meta.show_title = show_title_api
                show_air_date = None
                if isinstance(primary_show_data, AsObj): show_air_date = getattr(primary_show_data, 'first_air_date', None)
                elif isinstance(primary_show_data, dict): show_air_date = primary_show_data.get('firstAired') or primary_show_data.get('first_air_date')
                final_meta.show_year = self._get_year_from_date(str(show_air_date) if show_air_date else None)
                final_meta.season = season_num
                final_meta.episode_list = list(episode_num_tuple)
                if episode_num_tuple:
                    for ep_num in episode_num_tuple:
                        ep_details = primary_ep_map.get(ep_num)
                        if ep_details:
                            ep_title = None; air_date = None
                            if isinstance(ep_details, AsObj):
                                ep_title = getattr(ep_details, 'name', None)
                                air_date = getattr(ep_details, 'air_date', None)
                            elif isinstance(ep_details, dict):
                                ep_title = ep_details.get('name') or ep_details.get('episodeName')
                                air_date = ep_details.get('air_date') or ep_details.get('aired') 
                            if ep_title: final_meta.episode_titles[ep_num] = str(ep_title)
                            if air_date: final_meta.air_dates[ep_num] = str(air_date)
                        else:
                            log.debug(f"Episode S{season_num}E{ep_num} not found in selected primary source map ({primary_source}).")
            except Exception as e_populate:
                 log.error(f"Error populating final_meta for series '{show_title_guess}' from {primary_source}: {e_populate}", exc_info=True);
                 final_meta.source_api = None 
                 final_error_message = f"Error processing {primary_source} data: {e_populate}" 
        else:
            log.warning(f"Could not determine a primary metadata source for series: '{show_title_guess}' S{season_num}E{episode_num_tuple}.")
            tmdb_err = results_by_source['tmdb'].get('error'); tvdb_err = results_by_source['tvdb'].get('error')
            if tmdb_err and tvdb_err: final_error_message = f"TMDB Error: {tmdb_err} | TVDB Error: {tvdb_err}"
            elif tmdb_err: final_error_message = f"TMDB Error: {tmdb_err}"
            elif tvdb_err: final_error_message = f"TVDB Error: {tvdb_err}"
            else: final_error_message = "Metadata fetch failed from all sources or data invalid."
        if not final_meta.source_api:
             log.warning(f"Metadata fetch/population ultimately failed for series: '{show_title_guess}' S{season_num}E{episode_num_tuple}. Reason: {final_error_message or 'Unknown reason'}")
             if not final_meta.show_title: final_meta.show_title = show_title_guess
             if not final_meta.show_year: final_meta.show_year = year_guess
             raise MetadataError(final_error_message or "Unknown metadata fetch error")
        elif not final_meta.show_title:
             final_meta.show_title = show_title_guess 
        log.debug(f"fetch_series_metadata returning final result for '{show_title_guess}': Source='{final_meta.source_api}', Title='{final_meta.show_title}', Year={final_meta.show_year}, S={final_meta.season}, EPs={final_meta.episode_list}, EpTitles={len(final_meta.episode_titles)}, Score={final_meta.match_confidence}")
        return final_meta