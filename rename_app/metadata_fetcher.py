# --- START OF FILE metadata_fetcher.py ---

import logging
import time
import asyncio
from functools import wraps, partial
from pathlib import Path
from typing import Optional, Tuple, TYPE_CHECKING, Any, Iterable, Sequence, Dict, cast

from .api_clients import get_tmdb_client, get_tvdb_client
from .exceptions import MetadataError
from .models import MediaMetadata # Ensure MediaMetadata is imported

log = logging.getLogger(__name__)

# --- (Imports and Helper functions/classes remain the same) ---
try: import diskcache; DISKCACHE_AVAILABLE = True
except ImportError: DISKCACHE_AVAILABLE = False
try: import platformdirs; PLATFORMDIRS_AVAILABLE = True
except ImportError: PLATFORMDIRS_AVAILABLE = False
try: from thefuzz import process as fuzz_process; THEFUZZ_AVAILABLE = True
except ImportError: THEFUZZ_AVAILABLE = False
try: from tenacity import RetryError; TENACITY_AVAILABLE = True
except ImportError: TENACITY_AVAILABLE = False; RetryError = Exception
try: import dateutil.parser; DATEUTIL_AVAILABLE = True
except ImportError: DATEUTIL_AVAILABLE = False
try: import requests.exceptions as req_exceptions
except ImportError:
    class req_exceptions: ConnectionError=IOError; Timeout=IOError; RequestException=IOError; HTTPError=type('HTTPError',(RequestException,),{'response':type('MockResponse',(),{'status_code':0})()})
try: from tmdbv3api.exceptions import TMDbException
except ImportError: TMDbException = type('TMDbException', (Exception,), {})
try: from tmdbv3api.as_obj import AsObj
except ImportError: AsObj = None

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

def should_retry_api_error(exception):
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
    if isinstance(exception, TMDbException):
        msg_lower = str(exception).lower()
        if "invalid api key" in msg_lower or "authentication failed" in msg_lower:
            log.error(f"Retry check FAILED for TMDbException (API Key Issue): {exception}"); return False
        if "resource not found" in msg_lower or "could not be found" in msg_lower:
            log.debug(f"Retry check FAILED for TMDbException (Not Found): {exception}"); return False
        log.debug(f"Retry check FAILED for other TMDbException: {exception}"); return False
    if isinstance(exception, (ValueError, Exception)):
        msg = str(exception).lower()
        if "failed to get" in msg and ("not found" in msg or "no record" in msg or "invalid id" in msg or "404" in msg):
            log.debug(f"Retry check FAILED for TVDB (likely Not Found): {msg}"); return False
        if "unauthorized" in msg or "api key" in msg or "401" in msg:
            log.error(f"Retry check FAILED for TVDB (likely API Key/Auth Issue): {msg}"); return False
        if isinstance(exception, UnboundLocalError):
            log.error(f"Retry check FAILED for UnboundLocalError (internal logic error): {msg}"); return False
        if '500' in msg or '502' in msg or '503' in msg or '504' in msg or 'timeout' in msg:
            log.warning(f"Retry check PASSED for potential TVDB Server Error/Timeout: {type(exception).__name__}: {msg}")
            return True
        log.debug(f"Retry check FAILED for generic ValueError/Exception (TVDB?): {type(exception).__name__}: {msg}"); return False
    log.debug(f"Retry check FAILED by default for: {type(exception).__name__}"); return False

def find_best_match(title_to_find, api_results_tuple, result_key='title', id_key='id', score_cutoff=70):
    if not api_results_tuple: return None, None
    if not isinstance(api_results_tuple, tuple):
        first = next(iter(api_results_tuple), None)
        return first, None
    api_results = api_results_tuple; first_result = next(iter(api_results), None)
    if not THEFUZZ_AVAILABLE: return first_result, None
    choices = {}; log.debug(f"Attempting to build choices for fuzzy match '{title_to_find}'. Input assumed dicts.")
    try:
        for r in api_results:
            if not isinstance(r, dict): continue
            current_id = r.get(id_key); current_result = r.get(result_key)
            if current_id is not None and current_result is not None: choices[current_id] = str(current_result)
    except Exception as e_choices: log.error(f"Error creating choices dict: {e_choices}", exc_info=True); return first_result, None
    if not choices: return first_result, None
    best = None; best_score = 0.0
    try:
        if not isinstance(title_to_find, str): title_to_find = str(title_to_find)
        processed_choices = {k: str(v) for k, v in choices.items()}
        best_result_list = fuzz_process.extractBests(title_to_find, processed_choices, score_cutoff=score_cutoff, limit=1)
        if best_result_list:
             matched_value, score, best_id = best_result_list[0]; best_score = float(score)
             log.debug(f"Fuzzy match '{title_to_find}': '{matched_value}' (ID:{best_id}) score {best_score:.1f}")
             for r_dict in api_results:
                 if isinstance(r_dict, dict) and str(r_dict.get(id_key)) == str(best_id): return r_dict, best_score
             log.error(f"Fuzzy match found ID {best_id} but couldn't find corresponding dict."); return first_result, None
        else: log.warning(f"Fuzzy match failed for '{title_to_find}' (cutoff {score_cutoff}). Falling back."); return first_result, None
    except Exception as e_fuzz: log.error(f"Error during fuzz_process extract: {e_fuzz}", exc_info=True); return first_result, None

def get_external_ids(tmdb_obj=None, tvdb_obj=None):
    # (Function unchanged)
    ids = {'imdb_id': None, 'tmdb_id': None, 'tvdb_id': None}
    try:
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
            collection_info = None
            collection_attr = getattr(tmdb_obj, 'belongs_to_collection', None)
            if isinstance(collection_attr, (dict, AsObj)): collection_info = collection_attr
            elif isinstance(tmdb_obj, dict): collection_info = tmdb_obj.get('belongs_to_collection')
            if collection_info:
                col_id = getattr(collection_info, 'id', None) if not isinstance(collection_info, dict) else collection_info.get('id')
                col_name = getattr(collection_info, 'name', None) if not isinstance(collection_info, dict) else collection_info.get('name')
                if col_id:
                    try: ids['collection_id'] = int(col_id)
                    except (ValueError, TypeError): log.warning(f"Could not convert collection ID '{col_id}' to int.")
                if col_name: ids['collection_name'] = str(col_name)
    except AttributeError as e_tmdb: log.debug(f"AttributeError parsing TMDB external IDs: {e_tmdb}")
    except Exception as e_tmdb_other: log.warning(f"Unexpected error parsing TMDB external IDs: {e_tmdb_other}", exc_info=True)
    try:
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
# --- End Unchanged Section ---


class MetadataFetcher:
    def __init__(self, cfg_helper):
        # (Initialization unchanged)
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

    def _get_year_from_date(self, date_str):
        # (Unchanged)
        if not date_str or not DATEUTIL_AVAILABLE: return None
        try: return dateutil.parser.parse(date_str).year
        except (ValueError, TypeError): return None

    async def _run_sync(self, func, *args, **kwargs):
        # (Unchanged)
        loop = asyncio.get_running_loop()
        func_call = partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, func_call)

    async def _get_cache(self, key):
        # (Unchanged)
        if not self.cache_enabled or not self.cache: return None
        _cache_miss = object()
        try:
            cached_value = await self._run_sync(self.cache.get, key, default=_cache_miss)
            if cached_value is not _cache_miss:
                log.debug(f"Cache HIT for key: {key}")
                if isinstance(cached_value, tuple) and len(cached_value) == 3:
                    return cached_value
                else:
                    log.warning(f"Cache data for {key} has unexpected structure. Ignoring cache.")
                    await self._run_sync(self.cache.delete, key)
                    return None
            else:
                log.debug(f"Cache MISS for key: {key}");
                return None
        except Exception as e: log.warning(f"Error getting from cache key '{key}': {e}", exc_info=True); return None

    async def _set_cache(self, key, value):
        # (Unchanged)
        if not self.cache_enabled or not self.cache: return
        if not (isinstance(value, tuple) and len(value) == 3):
             log.error(f"Attempted to cache value with incorrect structure for key {key}. Aborting cache set.")
             return
        try:
            await self._run_sync(self.cache.set, key, value, expire=self.cache_expire)
            log.debug(f"Cache SET for key: {key}")
        except Exception as e: log.warning(f"Error setting cache key '{key}': {e}", exc_info=True)

    # --- _do_fetch_tmdb_movie (Retry loop updated, inner sync function unchanged) ---
    async def _do_fetch_tmdb_movie(self, title_arg, year_arg, lang='en'):
        if not self.tmdb:
            log.warning("TMDB client not available in _do_fetch_tmdb_movie.")
            return None, None, None

        from tmdbv3api import Movie
        # --- _sync_tmdb_movie_fetch (unchanged) ---
        def _sync_tmdb_movie_fetch(sync_title, sync_year, sync_lang):
            log.debug(f"Executing TMDB Movie Fetch [sync thread] for: '{sync_title}' (lang: {sync_lang}, year: {sync_year}, strategy: {self.tmdb_strategy}, tolerance: {self.year_tolerance})")
            search = Movie(); results_obj = None; processed_results = None; movie_match = None; match_score = None
            try:
                if not isinstance(sync_title, str): sync_title = str(sync_title)
                results_obj = search.search(sync_title)
                log.debug(f"TMDB raw movie search results [sync thread] for '{sync_title}': Count={len(results_obj) if results_obj else 0}")
                if not results_obj: log.warning(f"TMDB Search returned no results for movie '{sync_title}'."); return None, None, None
                processed_results = results_obj
            except TMDbException as e_search:
                msg_lower = str(e_search).lower()
                if "resource not found" in msg_lower or "could not be found" in msg_lower: log.warning(f"TMDB Search resulted in 'Not Found' for movie '{sync_title}': {e_search}"); return None, None, None
                log.error(f"TMDbException during TMDB movie search for '{sync_title}' [sync thread]: {e_search}", exc_info=True); raise e_search
            except Exception as e_search: log.error(f"Unexpected error during TMDB movie search for '{sync_title}' [sync thread]: {e_search}", exc_info=True); raise e_search
            if sync_year and processed_results:
                log.debug(f"Applying year filter ({sync_year} +/- {self.year_tolerance}) to TMDB movie results [sync thread].")
                filtered_list = []
                try:
                    for r in processed_results:
                        release_year = None; release_date_val = getattr(r, 'release_date', None)
                        if release_date_val and isinstance(release_date_val, str) and len(release_date_val) >= 4:
                            try: release_year = int(release_date_val.split('-')[0])
                            except (ValueError, IndexError, TypeError): pass
                        if release_year is not None and abs(release_year - sync_year) <= self.year_tolerance: log.debug(f"  -> Year filter PASSED for '{getattr(r, 'title', 'N/A')}' ({release_year}) [sync thread]"); filtered_list.append(r)
                        else: log.debug(f"  -> Year filter FAILED for '{getattr(r, 'title', 'N/A')}' ({release_year or 'N/A'}) [sync thread]")
                    if not filtered_list and processed_results: log.debug(f"Year filtering removed all TMDB movie results, keeping original {len(processed_results)} for matching [sync thread].")
                    else: processed_results = filtered_list
                except Exception as e_filter: log.error(f"Error during TMDB movie year filtering [sync thread]: {e_filter}", exc_info=True); processed_results = None
            if processed_results:
                if self.tmdb_strategy == 'fuzzy' and THEFUZZ_AVAILABLE:
                    log.debug(f"Attempting TMDB movie fuzzy match (cutoff: {self.tmdb_fuzzy_cutoff}) [sync thread].")
                    results_as_dicts = _tmdb_results_to_dicts(processed_results, result_type='movie')
                    if results_as_dicts:
                        matched_dict, score = find_best_match(sync_title, tuple(results_as_dicts), result_key='title', id_key='id', score_cutoff=self.tmdb_fuzzy_cutoff)
                        if matched_dict:
                            matched_id = matched_dict.get('id'); log.debug(f"Fuzzy match found ID: {matched_id}. Finding original AsObj... [sync thread]")
                            try: movie_match = next((r for r in processed_results if getattr(r, 'id', None) == matched_id), None); match_score = score
                            except Exception as e_find: log.error(f"Error finding movie AsObj after fuzzy match [sync thread]: {e_find}", exc_info=True)
                if not movie_match:
                    if self.tmdb_strategy == 'fuzzy': log.debug("Fuzzy match failed or unavailable for movie, falling back to 'first'.")
                    log.debug("Using 'first' result strategy for TMDB movie [sync thread].")
                    try: movie_match = next(iter(processed_results), None)
                    except Exception as e_first: log.error(f"Error getting first TMDB movie result [sync thread]: {e_first}", exc_info=True)
            if not movie_match: log.warning(f"No suitable TMDB movie match found for '{sync_title}' (after filtering/matching)."); return None, None, None
            movie_id = getattr(movie_match, 'id', None)
            if not movie_id: log.error(f"TMDB movie match lacks 'id': {movie_match}"); return None, None, None
            log.debug(f"TMDB matched movie '{getattr(movie_match, 'title', 'N/A')}' ID: {movie_id} [sync thread] (Score: {match_score if match_score is not None else 'N/A'})")
            movie_details = None; final_data_obj = movie_match
            try:
                movie_details = search.details(movie_id)
                if movie_details: final_data_obj = movie_details
            except TMDbException as e_details:
                 if "resource not found" in str(e_details).lower(): log.warning(f"TMDB movie details for ID {movie_id} ('{getattr(movie_match, 'title', 'N/A')}') not found. Using search result data.")
                 else: log.error(f"TMDbException fetching TMDB movie details ID {movie_id} [sync thread]: {e_details}"); raise e_details
            except Exception as e_details: log.error(f"Unexpected error fetching TMDB movie details ID {movie_id} [sync thread]: {e_details}"); raise e_details
            combined_data_for_ids = {}; ids = {}
            if final_data_obj:
                 try:
                     if hasattr(final_data_obj, '_data') and isinstance(final_data_obj._data, dict): combined_data_for_ids = final_data_obj._data.copy()
                     else: combined_data_for_ids = {'id': movie_id, 'title': getattr(final_data_obj, 'title', None), 'release_date': getattr(final_data_obj, 'release_date', None)}
                     ext_ids_data = {}
                     try:
                         if movie_details:
                            ext_ids_method = getattr(movie_details, 'external_ids', None)
                            if callable(ext_ids_method): ext_ids_data = ext_ids_method()
                            elif isinstance(ext_ids_method, dict): ext_ids_data = ext_ids_method
                            else: ext_ids_data = search.external_ids(movie_id)
                         else: ext_ids_data = search.external_ids(movie_id)
                     except TMDbException as e_ext:
                         if "resource not found" not in str(e_ext).lower(): log.warning(f"TMDbException fetching TMDB external IDs for movie ID {movie_id} [sync thread]: {e_ext}")
                         else: log.debug(f"TMDB external IDs not found for movie ID {movie_id}.")
                     except Exception as e_ext: log.warning(f"Unexpected error fetching TMDB external IDs for movie ID {movie_id} [sync thread]: {e_ext}")
                     combined_data_for_ids['external_ids'] = ext_ids_data
                     if 'belongs_to_collection' not in combined_data_for_ids:
                         collection_attr = getattr(final_data_obj, 'belongs_to_collection', None)
                         if isinstance(collection_attr, (dict, AsObj)): combined_data_for_ids['belongs_to_collection'] = collection_attr
                 except Exception as e_comb: log.error(f"Error creating combined_data_for_ids [sync thread]: {e_comb}")
            ids = get_external_ids(tmdb_obj=combined_data_for_ids)
            log.debug(f"_sync_tmdb_movie_fetch returning: data type={type(final_data_obj)}, ids={ids}, score={match_score}")
            return final_data_obj, ids, match_score
        # --- END _sync_tmdb_movie_fetch ---

        attempts_cfg = self.cfg('api_retry_attempts', 3); wait_sec_cfg = self.cfg('api_retry_wait_seconds', 2)
        max_attempts = max(1, attempts_cfg if attempts_cfg is not None else 3); wait_seconds = max(0, wait_sec_cfg if wait_sec_cfg is not None else 2)
        last_exception = None

        for attempt in range(max_attempts):
            try:
                log.debug(f"Attempt {attempt + 1}/{max_attempts} for TMDB movie: '{title_arg}' ({year_arg})")
                data_obj, ids_dict, score = await self._run_sync(_sync_tmdb_movie_fetch, title_arg, year_arg, lang)
                if data_obj is None:
                    log.info(f"TMDB movie '{title_arg}' ({year_arg}) not found or no match after filtering.")
                    # --- FIX: Raise MetadataError for "Not Found" only if it's the final attempt or non-retryable ---
                    # If it's simply not found, we return None, None, None silently unless it was a non-retryable error.
                    # The user_facing_error logic below handles non-retryable cases.
                    return None, None, None
                return data_obj, ids_dict, score
            except Exception as e:
                last_exception = e
                user_facing_error = None; should_stop_retries = False
                error_context = f"Movie: '{title_arg}'"

                if isinstance(e, (TMDbException, req_exceptions.HTTPError)):
                    msg_lower = str(e).lower(); status_code = 0
                    if isinstance(e, req_exceptions.HTTPError): status_code = getattr(getattr(e, 'response', None), 'status_code', 0)
                    if "invalid api key" in msg_lower or status_code == 401 or "authentication failed" in msg_lower:
                        user_facing_error = f"Invalid TMDB API Key or Authentication Failed ({error_context}). Please check your key in the configuration/environment."
                        log.error(f"{user_facing_error} Details: {e}"); should_stop_retries = True
                    elif status_code == 403:
                        user_facing_error = f"TMDB API request forbidden ({error_context}). Check API key permissions or potential IP blocks."
                        log.error(f"{user_facing_error} Details: {e}"); should_stop_retries = True
                    elif "resource not found" in msg_lower or status_code == 404:
                        log.warning(f"TMDB resource not found {error_context}. Error: {e}")
                        # Return None here as it's a definitive "not found"
                        return None, None, None
                    else: log.warning(f"Attempt {attempt + 1} TMDB API error ({error_context}): {type(e).__name__}: {e}")
                elif isinstance(e, (req_exceptions.ConnectionError, req_exceptions.Timeout)):
                     log.warning(f"Attempt {attempt + 1} failed ({error_context}): Network connection error: {type(e).__name__}: {e}")
                else: log.warning(f"Attempt {attempt + 1} failed ({error_context}): Unexpected error: {type(e).__name__}: {e}")

                if should_stop_retries: raise MetadataError(user_facing_error) from e
                if not should_retry_api_error(e):
                    log.error(f"Non-retryable error occurred ({error_context}).")
                    user_facing_error = user_facing_error or f"Non-retryable error fetching TMDB metadata ({error_context}). Details: {e}"
                    raise MetadataError(user_facing_error) from e

                if attempt < max_attempts - 1:
                    log.info(f"Retrying TMDB movie fetch for '{title_arg}' in {wait_seconds}s... ({attempt+1}/{max_attempts})")
                    await asyncio.sleep(wait_seconds)
                else:
                    log.error(f"All {max_attempts} retry attempts failed for TMDB movie '{title_arg}'.")
                    final_error_msg = f"Failed to fetch TMDB metadata ({error_context}) after {max_attempts} attempts."
                    if isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network connection."
                    elif isinstance(last_exception, req_exceptions.HTTPError) and 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely a temporary TMDB server issue."
                    raise MetadataError(final_error_msg) from last_exception
        return None, None, None # Fallback return


    # --- _do_fetch_tmdb_series (Retry loop updated, inner sync function unchanged) ---
    async def _do_fetch_tmdb_series(self, title_arg, season_arg, episodes_arg, year_guess_arg=None, lang='en'):
        if not self.tmdb:
            log.warning("TMDB client not available in _do_fetch_tmdb_series.")
            return None, None, None, None

        from tmdbv3api import TV, Season
        # --- _sync_tmdb_series_fetch (unchanged) ---
        def _sync_tmdb_series_fetch(sync_title, sync_season, sync_episodes, sync_year_guess, sync_lang):
            log.debug(f"Executing TMDB Series Fetch [sync thread] for: '{sync_title}' S{sync_season} E{sync_episodes} (lang: {sync_lang}, year: {sync_year_guess}, ...)")
            search = TV(); results_obj = None; processed_results = None; show_match = None; match_score = None
            try:
                if not isinstance(sync_title, str): sync_title = str(sync_title)
                results_obj = search.search(sync_title)
                log.debug(f"TMDB raw series search results [sync thread] for '{sync_title}': Count={len(results_obj) if results_obj else 0}")
                if not results_obj: log.warning(f"TMDB Search returned no results for series '{sync_title}'."); return None, None, None, None
                processed_results = results_obj
            except TMDbException as e_search:
                 msg_lower = str(e_search).lower()
                 if "resource not found" in msg_lower or "could not be found" in msg_lower: log.warning(f"TMDB Search resulted in 'Not Found' for series '{sync_title}': {e_search}"); return None, None, None, None
                 log.error(f"TMDbException during TMDB series search for '{sync_title}' [sync thread]: {e_search}", exc_info=True); raise e_search
            except Exception as e_search: log.error(f"Unexpected error during TMDB series search for '{sync_title}' [sync thread]: {e_search}", exc_info=True); raise e_search
            if sync_year_guess and processed_results:
                filtered_list = []
                try:
                    for r in processed_results:
                         result_year = None; first_air_date = getattr(r, 'first_air_date', None)
                         if first_air_date and isinstance(first_air_date, str) and len(first_air_date) >= 4:
                             try: result_year = int(first_air_date.split('-')[0])
                             except (ValueError, IndexError, TypeError): pass
                         if result_year is not None and abs(result_year - sync_year_guess) <= self.year_tolerance: filtered_list.append(r)
                    if not filtered_list and processed_results: log.debug(f"Year filtering removed all TMDB series results, keeping original {len(processed_results)}.")
                    else: processed_results = filtered_list
                    log.debug(f"Year filtering resulted in {len(processed_results)} TMDB series results.")
                except Exception as e_filter: log.error(f"Error during TMDB series year filtering: {e_filter}", exc_info=True); processed_results = None
            if processed_results:
                if self.tmdb_strategy == 'fuzzy' and THEFUZZ_AVAILABLE:
                    results_as_dicts = _tmdb_results_to_dicts(processed_results, result_type='series')
                    if results_as_dicts:
                        matched_dict, score = find_best_match(sync_title, tuple(results_as_dicts), result_key='name', id_key='id', score_cutoff=self.tmdb_fuzzy_cutoff)
                        if matched_dict:
                            matched_id = matched_dict.get('id');
                            try: show_match = next((r for r in processed_results if getattr(r, 'id', None) == matched_id), None); match_score = score
                            except Exception: pass
                if not show_match:
                    if self.tmdb_strategy == 'fuzzy': log.debug("Fuzzy match failed or unavailable for series, falling back to 'first'.")
                    try: show_match = next(iter(processed_results), None)
                    except Exception: pass
            if not show_match: log.warning(f"No suitable TMDB series match found for '{sync_title}' S{sync_season} (after filtering/matching)."); return None, None, None, None
            show_id = getattr(show_match, 'id', None)
            if not show_id: log.error(f"TMDB series match lacks 'id': {show_match}"); return None, None, None, None
            log.debug(f"TMDB matched series '{getattr(show_match, 'name', 'N/A')}' ID: {show_id} [sync thread] (Score: {match_score if match_score is not None else 'N/A'})")
            show_details = None; final_show_data_obj = show_match
            try:
                show_details = search.details(show_id)
                if show_details: final_show_data_obj = show_details
            except TMDbException as e_details:
                 if "resource not found" in str(e_details).lower(): log.warning(f"TMDB series details for ID {show_id} not found.")
                 else: log.error(f"TMDbException fetching series details ID {show_id}: {e_details}"); raise e_details
            except Exception as e_details: log.error(f"Unexpected error fetching series details ID {show_id}: {e_details}"); raise e_details
            ep_data = {}
            if sync_episodes:
                try:
                    season_fetcher = Season(); season_details = season_fetcher.details(tv_id=show_id, season_num=sync_season)
                    if hasattr(season_details, 'episodes'):
                        episodes_in_season = {}
                        for api_ep in season_details.episodes:
                            ep_num_api = getattr(api_ep, 'episode_number', None)
                            if ep_num_api is not None:
                                try: episodes_in_season[int(ep_num_api)] = api_ep
                                except (ValueError, TypeError): pass
                        for ep_num_needed in sync_episodes:
                            episode_obj = episodes_in_season.get(ep_num_needed)
                            if episode_obj: ep_data[ep_num_needed] = episode_obj
                    else: log.warning(f"TMDB season details S{sync_season} ID {show_id} lacks 'episodes'.")
                except TMDbException as e_season:
                    if "resource not found" in str(e_season).lower(): log.warning(f"TMDB season S{sync_season} for ID {show_id} not found.")
                    else: log.warning(f"TMDbException getting season S{sync_season} ID {show_id}: {e_season}")
                except Exception as e_season: log.warning(f"Unexpected error getting season S{sync_season} ID {show_id}: {e_season}")
            combined_show_data = {}; ids = {}
            if final_show_data_obj:
                 try:
                     if hasattr(final_show_data_obj, '_data') and isinstance(final_show_data_obj._data, dict): combined_show_data = final_show_data_obj._data.copy()
                     else: combined_show_data = {'id': show_id, 'name': getattr(final_show_data_obj, 'name', None), 'first_air_date': getattr(final_show_data_obj, 'first_air_date', None)}
                     ext_ids_data = {}; fetched_ext_ids = False
                     if show_details and hasattr(show_details, 'external_ids') and callable(show_details.external_ids):
                          try: ext_ids_data = show_details.external_ids(); fetched_ext_ids = True
                          except Exception: pass
                     if not fetched_ext_ids:
                         try: ext_ids_data = search.external_ids(show_id)
                         except TMDbException as e_ext:
                             if "resource not found" not in str(e_ext).lower(): log.warning(f"TMDbException fetching external IDs for series ID {show_id}: {e_ext}")
                             else: log.debug(f"TMDB external IDs not found for series ID {show_id}.")
                         except Exception as e_ext: log.warning(f"Unexpected error fetching external IDs for series ID {show_id}: {e_ext}")
                     combined_show_data['external_ids'] = ext_ids_data
                 except Exception as e_comb: log.error(f"Error creating combined_show_data [sync thread]: {e_comb}")
            ids = get_external_ids(tmdb_obj=combined_show_data)
            log.debug(f"_sync_tmdb_series_fetch returning: data type={type(final_show_data_obj)}, ep_map keys={list(ep_data.keys())}, ids={ids}, score={match_score}")
            return final_show_data_obj, ep_data, ids, match_score
        # --- END _sync_tmdb_series_fetch ---

        attempts_cfg = self.cfg('api_retry_attempts', 3); wait_sec_cfg = self.cfg('api_retry_wait_seconds', 2)
        max_attempts = max(1, attempts_cfg if attempts_cfg is not None else 3); wait_seconds = max(0, wait_sec_cfg if wait_sec_cfg is not None else 2)
        last_exception = None

        for attempt in range(max_attempts):
            try:
                log.debug(f"Attempt {attempt + 1}/{max_attempts} for TMDB series: '{title_arg}' S{season_arg}")
                show_obj, ep_map, ids_dict, score = await self._run_sync(_sync_tmdb_series_fetch, title_arg, season_arg, episodes_arg, year_guess_arg, lang)
                if show_obj is None:
                    log.info(f"TMDB series '{title_arg}' S{season_arg} not found or no match.")
                     # --- FIX: Raise MetadataError for "Not Found" only if it's the final attempt or non-retryable ---
                    return None, None, None, None
                return show_obj, ep_map, ids_dict, score
            except Exception as e:
                last_exception = e; user_facing_error = None; should_stop_retries = False
                error_context = f"Series: '{title_arg}' S{season_arg}"

                if isinstance(e, (TMDbException, req_exceptions.HTTPError)):
                    msg_lower = str(e).lower(); status_code = 0
                    if isinstance(e, req_exceptions.HTTPError): status_code = getattr(getattr(e, 'response', None), 'status_code', 0)
                    if "invalid api key" in msg_lower or status_code == 401 or "authentication failed" in msg_lower:
                        user_facing_error = f"Invalid TMDB API Key or Authentication Failed ({error_context}). Please check your key in the configuration/environment."
                        log.error(f"{user_facing_error} Details: {e}"); should_stop_retries = True
                    elif status_code == 403:
                        user_facing_error = f"TMDB API request forbidden ({error_context}). Check API key permissions or potential IP blocks."
                        log.error(f"{user_facing_error} Details: {e}"); should_stop_retries = True
                    elif "resource not found" in msg_lower or status_code == 404:
                        log.warning(f"TMDB resource not found ({error_context}). Error: {e}")
                        return None, None, None, None
                    else: log.warning(f"Attempt {attempt + 1} TMDB API error ({error_context}): {type(e).__name__}: {e}")
                elif isinstance(e, (req_exceptions.ConnectionError, req_exceptions.Timeout)):
                     log.warning(f"Attempt {attempt + 1} failed ({error_context}): Network connection error: {type(e).__name__}: {e}")
                else: log.warning(f"Attempt {attempt + 1} failed ({error_context}): Unexpected error: {type(e).__name__}: {e}")

                if should_stop_retries: raise MetadataError(user_facing_error) from e
                if not should_retry_api_error(e):
                    log.error(f"Non-retryable error occurred ({error_context}).")
                    user_facing_error = user_facing_error or f"Non-retryable error fetching TMDB metadata ({error_context}). Details: {e}"
                    raise MetadataError(user_facing_error) from e

                if attempt < max_attempts - 1:
                    log.info(f"Retrying TMDB series fetch for '{title_arg}' S{season_arg} in {wait_seconds}s... ({attempt+1}/{max_attempts})")
                    await asyncio.sleep(wait_seconds)
                else:
                    log.error(f"All {max_attempts} retry attempts failed for TMDB series '{title_arg}' S{season_arg}.")
                    final_error_msg = f"Failed to fetch TMDB metadata ({error_context}) after {max_attempts} attempts."
                    if isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network connection."
                    elif isinstance(last_exception, req_exceptions.HTTPError) and 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely a temporary TMDB server issue."
                    raise MetadataError(final_error_msg) from last_exception
        return None, None, None, None


    # --- _do_fetch_tvdb_series (Retry loop updated, inner sync function unchanged) ---
    async def _do_fetch_tvdb_series(self, title_arg: str, season_num_arg: int, episodes_arg: tuple, tvdb_id_arg: Optional[int] = None, year_guess_arg: Optional[int] = None, lang: str = 'en'):
        if not self.tvdb:
            log.warning("TVDB client not available in _do_fetch_tvdb_series.")
            return None, None, None, None

        # --- _sync_tvdb_series_fetch (unchanged) ---
        def _sync_tvdb_series_fetch(sync_title, sync_season_num, sync_episodes, sync_tvdb_id, sync_year_guess, sync_lang):
            log.debug(f"Executing TVDB Series Fetch [sync thread] for: '{sync_title}' S{sync_season_num} E{sync_episodes} (lang: {sync_lang}, year: {sync_year_guess}, id: {sync_tvdb_id}, ...)")
            show_data = None; best_match_id = sync_tvdb_id; search_results = None; match_score = None
            if not best_match_id:
                try:
                    log.debug(f"TVDB searching for: '{sync_title}' (Year guess: {sync_year_guess}) [sync thread]")
                    search_results = self.tvdb.search(sync_title)
                    log.debug(f"TVDB search returned {len(search_results) if search_results else 0} results [sync thread].")
                    if not search_results: log.warning(f"TVDB Search returned no results for series '{sync_title}'."); return None, None, None, None
                except (ValueError, Exception) as e_search:
                    msg = str(e_search).lower()
                    if "failed to get" in msg and ("not found" in msg or "no record" in msg or "invalid id" in msg or "404" in msg): log.warning(f"TVDB Search resulted in 'Not Found' for series '{sync_title}': {e_search} [sync thread]."); return None, None, None, None
                    log.warning(f"TVDB search failed for '{sync_title}': {type(e_search).__name__}: {e_search} [sync thread]", exc_info=True); raise e_search
                if search_results:
                    if sync_year_guess:
                        filtered_results = []
                        for r in search_results:
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
                             match_dict, score = find_best_match(sync_title, tuple(search_results), result_key='name', id_key='tvdb_id', score_cutoff=70)
                             if match_dict:
                                 matched_id_val = match_dict.get('tvdb_id');
                                 if matched_id_val: best_match_id = int(matched_id_val); match_score = score
                        except Exception as e_fuzz:
                            log.error(f"Error during TVDB fuzzy match: {e_fuzz}")
                            first = next(iter(search_results), None)
                            if first: best_match_id = first.get('tvdb_id')
                if not best_match_id: log.warning(f"TVDB could not find suitable match ID for series '{sync_title}' after search."); return None, None, None, None
            if best_match_id:
                try:
                    log.debug(f"TVDB fetching extended series data for ID: {best_match_id} [sync thread]")
                    show_data = self.tvdb.get_series_extended(best_match_id)
                    if not show_data or not isinstance(show_data, dict): log.warning(f"TVDB get_series_extended for ID {best_match_id} returned invalid data."); return None, None, None, None
                    log.debug(f"TVDB successfully fetched extended data for: {show_data.get('name', 'N/A')} (Score: {match_score if match_score is not None else 'N/A'})")
                except (ValueError, Exception) as e_fetch:
                    msg = str(e_fetch).lower()
                    if "failed to get" in msg and ("not found" in msg or "no record" in msg or "invalid id" in msg or "404" in msg): log.warning(f"TVDB get_series_extended failed for ID {best_match_id}: Not Found. Error: {e_fetch}"); return None, None, None, None
                    log.warning(f"TVDB get_series_extended failed for ID {best_match_id}: {type(e_fetch).__name__}: {e_fetch}", exc_info=True); raise e_fetch
            else: log.error(f"Internal logic error: best_match_id became None for '{sync_title}'."); return None, None, None, None
            ep_data = {}; ids = {}
            if show_data:
                try:
                    target_season_data = None; all_season_data = show_data.get('seasons', [])
                    if isinstance(all_season_data, list):
                         for season_info in all_season_data:
                              if not isinstance(season_info, dict): continue
                              season_num_from_api = season_info.get('number'); is_official = season_info.get('type', {}).get('type') == 'official'
                              if season_num_from_api is not None:
                                 try:
                                     if int(season_num_from_api) == int(sync_season_num) and is_official: target_season_data = season_info; break
                                 except (ValueError, TypeError): continue
                    if target_season_data:
                         all_episode_data = target_season_data.get('episodes', [])
                         if isinstance(all_episode_data, list):
                              episodes_in_season = {}
                              for ep in all_episode_data:
                                   if isinstance(ep, dict) and ep.get('number') is not None:
                                        try: episodes_in_season[int(ep['number'])] = ep
                                        except (ValueError, TypeError): pass
                              episode_iterator = sync_episodes if sync_episodes else []
                              for ep_num in episode_iterator:
                                   episode_details = episodes_in_season.get(ep_num)
                                   if episode_details: ep_data[ep_num] = episode_details
                         else: log.warning(f"TVDB S{sync_season_num} 'episodes' data is not a list: {type(all_episode_data)}")
                    else: log.warning(f"TVDB season {sync_season_num} not found or not 'official' for '{show_data.get('name')}'")
                except Exception as e_ep_extract: log.warning(f"TVDB error processing episode data: {e_ep_extract}", exc_info=True)
                try: ids = get_external_ids(tvdb_obj=show_data)
                except Exception as e_ids: log.warning(f"Error extracting external IDs from TVDB data: {e_ids}", exc_info=True)
            log.debug(f"_sync_tvdb_series_fetch returning: data type={type(show_data)}, ep_map keys={list(ep_data.keys())}, ids={ids}, score={match_score}")
            return show_data, ep_data, ids, match_score
        # --- END _sync_tvdb_series_fetch ---

        attempts_cfg = self.cfg('api_retry_attempts', 3); wait_sec_cfg = self.cfg('api_retry_wait_seconds', 2)
        max_attempts = max(1, attempts_cfg if attempts_cfg is not None else 3); wait_seconds = max(0, wait_sec_cfg if wait_sec_cfg is not None else 2)
        last_exception = None

        for attempt in range(max_attempts):
            try:
                log.debug(f"Attempt {attempt + 1}/{max_attempts} for TVDB series: '{title_arg}' S{season_num_arg}")
                show_dict, ep_map, ids_dict, score = await self._run_sync(_sync_tvdb_series_fetch, title_arg, season_num_arg, episodes_arg, tvdb_id_arg, year_guess_arg, lang)
                if show_dict is None:
                    log.info(f"TVDB series '{title_arg}' S{season_num_arg} not found or no match.")
                    # --- FIX: Raise MetadataError for "Not Found" only if it's the final attempt or non-retryable ---
                    return None, None, None, None
                return show_dict, ep_map, ids_dict, score
            except Exception as e:
                last_exception = e; user_facing_error = None; should_stop_retries = False
                error_context = f"Series: '{title_arg}' S{season_num_arg}"
                msg_lower = str(e).lower()

                if "unauthorized" in msg_lower or "api key" in msg_lower or "401" in msg_lower:
                     user_facing_error = f"Invalid TVDB API Key or Unauthorized ({error_context}). Please check your key in the configuration/environment."
                     log.error(f"{user_facing_error} Details: {e}"); should_stop_retries = True
                elif "failed to get" in msg_lower and ("not found" in msg_lower or "no record" in msg_lower or "404" in msg_lower):
                     log.warning(f"TVDB resource not found ({error_context}). Error: {e}")
                     return None, None, None, None
                elif isinstance(e, (req_exceptions.ConnectionError, req_exceptions.Timeout)):
                    log.warning(f"Attempt {attempt + 1} failed ({error_context}): Network connection error to TVDB: {type(e).__name__}: {e}")
                elif isinstance(e, req_exceptions.HTTPError):
                    status_code = getattr(getattr(e, 'response', None), 'status_code', 0)
                    if status_code == 403:
                        user_facing_error = f"TVDB API request forbidden ({error_context}). Check API key permissions or potential IP blocks."
                        log.error(f"{user_facing_error} Details: {e}"); should_stop_retries = True
                    elif 500 <= status_code <= 599:
                        log.warning(f"Attempt {attempt+1} TVDB API server error ({status_code}) ({error_context}): {e}")
                    else: log.warning(f"Attempt {attempt + 1} TVDB API HTTP error ({status_code}) ({error_context}): {type(e).__name__}: {e}")
                else: log.warning(f"Attempt {attempt + 1} failed ({error_context}): Unexpected TVDB error: {type(e).__name__}: {e}")

                if should_stop_retries: raise MetadataError(user_facing_error) from e
                if not should_retry_api_error(e):
                    log.error(f"Non-retryable error occurred ({error_context}).")
                    user_facing_error = user_facing_error or f"Non-retryable error fetching TVDB metadata ({error_context}). Details: {e}"
                    raise MetadataError(user_facing_error) from e

                if attempt < max_attempts - 1:
                    log.info(f"Retrying TVDB series fetch for '{title_arg}' S{season_num_arg} in {wait_seconds}s... ({attempt+1}/{max_attempts})")
                    await asyncio.sleep(wait_seconds)
                else:
                    log.error(f"All {max_attempts} retry attempts failed for TVDB series '{title_arg}' S{season_num_arg}.")
                    final_error_msg = f"Failed to fetch TVDB metadata ({error_context}) after {max_attempts} attempts."
                    if isinstance(last_exception, (req_exceptions.ConnectionError, req_exceptions.Timeout)): final_error_msg += " Check network connection."
                    elif isinstance(last_exception, req_exceptions.HTTPError) and 500 <= getattr(getattr(last_exception, 'response', None), 'status_code', 0) <= 599: final_error_msg += " Likely a temporary TVDB server issue."
                    elif isinstance(last_exception, ValueError) and "not found" in str(last_exception).lower(): final_error_msg = f"TVDB resource not found ({error_context}) after {max_attempts} attempts."
                    raise MetadataError(final_error_msg) from last_exception
        return None, None, None, None


    async def fetch_movie_metadata(self, movie_title_guess: str, year_guess: Optional[int] = None) -> MediaMetadata:
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
                 fetch_error_message = "Metadata fetch returned no usable TMDB data." # Set generic failure message

        if tmdb_movie_data:
            try:
                final_meta.source_api = "tmdb"
                final_meta.match_confidence = tmdb_score
                title_val = getattr(tmdb_movie_data, 'title', None); release_date_val = getattr(tmdb_movie_data, 'release_date', None)
                final_meta.movie_title = title_val; final_meta.release_date = release_date_val; final_meta.movie_year = self._get_year_from_date(final_meta.release_date)
                if isinstance(tmdb_ids, dict):
                     final_meta.ids = tmdb_ids; final_meta.collection_name = tmdb_ids.get('collection_name'); final_meta.collection_id = tmdb_ids.get('collection_id')
                else: final_meta.ids = {}
                # No metadata_error_message attribute on MediaMetadata
                log.debug(f"Successfully populated final_meta from TMDB for '{movie_title_guess}'. Score: {tmdb_score}")
            except Exception as e_populate:
                log.error(f"Error populating final_meta for '{movie_title_guess}': {e_populate}", exc_info=True);
                final_meta.source_api = None # Mark as failed if population error occurs
                fetch_error_message = fetch_error_message or f"Error processing TMDB data: {e_populate}" # Store population error if no fetch error existed

        if not final_meta.source_api:
             log.warning(f"Metadata fetch or population ultimately failed for movie: '{movie_title_guess}' (Year guess: {year_guess})")
             if not final_meta.movie_title: final_meta.movie_title = movie_title_guess
             if not final_meta.movie_year: final_meta.movie_year = year_guess
             # --- Add the error message back to the object IF it failed ---
             # This relies on MediaInfo having the attribute, which it does.
             # However, this function returns MediaMetadata. We should let the caller handle the error.
             # We raise the error here if one occurred.
             if fetch_error_message:
                 # We raise here so _fetch_metadata_for_batch can catch it and add to MediaInfo
                 raise MetadataError(fetch_error_message)

        # --- FIX: Remove Error from final log message ---
        log.debug(f"fetch_movie_metadata returning final result for '{movie_title_guess}': Source='{final_meta.source_api}', Title='{final_meta.movie_title}', Year={final_meta.movie_year}, IDs={final_meta.ids}, Collection={final_meta.collection_name}, Score={final_meta.match_confidence}")
        return final_meta


    async def fetch_series_metadata(self, show_title_guess: str, season_num: int, episode_num_list: Tuple[int, ...], year_guess: Optional[int] = None) -> MediaMetadata:
        log.debug(f"Fetching series metadata (async) for: '{show_title_guess}' S{season_num}E{episode_num_list} (Year guess: {year_guess})")
        final_meta = MediaMetadata(is_series=True)
        tmdb_show_data, tmdb_ep_map, tmdb_ids, tmdb_score = None, None, None, None
        tvdb_show_data, tvdb_ep_map, tvdb_ids, tvdb_score = None, None, None, None
        tmdb_error_message = None
        tvdb_error_message = None

        lang = self.cfg('tmdb_language', 'en'); episode_num_tuple = episode_num_list
        cache_key_base = f"series::{show_title_guess}_{season_num}_{episode_num_tuple}_{year_guess}_{lang}"

        if self.tmdb:
            tmdb_cache_key = cache_key_base + "::tmdb"
            cached_tmdb = await self._get_cache(tmdb_cache_key)
            if cached_tmdb:
                tmdb_show_data, cached_tmdb_ids, tmdb_score = cached_tmdb
                if cached_tmdb_ids and '_ep_map' in cached_tmdb_ids: tmdb_ep_map = cached_tmdb_ids.pop('_ep_map')
                tmdb_ids = cached_tmdb_ids
                log.debug(f"Using cached TMDB data for series: '{show_title_guess}' S{season_num} (Score: {tmdb_score})")
            else:
                try:
                    await self.rate_limiter.wait()
                    tmdb_show_data, tmdb_ep_map, tmdb_ids, tmdb_score = await self._do_fetch_tmdb_series(show_title_guess, season_num, episode_num_tuple, year_guess, lang)
                    cacheable_tmdb_ids = tmdb_ids or {}; cacheable_tmdb_ids['_ep_map'] = tmdb_ep_map
                    await self._set_cache(tmdb_cache_key, (tmdb_show_data, cacheable_tmdb_ids, tmdb_score))
                    if '_ep_map' in cacheable_tmdb_ids: tmdb_ep_map = cacheable_tmdb_ids.pop('_ep_map')
                except MetadataError as me:
                    log.error(f"TMDB series fetch failed: {me}"); tmdb_error_message = str(me)
                    tmdb_show_data, tmdb_ep_map, tmdb_ids, tmdb_score = None, None, None, None
                except Exception as e:
                    log.error(f"Unexpected error during TMDB series fetch for '{show_title_guess}' S{season_num}: {type(e).__name__}: {e}", exc_info=True); tmdb_error_message = f"Unexpected TMDB error: {e}"
                    tmdb_show_data, tmdb_ep_map, tmdb_ids, tmdb_score = None, None, None, None
        else: log.warning("TMDB client not available, skipping TMDB series fetch.")

        if cached_tmdb and tmdb_ids and '_ep_map' in tmdb_ids: tmdb_ep_map = tmdb_ids.pop('_ep_map')

        tmdb_has_all_requested_eps = not episode_num_tuple or (tmdb_ep_map and all(ep_num in tmdb_ep_map for ep_num in episode_num_tuple))
        needs_tvdb_fallback = (not tmdb_show_data) or (episode_num_tuple and not tmdb_has_all_requested_eps)

        if needs_tvdb_fallback and self.tvdb:
            tvdb_cache_key = cache_key_base + "::tvdb"
            cached_tvdb = await self._get_cache(tvdb_cache_key)
            if cached_tvdb:
                tvdb_show_data, cached_tvdb_ids, tvdb_score = cached_tvdb;
                if cached_tvdb_ids and '_ep_map' in cached_tvdb_ids: tvdb_ep_map = cached_tvdb_ids.pop('_ep_map')
                tvdb_ids = cached_tvdb_ids
                log.debug(f"Using cached TVDB data for series: '{show_title_guess}' S{season_num} (Score: {tvdb_score})")
            else:
                log.debug(f"Attempting TVDB fallback for '{show_title_guess}' S{season_num} (async)...")
                tvdb_id_from_tmdb = tmdb_ids.get('tvdb_id') if tmdb_ids else None
                try:
                    await self.rate_limiter.wait()
                    tvdb_show_data, tvdb_ep_map, tvdb_ids, tvdb_score = await self._do_fetch_tvdb_series(
                        title_arg=show_title_guess, season_num_arg=season_num, episodes_arg=episode_num_tuple,
                        tvdb_id_arg=tvdb_id_from_tmdb, year_guess_arg=year_guess, lang=lang
                    )
                    cacheable_tvdb_ids = tvdb_ids or {}; cacheable_tvdb_ids['_ep_map'] = tvdb_ep_map
                    await self._set_cache(tvdb_cache_key, (tvdb_show_data, cacheable_tvdb_ids, tvdb_score))
                    if '_ep_map' in cacheable_tvdb_ids: tvdb_ep_map = cacheable_tvdb_ids.pop('_ep_map')
                except MetadataError as me:
                    log.error(f"TVDB series fetch failed: {me}"); tvdb_error_message = str(me)
                    tvdb_show_data, tvdb_ep_map, tvdb_ids, tvdb_score = None, None, None, None
                except Exception as e:
                    log.error(f"Unexpected error during TVDB series fetch for '{show_title_guess}' S{season_num}: {type(e).__name__}: {e}", exc_info=True); tvdb_error_message = f"Unexpected TVDB error: {e}"
                    tvdb_show_data, tvdb_ep_map, tvdb_ids, tvdb_score = None, None, None, None
        elif needs_tvdb_fallback: log.warning(f"TVDB client not available or fallback not triggered for '{show_title_guess}' S{season_num}, skipping TVDB series attempt.")

        final_meta.source_api = None; primary_show_data = None; primary_ep_map = None; merged_ids = {}; primary_score = None; final_error_message = None

        if tmdb_show_data and tmdb_has_all_requested_eps:
            final_meta.source_api = "tmdb"; primary_show_data = tmdb_show_data; primary_ep_map = tmdb_ep_map; primary_score = tmdb_score
            if tmdb_ids: merged_ids.update(tmdb_ids)
            if tvdb_ids:
                 for k, v in tvdb_ids.items():
                     if v is not None and k not in merged_ids: merged_ids[k] = v
            log.debug(f"Using TMDB as primary source for '{show_title_guess}' S{season_num}.")
        elif tvdb_show_data:
            final_meta.source_api = "tvdb"; primary_show_data = tvdb_show_data; primary_ep_map = tvdb_ep_map; primary_score = tvdb_score
            if tmdb_ids: merged_ids.update(tmdb_ids)
            if tvdb_ids:
                 for k, v in tvdb_ids.items():
                     if v is not None: merged_ids[k] = v
            log.debug(f"Using TVDB as primary source for '{show_title_guess}' S{season_num}.")
        elif tmdb_show_data:
            final_meta.source_api = "tmdb"; primary_show_data = tmdb_show_data; primary_ep_map = tmdb_ep_map; primary_score = tmdb_score
            if tmdb_ids: merged_ids.update(tmdb_ids)
            log.debug(f"Using TMDB (potentially incomplete episodes) as source for '{show_title_guess}' S{season_num}.")

        final_meta.ids = merged_ids; final_meta.match_confidence = primary_score
        show_title_api = None; show_air_date = None
        if primary_show_data:
            try: # Wrap population in try-except
                show_title_api = getattr(primary_show_data, 'name', None)
                if show_title_api is None and isinstance(primary_show_data, dict): show_title_api = primary_show_data.get('name')
                final_meta.show_title = show_title_api
                show_air_date = getattr(primary_show_data, 'first_air_date', None)
                if show_air_date is None and isinstance(primary_show_data, dict): show_air_date = primary_show_data.get('firstAired') or primary_show_data.get('first_air_date')
                final_meta.show_year = self._get_year_from_date(show_air_date)
                if primary_ep_map and episode_num_tuple:
                    for ep_num in episode_num_tuple:
                        ep_details = primary_ep_map.get(ep_num)
                        if ep_details:
                            ep_title = getattr(ep_details, 'name', None); air_date = getattr(ep_details, 'air_date', None)
                            if isinstance(ep_details, dict):
                                if ep_title is None: ep_title = ep_details.get('name')
                                if air_date is None: air_date = ep_details.get('aired')
                            if ep_title: final_meta.episode_titles[ep_num] = ep_title
                            if air_date: final_meta.air_dates[ep_num] = air_date
                        else: log.debug(f"Episode S{season_num}E{ep_num} not found in API map for '{show_title_guess}'.")
                final_meta.season = season_num; final_meta.episode_list = list(episode_num_tuple)
            except Exception as e_populate:
                 log.error(f"Error populating final_meta for series '{show_title_guess}': {e_populate}", exc_info=True);
                 final_meta.source_api = None # Mark as failed if population error occurs
                 # Preserve original fetch error message if it exists
                 final_error_message = tmdb_error_message or tvdb_error_message or f"Error processing API data: {e_populate}"


        if not final_meta.source_api:
             if not final_error_message: # Combine API fetch errors if population was ok but sources failed
                 if tmdb_error_message and tvdb_error_message: final_error_message = f"TMDB Error: {tmdb_error_message} | TVDB Error: {tvdb_error_message}"
                 elif tmdb_error_message: final_error_message = f"TMDB Error: {tmdb_error_message}"
                 elif tvdb_error_message: final_error_message = f"TVDB Error: {tvdb_error_message}"
                 else: final_error_message = "Metadata fetch failed from all sources."
             log.warning(f"Metadata fetch/population failed for series: '{show_title_guess}' S{season_num}E{episode_num_tuple}. Reason: {final_error_message}")
             if not final_meta.show_title: final_meta.show_title = show_title_guess
             if not final_meta.show_year: final_meta.show_year = year_guess
             # Raise the consolidated error message
             raise MetadataError(final_error_message)
        elif not final_meta.show_title: final_meta.show_title = show_title_guess # Ensure title exists

        # --- FIX: Remove Error from final log message ---
        log.debug(f"fetch_series_metadata returning final result for '{show_title_guess}': Source='{final_meta.source_api}', Title='{final_meta.show_title}', Year={final_meta.show_year}, S={final_meta.season}, EPs={final_meta.episode_list}, EpTitles={len(final_meta.episode_titles)}, Score={final_meta.match_confidence}")
        return final_meta
# --- END OF FILE metadata_fetcher.py ---