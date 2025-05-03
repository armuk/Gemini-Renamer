# --- START OF FILE metadata_fetcher.py ---

# rename_app/metadata_fetcher.py (Synchronous Version)

import logging
import time
from functools import lru_cache
from .api_clients import get_tmdb_client, get_tvdb_client
from .exceptions import MetadataError
from .models import MediaMetadata

# Import fuzzy matching etc.
try: from thefuzz import process as fuzz_process; THEFUZZ_AVAILABLE = True
except ImportError: THEFUZZ_AVAILABLE = False
try: from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception, RetryError; TENACITY_AVAILABLE = True # Added RetryError
except ImportError: TENACITY_AVAILABLE = False; RetryError = Exception # Fallback for RetryError
try: import dateutil.parser; DATEUTIL_AVAILABLE = True
except ImportError: DATEUTIL_AVAILABLE = False

# Import API specific exceptions and requests exceptions for retry logic
try:
    from tvdb_v4_official.errors import NotFoundError as TvdbNotFoundError, TvdbApiException
except ImportError:
    # Define fallbacks if library not installed
    TvdbNotFoundError = type('TvdbNotFoundError', (Exception,), {})
    TvdbApiException = type('TvdbApiException', (Exception,), {})

try:
    import requests.exceptions as req_exceptions
except ImportError:
    # Define fallback class if requests isn't installed (though it likely is)
    class req_exceptions:
        ConnectionError = type('ConnectionError', (IOError,), {})
        Timeout = type('Timeout', (IOError,), {})
        RequestException = type('RequestException', (IOError,), {})
        HTTPError = type('HTTPError', (RequestException,), {'response': type('MockResponse', (), {'status_code': 0})()}) # Mock response for checks

# Import TMDB exceptions if they exist and are useful for status codes
try:
    from tmdbv3api.exceptions import TMDbException
except ImportError:
    TMDbException = type('TMDbException', (Exception,), {}) # Fallback

# Import AsObj explicitly for type checking
try: from tmdbv3api.as_obj import AsObj
except ImportError: AsObj = None # Fallback

log = logging.getLogger(__name__)

# --- Sync Rate Limiter ---
class SyncRateLimiter:
    def __init__(self, delay: float):
        self.delay = delay
        self.last_call = 0
    def wait(self):
        if self.delay <= 0: return
        now = time.monotonic()
        since_last = now - self.last_call
        if since_last < self.delay:
             wait_time = self.delay - since_last
             log.debug(f"Rate limiting: sleeping for {wait_time:.2f}s")
             time.sleep(wait_time)
        self.last_call = time.monotonic()

# --- Enhanced Retry Logic ---
def should_retry_api_error(exception):
    """
    Predicate for tenacity retry decorator.
    Retries on connection errors, timeouts, 429 (Too Many Requests),
    and 5xx server errors. Does NOT retry on 404, 401, etc.
    """
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
        return False # Don't retry other HTTP errors (like 404, 401, 403)

    # Check for TVDB specific API exceptions (if they have status codes or specific types)
    if isinstance(exception, TvdbApiException):
        # Assuming TvdbApiException might have a status code or identifiable retryable state
        # This part might need refinement based on tvdb-v4-official specifics
        status_code = getattr(exception, 'status_code', None) # Check if attribute exists
        if status_code == 429:
             log.warning("Retry triggered for TVDB API 429.")
             return True
        if status_code and 500 <= status_code <= 599:
            log.warning(f"Retry triggered for TVDB API Server Error {status_code}.")
            return True
        # IMPORTANT: Do NOT retry on TvdbNotFoundError
        if isinstance(exception, TvdbNotFoundError):
            log.debug("Not retrying for TvdbNotFoundError.")
            return False
        log.debug(f"Not retrying for TVDB API Exception: {exception} (Status: {status_code})")
        return False # Don't retry other TVDB API errors by default

    # Check for TMDB specific exceptions (less likely needed if requests covers it)
    if isinstance(exception, TMDbException):
        # TMDbException often wraps requests errors, check underlying cause if possible
        # Or check specific TMDb error messages if needed.
        log.debug(f"Not retrying for TMDbException: {exception}")
        return False

    # Default: don't retry unknown exceptions caught by the generic wrapper
    log.debug(f"Not retrying for generic exception type: {type(exception).__name__}")
    return False

def setup_sync_retry_decorator(cfg_helper):
    if not TENACITY_AVAILABLE:
        log.debug("Tenacity not installed, retries disabled.")
        def dummy_decorator(func): return func
        return dummy_decorator

    attempts_cfg = cfg_helper('api_retry_attempts', 3)
    wait_sec_cfg = cfg_helper('api_retry_wait_seconds', 2)
    attempts = max(1, attempts_cfg if attempts_cfg is not None else 3)
    wait_sec = max(0, wait_sec_cfg if wait_sec_cfg is not None else 2)

    log.debug(f"Setting up sync retry decorator: Attempts={attempts}, Wait={wait_sec}s")

    # Use the custom predicate function
    return retry(
        stop=stop_after_attempt(attempts),
        wait=wait_fixed(wait_sec),
        retry=retry_if_exception(should_retry_api_error), # Use custom predicate
        reraise=True # Reraise the exception if all retries fail
    )
# --- End Enhanced Retry Logic ---


# --- Fuzzy Matching ---
# (Function unchanged)
def find_best_match(title_to_find, api_results_tuple, result_key='title', id_key='id', score_cutoff=70):
    # api_results_tuple is expected to contain DICTIONARIES ONLY
    if not api_results_tuple: return None
    api_results = api_results_tuple

    if not THEFUZZ_AVAILABLE: log.debug("thefuzz library not available, returning first result."); return api_results[0] if api_results else None
    if not isinstance(api_results, tuple) or not api_results: log.debug(f"Fuzzy match input 'api_results_tuple' is empty or not a tuple: {type(api_results_tuple)}"); return None

    choices = {}
    log.debug(f"Attempting to build choices for fuzzy match '{title_to_find}'. Input assumed dicts.")
    try:
        for r in api_results:
            if not r or not isinstance(r, dict): log.debug(f"  -> Skipped non-dict item: {r}"); continue
            current_id = r.get(id_key)
            current_result = r.get(result_key)
            if current_id is not None and current_result is not None: choices[current_id] = str(current_result); log.debug(f"  -> Added choice: ID={current_id}, Value='{str(current_result)}'")
            else: log.debug(f"  -> Skipped item (missing ID '{id_key}' or Result '{result_key}'): {r}")
    except Exception as e_choices: log.error(f"Error creating choices dict for fuzzy matching '{title_to_find}': {e_choices}", exc_info=True); return None

    if not choices: log.debug(f"No valid choices found for fuzzy matching '{title_to_find}'."); return None
    log.debug(f"Fuzzy matching choices for '{title_to_find}': {choices}")

    best = None
    try:
        if not isinstance(title_to_find, str): title_to_find = str(title_to_find)
        processed_choices = {k: str(v) for k, v in choices.items()} # Ensure string values
        best = fuzz_process.extractOne(title_to_find, processed_choices, score_cutoff=score_cutoff)
    except Exception as e_fuzz: log.error(f"Error during fuzz_process.extractOne for '{title_to_find}': {e_fuzz}", exc_info=True); return None

    if best:
        matched_value, score, best_id = best; log.debug(f"Fuzzy match '{title_to_find}': '{matched_value}' (ID:{best_id}) score {score}")
        for r_dict in api_results_tuple:
            if isinstance(r_dict, dict) and str(r_dict.get(id_key)) == str(best_id):
                 log.debug(f"Returning matched dict: {r_dict}"); return r_dict
    else: log.warning(f"Fuzzy match failed for '{title_to_find}' (cutoff {score_cutoff})"); return None
    log.error(f"Reached end of find_best_match unexpectedly for '{title_to_find}'"); return None

# --- External ID Helper ---
# (Function unchanged)
def get_external_ids(tmdb_obj=None, tvdb_obj=None):
    """Extract external IDs, updated for tvdb_v4_official."""
    ids = {'imdb_id': None, 'tmdb_id': None, 'tvdb_id': None}
    # TMDB part
    try:
        if tmdb_obj:
            tmdb_id_val = getattr(tmdb_obj, 'id', None)
            if tmdb_id_val is None and isinstance(tmdb_obj, dict): tmdb_id_val = tmdb_obj.get('id')
            if tmdb_id_val is not None: ids['tmdb_id'] = tmdb_id_val

            ext_ids_data = {}
            if isinstance(tmdb_obj, dict): ext_ids_data = tmdb_obj.get('external_ids', {})
            else:
                ext_ids_attr = getattr(tmdb_obj, 'external_ids', None)
                if isinstance(ext_ids_attr, dict): ext_ids_data = ext_ids_attr
                elif callable(ext_ids_attr):
                    try: ext_ids_data = ext_ids_attr()
                    except Exception as e_call: log.debug(f"Error calling external_ids method on TMDB object: {e_call}")
            imdb_id_found = ext_ids_data.get('imdb_id')
            if imdb_id_found: ids['imdb_id'] = str(imdb_id_found)
            tvdb_id_found = ext_ids_data.get('tvdb_id')
            if tvdb_id_found and ids.get('tvdb_id') is None:
                try: ids['tvdb_id'] = int(tvdb_id_found)
                except (ValueError, TypeError): log.warning(f"Could not convert TMDB-provided TVDB ID '{tvdb_id_found}' to int.")
    except AttributeError as e_tmdb: log.debug(f"AttributeError parsing TMDB external IDs: {e_tmdb}")
    except Exception as e_tmdb_other: log.warning(f"Unexpected error parsing TMDB external IDs: {e_tmdb_other}", exc_info=True)
    # TVDB v4 Part
    try:
        if isinstance(tvdb_obj, dict):
             if ids.get('tvdb_id') is None:
                 tvdb_id_val = tvdb_obj.get('id')
                 if tvdb_id_val is not None:
                    try: ids['tvdb_id'] = int(tvdb_id_val)
                    except (ValueError, TypeError): log.warning(f"Could not convert TVDB-provided TVDB ID '{tvdb_id_val}' to int.")
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
                     except(ValueError, TypeError): log.warning(f"Could not convert TVDB-provided TMDB ID '{tmdb_id_found}' to int.")
    except Exception as e_tvdb_ids: log.warning(f"Error parsing external IDs from TVDB object: {e_tvdb_ids}", exc_info=True)
    return {k: v for k, v in ids.items() if v is not None}

# --- Metadata Fetcher Class ---
class MetadataFetcher:
    def __init__(self, cfg_helper):
        self.cfg = cfg_helper
        self.tmdb = get_tmdb_client()
        self.tvdb = get_tvdb_client()
        self.rate_limiter = SyncRateLimiter(cfg_helper('api_rate_limit_delay', 0.5))
        self.retry_decorator = setup_sync_retry_decorator(cfg_helper) # Setup decorator

        # Apply decorator to core methods IF tenacity is available
        if TENACITY_AVAILABLE:
            self._do_fetch_tmdb_series = self.retry_decorator(self._do_fetch_tmdb_series)
            self._do_fetch_tvdb_series = self.retry_decorator(self._do_fetch_tvdb_series)
            self._do_fetch_tmdb_movie = self.retry_decorator(self._do_fetch_tmdb_movie)
        else:
            log.info("Tenacity not found, retry logic will not be applied to fetch methods.")


    def _get_year_from_date(self, date_str):
        # (Function unchanged)
        if not date_str or not DATEUTIL_AVAILABLE: return None
        try: return dateutil.parser.parse(date_str).year
        except (ValueError, TypeError): return None

    @lru_cache(maxsize=128)
    def fetch_series_metadata(self, show_title_guess, season_num, episode_num_list, year_guess=None):
        log.debug(f"Fetching series metadata for: '{show_title_guess}' S{season_num}E{episode_num_list} (Year guess: {year_guess})")
        final_meta = MediaMetadata(is_series=True)
        tmdb_show_data, tmdb_ep_map, tmdb_ids = None, None, None
        tvdb_show_data, tvdb_ep_map, tvdb_ids = None, None, None
        episode_num_tuple = tuple(episode_num_list) if isinstance(episode_num_list, (list, tuple)) else tuple()

        # --- Attempt TMDB First ---
        if not self.tmdb:
            log.warning("TMDB client not available, skipping TMDB series fetch.")
        else:
            try:
                self.rate_limiter.wait() # Apply rate limit before potentially retried call
                tmdb_show_data, tmdb_ep_map, tmdb_ids = self._do_fetch_tmdb_series(show_title_guess, season_num, episode_num_tuple, year_guess=year_guess)
                log.debug(f"fetch_series_metadata: _do_fetch_tmdb_series returned: data type={type(tmdb_show_data)}, ep_map keys={list(tmdb_ep_map.keys()) if tmdb_ep_map else None}, ids type={type(tmdb_ids)}")
            except RetryError as e:
                 log.error(f"TMDB series fetch ultimately failed after retries for '{show_title_guess}': {e}")
            except Exception as e:
                 log.error(f"Unexpected error during TMDB series fetch for '{show_title_guess}': {e}", exc_info=True)
            # Ensure reset on error
            if tmdb_show_data is None:
                 tmdb_show_data, tmdb_ep_map, tmdb_ids = None, None, None

        # --- TVDB Fallback Logic ---
        needs_tvdb_fallback = (not tmdb_show_data) or (tmdb_show_data and not tmdb_ep_map)
        if needs_tvdb_fallback:
            if not self.tvdb:
                log.warning("TVDB client not available, skipping TVDB series fallback.")
            else:
                if not tmdb_show_data: log.debug("TMDB failed completely, trying TVDB...")
                else: log.debug("TMDB found show but failed on episode details, trying TVDB as fallback...")
                tvdb_id_from_tmdb = tmdb_ids.get('tvdb_id') if tmdb_ids else None
                try:
                    self.rate_limiter.wait() # Rate limit before potentially retried TVDB call
                    tvdb_show_data, tvdb_ep_map, tvdb_ids = self._do_fetch_tvdb_series(
                        show_title_guess, season_num, episode_num_tuple,
                        tvdb_id=tvdb_id_from_tmdb, year_guess=year_guess
                    )
                except RetryError as e:
                     log.error(f"TVDB series fetch ultimately failed after retries for '{show_title_guess}': {e}")
                except Exception as e:
                     log.error(f"Unexpected error during TVDB series fetch for '{show_title_guess}': {e}", exc_info=True)
                # Ensure reset on error
                if tvdb_show_data is None:
                    tvdb_show_data, tvdb_ep_map, tvdb_ids = None, None, None


        # --- Combine Results ---
        final_meta.source_api = "tmdb" if tmdb_show_data else ("tvdb" if tvdb_show_data else None)
        primary_show_data = tmdb_show_data if tmdb_show_data else tvdb_show_data
        primary_ep_map = tmdb_ep_map if tmdb_ep_map else tvdb_ep_map
        show_title_api = None
        if primary_show_data:
            show_title_api = getattr(primary_show_data, 'name', None)
            if show_title_api is None and isinstance(primary_show_data, dict): show_title_api = primary_show_data.get('name')
        final_meta.show_title = show_title_api or show_title_guess
        show_air_date = None
        if primary_show_data:
             show_air_date = getattr(primary_show_data, 'first_air_date', None)
             if show_air_date is None and isinstance(primary_show_data, dict): show_air_date = primary_show_data.get('first_air_date') or primary_show_data.get('firstAired')
        final_meta.show_year = self._get_year_from_date(show_air_date)
        final_meta.ids = {**(tvdb_ids or {}), **(tmdb_ids or {})} # Combine IDs, TMDB takes precedence if keys overlap
        final_meta.season = season_num
        final_meta.episode_list = list(episode_num_tuple)
        if primary_ep_map:
            episode_iterator = episode_num_tuple
            for ep_num in episode_iterator:
                ep_details = primary_ep_map.get(ep_num)
                if ep_details:
                    ep_title = getattr(ep_details, 'name', None)
                    if not ep_title and isinstance(ep_details, dict): ep_title = ep_details.get('name')
                    air_date = getattr(ep_details, 'air_date', None)
                    if not air_date and isinstance(ep_details, dict): air_date = ep_details.get('aired')
                    if ep_title: final_meta.episode_titles[ep_num] = ep_title
                    if air_date: final_meta.air_dates[ep_num] = air_date
                else: log.debug(f"Episode S{season_num}E{ep_num} not found in the chosen API results map.")

        if not final_meta.source_api and not final_meta.episode_titles:
             log.warning(f"Metadata fetch failed completely for series: '{show_title_guess}' S{season_num}E{final_meta.episode_list}")
        return final_meta

    # --- TMDB Series Fetching (Retry decorator applied in __init__) ---
    def _do_fetch_tmdb_series(self, title, season, episodes, year_guess=None):
        """Core logic for TMDB series/episode fetching. Uses first result strategy."""
        # Added client check
        if not self.tmdb:
            log.warning("TMDB client not available in _do_fetch_tmdb_series.")
            return None, None, None

        # Import moved inside to avoid potential issues if TMDB optional
        from tmdbv3api import TV, Season

        search = TV(); results_obj = search.search(title) # API CALL 1 (Search)
        log.debug(f"TMDB raw series search results for '{title}': Type={type(results_obj)}, Value={results_obj}")
        show_match = None

        # --- Start Fix: Process AsObj potentially with iteration ---
        if results_obj: # Check if results_obj is truthy
            potential_match = None
            try:
                first_item = next(iter(results_obj), None) # Reliably get first item
                if first_item:
                    log.debug(f"TMDB Search found results. Checking first result with year filter.")
                    potential_match = first_item
                    log.debug(f"Potential first match object: {potential_match}, type: {type(potential_match)}")
                else:
                    log.debug("TMDB Search result object was empty.")
            except TypeError:
                log.warning(f"Could not iterate over TMDB search results (type {type(results_obj)}). Cannot select first item.")
            except Exception as e_iter:
                 log.error(f"Error iterating TMDB search results: {e_iter}", exc_info=True)

            if potential_match: # Only proceed if we successfully got the first item
                 match_ok = True
                 if year_guess:
                     # ... (year filter logic unchanged) ...
                     log.debug(f"Applying year filter ({year_guess}) to first TMDB result.")
                     result_year = None; first_air_date = getattr(potential_match, 'first_air_date', None)
                     log.debug(f"  -> Extracted first_air_date: {first_air_date}")
                     if first_air_date and isinstance(first_air_date, str):
                          try: result_year = int(first_air_date.split('-')[0]); log.debug(f"  -> Parsed result_year: {result_year}")
                          except (ValueError, IndexError, TypeError) as e_parse: log.warning(f"  -> Failed to parse year from first_air_date: {e_parse}")
                     if result_year is None or abs(result_year - year_guess) > 1: log.debug(f"  -> Year filter check FAILED: result_year={result_year}, year_guess={year_guess}"); match_ok = False
                     else: log.debug(f"  -> Year filter check PASSED: result_year={result_year}, year_guess={year_guess}")
                 if match_ok: log.debug("Match OK is True. Assigning potential_match to show_match."); show_match = potential_match; log.debug(f"Using first TMDB series result as match: {show_match}")
                 else: log.debug("Match OK is False. show_match remains None.")
        # --- End Fix ---

        if not show_match: log.debug(f"No suitable TMDB show match found for '{title}' (using first result strategy)."); return None, None, None
        show_id = getattr(show_match, 'id', None)
        if not show_id: log.error(f"TMDB show match lacks 'id': {show_match}"); return None, None, None
        log.debug(f"TMDB matched show '{getattr(show_match, 'name', 'N/A')}' ID: {show_id} (using first result)")
        show_details = None; combined_show_data = {}; ep_data = {}; ids = {}

        try:
            # --- API CALL 2 (Details) ---
            # Apply rate limit specifically before this call *if needed granularly*
            # self.rate_limiter.wait() # Typically handled before the whole _do_fetch call
            show_details = search.details(show_id);
            log.debug(f"Fetched TMDB show details ID {show_id}: {type(show_details)}")
        except Exception as e_details: log.error(f"Failed to fetch TMDB show details ID {show_id}: {e_details}", exc_info=True) # Keep original error log

        # ... (Combine primary_obj, data extraction logic unchanged) ...
        primary_obj = show_details if show_details else show_match
        if primary_obj:
            try:
                if hasattr(primary_obj, '_data') and isinstance(primary_obj._data, dict): combined_show_data = primary_obj._data.copy(); log.debug("Created combined_show_data from _data attribute.")
                else: combined_show_data = {'id': show_id, 'name': getattr(primary_obj, 'name', None), 'first_air_date': getattr(primary_obj, 'first_air_date', None), 'overview': getattr(primary_obj, 'overview', None), 'vote_average': getattr(primary_obj, 'vote_average', None), 'vote_count': getattr(primary_obj, 'vote_count', None), 'poster_path': getattr(primary_obj, 'poster_path', None), 'backdrop_path': getattr(primary_obj, 'backdrop_path', None), 'genre_ids': getattr(primary_obj, 'genre_ids', None), 'number_of_seasons': getattr(primary_obj, 'number_of_seasons', None), 'number_of_episodes': getattr(primary_obj, 'number_of_episodes', None), 'status': getattr(primary_obj, 'status', None)}; log.debug(f"Created combined_show_data from attributes of {type(primary_obj)}.")
            except Exception as e_comb: log.error(f"Error creating combined_show_data from primary_obj: {e_comb}")
        if 'id' not in combined_show_data and show_id: combined_show_data['id'] = show_id
        if 'name' not in combined_show_data: combined_show_data['name'] = getattr(show_match, 'name', None)
        if 'first_air_date' not in combined_show_data: combined_show_data['first_air_date'] = getattr(show_match, 'first_air_date', None)

        ext_ids_data = {}; fetched_ext_ids = False
        # --- API CALL 3 (External IDs) ---
        if show_details and hasattr(show_details, 'external_ids') and callable(show_details.external_ids):
             try:
                 # self.rate_limiter.wait() # Rate limit usually before _do_fetch
                 ext_ids_data = show_details.external_ids(); fetched_ext_ids = True; log.debug("Fetched external IDs from details object.")
             except Exception as e_call_ext: log.debug(f"Ignoring error calling external_ids method on details: {e_call_ext}")
        if not fetched_ext_ids:
            try:
                 # self.rate_limiter.wait() # Rate limit usually before _do_fetch
                 ext_ids_data = search.external_ids(show_id); log.debug("Fetched external IDs using search.external_ids(show_id).")
            except Exception as e_ext: log.warning(f"Failed to fetch TMDB external IDs using show ID {show_id}: {e_ext}")
        combined_show_data['external_ids'] = ext_ids_data
        log.debug(f"TMDB combined_show_data prepared: Keys={list(combined_show_data.keys())}")

        # --- Fetch Season/Episode Details ---
        try:
            log.debug(f"TMDB: Attempting to fetch season {season} details for show ID {show_id}")
            season_fetcher = Season();
            # --- API CALL 4 (Season Details) ---
            # self.rate_limiter.wait() # Rate limit usually before _do_fetch
            season_details = season_fetcher.details(tv_id=show_id, season_num=season)
            log.debug(f"TMDB: Fetched season_details object type: {type(season_details)}")
            # ... (Rest of episode processing logic unchanged) ...
            try:
                 if hasattr(season_details, '_data') and isinstance(season_details._data, dict): log.debug(f"TMDB: season_details raw data (approx): {season_details._data}")
                 elif hasattr(season_details, '__dict__'): log.debug(f"TMDB: season_details raw data (dict): {season_details.__dict__}")
                 elif isinstance(season_details, dict): log.debug(f"TMDB: season_details raw data (dict): {season_details}")
                 else: log.debug(f"TMDB: season_details raw data (repr): {repr(season_details)}")
            except Exception as e_log_raw: log.warning(f"Could not log raw season_details content: {e_log_raw}")
            if hasattr(season_details, 'episodes'):
                log.debug(f"TMDB: Found 'episodes' attribute. Type: {type(season_details.episodes)}. Count: {len(season_details.episodes) if hasattr(season_details.episodes, '__len__') else 'N/A'}")
                episodes_in_season = {}
                for api_ep in season_details.episodes:
                    ep_num_api = getattr(api_ep, 'episode_number', None); ep_name_api = getattr(api_ep, 'name', 'N/A')
                    log.debug(f"  -> TMDB API Ep Raw: Number={ep_num_api} (Type: {type(ep_num_api)}), Name='{ep_name_api}', Object={api_ep}")
                    if ep_num_api is not None:
                        try: episodes_in_season[int(ep_num_api)] = api_ep
                        except (ValueError, TypeError): log.warning(f"  -> TMDB API Ep Skipped: Could not convert episode number '{ep_num_api}' to int.")
                    else: log.warning(f"  -> TMDB API Ep Skipped: Missing 'episode_number' attribute.")
                log.debug(f"TMDB: Built episodes_in_season map with keys: {list(episodes_in_season.keys())}")
                episode_iterator = episodes if episodes else []
                log.debug(f"TMDB: Checking for requested episode numbers: {episode_iterator}")
                for ep_num_needed in episode_iterator:
                    log.debug(f"  -> TMDB: Looking for episode number: {ep_num_needed} (Type: {type(ep_num_needed)}) in map keys.")
                    episode_obj = episodes_in_season.get(ep_num_needed)
                    if episode_obj: ep_data[ep_num_needed] = episode_obj; log.debug(f"  -> TMDB: Found match for E{ep_num_needed}. Storing object: {episode_obj}")
                    else: log.debug(f"  -> TMDB: E{ep_num_needed} not found in episodes_in_season map.")
            else: log.warning(f"TMDB season details ID {show_id} S{season} lacks 'episodes'.")
        except Exception as e_season:
             # Log actual exception type and message
             log.warning(f"TMDB error getting/processing season {season} ID {show_id}: {type(e_season).__name__}: {e_season}", exc_info=True)
             # Do not raise here, allow fallback if possible

        ids = get_external_ids(tmdb_obj=combined_show_data)
        # --- Fix 2: Correct the variable name ---
        final_show_data_obj = show_details if show_details else show_match
        # --- End Fix 2 ---
        log.debug(f"_do_fetch_tmdb_series returning: data type={type(final_show_data_obj)}, ep_map keys={list(ep_data.keys())}, ids={ids}")
        return final_show_data_obj, ep_data, ids

    # --- TVDB Series Fetching (Retry decorator applied in __init__) ---
    # Remove decorator from the wrapper
    def _fetch_tvdb_series(self, title, season, episodes, tvdb_id=None, year_guess=None):
         """Wrapper for TVDB v4 series fetching (handles client check, rate limit, error wrapping)."""
         if not self.tvdb:
             log.warning("TVDB client not available, skipping TVDB fetch.")
             return None, None, None
         try:
             self.rate_limiter.wait() # Apply rate limit before potentially retried call
             result = self._do_fetch_tvdb_series(title, season, episodes, tvdb_id, year_guess)
             log.debug(f"_fetch_tvdb_series (wrapper): _do_fetch_tvdb_series returned: Type={type(result)}, Value={result}")
             if isinstance(result, tuple) and len(result) == 3:
                 return result
             else:
                 log.error(f"TVDB series fetch function returned unexpected result type: {type(result)}")
                 return None, None, None
         except RetryError as e:
              log.error(f"TVDB series fetch ultimately failed after retries for '{title}': {e}")
              return None, None, None
         except Exception as e:
              # Catch other unexpected errors from _do_fetch_tvdb_series
              log.error(f"Unexpected error during TVDB series fetch for '{title}': {type(e).__name__}: {e}", exc_info=True)
              return None, None, None

    # Decorator applied in __init__ if tenacity available
    def _do_fetch_tvdb_series(self, title: str, season_num: int, episodes: tuple, tvdb_id: int = None, year_guess: int = None):
        """
        Core logic for TVDB v4 series fetching.
        (Retry decorator applied in __init__ if tenacity available)
        """
        # Added client check (belt-and-suspenders with wrapper check)
        if not self.tvdb:
             log.warning("TVDB client not available in _do_fetch_tvdb_series.")
             return None, None, None

        # ... (Internal logic for search, filter, match, fetch unchanged) ...
        show_data = None; best_match_id = tvdb_id; search_results = None
        if not best_match_id:
            try:
                log.debug(f"TVDB searching for: '{title}' (Year guess: {year_guess})")
                search_results = self.tvdb.search(title) # API CALL 1 (Search)
            except TvdbNotFoundError: # Catch specific Not Found
                log.debug(f"TVDB search returned NotFoundError for title '{title}'."); search_results = []
            except Exception as e_search:
                log.warning(f"TVDB search failed unexpectedly for '{title}': {e_search}", exc_info=True)
                search_results = None # Indicate failure
                # Re-raise if it's a retryable error, otherwise it gets caught by the wrapper
                if should_retry_api_error(e_search): raise e_search

            if search_results is not None:
                # ... (Filtering logic unchanged) ...
                if search_results and year_guess:
                    filtered_results = []
                    for r in search_results:
                        result_year_str = r.get('year')
                        if result_year_str:
                            try:
                                result_year = int(result_year_str)
                                if abs(result_year - year_guess) <= 1: filtered_results.append(r)
                            except (ValueError, TypeError): pass
                    if filtered_results: log.debug(f"Filtered TVDB search results by year {year_guess} (+/-1). Kept {len(filtered_results)}."); search_results = filtered_results
                    else: log.debug(f"TVDB search results found, but none matched year {year_guess} (+/-1). Using original.")

                if search_results:
                     try:
                         # ... (Fuzzy matching logic unchanged) ...
                         search_results_tuple = tuple(search_results) if isinstance(search_results, list) else tuple()
                         match = find_best_match(title, search_results_tuple, result_key='name', id_key='tvdb_id')
                         if match: best_match_id = match.get('tvdb_id'); log.debug(f"TVDB name search found match: {match.get('name', 'N/A')} (ID: {best_match_id})")
                         else: log.warning(f"TVDB search yielded results, but no good fuzzy match for '{title}' (after filter).")
                     except Exception as e_match_tvdb: log.error(f"Error during find_best_match for TVDB results of '{title}': {e_match_tvdb}", exc_info=True)
                else: log.debug(f"TVDB search returned no results for '{title}' (after filter).")

        if best_match_id:
            try:
                log.debug(f"TVDB fetching extended series data for ID: {best_match_id}")
                show_data = self.tvdb.get_series_extended(best_match_id) # API CALL 2 (Get Extended)
                if not show_data:
                    log.warning(f"TVDB get_series_extended for ID {best_match_id} returned empty.")
                    return None, None, None # Explicitly return None if fetch was "successful" but empty
                log.debug(f"TVDB successfully fetched extended data for: {show_data.get('name', 'N/A')}")
            except TvdbNotFoundError: # Catch specific Not Found
                log.warning(f"TVDB get_series_extended failed for ID {best_match_id}: Not Found.")
                return None, None, None # Not found is not an error to retry
            except Exception as e_fetch:
                log.warning(f"TVDB get_series_extended failed unexpectedly for ID {best_match_id}: {e_fetch}", exc_info=True)
                 # Re-raise if it's a retryable error, otherwise it gets caught by the wrapper
                if should_retry_api_error(e_fetch): raise e_fetch
                return None, None, None # Return None for non-retryable errors
        else:
            log.warning(f"TVDB: Could not find series '{title}' by ID or name search (after filter).");
            return None, None, None

        # ... (Episode extraction logic unchanged) ...
        ep_data = {}
        if show_data:
            try:
                target_season_data = None; all_season_data = show_data.get('seasons', [])
                if not isinstance(all_season_data, list): log.warning(f"TVDB 'seasons' not list: {type(all_season_data)}"); all_season_data = []
                for season_info in all_season_data:
                     if not isinstance(season_info, dict): continue
                     season_num_from_api = season_info.get('number'); is_official = season_info.get('type', {}).get('type') == 'official'
                     if season_num_from_api is not None:
                        try:
                            if int(season_num_from_api) == int(season_num) and is_official: target_season_data = season_info; break
                        except (ValueError, TypeError): continue
                if target_season_data:
                     log.debug(f"TVDB found season {season_num} data."); all_episode_data = target_season_data.get('episodes', [])
                     if not isinstance(all_episode_data, list): log.warning(f"TVDB S{season_num} 'episodes' not list: {type(all_episode_data)}"); all_episode_data = []
                     episodes_in_season = {};
                     for ep in all_episode_data:
                         if isinstance(ep, dict) and ep.get('number') is not None:
                             try: episodes_in_season[int(ep['number'])] = ep
                             except (ValueError, TypeError): pass
                     episode_iterator = episodes if episodes else []
                     for ep_num in episode_iterator:
                         episode_details = episodes_in_season.get(ep_num)
                         if episode_details: ep_data[ep_num] = episode_details; log.debug(f"TVDB extracted S{season_num}E{ep_num}: {episode_details.get('name')}")
                         else: log.debug(f"TVDB episode S{season_num}E{ep_num} not found.")
                else: log.warning(f"TVDB season {season_num} not found/official for '{show_data.get('name')}'.")
            except Exception as e_ep_extract: log.warning(f"TVDB error processing episode data for '{show_data.get('name')}': {e_ep_extract}", exc_info=True)


        ids = get_external_ids(tvdb_obj=show_data) if show_data else {}
        log.debug(f"_do_fetch_tvdb_series returning: data type={type(show_data)}, ep_map keys={list(ep_data.keys())}, ids={ids}")
        return show_data, ep_data, ids


    # --- TMDB Movie Fetching ---
    @lru_cache(maxsize=128) # Keep cache on public method
    def fetch_movie_metadata(self, movie_title_guess, year_guess=None):
        log.debug(f"Fetching movie metadata for: '{movie_title_guess}' (Year guess: {year_guess})")
        final_meta = MediaMetadata(is_movie=True)
        tmdb_movie_data, tmdb_ids = None, None

        # Added client check
        if not self.tmdb:
            log.warning("TMDB client not available, skipping TMDB movie fetch.")
        else:
            try:
                self.rate_limiter.wait() # Rate limit before potentially retried call
                tmdb_movie_data, tmdb_ids = self._do_fetch_tmdb_movie(movie_title_guess, year_guess)
                log.debug(f"fetch_movie_metadata: _do_fetch_tmdb_movie returned: data type={type(tmdb_movie_data)}, ids type={type(tmdb_ids)}")
                if tmdb_movie_data: log.debug(f"fetch_movie_metadata: tmdb_movie_data object seems truthy.")
                else: log.debug(f"fetch_movie_metadata: tmdb_movie_data object seems falsy.")
            except RetryError as e:
                 log.error(f"TMDB movie fetch ultimately failed after retries for '{movie_title_guess}': {e}")
            except Exception as e:
                 log.error(f"Unexpected error during TMDB movie fetch for '{movie_title_guess}': {type(e).__name__}: {e}", exc_info=True)
            # Ensure reset on error
            if tmdb_movie_data is None:
                 tmdb_movie_data, tmdb_ids = None, None

        # --- Combine results (logic mostly unchanged) ---
        if tmdb_movie_data:
            try:
                final_meta.source_api = "tmdb"
                # ... (Attribute extraction unchanged) ...
                title_val = getattr(tmdb_movie_data, 'title', None)
                if title_val is None and isinstance(tmdb_movie_data, dict): title_val = tmdb_movie_data.get('title')
                final_meta.movie_title = title_val or movie_title_guess
                release_date_val = getattr(tmdb_movie_data, 'release_date', None)
                if release_date_val is None and isinstance(tmdb_movie_data, dict): release_date_val = tmdb_movie_data.get('release_date')
                final_meta.release_date = release_date_val
                log.debug(f"fetch_movie_metadata: Extracted title='{title_val}', release_date='{release_date_val}' from tmdb_movie_data")
                final_meta.movie_year = self._get_year_from_date(final_meta.release_date) or year_guess
                final_meta.ids = tmdb_ids or {}
                log.debug(f"fetch_movie_metadata: Successfully populated final_meta from TMDB.")
            except Exception as e_populate:
                 log.error(f"Error populating final_meta from tmdb_movie_data ({type(tmdb_movie_data)}): {e_populate}", exc_info=True)
                 # Reset if population fails
                 final_meta.source_api = None
                 final_meta.ids = {}
                 tmdb_movie_data = None # Ensure outer check triggers fallback

        log.debug(f"fetch_movie_metadata: Before final check, final_meta.source_api = '{final_meta.source_api}'")

        # Fallback logic if TMDB failed or population failed
        if not final_meta.source_api: # Check if population succeeded
            log.warning(f"Metadata ultimately failed for movie: '{movie_title_guess}' ({year_guess})")
            final_meta.movie_title = movie_title_guess
            final_meta.movie_year = year_guess
            final_meta.ids = {} # Ensure IDs are empty on failure
            final_meta.release_date = None

        return final_meta

    # Remove decorator from wrapper
    def _fetch_tmdb_movie(self, title, year):
         """Wrapper for TMDB movie fetching (handles client check, rate limit, error wrapping)."""
         if not self.tmdb:
             log.warning("TMDB client not available, skipping TMDB movie fetch.")
             return None, None
         try:
             self.rate_limiter.wait()
             log.debug(f"Calling _do_fetch_tmdb_movie for '{title}'")
             result_data, result_ids = self._do_fetch_tmdb_movie(title, year)
             log.debug(f"_fetch_tmdb_movie (wrapper): _do_fetch_tmdb_movie returned: Type={type(result_data)}, ids type={type(result_ids)}")
             # Validate return types
             if isinstance(result_ids, dict):
                 # Return data (even if None) and the valid IDs dict
                 return result_data, result_ids
             elif result_data is None and result_ids is None:
                  return None, None
             else:
                  log.error(f"TMDB movie fetch function returned unexpected types: data={type(result_data)}, ids={type(result_ids)}")
                  return None, None
         except RetryError as e:
              log.error(f"TMDB movie fetch ultimately failed after retries for '{title}': {e}")
              return None, None
         except Exception as e:
              log.error(f"Unexpected error during TMDB movie fetch wrapper: {type(e).__name__}: {e}", exc_info=True)
              return None, None

    # Decorator applied in __init__ if tenacity available
    def _do_fetch_tmdb_movie(self, title, year):
        """
        Core logic for TMDB movie fetching. Uses first result strategy.
        (Retry decorator applied in __init__ if tenacity available)
        """
        # Added client check
        if not self.tmdb:
            log.warning("TMDB client not available in _do_fetch_tmdb_movie.")
            return None, None

        from tmdbv3api import Movie
        search = Movie(); results_obj = search.search(title) # API CALL 1 (Search)
        log.debug(f"TMDB raw movie search results for '{title}': Type={type(results_obj)}, Value={results_obj}")
        movie_match = None
        processed_results = results_obj # Start with the original object

        # --- Start Fix 3: Revised Filtering ---
        if year and processed_results: # Check if results exist before filtering
            filtered_list = []
            try:
                # Iterate directly over AsObj (assuming it's iterable)
                for r in processed_results:
                    release_year = None
                    release_date_val = getattr(r, 'release_date', None)
                    if release_date_val and isinstance(release_date_val, str):
                        try:
                            release_year = int(release_date_val.split('-')[0])
                        except (ValueError, IndexError, TypeError):
                            pass
                    # --- FIX: Compare release_year to year ---
                    if release_year is not None and abs(release_year - year) <= 0: # Allow only exact match or maybe 1 year difference if needed
                    # --- End FIX ---
                        filtered_list.append(r) # Add the matching AsObj item

                if filtered_list:
                    log.debug(f"Filtered TMDB movie results by year {year}. Kept {len(filtered_list)}.")
                    processed_results = filtered_list # Now processed_results is a list of AsObj items
                else:
                    log.debug(f"TMDB movie search found but none matched year {year}. Discarding results.")
                    processed_results = None # No matches, clear results
            except TypeError:
                log.warning(f"Could not iterate over TMDB movie results object (type {type(processed_results)}) for filtering. Proceeding without filter.")
                # Keep processed_results as the original AsObj if iteration fails
            except Exception as e_filter:
                 log.error(f"Error during TMDB movie year filtering: {e_filter}", exc_info=True)
                 processed_results = None # Error during filtering, treat as no results
        # --- End Fix 3: Revised Filtering ---


        # --- Start Fix 3: Revised Selection ---
        if processed_results: # Check if we still have results (either original AsObj or filtered list)
            try:
                # Attempt to get the first item (works for AsObj and list)
                first_item = next(iter(processed_results), None)
                if first_item:
                     log.debug(f"Using first TMDB movie result (after potential year filter) as match.")
                     movie_match = first_item
                else:
                     log.debug("Result object was empty after iteration.")
            except TypeError:
                log.warning(f"Could not iterate over TMDB movie results object (type {type(processed_results)}) to get first item.")
                # movie_match remains None
            except Exception as e_first:
                 log.error(f"Error accessing first item from results_obj (type {type(processed_results)}): {e_first}", exc_info=True)
                 # movie_match remains None
        # --- End Fix 3: Revised Selection ---


        if not movie_match:
            log.debug(f"No suitable TMDB movie match found for '{title}' (using first result strategy).")
            return None, None

        movie_id = getattr(movie_match, 'id', None)
        if not movie_id: log.error(f"TMDB movie match lacks 'id': {movie_match}"); return None, None
        log.debug(f"TMDB matched movie '{getattr(movie_match, 'title', 'N/A')}' ID: {movie_id} (using first result)")

        movie_details = None
        try:
            # self.rate_limiter.wait() # Handled before _do_fetch call
            movie_details = search.details(movie_id); # API CALL 2 (Details)
            log.debug(f"Fetched TMDB movie details ID {movie_id}: {type(movie_details)}")
        except Exception as e_details: log.error(f"Failed to fetch TMDB movie details ID {movie_id}: {e_details}", exc_info=True) # Keep original error

        # ... (Combine data logic unchanged) ...
        final_data_obj = movie_details if movie_details else movie_match
        combined_data_for_ids = {}
        if final_data_obj:
             try:
                 if hasattr(final_data_obj, '_data') and isinstance(final_data_obj._data, dict): combined_data_for_ids = final_data_obj._data.copy(); log.debug("Created combined_data_for_ids from _data attribute.")
                 else: combined_data_for_ids = {'id': movie_id, 'title': getattr(final_data_obj, 'title', None), 'release_date': getattr(final_data_obj, 'release_date', None)}; log.debug(f"Created combined_data_for_ids from attributes of {type(final_data_obj)}.")

                 ext_ids_data = {}
                 try:
                     # --- API CALL 3 (External IDs) ---
                     # self.rate_limiter.wait() # Handled before _do_fetch call
                     if movie_details and hasattr(movie_details, 'external_ids') and callable(movie_details.external_ids):
                         ext_ids_data = movie_details.external_ids()
                     else:
                         ext_ids_data = search.external_ids(movie_id) # Fallback
                 except Exception as e_ext: log.warning(f"Failed to fetch TMDB external IDs movie ID {movie_id}: {e_ext}")
                 combined_data_for_ids['external_ids'] = ext_ids_data
             except Exception as e_comb: log.error(f"Error creating combined_data_for_ids from final_data_obj: {e_comb}")


        ids = get_external_ids(tmdb_obj=combined_data_for_ids) # Use the dictionary
        log.debug(f"_do_fetch_tmdb_movie returning: data type={type(final_data_obj)}, ids={ids}")
        # Return the actual data object (AsObj) and the extracted IDs
        return final_data_obj, ids

# --- END OF FILE metadata_fetcher.py ---