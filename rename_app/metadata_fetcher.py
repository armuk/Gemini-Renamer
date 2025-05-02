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
try: from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type; TENACITY_AVAILABLE = True
except ImportError: TENACITY_AVAILABLE = False
try: import dateutil.parser; DATEUTIL_AVAILABLE = True
except ImportError: DATEUTIL_AVAILABLE = False

# Import new exceptions for specific handling
try:
    from tvdb_v4_official.errors import NotFoundError
except ImportError:
    NotFoundError = Exception # Fallback if library not installed yet

# Keep necessary imports like requests if retry decorator uses it
try: import requests
except ImportError: pass
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

# --- Retry Decorator Setup ---
def setup_sync_retry_decorator(cfg_helper):
    if not TENACITY_AVAILABLE:
        log.debug("Tenacity not installed, retries disabled.")
        def dummy_decorator(func): return func
        return dummy_decorator
    attempts_cfg = cfg_helper('api_retry_attempts', 3)
    wait_sec_cfg = cfg_helper('api_retry_wait_seconds', 2)
    attempts = attempts_cfg if attempts_cfg is not None else 3
    wait_sec = wait_sec_cfg if wait_sec_cfg is not None else 2
    retryable_exceptions = (IOError,)
    try:
        import requests
        retryable_exceptions += (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.RequestException)
    except ImportError: pass
    log.debug(f"Setting up sync retry decorator: Attempts={attempts}, Wait={wait_sec}s")
    return retry(
        stop=stop_after_attempt(max(1, attempts)),
        wait=wait_fixed(max(0, wait_sec)),
        retry=retry_if_exception_type(retryable_exceptions),
        reraise=True)

# --- Fuzzy Matching ---
# REMOVED @lru_cache decorator
def find_best_match(title_to_find, api_results_tuple, result_key='title', id_key='id', score_cutoff=70):
    if not api_results_tuple: return None
    api_results = api_results_tuple

    if not THEFUZZ_AVAILABLE:
        log.debug("thefuzz library not available, returning first result if available.")
        return api_results[0] if api_results else None

    if not isinstance(api_results, tuple) or not api_results:
        log.debug(f"Fuzzy match input 'api_results_tuple' is empty or not a tuple: {type(api_results_tuple)}")
        return None

    # Now assumes the input tuple contains ONLY dictionaries
    is_dict_list = True

    choices = {}
    log.debug(f"Attempting to build choices for fuzzy match '{title_to_find}'. Input assumed dicts.")
    try:
        for r in api_results:
            if not r or not isinstance(r, dict): # Ensure item is a dictionary
                 log.debug(f"  -> Skipped non-dict item: {r}")
                 continue

            current_id = r.get(id_key)
            current_result = r.get(result_key)

            if current_id is not None and current_result is not None:
                 choices[current_id] = str(current_result) # Ensure string value
                 log.debug(f"  -> Added choice: ID={current_id}, Value='{str(current_result)}'")
            else:
                 log.debug(f"  -> Skipped item (missing ID '{id_key}' or Result '{result_key}'): {r}")

    except Exception as e_choices:
         log.error(f"Error creating choices dict for fuzzy matching '{title_to_find}': {e_choices}. Item causing error (maybe): {r}", exc_info=True); return None

    if not choices: log.debug(f"No valid choices found for fuzzy matching '{title_to_find}'."); return None
    log.debug(f"Fuzzy matching choices for '{title_to_find}': {choices}")

    best = None
    try:
        if not isinstance(title_to_find, str): title_to_find = str(title_to_find)
        processed_choices = {k: str(v) for k, v in choices.items()}
        best = fuzz_process.extractOne(title_to_find, processed_choices, score_cutoff=score_cutoff)
    except Exception as e_fuzz: log.error(f"Error during fuzz_process.extractOne for '{title_to_find}': {e_fuzz}", exc_info=True); return None

    if best:
        matched_value, score, best_id = best; log.debug(f"Fuzzy match '{title_to_find}': '{matched_value}' (ID:{best_id}) score {score}")
        # Return the matching dictionary from the input tuple
        for r_dict in api_results_tuple:
            if isinstance(r_dict, dict) and str(r_dict.get(id_key)) == str(best_id):
                 log.debug(f"Returning matched dict: {r_dict}"); return r_dict
    else: log.warning(f"Fuzzy match failed for '{title_to_find}' (cutoff {score_cutoff})"); return None
    log.error(f"Reached end of find_best_match unexpectedly for '{title_to_find}'"); return None

# --- External ID Helper ---
def get_external_ids(tmdb_obj=None, tvdb_obj=None):
    """Extract external IDs, updated for tvdb_v4_official."""
    ids = {'imdb_id': None, 'tmdb_id': None, 'tvdb_id': None}
    # TMDB part
    try:
        if tmdb_obj:
            # Handle if tmdb_obj is the original AsObj or the extracted dict
            tmdb_id_val = getattr(tmdb_obj, 'id', tmdb_obj.get('id') if isinstance(tmdb_obj, dict) else None)
            if tmdb_id_val is not None: ids['tmdb_id'] = tmdb_id_val

            ext_ids_data = {}
            ext_ids_attr = getattr(tmdb_obj, 'external_ids', None)
            if isinstance(tmdb_obj, dict): ext_ids_attr = tmdb_obj.get('external_ids', ext_ids_attr)

            if isinstance(ext_ids_attr, dict): ext_ids_data = ext_ids_attr
            elif callable(ext_ids_attr):
                 try:
                      if not isinstance(tmdb_obj, dict): ext_ids_data = ext_ids_attr()
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
    # ... (__init__, _get_year_from_date, fetch_series_metadata) ...
    def __init__(self, cfg_helper):
        self.cfg = cfg_helper
        self.tmdb = get_tmdb_client()
        self.tvdb = get_tvdb_client()
        self.rate_limiter = SyncRateLimiter(cfg_helper('api_rate_limit_delay', 0.5))
        self.retry_decorator = setup_sync_retry_decorator(cfg_helper)

    def _get_year_from_date(self, date_str):
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
        try: tmdb_show_data, tmdb_ep_map, tmdb_ids = self._fetch_tmdb_series(show_title_guess, season_num, episode_num_tuple, year_guess=year_guess)
        except Exception as e: log.error(f"Unexpected error during _fetch_tmdb_series wrapper for '{show_title_guess}': {e}", exc_info=True)
        needs_tvdb_fallback = (not tmdb_show_data) or (tmdb_show_data and not tmdb_ep_map)
        if needs_tvdb_fallback:
            if not tmdb_show_data: log.debug("TMDB failed completely, trying TVDB...")
            else: log.debug("TMDB found show but failed on episode details, trying TVDB as fallback...")
            tvdb_id_from_tmdb = tmdb_ids.get('tvdb_id') if tmdb_ids else None
            try: tvdb_show_data, tvdb_ep_map, tvdb_ids = self._fetch_tvdb_series(show_title_guess, season_num, episode_num_tuple, tvdb_id=tvdb_id_from_tmdb, year_guess=year_guess)
            except Exception as e: log.error(f"Unexpected error during _fetch_tvdb_series wrapper for '{show_title_guess}': {e}", exc_info=True)
        final_meta.source_api = "tmdb" if tmdb_show_data else ("tvdb" if tvdb_show_data else None)
        primary_show_data = tmdb_show_data if tmdb_show_data else tvdb_show_data
        primary_ep_map = tmdb_ep_map if tmdb_ep_map else tvdb_ep_map
        show_title_api = None
        if primary_show_data:
            # Handle potential AsObj or dict
            show_title_api = getattr(primary_show_data, 'name', None)
            if show_title_api is None and isinstance(primary_show_data, dict): show_title_api = primary_show_data.get('name')
        final_meta.show_title = show_title_api or show_title_guess
        show_air_date = None
        if primary_show_data:
             show_air_date = getattr(primary_show_data, 'first_air_date', None)
             if show_air_date is None and isinstance(primary_show_data, dict): show_air_date = primary_show_data.get('first_air_date') or primary_show_data.get('firstAired')
        final_meta.show_year = self._get_year_from_date(show_air_date)
        final_meta.ids = {**(tvdb_ids or {}), **(tmdb_ids or {})}
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

    # --- TMDB Series Fetching ---
    def _fetch_tmdb_series(self, title, season, episodes, year_guess=None):
        """Wrapper for TMDB series fetching."""
        if not self.tmdb: return None, None, None
        fetch_func = self.retry_decorator(self._do_fetch_tmdb_series)
        try: self.rate_limiter.wait(); return fetch_func(title, season, episodes, year_guess=year_guess)
        except Exception as e: log.error(f"TMDB series fetch ultimately failed for '{title}': {type(e).__name__}: {e}", exc_info=True); return None, None, None

    def _do_fetch_tmdb_series(self, title, season, episodes, year_guess=None):
        """Core logic for TMDB series/episode fetching."""
        from tmdbv3api import TV, Season
        search = TV()
        results_raw = search.search(title) # Keep raw AsObj results
        log.debug(f"TMDB raw series search results for '{title}': Type={type(results_raw)}, Value={results_raw}")
        results_obj = results_raw # Use a different name for clarity

        # Filter the original AsObj list if year_guess is provided
        if year_guess and results_obj and isinstance(results_obj, list):
             filtered_results_obj = []
             for r in results_obj:
                 result_year = None; first_air_date = getattr(r, 'first_air_date', None)
                 if first_air_date and isinstance(first_air_date, str):
                     try: result_year = int(first_air_date.split('-')[0])
                     except (ValueError, IndexError, TypeError): pass
                 if result_year is not None and abs(result_year - year_guess) <= 1:
                     filtered_results_obj.append(r)
             if filtered_results_obj:
                 log.debug(f"Filtered TMDB series results (Obj) by year {year_guess} (+/-1). Kept {len(filtered_results_obj)}.");
                 results_obj = filtered_results_obj # Use the filtered list of AsObj
             else:
                 log.debug(f"TMDB series search found but none matched year {year_guess} (+/-1). Using original AsObj results.")

        # --- FIX: Convert AsObj results (original or filtered) to dicts for matching ---
        results_as_dicts = []
        if results_obj and isinstance(results_obj, list):
            for r in results_obj:
                try:
                    # Attempt to access the internal _data attribute first
                    if hasattr(r, '_data') and isinstance(r._data, dict):
                        results_as_dicts.append(r._data)
                    # Fallback to accessing known attributes if _data isn't there
                    elif hasattr(r, 'id') and hasattr(r, 'name'):
                         item_dict = {'id': r.id, 'name': r.name, 'first_air_date': getattr(r, 'first_air_date', None)}
                         results_as_dicts.append(item_dict)
                         log.debug(f"Converted AsObj to dict via attributes: {item_dict}")
                    elif isinstance(r, dict): # Handle if it somehow was already a dict
                         results_as_dicts.append(r)
                    else:
                        log.warning(f"Cannot extract dict from TMDB series result: {type(r)} - {r}")
                except Exception as e_conv:
                    log.warning(f"Error converting TMDB series result item {r} to dict: {e_conv}", exc_info=True)

        results_tuple = tuple(results_as_dicts) # Tuple of dicts
        # --- End FIX ---

        show_match = None # This will store the original AsObj if found
        show_match_dict = None # This will store the matched dictionary
        try:
            show_match_dict = find_best_match(title, results_tuple, result_key='name', id_key='id')

            if show_match_dict:
                match_id = show_match_dict.get('id')
                # Find the original AsObj from the raw (unfiltered) list
                for r_orig in results_raw:
                     # Use getattr for safety, compare IDs
                     if getattr(r_orig, 'id', None) == match_id:
                          show_match = r_orig # Assign the original AsObj
                          break
                if not show_match:
                     log.warning(f"Found matching dict ID {match_id} but couldn't find original AsObj in raw results.")
                     # Optionally try finding in the filtered list if needed, though less ideal
                     for r_filt in results_obj:
                          if getattr(r_filt, 'id', None) == match_id:
                               show_match = r_filt; break
        except Exception as e_match: log.error(f"Error during find_best_match processing for TMDB series '{title}': {e_match}", exc_info=True); return None, None, None

        if not show_match: log.debug(f"No suitable TMDB show match found for '{title}' (after potential year filter)."); return None, None, None
        show_id = getattr(show_match, 'id', None)
        if not show_id: log.error(f"TMDB show match lacks 'id': {show_match}"); return None, None, None
        log.debug(f"TMDB matched show '{getattr(show_match, 'name', 'N/A')}' ID: {show_id}")

        # ... (rest of the function uses show_match (AsObj) and show_id) ...
        show_details = None
        try: show_details = search.details(show_id); log.debug(f"Fetched TMDB show details ID {show_id}: {type(show_details)}")
        except Exception as e_details: log.error(f"Failed to fetch TMDB show details ID {show_id}: {e_details}", exc_info=True)
        combined_show_data = {}
        # Prioritize original match object, update with details if available
        if show_match:
             if hasattr(show_match, '_data') and isinstance(show_match._data, dict): combined_show_data = show_match._data.copy()
             elif hasattr(show_match, '__dict__'): combined_show_data = show_match.__dict__.copy()
             elif isinstance(show_match, dict): combined_show_data = show_match.copy()
        if show_details:
            details_dict = {}
            if hasattr(show_details, '_data') and isinstance(show_details._data, dict): details_dict = show_details._data.copy()
            elif hasattr(show_details, '__dict__'): details_dict = show_details.__dict__.copy()
            elif isinstance(show_details, dict): details_dict = show_details.copy()
            combined_show_data.update(details_dict) # Update/overwrite with details

        # Ensure essential info is present using fallbacks if necessary
        if 'id' not in combined_show_data and show_id: combined_show_data['id'] = show_id
        if 'name' not in combined_show_data: combined_show_data['name'] = getattr(show_match, 'name', None)
        if 'first_air_date' not in combined_show_data: combined_show_data['first_air_date'] = getattr(show_match, 'first_air_date', None)

        ext_ids_data = {}
        try: ext_ids_data = search.external_ids(show_id)
        except Exception as e_ext: log.warning(f"Failed to fetch TMDB external IDs show ID {show_id}: {e_ext}")
        if show_details and hasattr(show_details, 'external_ids') and callable(show_details.external_ids):
             try: ext_ids_data = show_details.external_ids()
             except Exception as e_call_ext: log.debug(f"Ignoring error calling external_ids method: {e_call_ext}")
        combined_show_data['external_ids'] = ext_ids_data
        log.debug(f"TMDB combined_show_data prepared: Keys={list(combined_show_data.keys())}")
        ep_data = {}
        try:
            log.debug(f"TMDB: Attempting to fetch season {season} details for show ID {show_id}")
            season_fetcher = Season(); self.rate_limiter.wait()
            season_details = season_fetcher.details(tv_id=show_id, season_num=season)
            log.debug(f"TMDB: Fetched season_details object type: {type(season_details)}")
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
        except Exception as e_season: log.warning(f"TMDB error getting/processing season {season} ID {show_id}: {type(e_season).__name__}: {e_season}", exc_info=True)

        ids = get_external_ids(tmdb_obj=combined_show_data) # Pass combined data
        # Return the original matched AsObj (show_match) as primary show data if details failed
        final_show_data = show_details if show_details else show_match
        return final_show_data, ep_data, ids

    # --- TVDB Series Fetching ---
    # ... (No changes needed from previous version) ...
    def _fetch_tvdb_series(self, title, season, episodes, tvdb_id=None, year_guess=None):
        """Wrapper for TVDB v4 series fetching with retries and rate limiting."""
        if not self.tvdb: log.debug("TVDB client not available, skipping TVDB fetch."); return None, None, None
        if not hasattr(self, '_do_fetch_tvdb_series'): log.error("Internal Error: _do_fetch_tvdb_series method not found."); return None, None, None
        fetch_func = self.retry_decorator(self._do_fetch_tvdb_series)
        try: self.rate_limiter.wait(); return fetch_func(title, season, episodes, tvdb_id, year_guess)
        except Exception as e: log.error(f"TVDB series fetch ultimately failed for '{title}': {type(e).__name__}: {e}", exc_info=True); return None, None, None
    def _do_fetch_tvdb_series(self, title: str, season_num: int, episodes: tuple, tvdb_id: int = None, year_guess: int = None):
        """
        Fetches series and episode data using tvdb_v4_official library.
        Prioritizes passed-in tvdb_id, otherwise searches by title.
        Optionally filters search results by year_guess before fuzzy matching.
        Returns (show_data_dict, ep_data_dict, ids_dict) or (None, None, None).
        """
        show_data = None; best_match_id = tvdb_id; search_results = None
        if not best_match_id:
            try:
                log.debug(f"TVDB searching for: '{title}' (Year guess: {year_guess})")
                search_results = self.tvdb.search(title)
            except NotFoundError: log.debug(f"TVDB search returned NotFoundError for title '{title}'."); search_results = []
            except Exception as e_search: log.warning(f"TVDB search failed unexpectedly for '{title}': {e_search}", exc_info=True); search_results = None
            if search_results is not None:
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
                         search_results_tuple = tuple(search_results) if isinstance(search_results, list) else tuple()
                         match = find_best_match(title, search_results_tuple, result_key='name', id_key='tvdb_id')
                         if match: best_match_id = match.get('tvdb_id'); log.debug(f"TVDB name search found match: {match.get('name', 'N/A')} (ID: {best_match_id})")
                         else: log.warning(f"TVDB search yielded results, but no good fuzzy match for '{title}' (after filter).")
                     except Exception as e_match_tvdb: log.error(f"Error during find_best_match for TVDB results of '{title}': {e_match_tvdb}", exc_info=True)
                else: log.debug(f"TVDB search returned no results for '{title}' (after filter).")
        if best_match_id:
            try:
                log.debug(f"TVDB fetching extended series data for ID: {best_match_id}")
                show_data = self.tvdb.get_series_extended(best_match_id)
                if not show_data: log.warning(f"TVDB get_series_extended for ID {best_match_id} returned empty."); return None, None, None
                log.debug(f"TVDB successfully fetched extended data for: {show_data.get('name', 'N/A')}")
            except NotFoundError: log.warning(f"TVDB get_series_extended failed for ID {best_match_id}: Not Found."); return None, None, None
            except Exception as e_fetch: log.warning(f"TVDB get_series_extended failed unexpectedly for ID {best_match_id}: {e_fetch}", exc_info=True); return None, None, None
        else: log.warning(f"TVDB: Could not find series '{title}' by ID or name search (after filter)."); return None, None, None
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
        return show_data, ep_data, ids

    # --- TMDB Movie Fetching ---
    @lru_cache(maxsize=128)
    def fetch_movie_metadata(self, movie_title_guess, year_guess=None):
        # ... (unchanged) ...
        log.debug(f"Fetching movie metadata for: '{movie_title_guess}' (Year guess: {year_guess})")
        final_meta = MediaMetadata(is_movie=True)
        tmdb_movie_data, tmdb_ids = None, None
        try: tmdb_movie_data, tmdb_ids = self._fetch_tmdb_movie(movie_title_guess, year_guess)
        except Exception as e: log.error(f"Unexpected error during _fetch_tmdb_movie wrapper: {type(e).__name__}: {e}", exc_info=True)
        if tmdb_movie_data:
            final_meta.source_api = "tmdb"
            title_val = getattr(tmdb_movie_data, 'title', None)
            if title_val is None and isinstance(tmdb_movie_data, dict): title_val = tmdb_movie_data.get('title')
            final_meta.movie_title = title_val or movie_title_guess
            release_date_val = getattr(tmdb_movie_data, 'release_date', None)
            if release_date_val is None and isinstance(tmdb_movie_data, dict): release_date_val = tmdb_movie_data.get('release_date')
            final_meta.release_date = release_date_val
            final_meta.movie_year = self._get_year_from_date(final_meta.release_date) or year_guess
            final_meta.ids = tmdb_ids or {}
        else:
             log.warning(f"Metadata failed for movie: '{movie_title_guess}' ({year_guess})")
             final_meta.movie_title = movie_title_guess
             final_meta.movie_year = year_guess
        return final_meta

    def _fetch_tmdb_movie(self, title, year):
        # ... (wrapper unchanged) ...
        """Wrapper for TMDB movie fetching."""
        if not self.tmdb: return None, None
        fetch_func = self.retry_decorator(self._do_fetch_tmdb_movie)
        try: self.rate_limiter.wait(); return fetch_func(title, year)
        except Exception as e: log.error(f"TMDB movie fetch ultimately failed for '{title}': {type(e).__name__}: {e}", exc_info=True); return None, None

    def _do_fetch_tmdb_movie(self, title, year):
        # ... (core logic with AsObj -> Dict fix for find_best_match) ...
        """Core logic for TMDB movie fetching."""
        from tmdbv3api import Movie
        search = Movie()
        results_raw = search.search(title) # Keep raw results
        log.debug(f"TMDB raw movie search results for '{title}': Type={type(results_raw)}, Value={results_raw}")
        results_obj = results_raw # Use different name for clarity

        # Filter original AsObj list
        if year and results_obj and isinstance(results_obj, list):
            filtered_results_obj = []
            for r in results_obj:
                release_year = None; release_date_val = getattr(r, 'release_date', None)
                if release_date_val and isinstance(release_date_val, str):
                    try: release_year = int(release_date_val.split('-')[0])
                    except (ValueError, IndexError, TypeError): pass
                if release_year == year: filtered_results_obj.append(r)
            if filtered_results_obj: log.debug(f"Filtered TMDB movie results (Obj) by year {year}. Kept {len(filtered_results_obj)}."); results_obj = filtered_results_obj
            else: log.debug(f"TMDB movie search found but none matched year {year}. Using original AsObj results.")

        movie_match = None # Stores the original AsObj
        movie_match_dict = None # Stores the matched dictionary
        try:
            # Convert AsObj results (original or filtered) to dicts for matching
            results_as_dicts = []
            if results_obj and isinstance(results_obj, list):
                for r in results_obj:
                    try:
                        if hasattr(r, '_data') and isinstance(r._data, dict):
                           results_as_dicts.append(r._data)
                        elif hasattr(r, 'id') and hasattr(r, 'title'):
                           item_dict = {'id': r.id, 'title': r.title, 'release_date': getattr(r, 'release_date', None)}
                           results_as_dicts.append(item_dict)
                           log.debug(f"Converted AsObj to dict via attributes: {item_dict}")
                        elif isinstance(r, dict): results_as_dicts.append(r)
                        else: log.warning(f"Cannot extract dict from TMDB movie result: {type(r)} - {r}")
                    except Exception as e_conv: log.warning(f"Error converting TMDB movie result item {r} to dict: {e_conv}", exc_info=True)
            results_tuple = tuple(results_as_dicts)

            movie_match_dict = find_best_match(title, results_tuple, result_key='title', id_key='id')

            # Retrieve original AsObj
            if movie_match_dict:
                 match_id = movie_match_dict.get('id')
                 for r_orig in results_raw:
                      if getattr(r_orig, 'id', None) == match_id:
                           movie_match = r_orig; break
                 if not movie_match:
                      log.warning(f"Found matching dict ID {match_id} but couldn't find original AsObj in raw results.")
                      for r_filt in results_obj: # Try filtered list
                           if getattr(r_filt, 'id', None) == match_id:
                                movie_match = r_filt; break
        except Exception as e_match: log.error(f"Error during find_best_match processing for TMDB movie '{title}': {e_match}", exc_info=True); return None, None

        if not movie_match: log.debug(f"No suitable TMDB movie match for '{title}' (Year: {year})."); return None, None
        movie_id = getattr(movie_match, 'id', None)
        if not movie_id: log.error(f"TMDB movie match lacks 'id': {movie_match}"); return None, None
        log.debug(f"TMDB matched movie '{getattr(movie_match, 'title', 'N/A')}' ID: {movie_id}")

        movie_details = None
        try: movie_details = search.details(movie_id); log.debug(f"Fetched TMDB movie details ID {movie_id}: {type(movie_details)}")
        except Exception as e_details: log.error(f"Failed to fetch TMDB movie details ID {movie_id}: {e_details}", exc_info=True)

        combined_data = {}
        # Prioritize original match, update with details
        if movie_match:
             if hasattr(movie_match, '_data') and isinstance(movie_match._data, dict): combined_data = movie_match._data.copy()
             elif hasattr(movie_match, '__dict__'): combined_data = movie_match.__dict__.copy()
             elif isinstance(movie_match, dict): combined_data = movie_match.copy()
        if movie_details:
            details_dict = {}
            if hasattr(movie_details, '_data') and isinstance(movie_details._data, dict): details_dict = movie_details._data.copy()
            elif hasattr(movie_details, '__dict__'): details_dict = movie_details.__dict__.copy()
            elif isinstance(movie_details, dict): details_dict = movie_details.copy()
            combined_show_data.update(details_dict)
        # Ensure essential info
        if 'id' not in combined_data and movie_id: combined_data['id'] = movie_id
        if 'title' not in combined_data: combined_data['title'] = getattr(movie_match, 'title', None)
        if 'release_date' not in combined_data: combined_data['release_date'] = getattr(movie_match, 'release_date', None)

        ext_ids_data = {}
        try: ext_ids_data = search.external_ids(movie_id)
        except Exception as e_ext: log.warning(f"Failed to fetch TMDB external IDs movie ID {movie_id}: {e_ext}")
        if movie_details and hasattr(movie_details, 'external_ids') and callable(movie_details.external_ids):
             try: ext_ids_data = movie_details.external_ids()
             except Exception as e_call_ext: log.debug(f"Ignoring error calling external_ids method: {e_call_ext}")
        combined_data['external_ids'] = ext_ids_data
        log.debug(f"TMDB combined_movie_data prepared: Keys={list(combined_data.keys())}")

        ids = get_external_ids(tmdb_obj=combined_data)
        # Return the detailed object if fetched, otherwise the matched object
        final_data_obj = movie_details if movie_details else movie_match
        return final_data_obj, ids