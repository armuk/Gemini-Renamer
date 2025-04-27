# rename_app/metadata_fetcher.py (Synchronous Version)

import logging
import time
from functools import lru_cache
from .api_clients import get_tmdb_client, get_tvdb_client
# from .utils import RateLimiter <--- REMOVE THIS LINE
from .exceptions import MetadataError
from .models import MediaMetadata
# Import fuzzy matching etc.
try: from thefuzz import process as fuzz_process; THEFUZZ_AVAILABLE = True
except ImportError: THEFUZZ_AVAILABLE = False
try: from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type; TENACITY_AVAILABLE = True
except ImportError: TENACITY_AVAILABLE = False
try: import dateutil.parser; DATEUTIL_AVAILABLE = True
except ImportError: DATEUTIL_AVAILABLE = False
# Keep necessary imports like requests if retry decorator uses it
try: import requests
except ImportError: pass


log = logging.getLogger(__name__)

# --- Sync Rate Limiter (Defined locally) ---
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

# --- Retry Decorator Setup (Sync) ---
def setup_sync_retry_decorator(cfg_helper):
    if not TENACITY_AVAILABLE:
        log.debug("Tenacity not installed, retries disabled.")
        def dummy_decorator(func): return func
        return dummy_decorator
    attempts = cfg_helper('api_retry_attempts', 3)
    wait_sec = cfg_helper('api_retry_wait_seconds', 2)
    # Define sync exceptions (requests used by API libs)
    retryable_exceptions = (IOError,) # Base IO Error
    try:
        # Add requests exceptions if available
        import requests
        retryable_exceptions += (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.RequestException)
    except ImportError: pass
    # Add known API library specific exceptions if needed
    # Example:
    # try: from tvdb_api import tvdb_error; retryable_exceptions += (tvdb_error.TvdbError,)
    # except ImportError: pass

    log.debug(f"Setting up sync retry decorator: Attempts={attempts}, Wait={wait_sec}s")
    return retry(
        stop=stop_after_attempt(max(1, attempts)),
        wait=wait_fixed(max(0, wait_sec)),
        retry=retry_if_exception_type(retryable_exceptions),
        reraise=True)

# --- Fuzzy Matching ---
@lru_cache(maxsize=64)
def find_best_match(title_to_find, api_results, result_key='title', id_key='id', score_cutoff=70):
    # (Implementation as before)
    if not api_results: return None
    if not THEFUZZ_AVAILABLE: return api_results[0] if api_results else None
    choices = {getattr(r, id_key, None): getattr(r, result_key, '') for r in api_results if getattr(r, id_key, None)}
    if not choices: return None
    best = fuzz_process.extractOne(title_to_find, choices, score_cutoff=score_cutoff)
    if best:
        best_id, score, _ = best; log.debug(f"Fuzzy match '{title_to_find}': '{choices[best_id]}' (ID:{best_id}) score {score}")
        for r in api_results:
            if getattr(r, id_key, None) == best_id: return r
    else: log.warning(f"Fuzzy match failed for '{title_to_find}' (cutoff {score_cutoff})"); return None

# --- External ID Helper ---
def get_external_ids(tmdb_obj=None, tvdb_obj=None):
    # (Implementation as before)
    ids = {'imdb_id': None, 'tmdb_id': None, 'tvdb_id': None}
    try: # TMDB
        if tmdb_obj:
            ids['tmdb_id'] = getattr(tmdb_obj, 'id', None)
            ext_ids_data = {}
            ext_ids_attr = getattr(tmdb_obj, 'external_ids', None)
            if isinstance(ext_ids_attr, dict): ext_ids_data = ext_ids_attr
            elif callable(ext_ids_attr):
                try: ext_ids_data = ext_ids_attr()
                except Exception: pass
            ids['imdb_id'] = ext_ids_data.get('imdb_id') or ids['imdb_id']
            ids['tvdb_id'] = ext_ids_data.get('tvdb_id') or ids['tvdb_id']
    except AttributeError: pass
    try: # TVDB
        if tvdb_obj:
            tvdb_data = getattr(tvdb_obj, 'data', tvdb_obj)
            ids['tvdb_id'] = getattr(tvdb_data, 'id', None) or ids['tvdb_id']
            ids['imdb_id'] = getattr(tvdb_data, 'imdbId', None) or ids['imdb_id']
    except AttributeError: pass
    return {k: v for k, v in ids.items() if v}

# --- Metadata Fetcher Class (Sync) ---
class MetadataFetcher:
    def __init__(self, cfg_helper):
        self.cfg = cfg_helper
        self.tmdb = get_tmdb_client() # Assumes initialized elsewhere
        self.tvdb = get_tvdb_client() # Assumes initialized elsewhere
        self.rate_limiter = SyncRateLimiter(cfg_helper('api_rate_limit_delay', 0.5))
        self.retry_decorator = setup_sync_retry_decorator(cfg_helper)

    def _get_year_from_date(self, date_str):
        # (Implementation as before)
        if not date_str or not DATEUTIL_AVAILABLE: return None
        try: return dateutil.parser.parse(date_str).year
        except (ValueError, TypeError): return None

    @lru_cache(maxsize=128)
    def fetch_series_metadata(self, show_title_guess, season_num, episode_num_list):
        # (Implementation as before - calls _fetch_tmdb_series etc.)
        log.debug(f"Fetching series metadata for: '{show_title_guess}' S{season_num}E{episode_num_list}")
        final_meta = MediaMetadata(is_series=True)
        tmdb_show_data, tmdb_ep_map, tmdb_ids = self._fetch_tmdb_series(show_title_guess, season_num, episode_num_list)
        tvdb_show_data, tvdb_ep_map, tvdb_ids = None, None, None

        if not tmdb_show_data: # Try TVDB if TMDB fails
            imdb_id = tmdb_ids.get('imdb_id') if tmdb_ids else None
            tvdb_id = tmdb_ids.get('tvdb_id') if tmdb_ids else None
            log.debug("TMDB failed or no match, trying TVDB...")
            tvdb_show_data, tvdb_ep_map, tvdb_ids = self._fetch_tvdb_series(show_title_guess, season_num, episode_num_list, imdb_id, tvdb_id)

        # Combine results
        final_meta.source_api = "tmdb" if tmdb_show_data else ("tvdb" if tvdb_show_data else None)
        # Use __dict__.get for TMDB data as details might be dicts
        final_meta.show_title = tmdb_show_data.get('name') if isinstance(tmdb_show_data, dict) else getattr(tmdb_show_data, 'name', None)
        if not final_meta.show_title: # Fallback to TVDB or guess
            final_meta.show_title = getattr(tvdb_show_data, 'seriesName', show_title_guess)

        tmdb_air_date = tmdb_show_data.get('first_air_date') if isinstance(tmdb_show_data, dict) else getattr(tmdb_show_data, 'first_air_date', None)
        tvdb_air_date = getattr(tvdb_show_data, 'firstAired', None)
        final_meta.show_year = self._get_year_from_date(tmdb_air_date or tvdb_air_date)

        final_meta.season = season_num
        final_meta.episode_list = episode_num_list
        final_meta.ids = {** (tvdb_ids or {}), **(tmdb_ids or {})} # TMDB overrides TVDB

        for ep_num in episode_num_list:
             tmdb_ep = tmdb_ep_map.get(ep_num) if tmdb_ep_map else None
             tvdb_ep = tvdb_ep_map.get(ep_num) if tvdb_ep_map else None
             # Prefer TMDB title/date
             ep_title = getattr(tmdb_ep, 'name', None) or (tvdb_ep.get('episodeName') if tvdb_ep else None)
             air_date = getattr(tmdb_ep, 'air_date', None) or (tvdb_ep.get('firstAired') if tvdb_ep else None)
             if ep_title: final_meta.episode_titles[ep_num] = ep_title
             if air_date: final_meta.air_dates[ep_num] = air_date

        if not final_meta.source_api: log.warning(f"Metadata failed for series: '{show_title_guess}' S{season_num}")
        return final_meta

    # Wrap helper fetch methods with retry logic
    def _fetch_tmdb_series(self, title, season, episodes):
        # (Implementation as before)
        if not self.tmdb: return None, None, None
        fetch_func = self.retry_decorator(self._do_fetch_tmdb_series)
        try:
            self.rate_limiter.wait()
            return fetch_func(title, season, episodes)
        except Exception as e: log.error(f"TMDB series fetch ultimately failed for '{title}': {e}"); return None, None, None

    def _do_fetch_tmdb_series(self, title, season, episodes):
        # (Implementation as before)
        from tmdbv3api import Tv # Import here
        search = Tv()
        results = search.search(title)
        show_match = find_best_match(title, results, result_key='name')
        if not show_match: return None, None, None
        show_id = show_match.id
        show_details = search.details(show_id)
        try: show_external_ids = search.external_ids(show_id)
        except Exception: show_external_ids = {}
        # Combine details and external IDs safely
        combined_show_data = show_details.__dict__ if hasattr(show_details, '__dict__') else {}
        combined_show_data['external_ids'] = show_external_ids

        ep_data = {}
        for ep_num in episodes:
            self.rate_limiter.wait()
            try:
                ep_details = search.episode_details(show_id, season, ep_num)
                if ep_details: ep_data[ep_num] = ep_details
            except Exception as e_ep: log.debug(f"TMDB error S{season}E{ep_num}: {e_ep}")
        return combined_show_data, ep_data, get_external_ids(tmdb_obj=combined_show_data)


    def _fetch_tvdb_series(self, title, season, episodes, imdb_id=None, tvdb_id=None):
        # (Implementation as before)
        if not self.tvdb: return None, None, None
        fetch_func = self.retry_decorator(self._do_fetch_tvdb_series)
        try:
            self.rate_limiter.wait()
            return fetch_func(title, season, episodes, imdb_id, tvdb_id)
        except Exception as e: log.error(f"TVDB series fetch ultimately failed for '{title}': {e}"); return None, None, None

    def _do_fetch_tvdb_series(self, title, season, episodes, imdb_id, tvdb_id):
        # (Implementation as before)
        show = None
        if tvdb_id:
             try: show = self.tvdb.get_series_by_id(tvdb_id); log.debug(f"TVDB found by TVDB ID: {tvdb_id}")
             except Exception: pass
        if not show and imdb_id:
             try: show = self.tvdb.get_series_by_imdb_id(imdb_id); log.debug(f"TVDB found by IMDB ID: {imdb_id}")
             except Exception: pass
        if not show:
             try:
                 results = self.tvdb.search(title)
                 if results: show = results[0]; log.debug(f"TVDB found by name: {title}")
             except Exception as e: log.debug(f"TVDB name search failed: {e}")

        if not show: return None, None, None
        show_data = getattr(show, 'data', show)

        ep_data = {}
        for ep_num in episodes:
            try:
                ep_details = show[season][ep_num]
                ep_data[ep_num] = {
                    'id': getattr(ep_details, 'id', None),
                    'episodeName': getattr(ep_details, 'episodeName', None),
                    'firstAired': getattr(ep_details, 'firstAired', None),
                }
            except Exception as e_ep: log.debug(f"TVDB error S{season}E{ep_num}: {e_ep}")
        return show_data, ep_data, get_external_ids(tvdb_obj=show_data)


    @lru_cache(maxsize=128)
    def fetch_movie_metadata(self, movie_title_guess, year_guess):
        # (Implementation as before)
        log.debug(f"Fetching movie metadata for: '{movie_title_guess}' ({year_guess})")
        final_meta = MediaMetadata(is_movie=True)
        tmdb_movie_data, tmdb_ids = self._fetch_tmdb_movie(movie_title_guess, year_guess)

        if tmdb_movie_data:
            final_meta.source_api = "tmdb"
            final_meta.movie_title = tmdb_movie_data.get('title', movie_title_guess)
            final_meta.release_date = tmdb_movie_data.get('release_date')
            final_meta.movie_year = self._get_year_from_date(final_meta.release_date) or year_guess
            final_meta.ids = tmdb_ids or {}
        else:
             log.warning(f"Metadata failed for movie: '{movie_title_guess}' ({year_guess})")
             final_meta.movie_title = movie_title_guess
             final_meta.movie_year = year_guess

        return final_meta


    def _fetch_tmdb_movie(self, title, year):
        # (Implementation as before)
        if not self.tmdb: return None, None
        fetch_func = self.retry_decorator(self._do_fetch_tmdb_movie)
        try:
            self.rate_limiter.wait()
            return fetch_func(title, year)
        except Exception as e: log.error(f"TMDB movie fetch ultimately failed for '{title}': {e}"); return None, None

    def _do_fetch_tmdb_movie(self, title, year):
        # (Implementation as before)
        from tmdbv3api import Movie # Import here
        search = Movie()
        results = search.search(title, year=year)
        movie_match = find_best_match(title, results, result_key='title')
        if not movie_match: return None, None
        movie_id = movie_match.id
        movie_details = search.details(movie_id)
        try: movie_external_ids = search.external_ids(movie_id)
        except Exception: movie_external_ids = {}
        combined_data = movie_details.__dict__ if hasattr(movie_details, '__dict__') else {}
        combined_data['external_ids'] = movie_external_ids
        return combined_data, get_external_ids(tmdb_obj=combined_data)