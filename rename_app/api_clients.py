import logging
# Import specific API libraries if needed, or just handle config here
# from tmdbv3api import TMDb
# from tvdb_api import Tvdb

log = logging.getLogger(__name__)

# Global client instances (consider making them properties of a class)
_tmdb_client = None
_tvdb_client = None
_clients_initialized = False

def initialize_api_clients(cfg_helper):
    """Initializes API clients based on config and keys."""
    global _tmdb_client, _tvdb_client, _clients_initialized
    if _clients_initialized:
        log.debug("API clients already initialized.")
        return True # Don't re-initialize

    tmdb_key = cfg_helper.get_api_key('tmdb')
    tvdb_key = cfg_helper.get_api_key('tvdb')
    language = cfg_helper('tmdb_language', 'en') # Use same language for both?

    keys_loaded = False

    if tmdb_key:
        try:
            from tmdbv3api import TMDb # Import here to avoid import error if missing
            _tmdb_client = TMDb()
            _tmdb_client.api_key = tmdb_key
            _tmdb_client.language = language
            # TODO: Configure requests session for caching? tmdbv3api might not support easily.
            log.info(f"TMDB API Client initialized (Lang: {language}).")
            keys_loaded = True
        except ImportError: log.warning("TMDB requires 'tmdbv3api'.")
        except Exception as e: log.error(f"Failed to init TMDB Client: {e}")
    else: log.debug("TMDB API Key not found.")

    if tvdb_key:
        try:
            from tvdb_api import Tvdb # Import here
            # TODO: Check tvdb_api init requirements (v4?)
            _tvdb_client = Tvdb(apikey=tvdb_key, language=language, banners=False)
            log.info(f"TVDB API Client initialized (Lang: {language}).")
            keys_loaded = True
        except ImportError: log.warning("TVDB requires 'tvdb_api'.")
        except Exception as e: log.error(f"Failed to init TVDB Client: {e}")
    else: log.debug("TVDB API Key not found.")

    _clients_initialized = True
    if not keys_loaded: log.warning("No API keys loaded or clients initialized.")
    return keys_loaded

def get_tmdb_client():
    if not _clients_initialized: log.warning("API clients not initialized yet."); return None
    return _tmdb_client

def get_tvdb_client():
    if not _clients_initialized: log.warning("API clients not initialized yet."); return None
    return _tvdb_client