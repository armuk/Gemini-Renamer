import logging, sys
# Import specific API libraries if needed, or just handle config here
from tmdbv3api import TMDb
from tvdb_api import Tvdb

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
    print(f"DEBUG_CHECK: TVDB Key read by cfg_helper: [{tvdb_key}]", file=sys.stderr)

    # --- MOVE LANGUAGE DEFINITION UP ---
    language = cfg_helper('tmdb_language', 'en')
    # --- END MOVE ---

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

    # --- Temporary Hardcoding Test ---
    # if True: # Force attempt
    #     try:
    #         from tvdb_api import Tvdb
    #         # Now 'language' is defined and can be used
    #         _tvdb_client = Tvdb(apikey="681d03a8-d9ee-41d7-b8ee-36b583bbee89", language=language, banners=False)
    #         log.info("TVDB Init Successful (Hardcoded Key Test)")
    #         keys_loaded = True # Count this as loaded for the test
    #     except Exception as e:
    #         log.error(f"TVDB Init Failed (Hardcoded): {e}", exc_info=True)
    # --- End Temporary Hardcoding Test ---

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