# rename_app/api_clients.py

import logging
import sys
from tmdbv3api import TMDb

# --- TVDB v4 Import ---
# Import the correct library needed by metadata_fetcher.py
try:
    from tvdb_v4_official import TVDB
    TVDB_V4_AVAILABLE = True
except ImportError:
    TVDB_V4_AVAILABLE = False
    # Define TVDB as Exception if not available to avoid NameError later,
    # although the check below should prevent usage.
    TVDB = Exception

log = logging.getLogger(__name__)

# Global client instances
_tmdb_client = None
_tvdb_client = None
_clients_initialized = False

def initialize_api_clients(cfg_helper):
    """Initializes API clients based on config and keys. Uses tvdb_v4_official."""
    global _tmdb_client, _tvdb_client, _clients_initialized
    if _clients_initialized:
        log.debug("API clients already initialized.")
        return True # Don't re-initialize

    tmdb_key = cfg_helper.get_api_key('tmdb')
    tvdb_key = cfg_helper.get_api_key('tvdb')
    # Language is primarily used for TMDB client init,
    # tvdb-v4-official usually handles lang per request.
    language = cfg_helper('tmdb_language', 'en')

    keys_loaded = False

    # --- TMDB Initialization ---
    if tmdb_key:
        try:
            _tmdb_client = TMDb()
            _tmdb_client.api_key = tmdb_key
            _tmdb_client.language = language # Set language for TMDB
            log.info(f"TMDB API Client initialized (Lang: {language}).")
            keys_loaded = True
        except ImportError:
             # This shouldn't happen if tmdbv3api is a core dependency, but good practice
            log.warning("TMDB requires 'tmdbv3api'.")
        except Exception as e:
            log.error(f"Failed to init TMDB Client: {e}")
    else:
        log.debug("TMDB API Key not found.")


    # --- TVDB v4 Initialization ---
    if tvdb_key:
        if TVDB_V4_AVAILABLE: # Check if the library was imported successfully
            try:
                # Use tvdb_v4_official's TVDB class.
                # api_key is the typical parameter name.
                # Language/banners are usually not constructor args for v4.
                _tvdb_client = TVDB(apikey=tvdb_key)
                # Updated log message as language isn't set here.
                log.info("TVDB API Client (v4) initialized.")
                keys_loaded = True
            except Exception as e:
                # More specific error log
                log.error(f"Failed to init TVDB Client (v4): {e}", exc_info=True)
        else:
            # Updated warning message for correct library name.
            log.warning("TVDB API functionality requires 'tvdb-v4-official' library.")
    else:
        log.debug("TVDB API Key not found.")

    _clients_initialized = True
    if not keys_loaded:
        log.warning("No API keys loaded or clients initialized successfully.")
    return keys_loaded

def get_tmdb_client():
    """Returns the initialized TMDB client instance, or None."""
    if not _clients_initialized:
        log.warning("Attempted to get TMDB client before initialization.")
        return None
    return _tmdb_client

def get_tvdb_client():
    """Returns the initialized TVDB v4 client instance, or None."""
    if not _clients_initialized:
        log.warning("Attempted to get TVDB client before initialization.")
        return None
    # Returns the instance initialized with TVDB (from tvdb_v4_official)
    return _tvdb_client