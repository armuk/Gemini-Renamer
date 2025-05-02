# simple_tvdb_test.py
import logging
logging.basicConfig(level=logging.DEBUG) # Show library debug messages

# Make sure tvdb_api is installed in your environment
try:
    from tvdb_api import Tvdb, tvdb_error, tvdb_notauthorized
except ImportError:
    print("Error: tvdb_api library not found. Install it: pip install tvdb_api")
    exit()

# --- Replace with your actual key ---
YOUR_TVDB_API_KEY = "681d03a8-d9ee-41d7-b8ee-36b583bbee89"
# --- Replace with a known show title ---
SEARCH_TITLE = "The Studio"
# --- Set desired language ---
LANGUAGE = "en"

print(f"Attempting to initialize Tvdb with key: {YOUR_TVDB_API_KEY}")
try:
    # Initialize (try without PIN first)
    t = Tvdb(apikey=YOUR_TVDB_API_KEY, language=LANGUAGE, banners=False)
    print("Initialization successful.")

    # --- ADD EXPLICIT AUTHORIZE CALL ---
    print("Attempting to authorize...")
    t.authorize() # Call the authorize method
    print("Authorize call completed (check logs for errors).")
    # --- END ADD ---

    # Optional: Check if a login method exists (uncomment to check)
    print("Available methods:", dir(t))
    if hasattr(t, 'login'):
         print("Attempting login...")
         t.login() # Or whatever the method is called
         print("Login call completed (check logs for errors).")

    print(f"\nAttempting to search for: '{SEARCH_TITLE}'")
    results = t.search(SEARCH_TITLE) # This is where the error happens in your app

    print(f"\nSearch successful! Results:")
    if results:
        for series in results:
            print(f"- {getattr(series, 'seriesName', 'N/A')} (ID: {getattr(series, 'id', 'N/A')})")
    else:
        print("- No results found.")

except tvdb_notauthorized:
    print("\nERROR: TVDB reported 'Not Authorized'. Check API key validity and type on TVDB website.")
except tvdb_error.TvdbError as e:
    print(f"\nERROR: TVDB library error: {e}")
except Exception as e:
    print(f"\nERROR: An unexpected error occurred: {e}")
    import traceback
    traceback.print_exc()
