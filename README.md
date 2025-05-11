# Gemini-Renamer (Version 4.0.0-refactored)

🚀 An advanced, modular, and extensible media file renaming tool, meticulously crafted in Python.
Gemini-Renamer intelligently processes your media library, leveraging metadata from online sources and highly customizable configurations to automate the renaming of movies and TV series episodes. It prioritizes safety with features like dry runs, transactional file operations, and a robust undo system.

---

## Key Features

*   **Intelligent Metadata-Based Renaming:**
    *   Fetches rich metadata from TMDB (The Movie Database) and TVDB (The TV Database v4).
    *   Configurable preference for series metadata source (TMDB or TVDB first).
    *   Handles movies and TV series episodes, including multi-episode files and specials.
    *   Stricter matching logic with score cutoffs to ensure accuracy.
*   **Flexible Configuration:**
    *   Primary configuration via `config.toml` with support for profiles (e.g., `default`, `movies_only`).
    *   Sensitive API keys and language preferences managed securely in a `.env` file.
    *   Interactive setup command (`setup`) for easy initial API key configuration.
    *   Generate a default configuration file (`config generate`).
    *   Validate existing configuration files (`config validate`).
*   **Customizable Naming Formats:**
    *   Highly flexible format strings for series, movies, specials, and their respective folders.
    *   Dedicated format string for subtitle filesOkay, here', including language and flag detection.
    *   Placeholders for a wide range of metadata (titles, years, season/episode numbers, dates, IDs) and technical information.
*   **Comprehensive File Handling:**
    *   Scans directories recursively or non-recursively.
    *   Supports a wide range of configurable video, subtitle, and associated file extensions.
    *   Handles filename conflicts with strategies like `skip`, `overwrite`, `suffix`, or `fail`.
    *   Automatically creates destination folders if they don't exist.
    *   Configurable handling for files where type cannot be determined or metadata is missing (`skip`, `guessit_only`, `move_to_unknown`).
    *   Preserves original file modification times (optional).
    *   Scene tag preservation in filenames, with a configurable list of tagss a new `README.md` reflecting the current capabilities of your Gemini-Renamer application, based on our discussion and the codebase:

markdown
# Gemini-Renamer

🚀 **Gemini-Renamer** is an intelligent, modular, and extensible media file renaming tool built with Python.
It leverages metadata from online sources (TMDB, TVDB) and user-defined configurations to automate the renaming of your movie and TV show collections safely and efficiently, complete with robust undo capabilities.

---

## Key Features

*   **Intelligent Metadata-Based Renaming:**
    *   Fetches rich metadata for movies and TV series from TheMovieDB (TMDB) and TheTVDB.
    *   Configurable preference for series metadata source (TMDB first or TVDB first).
    * to keep and order respected.
*   **Safety & Control:**
    *   **Dry Run Mode (`--live` not specified):** Preview all proposed changes without modifying any files.
    *   **Transactional File Operations:** Uses a two-phase (temp -> final) move/rename process to minimize risk during live runs.
    *   **Undo System:** Logs all file operations (creations, moves, renames) for a given run.
        *   List past rename batches (`undo --list`).
        *   Preview undo operations (`undo <batch_id> --dry-run`).
        *   Revert entire rename batches (`undo <batch_id>`).
        *   Optional integrity checks (size, mtime, partial/full hash) before reverting.
    *   **Interactive Mode (`--interactive`):** Confirm each batch of operations during a live run, with options to skip, re-plan with guessit-only, or manually input an API ID.
*   **Performance & Robustness:**
    *   As   Accurate matching using title, year (with tolerance), and fuzzy string comparison.
    *   Option for stricter "first result" API matching using a minimum score.
*   **Flexible File & Folder Formatting:**
    *   Highly customizable filename and folder structure patterns using a wide range of placeholders (see "Formatting Placeholders" below).
    *   Separate formatting rules for movies, series, and series specials (Season 00).
    *   Automatic subtitle renaming, including language code and flag detection (e.g., `.eng.forced.srt`).
*   **Comprehensive File Handling:**
    *   Scans directories recursively or non-recursively.
    *   Handles various video, subtitle, and associated file extensions (configurable).
    *   Configurable conflict resolution: `skip`, `overwrite`, `suffix`, or `fail`.
    *   Optional preservation of original file modification times.
    *   Configurable handling for files where type cannot be determined or metadata is not found (`skip`, `guessit_only`, `move_to_unknown`).
*   **Robust Operations & Safety:**
    *   **Dry Run Mode:** Preview all changes before any files are touched.
    *   **Transactional Renames:** Two-phase rename process (original -> temp -> final) to prevent data loss during live operations.
    *   **Undo Functionality:** Log every operation to an SQLite database, allowing you to revert entire rename batches.
        *   List previous rename batches.
        *   Preview undo operations.
        *   Optional integrity checks (file size, mtime, full/partial hash) before reverting.
    *   **Backup & Staging (Optional):**
        *   Backup original files to a specified directory before renaming.
        *   Move renamed files to a stagingynchronous metadata fetching for improved speed.
    *   Automatic retries for API calls with configurable attempts and delays (`tenacity`).
    *   Rate limiting to respect API usage policies.
    *   Optional caching of API responses (`diskcache`) to speed up subsequent runs.
    *   Two scanning strategies (`memory` for smaller libraries, `low_memory` using a temporary database for very large libraries).
    *   Extraction of technical stream info (resolution, codecs) using `pymediainfo` (optional, enabled if placeholders are used).
*   **User Experience:**
    *   Rich terminal output (tables, progress bars, styled text) via the `rich` library, with graceful fallbacks for basic terminals.
    *   Detailed logging with configurable levels (DEBUG, INFO, WARNING, ERROR) to console and/or a log file.
    *   Clear separation of concerns in a modular codebase.

---

## Installation

1.  **Clone the Repository:**

    git clone https://github.com/yourusername/Gemini-Renamer.git # Replace with your actual repo URL
    cd Gemini-Renamer


2.  **(Recommended) Create and Activate a Virtual Environment:**

    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate


3.  **Install Dependencies:**

    pip install -r requirements.txt


---

## Initial Setup

Before the first run, set up your API keys:

python3 rename_main.py setup

This will guide you through creating a `.env` file in your current directory (or a custom path using `--dotenv-path`) and entering your TMDB and TVDB API keys.

You can also generate a default `config.toml` file:

python3 rename_main.py config generate

Review and customize `config.toml` (and `.env`) to your preferences.

---

## Usage

**General Syntax:**
`python3 rename_main.py [global options] <command> [command-specific options]`

**Examples:**

*   **Dry run (preview changes) for a directory:**

    python3 rename_main.py rename "/path/to/ directory instead of in-place.
        *   Option to move original files to system trash.
*   **Advanced Customization & Control:**
    *   Configuration via `config.toml` (with support for profiles) and `.env` (for API keys).
    *   Generate a default `config.toml` on first run or via CLI command.
    *   Interactive CLI setup for API keys.
    *   CLI options to override most configuration settings for specific runs.
    *   Ignore specific directories or file patterns.
    *   Preserve and order configurable "scene tags" (e.g., PROPER, REPACK) in filenames.
    *   Extract and use technical stream information (resolution, codecs) in filenames if enabled (requires `pymediainfo`).
*   **User Experience & Developer Friendly:**
    *   Rich console output (tables, progress bars, styled text) via the `rich` library, with graceful fallbacks for basic terminals.
    *   Detailed logging with configurable levels.
    *   Modular codebase for easier maintenance and extension.
    *   Asynchronous operations for efficient API communication.
    *   Tenacity-based API retries for network resilience.
    *   Optional API response caching using `diskcache` for improved performance on subsequent runs.
    *   `ProcessingStatus` enum for standardized internal status reporting.

---

## Installation

1.  **Clone the repository:**

    git clone <your-repo-url>
    cd Gemini-Renamer


2.  **(Recommended) Create and activate a virtual environment:**
your/media"


*   **Live run to rename files:**

    python3 rename_main.py rename "/path/to/your/media" --live


*   **Interactive live run:**

    python3 rename_main.py rename "/path/to/your/media" --live -i


*   **Using a specific configuration profile:**

    python3 rename_main.py --profile movies_profile rename "/path/to/movies" --live


*   **List undo batches:**

    python3 rename_main.py undo --list


*   **Undo a specific batch:**

    python3 rename_main.py undo <batch_id_from_list>


*   **Override config settings via CLI:**

    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate


3.  **Install dependencies:**

    pip install -r requirements.txt


4.  **API Key Setup:**
    *   The first time you run a command that requires API keys (like `rename`), or if you run `python3 rename_main.py setup`, you'll be guided through an interactive setup to create/update a `.env` file with your TMDB and TVDB API keys.
    *   Alternatively, create a `.env` file in the project root with your keys:
    env
        TMDB_API_KEY=your_tmdb_api_key_here
        TVDB_API_KEY=your_tvdb_api_key_here
        # Optional:
        # TMDB_LANGUAGE
    python3 rename_main.py rename "/path/to/series" --live --series-format "{show_title} - S{season:02}E{episode:02} - {episode_title}" --preserve-mtime


Refer to `python3 rename_main.py --help` for a full list of commands and options.

---

## Configuration Files

*   **`config.toml`:**
    *   Located in the project directory, user's config directory (e.g., `~/.config/rename_app/config.toml`), or specified by `--config`.
    *   Defines default settings and custom profiles for renaming=en
    

5.  **Configuration File (`config.toml`):**
    *   If `config.toml` doesn't exist in an expected location (project root, user config dir), the application can generate a default one for you on first run (if not in quiet mode) or by using the `python3 rename_main.py config generate` command.
    *   Review and customize `config.toml` to your preferences.

---

 formats, file handling, API behavior, logging, caching, and undo features.
    *   Use `python3 rename_main.py config generate` to create a well-commented default file.
*   **`.env`:**
    *   Located in the current working directory by default (or specified by `--dotenv-path` during setup).
    *## Usage

The primary way to use Gemini-Renamer is through its command-line interface.

**General Syntax:**
`   Stores API keys (`TMDB_API_KEY`, `TVDB_API_KEY`) and optionally `TMDB_LANGUAGEpython3 rename_main.py [global options] <command> [command-specific options]`

**Available Commands:**

*`.
    *   **Important:** Add `.env` to your `.gitignore` file to avoid committing sensitive keys.

---

   `rename <directory>`: Scan and rename files in the specified directory.
*   `undo`: Revert rename operations.
*   `config`: Manage application configuration.
*   `setup`: Interactively set up API keys.

**## Formatting Placeholders

Customize output filenames and directories using these placeholders in your `config.toml` format strings (e.g., `series_format`, `movie_format`, `folder_format_series`).

**Common Placeholders:**
Global Options (Examples):**
*   `--config <path>`: Path to a custom `config.toml` file.
*   `--profile <name>`: Use a specific profile from your `config.toml`.
*   *   `{original_filename}`: The original full filename (e.g., `my.movie.2023.1080p.mkv`).
*   `{original_stem}`: The original filename without`--log-level <LEVEL>`: Override console logging level (DEBUG, INFO, WARNING, ERROR).
* extension (e.g., `my.movie.2023.1080p`).
*      `--quiet` or `-q`: Suppress non-essential console output.

**Rename Command Examples:**

*`{ext}`: The original file extension, including the dot (e.g., `.mkv`). *(Often   **Dry run (highly recommended for first use):**

    python3 rename_main.py rename automatically appended; typically remove from the format string itself unless precise control is needed).*
*   `{scene_tags_ "/path/to/your/media" --log-level INFO

*   **Live run (dot}`: Preserved scene tags, dot-prefixed (e.g., `.PROPER.REPACK`). Order is based on `scene_tags_to_preserve` in config.

**Movie Specific:**
*   `perform actual renaming):**

    python3 rename_main.py rename "/path/to/your/media" --live

*   **Recursive scan, preserve modification times, and use a specific profile:**
{movie_title}`: Title of the movie.
*   `{movie_year}`: Year of the movie.
*   `{collection}`: Name of the movie's collection (e.g., "The Lord
    python3 rename_main.py --profile series_keepers rename "/path/to/shows" --live of the Rings Collection").
*   `{release_date}`: Full release date (YYYY-MM-DD).
* -r --preserve-mtime

*   **Force movie processing mode and use German language for TMDB:**   `{tmdb_id}`, `{imdb_id}`, `{tvdb_id}`: Relevant API/IM

    python3 rename_main.py --tmdb-language de rename "/path/to/movies" --processing-mode movie --live


**Undo Command Examples:**

*   **List available renameDb IDs if found.
*   `{source_api}`: API source used for metadata (e.g., "tmdb").

**Series Specific:**
*   `{show_title}`: Title of the series.
* batches:

    python3 rename_main.py undo --list

*   `{show_year}`: Year the series first aired.
*   `{season}`: Season numberPreview what an undo operation would do:**

    python3 rename_main.py undo <batch_ (e.g., `2`). Use formatting like `{season:02}` for `02`.
*   `{id_from_list> --dry-run

*   **Perform an undo operation:**

episode}`: First episode number in the file (e.g., `7`). Use formatting like `{episode:0    python3 rename_main.py undo <batch_id_from_list>


**Config Command Examples:**

*   **Generate a default `config.toml` file:**
  
    python3 rename2}` for `07`.
*   `{episode_list}`: List of all episode numbers in the file_main.py config generate
    python3 rename_main.py config generate --output /custom/path/config (e.g., `[7, 8]`). *Note: For direct use in string, consider custom logic or use `{ep_identifier}`.*
*   `{ep_identifier}`: Compact episode identifier. For single.toml --force
    
*   **Show the currently loaded configuration for a profile:**

    python3 rename_main.py config show --profile movies

*   **Validate your `config.toml episodes: `E07`. For multi-episodes: `E07-E08`.

    python3 rename_main.py config validate


---

{episode_title}`: Title of the episode (or first title for multi-episode files; " & " separated for multiple if available).
*   `{air_date}`: Air date of the episode (YYYY-MM-DD).
*   `{tmdb_id}`, `{imdb_id}`, `{tvdb_id}`: Relevant API/IMDb IDs if found.
*   `{source_api}`: API source used for metadata (e.g., "tmdb", "tvdb").

**Stream Info (Requires `extract_stream_info = true` in config or `--use-stream-info` flag, and relevant placeholders in format strings):**
*   `{resolution}`: Video resolution (e.g., `1080p`, `720p`, `2160p`, `SD`).
*   `{vcodec}`: Simplified video codec (e.g., `h264`, `h265`, `xvid`).
*   `{acodec}`:## Formatting Placeholders

Customize output filenames and directories using these placeholders in your `config.toml` format strings (e.g., `series_format`, `movie_format`, `folder_format_series`).

**Common:**
*   `{original_filename}`: Original full filename (e.g., `My.Movie.2023.1080p.mkv`).
*   `{original_stem}`: Original filename without extension (e.g., `My.Movie.2023.1080p`).
*   `{ext}`: Original file extension (e.g., `.mkv`). *Note: Usually added automatically by the app; typically omit from your format string itself unless you need specific control.*
*   `{scene_tags_dot}` Simplified primary audio codec (e.g., `ac3`, `dts`, `aac`).
*   `{achannels}`: Audio channels (e.g., `2.0`, `5.1`, `7.1: Preserved scene tags, dot-separated (e.g., `.PROPER.REPACK`). Order influenced by `scene_tags_to_preserve` config.

**Movie Specific:**
*   `{movie_title}`: Title of the movie.
*   `{movie_year}`: Year of the movie's release.
*   `{collection}`: Name of the movie collection, if found (e.g., 'James Bond Collection').
*   `{release_date}`: Full release date (YYYY-MM-DD).
*   `{tmdb_id}`,`).

**Subtitle Specific (Used in `subtitle_format`):**
*   `{stem}`: The renamed stem of the associated video file.
*   `{lang_code}`: 3-letter ISO 639 `{imdb_id}`, `{tvdb_id}`: Relevant IDs if found.

**Series Specific:**
*   `{show_title}`: Title of the series.
*   `{show_year}`: Year the-2/B language code (e.g., `eng`, `fra`).
*   `{lang_dot}`: Language code prefixed with a dot (e.g., `.eng`) or empty if no language.
*   `{flags}`: Concatenated subtitle flags (e.g., `forcedsdh`).
*   `{ series first aired.
*   `{season}`: Season number (e.g., `2`). Use Pythonflags_dot}`: Flags prefixed with dots (e.g., `.forced.sdh`).
*   `{encoding}`: Detected encoding (e.g., `utf-8`, `cp1252`).
* formatting for padding: `{season:02d}` -> `02`.
*   `{episode}`: First episode number (e.g., `7`). Use Python formatting: `{episode:02d}` -> `0   `{encoding_dot}`: Encoding prefixed with a dot (e.g., `.utf-8`).

7`.
*   `{ep_identifier}`: Compact episode identifier. For single episodes: `E07`. For**Guessit Specific (Use with caution, availability depends on original filename):**
*   Many other keys returned by the multi-episodes: `E01-E03`.
*   `{episode_title}`: Title of `guessit` library might be available directly (e.g., `{screen_size}`, `{source}`, `{release the episode (or first for multi-episode files).
*   `{air_date}`: Air date of the episode_group}`). Their presence depends on the information parsable from the original filename.

---

## Contributing

Pull requests are welcome! (YYYY-MM-DD).

*   `{tmdb_id}`, `{imdb_id}`, `{tvdb_id}`: Relevant IDs if found.

**Stream Info (Requires `extract_stream_info = true For major changes, please open an issue first to discuss what you would like to change.
Please ensure tests pass and` in config or `--use-stream-info` flag):**

*   `{resolution}`: Video resolution consider adding new tests for new features.

---

## License

This project is licensed under the MIT License.