# Gemini-Renamer (Version 4.0.0-refactored)

🚀 **Gemini-Renamer** is an intelligent, modular, and extensible media file renaming tool built with Python.
It leverages metadata from online sources (TMDB, TVDB) and user-defined configurations to automate the renaming of your movie and TV show collections safely and efficiently, complete with robust undo capabilities.

---

## Key Features

*   **Intelligent Metadata-Based Renaming:**
    *   Fetches rich metadata for movies and TV series from TheMovieDB (TMDB) and TheTVDB (v4 API).
    *   Configurable preference for series metadata source (TMDB first or TVDB first).
    *   Accurate matching using title, year (with configurable tolerance), and fuzzy string comparison (via `thefuzz`).
    *   Option for stricter "first result" API matching using a minimum confidence score (`tmdb_first_result_min_score`).
*   **Flexible File & Folder Formatting:**
    *   Highly customizable filename and folder structure patterns using a wide range of placeholders (see "Formatting Placeholders" below).
    *   Separate formatting rules for movies, series, and series specials (Season 00).
    *   Automatic subtitle renaming, including language code and flag detection (e.g., `.eng.forced.srt`).
*   **Comprehensive File Handling:**
    *   Scans directories recursively or non-recursively.
    *   Supports various configurable video, subtitle, and associated file extensions.
    *   Configurable conflict resolution strategies: `skip`, `overwrite`, `suffix`, or `fail`.
    *   Optional preservation of original file modification times.
    *   Configurable handling for files where type cannot be determined or metadata is not found (`skip`, `guessit_only`, `move_to_unknown`).
    *   Ignores specified directory names and file/directory patterns (e.g., hidden files, samples).
*   **Robust Operations & Safety:**
    *   **Dry Run Mode:** Preview all changes before any files are touched (default behavior).
    *   **Transactional Renames:** Uses a two-phase rename process (original -> temp -> final) to minimize risk during live operations.
    *   **Undo Functionality:** Logs every file operation to an SQLite database, allowing you to:
        *   List previous rename batches (`undo --list`).
        *   Preview undo operations for a specific batch (`undo <batch_id> --dry-run`).
        *   Revert entire rename batches (`undo <batch_id>`).
        *   Optional integrity checks (file size, mtime, full/partial hash) before reverting.
    *   **Backup & Staging (Optional CLI flags for `rename`):**
        *   `--backup-dir <path>`: Backup original files to a specified directory before renaming.
        *   `--stage-dir <path>`: Move renamed files to a staging directory instead of in-place.
        *   `--trash`: Move original files to the system trash.
*   **Advanced Customization & Control:**
    *   Primary configuration via `config.toml` file, supporting profiles for different scenarios.
    *   Secure API key management using a `.env` file.
    *   Interactive CLI setup (`setup`) for easy initial API key configuration.
    *   Command-line options to override most `config.toml` settings for specific runs.
    *   Preserve and order configurable "scene tags" (e.g., PROPER, REPACK) in filenames, with order respected from the config.
    *   Extract and use technical stream information (resolution, video/audio codecs, audio channels) in filenames if enabled (requires `pymediainfo` and relevant placeholders in format strings).
*   **User Experience & Developer Friendly:**
    *   Rich console output (tables, progress bars, styled text) via the `rich` library, with graceful fallbacks for basic terminals.
    *   Detailed logging with configurable levels (DEBUG, INFO, WARNING, ERROR) to console and/or a log file.
    *   Modular codebase for easier maintenance and extension.
    *   Asynchronous operations for efficient API communication (metadata fetching).
    *   `tenacity`-based API retries for improved network resilience.
    *   Optional API response caching using `diskcache` for faster subsequent runs.
    *   Two file scanning strategies: `memory` (default) and `low_memory` (for very large collections).
    *   `ProcessingStatus` enum for standardized internal status reporting and clearer log messages.
    *   Commands to generate a default `config.toml` (`config generate`) and validate an existing one (`config validate`).

---

## Installation

1.  **Clone the Repository:**
    ```bash
    git clone <your-repository-url-here> # e.g., https://github.com/yourusername/Gemini-Renamer.git
    cd Gemini-Renamer
    ```

2.  **(Recommended) Create and Activate a Virtual Environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **API Key Setup:**
    Run the interactive setup:
    ```bash
    python3 rename_main.py setup
    ```
    This will guide you to create/update a `.env` file (by default in the current directory) with your TMDB and TVDB API keys.

5.  **Configuration File (`config.toml`):**
    *   If `config.toml` doesn't exist, you can generate a default one:
        ```bash
        python3 rename_main.py config generate
        ```
    *   Review and customize the generated `config.toml` (and the `.env` file) to your preferences.

---

## Usage

The primary way to use Gemini-Renamer is through its command-line interface.

**General Syntax:**
`python3 rename_main.py [global options] <command> [command-specific options]`

**Available Commands:**

*   `rename <directory>`: Scan and rename files in the specified directory.
*   `undo`: Revert rename operations.
*   `config`: Manage application configuration (`show`, `validate`, `generate`).
*   `setup`: Interactively set up API keys in the `.env` file.

**Global Options (Examples):**
*   `--config <path>`: Path to a custom `config.toml` file.
*   `--profile <name>`: Use a specific profile from your `config.toml` (default is 'default').
*   `--log-level <LEVEL>`: Override console logging level (DEBUG, INFO, WARNING, ERROR).
*   `--quiet` or `-q`: Suppress non-essential console output.

**`rename` Command Examples:**

*   **Dry run (highly recommended to preview changes):**
    ```bash
    python3 rename_main.py rename "/path/to/your/media"
    ```
*   **Live run (perform actual renaming):**
    ```bash
    python3 rename_main.py rename "/path/to/your/media" --live
    ```
*   **Recursive scan, preserve modification times, and use a 'series_keepers' profile:**
    ```bash
    python3 rename_main.py --profile series_keepers rename "/path/to/shows" --live -r --preserve-mtime
    ```
*   **Force movie processing mode and use German language for TMDB:**
    ```bash
    python3 rename_main.py --tmdb-language de rename "/path/to/movies" --processing-mode movie --live
    ```
*   **Interactive live run (confirm each batch):**
    ```bash
    python3 rename_main.py rename "/path/to/your/media" --live -i
    ```

**`undo` Command Examples:**

*   **List available rename batches:**
    ```bash
    python3 rename_main.py undo --list
    ```
*   **Preview what an undo operation would do for a specific batch:**
    ```bash
    python3 rename_main.py undo <batch_id_from_list> --dry-run
    ```
*   **Perform an undo operation for a specific batch:**
    ```bash
    python3 rename_main.py undo <batch_id_from_list>
    ```

**`config` Command Examples:**

*   **Generate a default `config.toml` file (will prompt if it exists unless `--force` is used):**
    ```bash
    python3 rename_main.py config generate
    # python3 rename_main.py config generate --output /custom/path/config.toml --force
    ```
*   **Show the currently loaded/effective configuration for a profile:**
    ```bash
    python3 rename_main.py config show --profile movies
    ```
*   **Validate your `config.toml` file against the expected schema:**
    ```bash
    python3 rename_main.py config validate
    ```

For a full list of options for each command, use `python3 rename_main.py <command> --help`.

---

## Formatting Placeholders

Customize output filenames and directories using these placeholders in your `config.toml` format strings (e.g., `series_format`, `movie_format`, `folder_format_series`).

**Common Placeholders:**
*   `{original_filename}`: The original full filename (e.g., `My.Movie.2023.1080p.mkv`).
*   `{original_stem}`: The original filename without its extension (e.g., `My.Movie.2023.1080p`).
*   `{ext}`: The original file extension, including the dot (e.g., `.mkv`). *Note: Usually appended automatically by the app; typically omit from your format string itself unless precise control is needed.*
*   `{scene_tags_dot}`: Preserved scene tags, dot-separated (e.g., `.PROPER.REPACK`). The order of tags is respected based on your `scene_tags_to_preserve` list in the config.

**Movie Specific Placeholders:**
*   `{movie_title}`: Title of the movie (from metadata, falls back to guess).
*   `{movie_year}`: Year of the movie's release.
*   `{collection}`: Name of the movie's collection, if available (e.g., "The Lord of the Rings Collection").
*   `{release_date}`: Full release date of the movie (YYYY-MM-DD).
*   `{tmdb_id}`, `{imdb_id}`, `{tvdb_id}`: Relevant API/IMDb IDs if found for the movie.
*   `{source_api}`: API source used for the movie's metadata (e.g., "tmdb").

**Series Specific Placeholders:**
*   `{show_title}`: Title of the TV series.
*   `{show_year}`: Year the TV series first aired.
*   `{season}`: Season number (e.g., `2`). Use Python's f-string formatting for padding, like `{season:02d}` to get `02`.
*   `{episode}`: First episode number in the file (e.g., `7`). Use f-string formatting: `{episode:02d}` for `07`.
*   `{ep_identifier}`: Compact episode identifier. For single episodes: `E07`. For multi-episode files (e.g., episodes 7, 8, 9): `E07-E09`.
*   `{episode_title}`: Title of the episode. For multi-episode files, it will be the title of the first episode or a combined title if available (e.g., "Title One & Title Two").
*   `{air_date}`: Air date of the (first) episode (YYYY-MM-DD).
*   `{tmdb_id}`, `{imdb_id}`, `{tvdb_id}`: Relevant API/IMDb IDs if found for the series.
*   `{source_api}`: API source used for the series' metadata (e.g., "tmdb", "tvdb").

**Stream Info Placeholders (Require `extract_stream_info = true` in config or `--use-stream-info` flag, and `pymediainfo` installed):**
*   `{resolution}`: Video resolution (e.g., `1080p`, `720p`, `2160p`, `SD`).
*   `{vcodec}`: Simplified video codec (e.g., `h264`, `h265`, `xvid`, `mpeg2`).
*   `{acodec}`: Simplified primary audio codec (e.g., `ac3`, `dts`, `aac`, `mp3`).
*   `{achannels}`: Audio channels (e.g., `2.0`, `5.1`, `7.1`).

**Subtitle Specific Placeholders (Used in `subtitle_format`):**
*   `{stem}`: The renamed stem of the associated video file.
*   `{lang_code}`: 3-letter ISO 639-2/B language code (e.g., `eng`, `fra`).
*   `{lang_dot}`: Language code prefixed with a dot (e.g., `.eng`) or an empty string if no language is detected.
*   `{flags}`: Concatenated subtitle flags without separators (e.g., `forcedsdh`).
*   `{flags_dot}`: Subtitle flags prefixed with dots (e.g., `.forced.sdh`).
*   `{encoding}`: Detected character encoding of the subtitle file (e.g., `utf-8`, `cp1252`).
*   `{encoding_dot}`: Encoding prefixed with a dot (e.g., `.utf-8`).

**Guessit Specific Placeholders (Use with caution; availability depends on original filename):**
*   Many other keys returned by the `guessit` library might be available directly (e.g., `{screen_size}`, `{source}`, `{release_group}`). Their presence and values depend entirely on the information parsable from the original filename. Refer to `guessit` documentation for possibilities.

---

## Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.
Please ensure your code adheres to the existing style, and consider adding tests for new features or bug fixes.

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details (if one exists in your project).