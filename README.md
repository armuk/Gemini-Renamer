# Gemini-Renamer

🚀 A modular and extensible file renaming tool powered by Python.  
It uses metadata and custom configurations to automate renaming tasks safely, with undo capabilities.

---

## Features
- Rename files intelligently based on metadata
- Undo and rollback renaming operations
- Configurable behavior with `config.toml` and `.env`
- CLI interface for easy batch operations
- Well-structured, extensible Python modules

---

## Installation

# Clone the repo
git clone https://github.com/yourusername/Gemini-Renamer.git
cd Gemini-Renamer

# (Optional) Create a virtual environment
python3 -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies

pip install -r requirements.txt

## Usage

python rename_main.py --input-folder /path/to/files --config config.toml

Example

python rename_main.py --dry-run --verbose

#Available CLI options:
     --input-folder: Path to the directory containing files to rename

    --dry-run: Show what would happen without making changes

    --verbose: Print detailed logs

    (more options configured via cli.py)

# Configuration

    config.toml: General app settings

    .env: Environment-specific variables (e.g., API keys)

## Formatting Placeholders

You can customize the output filenames and directories using format strings in your `config.toml` file. The following placeholders are available:

**Common:**
*   `{original_filename}`: The original full filename.
*   `{original_stem}`: The original filename without the extension.
*   `{ext}`: The original file extension (including the dot). *Note: Usually added automatically, remove from format string itself.*
*   `{scene_tags}`: List of preserved scene tags (e.g., `['PROPER', 'REPACK']`).
*   `{scene_tags_dot}`: Preserved scene tags prefixed with dots (e.g., `.PROPER.REPACK`).

**Movie Specific:**
*   `{movie_title}`: Title of the movie (from metadata or guess).
*   `{movie_year}`: Year of the movie (from metadata or guess).
*   `{collection}`: Name of the movie collection, if found (e.g., 'James Bond Collection'). *(New)*
*   `{release_date}`: Full release date (YYYY-MM-DD).
*   `{tmdb_id}`, `{imdb_id}`, `{tvdb_id}`: Relevant IDs if found.

**Series Specific:**
*   `{show_title}`: Title of the series (from metadata or guess).
*   `{show_year}`: Year the series first aired.
*   `{season}`: Season number (use formatting like `{season:0>2}` for padding, e.g., 01).
*   `{episode}`: First episode number in the file (use formatting like `{episode:0>2}` for padding, e.g., 07).
*   `{episode_list}`: List of all episode numbers in the file (e.g., `[1, 2, 3]`).
*   `{episode_range}`: Compact episode range (e.g., `E01-E03`) for multi-episode files.
*   `{episode_title}`: Title of the episode (or first title for multi-episode).
*   `{air_date}`: Air date of the episode (YYYY-MM-DD).
*   `{tmdb_id}`, `{imdb_id}`, `{tvdb_id}`: Relevant IDs if found.

**Stream Info (Requires `extract_stream_info = true` in config or `--use-stream-info` flag):** *(New)*
*   `{resolution}`: Video resolution (e.g., '1080p', '720p', '2160p', 'SD').
*   `{vcodec}`: Simplified video codec (e.g., 'h264', 'h265', 'xvid', 'mpeg2').
*   `{acodec}`: Simplified primary audio codec (e.g., 'ac3', 'dts', 'aac', 'dts-hd.ma', 'eac3').
*   `{achannels}`: Audio channels (e.g., '2.0', '5.1', '7.1').

**Subtitle Specific (Used in `subtitle_format`):**
*   `{stem}`: The renamed stem of the associated video file.
*   `{lang_code}`: 3-letter language code (e.g., 'eng', 'fra').
*   `{lang_dot}`: Language code prefixed with a dot (e.g., '.eng') or empty string if no language.
*   `{flags}`: Concatenated flags (e.g., 'forcedsdh').
*   `{flags_dot}`: Flags prefixed with dots (e.g., '.forced.sdh').
*   `{encoding}`: Detected encoding (e.g., 'utf-8').
*   `{encoding_dot}`: Encoding prefixed with a dot (e.g., '.utf-8').

**Guessit Specific:**
*   Any key returned by the `guessit` library can potentially be used (e.g., `{screen_size}`, `{source}`, `{release_group}`). Refer to `guessit` documentation for possibilities. Use with caution as availability depends on the original filename.

# Testing

pytest tests/

Tests cover:

    File system operations

    Renaming logic

    Config and metadata handling

    Utilities



# ßßContributing

Pull requests are welcome!
For major changes, please open an issue first to discuss what you would like to change.
License

MIT License