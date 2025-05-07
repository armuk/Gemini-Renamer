# --- START OF FILE utils.py ---

# rename_app/utils.py

import re # Need regex for filename manipulations
import os # Need os for path manipulations
import json # Need json for temp file alternative (not used in SQLite path)
import sqlite3 # Need for low_memory strategy
import tempfile # Need for low_memory strategy
import logging # Keep logging import for module-level logger
from pathlib import Path # Need Path for file path manipulations
from functools import lru_cache # Need lru_cache for caching results
from collections import defaultdict # Need defaultdict for grouping
from typing import List, Tuple, Optional, Set, Dict, Any, Iterator # Need various types for type hints
from itertools import groupby # Need groupby for grouping items

# --- TQDM (Progress Bars) Import ---
try: from tqdm import tqdm; TQDM_AVAILABLE = True
except ImportError: TQDM_AVAILABLE = False;
def tqdm(iterable, *args, **kwargs): yield from iterable

# --- Other Imports ---
try: import langcodes; LANGCODES_AVAILABLE = True
except ImportError: LANGCODES_AVAILABLE = False
try: import chardet; CHARDET_AVAILABLE = True
except ImportError: CHARDET_AVAILABLE = False
try: from guessit import guessit; GUESSIT_AVAILABLE = True
except ImportError: GUESSIT_AVAILABLE = False

# --- Import pymediainfo and set flag at module level ---
try:
    from pymediainfo import MediaInfo as MediaInfoParser
    PYMEDIAINFO_AVAILABLE = True
except ImportError:
    PYMEDIAINFO_AVAILABLE = False # Flag remains defined even if import fails

log = logging.getLogger(__name__) # Define logger for the module

# --- Filename Utils ---
# (sanitize_os_chars, sanitize_filename, extract_scene_tags, detect_encoding, parse_subtitle_language, _get_base_stem - unchanged)
def sanitize_os_chars(name: str) -> str:
    """Removes only OS-prohibited characters, leaves spaces/dots/etc."""
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
    return sanitized if sanitized else "_invalid_char_removal_"

def sanitize_filename(filename: str) -> str:
    """Cleans a filename by removing/replacing invalid characters and handling edge cases."""
    if not filename or filename.isspace(): return "_invalid_name_"
    starts_with_dot = filename.startswith('.')
    sanitized = re.sub(r'[<>:"/\\|?*\s]+', '_', filename)
    sanitized = re.sub(r'_+', '_', sanitized)
    if not sanitized: return "_invalid_name_"
    if all(c == '.' for c in sanitized): return "_invalid_dots_"
    if all(c == '_' for c in sanitized): return "_invalid_underscores_"
    while sanitized and sanitized[-1] in '._': sanitized = sanitized[:-1]
    if len(sanitized) > 1 and not starts_with_dot and sanitized.startswith('_'): sanitized = sanitized.lstrip('_')
    if not sanitized: return "_invalid_name_"
    if starts_with_dot and not sanitized.startswith('.'): sanitized = "." + sanitized
    if sanitized in ['.', '..']: return "_invalid_dots_"
    return sanitized

@lru_cache(maxsize=128)
def extract_scene_tags(filename: str, tags_to_match: Tuple[str, ...]) -> Tuple[List[str], str]:
    found_tags_map = {};
    if not isinstance(tags_to_match, tuple): tags_to_match = tuple(tags_to_match)
    if not tags_to_match: return [], ""
    pattern = r'\b(' + '|'.join(re.escape(tag) for tag in tags_to_match) + r')\b'
    try:
        matches = re.finditer(pattern, filename, re.IGNORECASE); tag_lookup = {tag.lower(): tag for tag in tags_to_match}
        found_in_order = []
        for match in matches:
            matched_lower = match.group(1).lower()
            if matched_lower in tag_lookup:
                original_casing = tag_lookup[matched_lower]
                if original_casing not in found_tags_map.values(): found_tags_map[matched_lower] = original_casing; found_in_order.append(original_casing)
    except re.error as e: log.error(f"Invalid scene tag regex: {e}"); return [], ""
    sorted_tags = []
    if found_in_order:
        try: tag_order = {tag: i for i, tag in enumerate(tags_to_match)}; sorted_tags = sorted(list(found_tags_map.values()), key=lambda tag: tag_order.get(tag, float('inf')))
        except Exception as e_sort: log.warning(f"Error sorting scene tags: {found_tags_map.values()}. Using alphabetical. Error: {e_sort}"); sorted_tags = sorted(list(found_tags_map.values()))
    tags_dot_str = "".join(f".{tag}" for tag in sorted_tags); return sorted_tags, tags_dot_str

@lru_cache(maxsize=256)
def detect_encoding(file_path: Path) -> Optional[str]:
    if not CHARDET_AVAILABLE: return None
    try:
        sample_size = 8192
        with open(file_path, 'rb') as f: sample = f.read(sample_size);
        if not sample: return 'empty'
        result = chardet.detect(sample);
        if not result: return None
        encoding = result.get('encoding'); confidence = result.get('confidence')
        log.debug(f"Encoding for {file_path.name}: {encoding} (Conf: {confidence if confidence is not None else 'N/A'})")
        return encoding if encoding and confidence is not None and confidence > 0.6 else None
    except FileNotFoundError: log.warning(f"Encoding detection failed: File not found '{file_path}'"); return None
    except Exception as e: log.warning(f"Encoding detection failed for '{file_path.name}': {e}"); return None

@lru_cache(maxsize=128)
def parse_subtitle_language(filename: str, detect_enc: bool = False, file_path: Optional[Path] = None) -> Tuple[Optional[str], List[str], Optional[str]]:
    if not LANGCODES_AVAILABLE: return None, [], None
    lang_code_3b, flags, encoding = None, set(), None; log.debug(f"Parsing subtitle language for: {filename}")
    base, _ = os.path.splitext(filename); guess = {}
    if GUESSIT_AVAILABLE:
        try:
            guess = guessit(filename, options={'expected_type': 'subtitle'}); log.debug(f"Guessit result for subtitle: {guess}")
            lang_obj = guess.get('language'); enc_guess = guess.get('encoding')
            if lang_obj:
                try: lang_code_3b = lang_obj.to_alpha3(variant='B'); log.debug(f"Guessit found lang: {lang_code_3b}")
                except Exception as e: log.warning(f"Langcodes error on guessit lang {lang_obj}: {e}")
            if enc_guess: encoding = enc_guess.lower()
            if guess.get('forced', False): flags.add('forced')
            if guess.get('hearing_impaired', False): flags.add('sdh')
            flags.update(f.lower() for f in guess.get('other', []) if f.lower() in ['forced', 'sdh', 'cc'])
        except Exception as e: log.debug(f"Guessit failed on subtitle '{filename}': {e}")
    for flag in ['forced', 'sdh', 'cc']:
         if re.search(r'(?:[^\w]|^)(' + flag + r')(?:[^\w]|$)', base, re.IGNORECASE): flags.add(flag)
    if not lang_code_3b:
        lang_options = r'eng|en|fre|fr|ger|de|spa|es|ita|it|jpn|jp|kor|ko|chi|zh|rus|ru|nld|nl|swe|sv|nor|no|dan|da|fin|fi|cze|ces|cs|pob|por|english|french|german|spanish|italian|japanese|chinese|korean|russian|dutch|swedish|norwegian|danish|finnish|czech'
        try:
            pattern = r'(?:[._\-\s(\[]|^)(?P<lang>' + lang_options + r')(?=[._\-\s)\]]|$)'
            matches = list(re.finditer(pattern, base, re.IGNORECASE))
            if matches:
                 log.debug(f"Regex fallback potential langs: {[m.group('lang') for m in matches]}")
                 for match in matches:
                     lang_part = match.group('lang').lower();
                     if len(lang_part) < 2: continue
                     lookup_part = lang_part; map_aliases = {'pob': 'por', 'czech': 'ces', 'german': 'deu', 'french':'fra', 'spanish':'spa', 'japanese':'jpn', 'chinese':'zho', 'korean':'kor', 'russian':'rus', 'dutch':'nld', 'swedish':'swe', 'norwegian':'nor', 'danish':'dan', 'finnish':'fin', 'italian':'ita'}
                     lookup_part = map_aliases.get(lookup_part, lookup_part)
                     try: lang_obj = langcodes.get(lookup_part, normalize=True); lang_code_3b = lang_obj.to_alpha3(variant='B'); log.debug(f"Regex using lang: {lang_code_3b} from '{lang_part}' (lookup: '{lookup_part}')"); break
                     except LookupError: continue
                 if not lang_code_3b: log.debug(f"Regex potentials failed langcodes lookup.")
        except re.error as e: log.error(f"Language regex fallback error: {e}")
    if detect_enc and not encoding and file_path:
        detected_enc = detect_encoding(file_path);
        if detected_enc and detected_enc not in ['ascii', 'empty']:
             enc_lower = detected_enc.lower();
             if 'utf-8' in enc_lower: encoding = 'utf-8'
             elif '1252' in enc_lower: encoding = 'cp1252'
             elif 'iso-8859-1' in enc_lower: encoding = 'latin1'
             elif 'utf-16' in enc_lower: encoding = 'utf-16'
             else: encoding = enc_lower.replace('_','-')
    if 'cc' in flags: flags.remove('cc'); flags.add('sdh');
    log.debug(f"Parse result: lang={lang_code_3b}, flags={flags}, enc={encoding}")
    return lang_code_3b, sorted(list(flags)), encoding

def _get_base_stem(file_path: Path, assoc_extensions: set) -> str:
    name = file_path.name; original_stem = file_path.stem; ext = file_path.suffix.lower()
    if ext not in assoc_extensions: return original_stem
    possible_base = original_stem; subtitle_ext = {'.srt', '.sub', '.ssa', '.ass', '.vtt'}
    if ext not in subtitle_ext: return original_stem
    suffixes_to_check = sorted( list(set(['forced', 'sdh', 'cc'] + [c for codes in ['eng', 'en', 'fre', 'fr', 'ger', 'de', 'spa', 'es', 'ita', 'it', 'jpn', 'jp', 'kor', 'ko', 'chi', 'zh', 'rus', 'ru', 'nld', 'nl', 'swe', 'sv', 'nor', 'no', 'dan', 'da', 'fin', 'fi', 'cze', 'ces', 'cs', 'pob', 'por'] for c in codes.split()])), key=len, reverse=True)
    temp_base = possible_base; removed_suffix = False; max_suffix_parts = 3
    for _ in range(max_suffix_parts):
        found_this_pass = False; current_base_lower = temp_base.lower()
        for suffix in suffixes_to_check:
            pattern_dot = r"\." + re.escape(suffix) + r"$"; pattern_under = r"_" + re.escape(suffix) + r"$"; pattern_hyphen = r"-" + re.escape(suffix) + r"$"
            if re.search(pattern_dot, temp_base, re.IGNORECASE): match = re.search(pattern_dot, temp_base, re.IGNORECASE); temp_base = temp_base[:match.start()]; log.debug(f"Stripped dot suffix '{suffix}' -> '{temp_base}'"); removed_suffix = True; found_this_pass = True; break
            elif re.search(pattern_under, temp_base, re.IGNORECASE): match = re.search(pattern_under, temp_base, re.IGNORECASE); temp_base = temp_base[:match.start()]; log.debug(f"Stripped under suffix '{suffix}' -> '{temp_base}'"); removed_suffix = True; found_this_pass = True; break
            elif re.search(pattern_hyphen, temp_base, re.IGNORECASE): match = re.search(pattern_hyphen, temp_base, re.IGNORECASE); temp_base = temp_base[:match.start()]; log.debug(f"Stripped hyphen suffix '{suffix}' -> '{temp_base}'"); removed_suffix = True; found_this_pass = True; break
        if not found_this_pass: break
    if removed_suffix and temp_base: log.debug(f"Adjusted stem for grouping '{name}' from '{original_stem}' to '{temp_base}'"); return temp_base
    else:
        if not removed_suffix: log.debug(f"No suffix stripped for '{name}', using original stem '{original_stem}'")
        else: log.debug(f"Suffix stripping resulted in empty base for '{name}', using original stem '{original_stem}'")
        return original_stem

# --- NEW: Function to Extract Stream Info ---
@lru_cache(maxsize=256) # Cache results for the same file path
def extract_stream_info(file_path: Path) -> Dict[str, Optional[str]]:
    """
    Extracts resolution, video codec, audio codec, and channels using pymediainfo.

    Args:
        file_path: Path object for the media file.

    Returns:
        A dictionary containing extracted info (keys: 'resolution', 'vcodec',
        'acodec', 'achannels'), or an empty dict if info cannot be extracted
        or pymediainfo is unavailable. Values can be None if specific info
        is missing in the file.
    """
    results = {
        'resolution': None,
        'vcodec': None,
        'acodec': None,
        'achannels': None
    }
    # --- FIX: Check the module-level flag HERE ---
    if not PYMEDIAINFO_AVAILABLE:
        # Log only once or less frequently if needed
        # log.debug("pymediainfo library not available, skipping stream info extraction.")
        return results # Return defaults if library missing

    if not file_path or not file_path.is_file():
        log.warning(f"Cannot extract stream info: File not found or not a file: {file_path}")
        return results

    try:
        log.debug(f"Parsing stream info for: {file_path.name}")
        # MediaInfoParser is now correctly imported if PYMEDIAINFO_AVAILABLE is True
        media_info = MediaInfoParser.parse(str(file_path))

        # --- Video Track ---
        video_track = next((t for t in media_info.tracks if t.track_type == 'Video'), None)
        if video_track:
            # Resolution (prefer height)
            height = getattr(video_track, 'height', None)
            if height:
                if height >= 3800: results['resolution'] = '2160p' # UHD/4K
                elif height >= 1000: results['resolution'] = '1080p'
                elif height >= 680: results['resolution'] = '720p'
                elif height >= 500: results['resolution'] = '576p' # PAL DVDish
                elif height >= 440: results['resolution'] = '480p' # NTSC DVDish
                else: results['resolution'] = 'SD'
            else: # Fallback to width if height is missing
                width = getattr(video_track, 'width', None)
                if width:
                     if width >= 3800: results['resolution'] = '2160p'
                     elif width >= 1900: results['resolution'] = '1080p'
                     elif width >= 1200: results['resolution'] = '720p'

            # Video Codec (use 'format' and simplify)
            vformat = getattr(video_track, 'format', None)
            if vformat:
                vformat = vformat.lower()
                if 'avc' in vformat or 'h264' in vformat: results['vcodec'] = 'h264'
                elif 'hevc' in vformat or 'h265' in vformat: results['vcodec'] = 'h265'
                elif 'vp9' in vformat: results['vcodec'] = 'vp9'
                elif 'av1' in vformat: results['vcodec'] = 'av1'
                elif 'mpeg-4 visual' in vformat or 'xvid' in vformat: results['vcodec'] = 'xvid'
                elif 'mpeg video' in vformat: # MPEG-1/2
                    version = getattr(video_track, 'format_version', '')
                    if 'version 2' in version.lower(): results['vcodec'] = 'mpeg2'
                    else: results['vcodec'] = 'mpeg1'
                else: results['vcodec'] = vformat.split('/')[0].strip() # Best guess

        # --- Audio Track (use first found for simplicity) ---
        audio_track = next((t for t in media_info.tracks if t.track_type == 'Audio'), None)
        if audio_track:
            # Audio Codec (use 'format')
            aformat = getattr(audio_track, 'format', None)
            if aformat:
                aformat = aformat.lower()
                # Common mappings
                if 'aac' in aformat: results['acodec'] = 'aac'
                elif 'ac-3' in aformat: results['acodec'] = 'ac3'
                elif 'e-ac-3' in aformat: results['acodec'] = 'eac3' # Dolby Digital Plus
                elif 'dts' in aformat: results['acodec'] = 'dts' # Could be dts-hd ma etc., simplify for now
                elif 'truehd' in aformat: results['acodec'] = 'truehd'
                elif 'opus' in aformat: results['acodec'] = 'opus'
                elif 'vorbis' in aformat: results['acodec'] = 'vorbis'
                elif 'flac' in aformat: results['acodec'] = 'flac'
                elif 'mp3' in aformat or 'mpeg audio' in aformat: results['acodec'] = 'mp3'
                elif 'pcm' in aformat: results['acodec'] = 'pcm'
                else: results['acodec'] = aformat.split('/')[0].strip() # Best guess

            # Audio Channels (use 'channel_s')
            channels = getattr(audio_track, 'channel_s', None)
            if channels:
                try:
                    num_channels = int(channels)
                    if num_channels >= 8: results['achannels'] = '7.1' # Or more
                    elif num_channels >= 6: results['achannels'] = '5.1' # Common 6ch layout
                    elif num_channels == 2: results['achannels'] = '2.0'
                    elif num_channels == 1: results['achannels'] = '1.0'
                    else: results['achannels'] = f"{num_channels}.0" # Handle other cases like 4.0
                except (ValueError, TypeError):
                     log.warning(f"Could not parse audio channels '{channels}' for {file_path.name}")

    except ImportError: # Should be caught by initial flag, but defensive check
        log.error("pymediainfo import failed during stream info extraction attempt (should not happen).")
        return results
    except Exception as e:
        log.error(f"Error parsing media info for '{file_path.name}': {e}", exc_info=True)
        # Return default dict on error

    log.debug(f"Extracted stream info for {file_path.name}: {results}")
    return results

# --- Scan Function ---

# --- ENHANCEMENT: Added Low Memory Scan Strategy ---
def scan_media_files(target_dir: Path, cfg_helper) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """
    Scans the target directory for media files and groups them by base stem.
    Yields batches one by one.

    Args:
        target_dir: The directory Path object to scan.
        cfg_helper: The ConfigHelper instance for accessing settings.

    Yields:
        Tuples of (base_stem, batch_dict), where batch_dict contains
        'video' (Path or None) and 'associated' (List[Path]).

    Strategies:
        - 'memory': Faster for moderate directories, loads stems and paths into memory.
        - 'low_memory': Slower (due to DB IO), uses a temporary SQLite DB
                       to handle extremely large directories with lower RAM usage.
    """
    scan_strategy = cfg_helper('scan_strategy', 'memory')
    log.info(f"Scanning directory: {target_dir} (Strategy: {scan_strategy})")

    # --- Common Config ---
    allowed_video_ext = set(cfg_helper.get_list('video_extensions', default_value=[]))
    allowed_assoc_ext = set(cfg_helper.get_list('associated_extensions', default_value=[]))
    all_allowed_ext = allowed_video_ext.union(allowed_assoc_ext)
    is_recursive = cfg_helper('recursive', False)

    if not allowed_video_ext and not allowed_assoc_ext:
        log.warning("No video or associated extensions configured. Scan will find nothing.")
        return # Yield nothing

    # --- Strategy Branching ---
    if scan_strategy == 'low_memory':
        yield from _scan_media_files_low_memory(
            target_dir, is_recursive, all_allowed_ext, allowed_video_ext
        )
    else: # Default to 'memory' strategy
        yield from _scan_media_files_memory(
            target_dir, is_recursive, all_allowed_ext, allowed_video_ext, allowed_assoc_ext
        )
# --- End Strategy Branching ---

def _scan_media_files_memory(target_dir: Path, is_recursive: bool, all_allowed_ext: set, allowed_video_ext: set, allowed_assoc_ext: set) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """Original memory-based scanning strategy."""
    log.debug("Using 'memory' scanning strategy.")
    all_files_by_base_stem = defaultdict(list)
    processed_stems = set() # Tracks stems already yielded

    try:
        base_path = target_dir.resolve()
        if not base_path.is_dir():
            log.error(f"Target path is not a valid directory: {base_path}")
            return
        items_generator = base_path.rglob('*') if is_recursive else base_path.glob('*')
        iterator = tqdm(items_generator, desc="Scanning (memory)", unit="item", disable=not TQDM_AVAILABLE)
    except Exception as e:
        log.error(f"Error listing files in '{target_dir}' (memory scan): {e}")
        return

    # --- First Pass: Group by stem in memory ---
    file_count = 0
    for item_path in iterator:
        log.debug(f"Scanner considering item: {item_path} (Exists: {item_path.exists()}, Is File: {item_path.is_file() if item_path.exists() else 'N/A'})") # ADD THIS
        item_ext = item_path.suffix.lower()
        if not item_ext or item_ext not in all_allowed_ext or item_path.name.startswith('.'):
            log.debug(f"  -> Skipping {item_path.name} due to ext '{item_ext}' (allowed: {all_allowed_ext}) or dotfile.") # ADD THIS
            continue
        try:
            if not item_path.is_file(): continue
        except OSError as e: log.warning(f"Cannot access item {item_path}: {e}"); continue

        file_count += 1
        base_stem = _get_base_stem(item_path, all_allowed_ext)
        log.debug(f"Scan (memory) found: '{item_path.name}' -> Base Stem: '{base_stem}'")
        all_files_by_base_stem[base_stem].append(item_path)

    log.info(f"Scan (memory) phase 1 complete. Found {file_count} relevant files, {len(all_files_by_base_stem)} unique base stems.")

    # --- Second Pass: Process groups and yield ---
    log.debug("Processing grouped stems (memory)...")
    yield_count = 0
    # Sort stems for deterministic output (optional)
    sorted_stems = sorted(all_files_by_base_stem.keys())

    memory_iterator = tqdm(sorted_stems, desc="Grouping (memory)", unit="stem", disable=not TQDM_AVAILABLE) if TQDM_AVAILABLE else sorted_stems

    for base_stem in memory_iterator:
        file_list = all_files_by_base_stem[base_stem]
        video_file = None
        associated_files = []
        ambiguous = False
        for file_path in file_list:
            ext = file_path.suffix.lower()
            if ext in allowed_video_ext:
                if video_file is not None:
                     log.warning(f"Ambiguous: Multiple videos match base stem '{base_stem}'. Found '{file_path.name}' and '{video_file.name}'. Skipping this stem.")
                     ambiguous = True
                     break # Stop processing this stem
                video_file = file_path
            elif ext in allowed_assoc_ext:
                associated_files.append(file_path)

        if not ambiguous and video_file:
            # Filter associated_files to remove the video file itself (safety check)
            final_associated = [f for f in associated_files if f.resolve() != video_file.resolve()]
            log.debug(f"Yielding batch (memory) for base stem '{base_stem}'")
            yield_count +=1
            yield (base_stem, {"video": video_file, "associated": final_associated})

    log.info(f"Scan (memory) finished. Yielded {yield_count} valid batches.")

def _scan_media_files_low_memory(target_dir: Path, is_recursive: bool, all_allowed_ext: set, allowed_video_ext: set) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """Low memory scanning strategy using a temporary SQLite database."""
    log.debug("Using 'low_memory' scanning strategy.")
    db_conn = None
    db_cursor = None
    # Use NamedTemporaryFile for automatic cleanup
    with tempfile.NamedTemporaryFile(prefix="renamer_scan_", suffix=".db", delete=True) as temp_db_file:
        db_path = temp_db_file.name
        log.info(f"Using temporary database for scan: {db_path}")

        try:
            # --- Phase 1: Scan and Insert into Temp DB ---
            db_conn = sqlite3.connect(db_path, isolation_level=None) # Autocommit mode might be simpler
            db_cursor = db_conn.cursor()
            db_cursor.execute("PRAGMA journal_mode=OFF;") # Optimize for speed
            db_cursor.execute("PRAGMA synchronous=OFF;") # Optimize for speed
            db_cursor.execute("""
                CREATE TABLE files (
                    base_stem TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    is_video INTEGER NOT NULL -- 1 for video, 0 for associated
                );
            """)
            # Index after table creation
            db_cursor.execute("CREATE INDEX idx_stem ON files (base_stem);")

            log.debug("Starting directory traversal and DB insertion...")
            items_processed = 0
            items_inserted = 0
            try:
                base_path = target_dir.resolve()
                if not base_path.is_dir():
                     log.error(f"Target path is not a valid directory: {base_path}")
                     return # Stop generation
                items_generator = base_path.rglob('*') if is_recursive else base_path.glob('*')
                # Progress bar for scanning phase
                scan_iterator = tqdm(items_generator, desc="Scanning (low_mem)", unit="item", disable=not TQDM_AVAILABLE)
            except Exception as e:
                log.error(f"Error listing files in '{target_dir}' (low_mem scan): {e}")
                return # Stop generation

            for item_path in scan_iterator:
                items_processed += 1
                item_ext = item_path.suffix.lower()
                if not item_ext or item_ext not in all_allowed_ext or item_path.name.startswith('.'):
                    continue
                try:
                    if not item_path.is_file(): continue
                except OSError as e: log.warning(f"Cannot access item {item_path}: {e}"); continue

                base_stem = _get_base_stem(item_path, all_allowed_ext)
                is_video = 1 if item_ext in allowed_video_ext else 0
                try:
                    db_cursor.execute(
                        "INSERT INTO files (base_stem, file_path, is_video) VALUES (?, ?, ?)",
                        (base_stem, str(item_path.resolve()), is_video)
                    )
                    items_inserted += 1
                except sqlite3.Error as e_ins:
                    log.error(f"Failed to insert file '{item_path}' into temp DB: {e_ins}")

            log.info(f"Scan (low_mem) phase 1 complete. Processed ~{items_processed} items, inserted {items_inserted} relevant files into temp DB.")

            # --- Phase 2: Query DB, Group implicitly, and Yield Batches ---
            log.debug("Querying distinct stems from temp DB...")
            try:
                db_cursor.execute("SELECT DISTINCT base_stem FROM files ORDER BY base_stem;")
                distinct_stems = [row[0] for row in db_cursor.fetchall()]
            except sqlite3.Error as e_dist:
                 log.error(f"Failed to query distinct stems from temp DB: {e_dist}")
                 return # Stop generation

            log.info(f"Found {len(distinct_stems)} unique base stems in temp DB. Processing batches...")

            yield_count = 0
            group_iterator = tqdm(distinct_stems, desc="Grouping (low_mem)", unit="stem", disable=not TQDM_AVAILABLE) if TQDM_AVAILABLE else distinct_stems

            for base_stem in group_iterator:
                try:
                    # Fetch all files for the current stem, videos first
                    db_cursor.execute(
                        "SELECT file_path, is_video FROM files WHERE base_stem = ? ORDER BY is_video DESC;",
                        (base_stem,)
                    )
                    stem_files = db_cursor.fetchall() # Fetch all for this stem
                except sqlite3.Error as e_fetch:
                    log.error(f"Failed to fetch files for stem '{base_stem}': {e_fetch}")
                    continue # Skip to next stem

                video_file : Optional[Path] = None
                associated_files : List[Path] = []
                ambiguous = False
                for file_path_str, is_video_flag in stem_files:
                    file_path = Path(file_path_str) # Convert back to Path
                    if is_video_flag == 1:
                        if video_file is not None:
                            log.warning(f"Ambiguous (low_mem): Multiple videos match base stem '{base_stem}'. Found '{file_path.name}' and '{video_file.name}'. Skipping this stem.")
                            ambiguous = True
                            break # Stop processing this stem
                        video_file = file_path
                    else: # is associated
                        associated_files.append(file_path)

                if not ambiguous and video_file:
                    # Associated files list already excludes the video here because of the query logic
                    log.debug(f"Yielding batch (low_mem) for base stem '{base_stem}'")
                    yield_count += 1
                    yield (base_stem, {"video": video_file, "associated": associated_files})

            log.info(f"Scan (low_mem) finished. Yielded {yield_count} valid batches.")

        except Exception as e:
            log.exception(f"Error during low_memory scan: {e}")
            # Ensure cleanup happens in finally
        finally:
            log.debug("Closing temporary scan database connection...")
            if db_cursor:
                try: db_cursor.close()
                except sqlite3.Error: pass # Ignore close errors
            if db_conn:
                try: db_conn.close()
                except sqlite3.Error: pass # Ignore close errors
            # Temp file is deleted automatically by context manager when closed/scope exit

# --- END OF FILE utils.py ---