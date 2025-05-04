# --- START OF FILE utils.py ---

# rename_app/utils.py

import re
import os
from pathlib import Path
from functools import lru_cache
import logging # Keep logging import for module-level logger
from collections import defaultdict
# --- Add Iterator and Groupby for potential alternative ---
from typing import List, Tuple, Optional, Set, Dict, Any, Iterator
from itertools import groupby
# --- End Add ---


# --- TQDM Import ---
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


# --- START ENHANCEMENT: Added Memory Usage Comments ---
def scan_media_files(target_dir: Path, cfg_helper):
    """
    Scans the target directory for media files and groups them by base stem.

    Args:
        target_dir: The directory Path object to scan.
        cfg_helper: The ConfigHelper instance for accessing settings.

    Returns:
        A dictionary where keys are base stems and values are dictionaries
        containing 'video' (Path or None) and 'associated' (List[Path]).

    Memory Usage Considerations:
        - This function uses Path.glob() or Path.rglob(), which are generators and
          efficient for iterating over directory contents without loading all
          filenames into memory at once.
        - However, it then collects all *relevant* file paths (matching video
          or associated extensions) into the `all_files_by_base_stem`
          defaultdict before final processing.
        - For directories with a very large number of relevant files (e.g.,
          millions), the memory usage of this dictionary could become significant.
        - For such extreme cases, a more memory-efficient (but potentially slower
          and more complex) approach would involve:
            1. Iterating once to write `(base_stem, file_path)` pairs to a
               temporary file.
            2. Sorting the temporary file externally (or in chunks).
            3. Iterating through the sorted file, grouping by stem using
               `itertools.groupby`, and yielding batches one by one.
        - The current implementation is generally efficient and simpler for
          typical use cases.
    """
    log.info(f"Scanning directory: {target_dir} (Grouping by calculated base stem)")

    # Using defaultdict is efficient for building the groups
    all_files_by_base_stem = defaultdict(list)
    skipped_stems = set() # Tracks stems skipped due to ambiguity

    # Get configuration settings
    allowed_video_ext = set(cfg_helper.get_list('video_extensions', default_value=[]));
    allowed_assoc_ext = set(cfg_helper.get_list('associated_extensions', default_value=[]))
    all_allowed_ext = allowed_video_ext.union(allowed_assoc_ext) # Combine for initial check
    is_recursive = cfg_helper('recursive', False);

    if not allowed_video_ext:
        log.warning("No video extensions configured.")
        # Continue scanning for associated files potentially, but won't form batches

    # Prepare generator for directory walking
    try:
        base_path = target_dir.resolve()
        if not base_path.is_dir(): # Check if target_dir is valid after resolve
            log.error(f"Target path is not a valid directory: {base_path}")
            return {}
        items_generator = base_path.rglob('*') if is_recursive else base_path.glob('*')
        # Wrap with tqdm if available
        iterator = tqdm(items_generator, desc="Scanning", unit="item", disable=not TQDM_AVAILABLE) if TQDM_AVAILABLE else items_generator
    except Exception as e:
        log.error(f"Error listing files in '{target_dir}': {e}")
        return {}

    # --- First Pass: Iterate and group by calculated stem ---
    # This pass uses generators for listing files, but collects paths in memory dict.
    for item_path in iterator:
        item_ext = item_path.suffix.lower()
        # Skip if not a relevant extension or is a hidden file/dir
        if not item_ext or item_ext not in all_allowed_ext or item_path.name.startswith('.'):
            continue
        try:
            # Ensure it's a file before proceeding
            if not item_path.is_file():
                continue
        except OSError as e:
            log.warning(f"Cannot access item {item_path}: {e}")
            continue

        # Calculate the base stem (grouping key)
        base_stem = _get_base_stem(item_path, all_allowed_ext); # Use all extensions for stemming
        log.debug(f"Scan found: '{item_path.name}' -> Base Stem: '{base_stem}'")
        all_files_by_base_stem[base_stem].append(item_path)

    # --- Second Pass: Process grouped stems into final batches ---
    log.debug(f"Processing {len(all_files_by_base_stem)} unique base stems found.")
    file_groups = defaultdict(lambda: {"video": None, "associated": []})
    for base_stem, file_list in all_files_by_base_stem.items():
        if base_stem in skipped_stems: continue # Skip if already marked ambiguous
        video_file = None; associated_files = []
        for file_path in file_list:
            ext = file_path.suffix.lower()
            if ext in allowed_video_ext:
                if video_file is not None:
                     # Ambiguity detected: more than one video file for this stem
                     log.warning(f"Ambiguous: Multiple videos match base stem '{base_stem}'. Found '{file_path.name}' and '{video_file.name}'. Skipping this stem.")
                     # Mark stem as skipped and break inner loop
                     skipped_stems.add(base_stem)
                     video_file = None # Ensure no batch is created
                     break
                video_file = file_path
            elif ext in allowed_assoc_ext:
                # Only add if it's not the same as the potential video file
                # (though _get_base_stem should usually differentiate)
                associated_files.append(file_path)
            # Files with other extensions are already skipped

        # Check again if skipped due to ambiguity before adding to file_groups
        if video_file and base_stem not in skipped_stems:
            # Filter associated_files to remove the video file itself if it somehow got added
            final_associated = [f for f in associated_files if f.resolve() != video_file.resolve()]
            file_groups[base_stem]['video'] = video_file
            file_groups[base_stem]['associated'] = final_associated
            log.debug(f"Confirmed batch for base stem '{base_stem}' with Video='{video_file.name}', Associated={[f.name for f in final_associated]}")

    # Final result dictionary excludes skipped stems
    valid_batches = { s: d for s, d in file_groups.items() if s not in skipped_stems and d.get('video') is not None }
    log.debug(f"Scan finished. Found {len(valid_batches)} valid batches.")
    return valid_batches
# --- END ENHANCEMENT ---

# --- END OF FILE utils.py ---