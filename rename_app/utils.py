# rename_app/utils.py

import re
import os
from pathlib import Path
from functools import lru_cache
import logging # Keep logging import for module-level logger
from collections import defaultdict
from typing import List, Tuple, Optional, Set, Dict, Any

# --- TQDM Import ---
try: from tqdm import tqdm; TQDM_AVAILABLE = True
except ImportError: TQDM_AVAILABLE = False; 
def tqdm(iterable, *args, **kwargs): yield from iterable

# --- Other Imports ---
try: import langcodes; LANGCODES_AVAILABLE = True
except ImportError: LANGCODES_AVAILABLE = False # Removed log warning here, handle later
try: import chardet; CHARDET_AVAILABLE = True
except ImportError: CHARDET_AVAILABLE = False # Removed log warning here
try: from guessit import guessit; GUESSIT_AVAILABLE = True
except ImportError: GUESSIT_AVAILABLE = False # Removed log warning here

log = logging.getLogger(__name__) # Define logger for the module

# --- Filename Utils ---

# ADD THIS FUNCTION
def sanitize_os_chars(name: str) -> str:
    """Removes only OS-prohibited characters, leaves spaces/dots/etc."""
    # Basic sanitation: remove common invalid chars for Windows/Mac/Linux
    # Replace with underscore
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Maybe collapse multiple underscores resulting from this specific replacement?
    # sanitized = re.sub(r'_+', '_', sanitized) # Optional: Decided against for now
    # Avoid completely empty names if ONLY invalid chars were passed
    return sanitized if sanitized else "_invalid_char_removal_"
# --- END ADDED FUNCTION ---

def sanitize_filename(filename: str) -> str:
    """
    Cleans a filename by removing/replacing invalid characters and handling edge cases.
    """
    # 1. Handle empty input immediately
    if not filename or filename.isspace():
        return "_invalid_name_"

    # 2. Store if original started with a dot (for hidden files)
    starts_with_dot = filename.startswith('.')

    # 3. Replace invalid characters AND whitespace with a single underscore
    sanitized = re.sub(r'[<>:"/\\|?*\s]+', '_', filename)

    # 4. Collapse multiple underscores
    sanitized = re.sub(r'_+', '_', sanitized)

    # 5. Check intermediate edge cases *after* collapsing
    if not sanitized: return "_invalid_name_" # Check if empty after initial cleaning
    if all(c == '.' for c in sanitized): return "_invalid_dots_"
    # If it consists only of underscores AFTER collapsing, return specific marker
    # --- Remove log reference from previous attempt ---
    # if all(c == '_' for c in sanitized): log.warning(...); return "_invalid_underscores_"
    if all(c == '_' for c in sanitized): return "_invalid_underscores_"

    # 6. Strip TRAILING underscores and dots repeatedly FIRST
    while sanitized and sanitized[-1] in '._':
        sanitized = sanitized[:-1]

    # 7. Strip LEADING underscores *only if not starting with preserved dot*
    #    And only if result would not become empty (i.e., has more than one char)
    if len(sanitized) > 1 and not starts_with_dot and sanitized.startswith('_'):
        sanitized = sanitized.lstrip('_')

    # 8. Final check for empty name after all stripping (e.g., if input was just '.')
    if not sanitized:
        # --- Remove log reference from previous attempt ---
        # log.warning(f"Filename '{filename}' sanitized to empty string.")
        return "_invalid_name_"

    # 9. Restore leading dot if necessary and if not already present
    if starts_with_dot and not sanitized.startswith('.'):
         sanitized = "." + sanitized

    # 10. Final check to ensure it doesn't end up as just "." or ".."
    if sanitized in ['.', '..']:
         # --- Remove log reference from previous attempt ---
         # log.warning(f"Filename '{filename}' sanitized to invalid dot-name '{sanitized}'.")
         return "_invalid_dots_" # Or perhaps _invalid_name_

    return sanitized


# --- Rest of utils.py ---
# (extract_scene_tags, detect_encoding, parse_subtitle_language, _get_base_stem, scan_media_files - unchanged)
# ... (Keep all other functions as they were in the last correct version) ...

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

def scan_media_files(target_dir: Path, cfg_helper):
    log.info(f"Scanning directory: {target_dir} (Grouping by calculated base stem)")
    file_groups = defaultdict(lambda: {"video": None, "associated": []}); skipped_stems = set()
    allowed_video_ext = set(cfg_helper.get_list('video_extensions', default_value=[])); allowed_assoc_ext = set(cfg_helper.get_list('associated_extensions', default_value=[]))
    is_recursive = cfg_helper('recursive', False);
    if not allowed_video_ext: log.warning("No video extensions configured.")
    glob_pattern = '**/*' if is_recursive else '*'
    try: base_path = target_dir.resolve(); items_generator = base_path.rglob('*') if is_recursive else base_path.glob('*'); iterator = tqdm(items_generator, desc="Scanning", unit="item", disable=not TQDM_AVAILABLE) if TQDM_AVAILABLE else items_generator
    except Exception as e: log.error(f"Error listing files in '{target_dir}': {e}"); return {}
    all_files_by_base_stem = defaultdict(list)
    for item_path in iterator:
        try:
            if not item_path.exists() or not item_path.is_file(): continue
        except OSError as e: log.warning(f"Cannot access item {item_path}: {e}"); continue
        if item_path.name.startswith('.'): continue
        base_stem = _get_base_stem(item_path, allowed_assoc_ext); log.debug(f"Scan found: '{item_path.name}' -> Base Stem: '{base_stem}'"); all_files_by_base_stem[base_stem].append(item_path)
    log.debug(f"Processing {len(all_files_by_base_stem)} unique base stems found.")
    for base_stem, file_list in all_files_by_base_stem.items():
        if base_stem in skipped_stems: continue
        video_file = None; associated_files = []
        for file_path in file_list:
            ext = file_path.suffix.lower()
            if ext in allowed_video_ext:
                if video_file is not None: log.warning(f"Ambiguous: Multiple videos match base stem '{base_stem}'. Skipping."); video_file = None; skipped_stems.add(base_stem); break
                video_file = file_path
            elif ext in allowed_assoc_ext: associated_files.append(file_path)
        if video_file and base_stem not in skipped_stems:
            final_associated = [f for f in associated_files if f != video_file]; file_groups[base_stem]['video'] = video_file; file_groups[base_stem]['associated'] = final_associated
            log.debug(f"Confirmed batch for base stem '{base_stem}' with Video='{video_file.name}', Associated={[f.name for f in final_associated]}")
        # Removed discard log
    valid_batches = { s: d for s, d in file_groups.items() if s not in skipped_stems and d.get('video') is not None }
    log.debug(f"Scan finished. Found {len(valid_batches)} valid batches.")
    return valid_batches