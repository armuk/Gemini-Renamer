# rename_app/renamer_engine.py

import logging
import re
import os
import uuid
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Any, Set, Tuple

from .models import MediaInfo, MediaMetadata, RenamePlan, RenameAction
from .utils import (
    sanitize_filename, parse_subtitle_language, extract_scene_tags,
    sanitize_os_chars, LANGCODES_AVAILABLE, extract_stream_info # Import LANGCODES_AVAILABLE too, Added extract_stream_info
)
from .exceptions import RenamerError

try: from guessit import guessit; GUESSIT_AVAILABLE = True
except ImportError: GUESSIT_AVAILABLE = False

log = logging.getLogger(__name__)

# Regex for multi-episode range patterns (e.g., S01E01-E02, S01E01-02, S01E01.E02)
# Requires 2+ digits for episode numbers and specific separators.
# Uses a negative lookahead (?![\d]) to prevent matching if followed immediately by another digit.
MULTI_EP_RANGE_PATTERN = re.compile(
    r'[Ss](?P<snum>\d+)'          # Capture season number (e.g., S01)
    r'[\s._-]*[Ee]'              # Optional separator before E
    r'(?P<ep1>\d{2,})'           # Capture first episode number (2+ digits)
    r'(?:'                       # Non-capturing group for the separator and second episode
       r'\s*[-]\s*[Ee]?'         # Hyphen separator, optional E (e.g., -E02 or -02)
       r'|'                      # OR
       r'\s*[._]\s*[Ee]'         # Dot or Underscore separator, requires E (e.g., .E02)
    r')'
    r'(?P<ep2>\d{2,})'           # Capture second episode number (2+ digits)
    r'(?![\d])',                 # Must NOT be followed by another digit (prevents S01E01E02E03)
    re.IGNORECASE
)

# Regex for consecutive multi-episode patterns (e.g., S01E01E02)
# Requires 2+ digits for *each* episode number part.
MULTI_EP_CONSECUTIVE_PATTERN = re.compile(
    r'[Ss](?P<snum>\d+)'         # Capture season number (e.g., S01)
    r'[Ee](?P<ep1>\d{2,})'        # Capture first episode number (2+ digits)
    r'[Ee](?P<ep2>\d{2,})'        # Capture second episode number (2+ digits)
    r'(?![\d])',                 # Must NOT be followed by another digit
    re.IGNORECASE
)
# (sanitize_os_chars should be defined elsewhere or imported if needed globally)
# ...

class RenamerEngine:
    # (__init__, parse_filename, _determine_file_type remain the same)
    def __init__(self, cfg_helper): self.cfg = cfg_helper

    def parse_filename(self, file_path: Path) -> Dict:
        if not GUESSIT_AVAILABLE: log.error("Guessit library not available."); return {}
        try: guess = guessit(str(file_path)); log.debug(f"Guessit: {guess}"); return guess
        except Exception as e: log.error(f"Guessit failed: {e}"); return {} # Return empty dict on failure

    def _determine_file_type(self, guess_info: Dict) -> str:
        if not isinstance(guess_info, dict):
            log.warning(f"Cannot determine file type, guess_info is not a dictionary: {type(guess_info)}")
            return 'unknown'
        file_type = guess_info.get('type');
        if file_type == 'episode': return 'series'
        if file_type == 'movie': return 'movie'
        # Fallback checks based on common keys
        if 'season' in guess_info and ('episode' in guess_info or 'episode_list' in guess_info): return 'series'
        if guess_info.get('year') and 'title' in guess_info and file_type != 'episode': return 'movie' # Avoid classifying series as movies just based on year/title
        log.debug(f"Could not determine type from guess: {guess_info}"); return 'unknown'

    def _prepare_format_data(self, media_info: MediaInfo) -> Dict[str, Any]:
        if not isinstance(media_info.guess_info, dict):
            log.error(f"Cannot prepare format data for '{media_info.original_path.name}'. "
                      f"Expected guess_info to be a dict, but got {type(media_info.guess_info)}. "
                      f"Falling back to minimal data.")
            # Return a minimal dictionary with essential fallback data
            return {
                'ext': media_info.original_path.suffix,
                'original_filename': media_info.original_path.name,
                'original_stem': media_info.original_path.stem,
                'title': media_info.original_path.stem, # Fallback title
                'show_title': media_info.original_path.stem,
                'movie_title': media_info.original_path.stem,
                'season': 0,
                'episode': 0,
                'episode_list': [],
                'episode_range': '', # Add default
                'year': None,
                'movie_year': None,
                'show_year': None,
                'episode_title': 'Unknown Episode',
                'scene_tags': [],
                'scene_tags_dot': '',
                'resolution': '',
                'vcodec': '',
                'acodec': '',
                'achannels': '',
                'collection': '',
                'air_date': '',
                'release_date': '',
                'ids': {},
                'source_api': '',
            }

        data = media_info.guess_info.copy()
        metadata = media_info.metadata
        original_path = media_info.original_path

        # --- Basic file info ---
        data['ext'] = original_path.suffix
        data['original_filename'] = original_path.name
        data['original_stem'] = original_path.stem

        # --- Scene Tags ---
        tags_to_preserve = self.cfg.get_list('scene_tags_to_preserve', [])
        scene_tags_list, scene_tags_dot = [], ""
        if tags_to_preserve and data.get('original_filename'):
             scene_tags_list, scene_tags_dot = extract_scene_tags(data['original_filename'], tuple(tags_to_preserve))
        data['scene_tags'] = scene_tags_list
        data['scene_tags_dot'] = scene_tags_dot

        # --- Prepare Episode Info ---
        final_episode_list: List[int] = []
        guess_ep_data = None

        # 1. Prioritize guessit keys ('episode_list', 'episode', 'episode_number')
        if isinstance(media_info.guess_info.get('episode_list'), list):
            guess_ep_data = media_info.guess_info['episode_list']
            log.debug("Using guessit 'episode_list' for initial episode data.")
        elif 'episode' in media_info.guess_info:
            guess_ep_data = media_info.guess_info['episode']
            log.debug("Using guessit 'episode' for initial episode data.")
        elif 'episode_number' in media_info.guess_info:
            guess_ep_data = media_info.guess_info['episode_number']
            log.debug("Using guessit 'episode_number' for initial episode data.")

        # 2. Process data found from guessit
        if guess_ep_data is not None:
            ep_data_list = guess_ep_data if isinstance(guess_ep_data, list) else [guess_ep_data]
            for ep in ep_data_list:
                try:
                    ep_int = int(str(ep))  # Convert to string first for safety
                    if ep_int > 0:         # Only add positive episode numbers
                        final_episode_list.append(ep_int)
                except (ValueError, TypeError):
                    log.warning(f"Could not parse episode '{ep}' from guessit data for '{original_path.name}'.")
            final_episode_list = sorted(list(set(final_episode_list))) # Unique & sorted

        # 3. Attempt Regex Fallback ONLY if guessit didn't yield multiple episodes
        if len(final_episode_list) <= 1:
            log.debug(f"Guessit episode list singular or empty ({final_episode_list}), attempting regex detection...")
            stem = data['original_stem'] # Use the original stem for regex matching
            regex_ep_list: Optional[List[int]] = None

            range_match = MULTI_EP_RANGE_PATTERN.search(stem)
            if range_match:
                try:
                    ep1 = int(range_match.group('ep1'))
                    ep2 = int(range_match.group('ep2'))
                    if ep1 < ep2: # Basic sanity check
                        regex_ep_list = list(range(ep1, ep2 + 1))
                        log.info(f"Detected episode range via RANGE regex ({ep1}-{ep2}) for '{original_path.name}'")
                    else:
                         log.warning(f"Regex range detection skipped for '{original_path.name}', end episode not greater than start ({ep1} vs {ep2}).")
                except (ValueError, TypeError, IndexError) as e:
                    log.warning(f"Error parsing regex RANGE match for '{original_path.name}': {e}")

            # Only try consecutive pattern if range didn't match
            if regex_ep_list is None:
                consecutive_match = MULTI_EP_CONSECUTIVE_PATTERN.search(stem)
                if consecutive_match:
                    try:
                        ep1 = int(consecutive_match.group('ep1'))
                        ep2 = int(consecutive_match.group('ep2'))
                        # Allow if consecutive or very small gap (e.g., S01E01E03 might be intended)
                        # Let's stick to strictly consecutive for now for less ambiguity
                        if ep1 + 1 == ep2:
                            regex_ep_list = [ep1, ep2]
                            log.info(f"Detected consecutive episodes via CONSECUTIVE regex ({ep1}, {ep2}) for '{original_path.name}'")
                        else:
                             log.warning(f"Regex consecutive match '{consecutive_match.group(0)}' doesn't look sequential ({ep1} -> {ep2}). Ignoring.")
                    except (ValueError, TypeError, IndexError) as e:
                        log.warning(f"Error parsing regex CONSECUTIVE match for '{original_path.name}': {e}")

            # If regex found a valid list, OVERWRITE the final_episode_list
            if regex_ep_list:
                 log.debug(f"Overwriting guessit episode list with regex result: {regex_ep_list}")
                 final_episode_list = regex_ep_list
            else:
                 log.debug("No valid multi-episode pattern found via regex.")

        # --- Finalize Episode Data for Formatting ---
        # Ensure unique and sorted integers, even if single episode
        final_episode_list = sorted(list(set(final_episode_list)))
        data['episode_list'] = final_episode_list
        # Use 0 as default if list is empty, otherwise take the first element
        data['episode'] = final_episode_list[0] if final_episode_list else 0
        data['episode_range'] = '' # Default empty
        if len(final_episode_list) > 1:
            # Pad with 2 digits minimum
            data['episode_range'] = f"E{final_episode_list[0]:0>2}-E{final_episode_list[-1]:0>2}"
            log.debug(f"Setting episode range format data: {data['episode_range']}")

        # # --- Initial Episode Info from Guessit ---
        # # Prioritize 'episode_list' if guessit found multiple
        # initial_episode_list: List[int] = []
        # if isinstance(data.get('episode_list'), list) and len(data['episode_list']) > 1:
        #     try:
        #         initial_episode_list = [int(ep) for ep in data['episode_list'] if isinstance(ep, (int, str)) and str(ep).isdigit()]
        #         log.debug(f"Using multi-episode list from guessit: {initial_episode_list}")
        #     except (ValueError, TypeError):
        #         log.warning("Could not parse episode_list from guessit.")
        #         initial_episode_list = []
        # # Fallback to single 'episode' or 'episode_number'
        # if not initial_episode_list:
        #     ep_num = data.get('episode') or data.get('episode_number')
        #     if ep_num is not None:
        #         try:
        #             initial_episode_list = [int(ep_num)]
        #         except (ValueError, TypeError):
        #             log.warning(f"Could not parse single episode number: {ep_num}")

        # # --- Enhanced Multi-Episode Detection (if needed) ---
        # final_episode_list = initial_episode_list
        # if len(final_episode_list) <= 1: # Only attempt regex if guessit didn't give a clear multi-ep list
        #     log.debug(f"Guessit episode list is singular or empty ({final_episode_list}), attempting regex detection...")
        #     stem = data['original_stem']
        #     range_match = MULTI_EP_RANGE_PATTERN.search(stem)
        #     consecutive_match = MULTI_EP_CONSECUTIVE_PATTERN.search(stem)

        #     if range_match:
        #         try:
        #             ep1 = int(range_match.group('ep1'))
        #             ep2_str = range_match.group('ep2') # Can have multiple captures, use the last one
        #             if ep2_str: # Ensure ep2 was captured
        #                  ep2 = int(ep2_str)
        #                  if ep1 < ep2: # Basic sanity check
        #                      final_episode_list = list(range(ep1, ep2 + 1))
        #                      log.info(f"Detected episode range via regex ({ep1}-{ep2}) for '{original_path.name}'")
        #                  else:
        #                       log.warning(f"Regex range detection skipped for '{original_path.name}', end episode not greater than start ({ep1} vs {ep2}).")
        #             else:
        #                  log.warning(f"Regex range detection failed for '{original_path.name}', could not extract end episode.")
        #         except (ValueError, TypeError, IndexError) as e:
        #             log.warning(f"Error parsing regex episode range match for '{original_path.name}': {e}")
        #     elif consecutive_match:
        #          try:
        #             ep1 = int(consecutive_match.group('ep1'))
        #             ep2 = int(consecutive_match.group('ep2'))
        #             # Basic assumption: E01E02 means episodes 1 and 2
        #             if ep1 + 1 == ep2:
        #                 final_episode_list = [ep1, ep2]
        #                 log.info(f"Detected consecutive episodes via regex ({ep1}, {ep2}) for '{original_path.name}'")
        #             else:
        #                 log.warning(f"Regex consecutive match '{consecutive_match.group(0)}' doesn't look sequential. Sticking to single ep: {ep1}")
        #                 final_episode_list = [ep1] # Revert to just the first if not sequential
        #          except (ValueError, TypeError, IndexError) as e:
        #              log.warning(f"Error parsing regex consecutive episode match for '{original_path.name}': {e}")


        # # --- Finalize Episode Data for Formatting ---
        # final_episode_list = sorted(list(set(final_episode_list))) # Ensure unique and sorted integers
        # data['episode_list'] = final_episode_list
        # data['episode'] = final_episode_list[0] if final_episode_list else data.get('season', 0) # Use season if no ep? Or just 0? Let's use 0.
        # data['episode'] = final_episode_list[0] if final_episode_list else 0 # Use 0 if list is empty
        # data['episode_range'] = '' # Default empty
        # if len(final_episode_list) > 1:
        #     data['episode_range'] = f"E{final_episode_list[0]:0>2}-E{final_episode_list[-1]:0>2}"
        #     log.debug(f"Setting episode range format data: {data['episode_range']}")


        # --- Metadata Integration (incorporating the final_episode_list) ---
        # Add defaults first
        data.setdefault('collection', '')
        data.setdefault('source_api', '')
        data.setdefault('ids', {})
        data.setdefault('air_date', '') # Default air_date
        data.setdefault('release_date', '') # Default release_date

        if metadata:
            data['source_api'] = metadata.source_api or ''
            data['ids'] = metadata.ids or {} # Ensure ids is a dict

            if metadata.is_movie:
                # Movie specific metadata overwrite/population
                movie_title_raw = metadata.movie_title if metadata.movie_title else data.get('title', 'Unknown_Movie')
                data['movie_title'] = sanitize_os_chars(movie_title_raw) if movie_title_raw else 'Unknown_Movie'
                data['movie_year'] = metadata.movie_year or data.get('year')
                data['release_date'] = metadata.release_date
                data['collection'] = metadata.collection_name or ''

            elif metadata.is_series:
                # Series specific metadata overwrite/population
                show_title_raw = metadata.show_title if metadata.show_title else data.get('title', 'Unknown_Show')
                data['show_title'] = sanitize_os_chars(show_title_raw) if show_title_raw else 'Unknown_Show'
                data['show_year'] = metadata.show_year
                data['season'] = metadata.season if metadata.season is not None else data.get('season', 0) # Ensure season is int

                # Use the potentially updated final_episode_list here
                ep_list_for_titles = data['episode_list'] # Already sorted and unique

                # Generate episode title(s) based on the final list
                if len(ep_list_for_titles) > 1:
                    titles_raw = [metadata.episode_titles.get(ep, f'Ep_{ep}') for ep in ep_list_for_titles]
                    titles = [sanitize_os_chars(t) if t else f'Ep_{ep}' for ep, t in zip(ep_list_for_titles, titles_raw)]
                    specific_titles = [t for t in titles if not t.startswith("Ep_")]
                    # Join specific titles if available, otherwise just use the first title found (raw or generated)
                    data['episode_title'] = " & ".join(specific_titles) if specific_titles else (titles[0] if titles else 'Multiple Episodes')
                elif ep_list_for_titles:
                    first_ep = ep_list_for_titles[0]
                    ep_title_meta = metadata.episode_titles.get(first_ep)
                    data['episode_title'] = sanitize_os_chars(ep_title_meta) if ep_title_meta else data.get('episode_title', f"Episode_{first_ep}")
                else: # Handle case where list is empty
                    data['episode_title'] = data.get('episode_title', 'Unknown Episode')

                # Get air date for the first episode in the list
                first_ep_num = ep_list_for_titles[0] if ep_list_for_titles else None
                if first_ep_num is not None:
                    data['air_date'] = metadata.air_dates.get(first_ep_num, data.get('date'))


        # --- Fallbacks for non-metadata cases ---
        if 'show_title' not in data:
            show_title_guess = data.get('title', 'Unknown Show')
            data['show_title'] = sanitize_os_chars(show_title_guess) if show_title_guess else 'Unknown Show'
        if 'movie_title' not in data:
            movie_title_guess = data.get('title', 'Unknown Movie')
            data['movie_title'] = sanitize_os_chars(movie_title_guess) if movie_title_guess else 'Unknown Movie'
        if 'episode_title' not in data:
            # Ensure fallback uses the finalized first episode number
            ep_num_fallback = data['episode']
            ep_title_guess = data.get('episode_title', f"Episode_{ep_num_fallback}")
            data['episode_title'] = sanitize_os_chars(ep_title_guess) if ep_title_guess else f"Episode_{ep_num_fallback}"

        # Ensure essential numeric/year keys have fallbacks AFTER metadata processing
        data.setdefault('season', 0)
        data.setdefault('movie_year', data.get('year'))
        data.setdefault('show_year', data.get('year'))

        # --- Selective Stream Info Extraction ---
        stream_info_enabled = self.cfg('extract_stream_info', False)
        should_extract_streams = False

        if stream_info_enabled:
            log.debug("Stream info extraction enabled by config. Checking format strings...")
            stream_placeholders = {"{resolution}", "{vcodec}", "{acodec}", "{achannels}"}
            relevant_formats = []
            if media_info.file_type == 'series':
                 relevant_formats.extend([
                     self.cfg('series_format'),
                     self.cfg('series_format_specials'),
                     self.cfg('folder_format_series'),
                     self.cfg('folder_format_specials')
                 ])
            elif media_info.file_type == 'movie':
                 relevant_formats.extend([
                     self.cfg('movie_format'),
                     self.cfg('folder_format_movie')
                 ])
            relevant_formats.append(self.cfg('subtitle_format'))

            formats_to_check = [f for f in relevant_formats if f]

            for fmt in formats_to_check:
                if any(ph in fmt for ph in stream_placeholders):
                    should_extract_streams = True
                    log.debug(f"Found stream placeholder in format: '{fmt}'. Extraction needed for '{original_path.name}'.")
                    break

            if not should_extract_streams:
                log.debug(f"No stream info placeholders found in relevant format strings for '{original_path.name}'. Skipping extraction.")
        else:
             log.debug("Stream info extraction disabled by config.")


        data['resolution'] = ''
        data['vcodec'] = ''
        data['acodec'] = ''
        data['achannels'] = ''

        if should_extract_streams:
            log.debug(f"Proceeding with stream info extraction for {original_path.name}")
            try:
                stream_info = extract_stream_info(original_path)
                if stream_info:
                    for key, value in stream_info.items():
                        if value:
                            data[key] = value
            except Exception as e_stream:
                log.error(f"Failed to extract stream info for {original_path.name}: {e_stream}")

        # Add remaining guessit keys if not already set (safer at the end)
        for gk, gv in media_info.guess_info.items():
            data.setdefault(gk, gv)

        log.debug(f"Prepared format data: {data}")
        return data

    def _format_new_name(self, media_info: MediaInfo, format_data: Dict) -> str:
        mode = media_info.file_type
        format_str = None
        fallback_format = "{original_stem}_renamed".replace("{ext}", "")

        if mode == 'series':
            is_special = format_data.get('season') == 0
            # --- START Special Handling ---
            if is_special:
                format_str_specials = self.cfg('series_format_specials', default_value=None)
                if format_str_specials:
                    log.debug("Using 'series_format_specials' for S00 episode.")
                    format_str = format_str_specials.replace("{ext}", "")
            # --- END Special Handling ---
            # Fallback to regular series format if not special or special format not defined
            if not format_str:
                format_str = self.cfg('series_format', default_value='').replace("{ext}", "")
                if not format_str:
                    log.warning("Missing format 'series_format'. Using fallback: '{fallback_format}'")
                    format_str = fallback_format
        elif mode == 'movie':
            format_str = self.cfg('movie_format', default_value='').replace("{ext}", "")
            if not format_str:
                log.warning("Missing format 'movie_format'. Using fallback: '{fallback_format}'")
                format_str = fallback_format
        else: # Should not happen if plan_rename checks type
            log.error(f"Unexpected file type '{mode}' in _format_new_name. Using fallback.")
            format_str = fallback_format

        try:
            # Use defaultdict to avoid KeyErrors for missing placeholders
            new_stem = format_str.format(**defaultdict(str, format_data))
        except Exception as e:
            raise RenamerError(f"Failed formatting stem: {e}. Format='{format_str}', DataKeys={list(format_data.keys())}") from e

        scene_tags_dot = format_data.get('scene_tags_dot', '')
        if self.cfg('scene_tags_in_filename', True) and scene_tags_dot and scene_tags_dot not in new_stem:
            new_stem += scene_tags_dot

        # Apply filename sanitization, not just OS chars
        final_stem = sanitize_filename(new_stem + format_data.get('ext', '')) # Sanitize with extension
        final_stem, _ = os.path.splitext(final_stem) # Remove extension again

        if not final_stem:
            log.warning(f"Stem empty after OS sanitation for '{media_info.original_path.name}'. Using original.")
            # Sanitize the original stem as a last resort
            return sanitize_filename(media_info.original_path.stem + format_data.get('ext', ''))
        return final_stem

    def _format_folder_path(self, media_info: MediaInfo, format_data: Dict) -> Optional[Path]:
        if not self.cfg('create_folders'):
            return None

        mode = media_info.file_type
        folder_format = None

        if mode == 'series':
            is_special = format_data.get('season') == 0
            # --- START Special Handling ---
            if is_special:
                folder_format_specials = self.cfg('folder_format_specials', default_value=None)
                if folder_format_specials:
                    log.debug("Using 'folder_format_specials' for S00 episode folder.")
                    folder_format = folder_format_specials
            # --- END Special Handling ---
            # Fallback to regular series format if not special or special format not defined
            if not folder_format:
                folder_format = self.cfg('folder_format_series')
        elif mode == 'movie':
            folder_format = self.cfg('folder_format_movie')

        if not folder_format:
            log.debug(f"No applicable folder format found for type '{mode}' (Season: {format_data.get('season')}).")
            return None

        try:
            relative_str = folder_format.format(**defaultdict(str, format_data))
            # Sanitize each part of the path using the OS-specific sanitizer
            sanitized_parts = [sanitize_os_chars(part) for part in Path(relative_str).parts if part and part != '.']
            if not sanitized_parts:
                log.warning(f"Folder path resulted in empty components after sanitation. Format: '{folder_format}'")
                return None
            return Path(*sanitized_parts)
        except Exception as e:
            log.error(f"Failed formatting folder: {e}. Format='{folder_format}', DataKeys={list(format_data.keys())}")
            return None

    def _format_associated_name(self, assoc_path: Path, new_video_stem: str, format_data: Dict) -> str:
        original_extension = assoc_path.suffix
        # Make subtitle check case-insensitive
        subtitle_extensions = {ext.lower() for ext in self.cfg.get_list('subtitle_extensions', default_value=['.srt', '.sub'])}
        log.debug(f"Formatting associated file: '{assoc_path.name}' with video stem: '{new_video_stem}'")
        log.debug(f"Checking extension '{original_extension.lower()}' against subtitle types: {subtitle_extensions}")

        if original_extension.lower() in subtitle_extensions:
            detect_enc = self.cfg('subtitle_encoding_detection', True)
            lang_code, flags, encoding = (parse_subtitle_language(assoc_path.name, detect_enc, assoc_path)
                                           if LANGCODES_AVAILABLE
                                           else (None, [], None)) # Fallback if langcodes missing
            sub_format = self.cfg('subtitle_format', default_value="{stem}{lang_dot}{flags_dot}").replace("{ext}", "")
            log.debug(f"Subtitle detected. Lang='{lang_code}', Flags={flags}, Format='{sub_format}'")

            sub_data = {
                'stem': new_video_stem,
                'lang_code': lang_code or '', 'lang_dot': f".{lang_code}" if lang_code else '',
                'flags': "".join(flags), 'flags_dot': "".join(f".{f}" for f in flags),
                'encoding': encoding or '', 'encoding_dot': f".{encoding}" if encoding else '',
                'scene_tags': format_data.get('scene_tags', []), 'scene_tags_dot': format_data.get('scene_tags_dot', '')
            }
            try:
                # Use defaultdict to avoid KeyErrors
                new_name_unclean = sub_format.format(**defaultdict(str, sub_data)); log.debug(f"Formatted subtitle stem (unclean): '{new_name_unclean}'"); new_name_cleaned = re.sub(r'\.+', '.', new_name_unclean).strip('._ '); log.debug(f"Formatted subtitle stem (cleaned): '{new_name_cleaned}'")
                new_name = f"{new_name_cleaned}{original_extension}"
                if not new_name_cleaned or new_name == original_extension: fallback_name = f"{new_video_stem}{sub_data['lang_dot']}{sub_data['flags_dot']}{original_extension}".replace('..','.'); log.warning(f"Formatted subtitle name was empty or unchanged ('{new_name}'), using fallback: '{fallback_name}'"); new_name = fallback_name
            except Exception as e: log.error(f"Failed formatting subtitle '{assoc_path.name}'. Format: '{sub_format}'. Error: {e}. Falling back."); fallback_name = f"{new_video_stem}{('.' + lang_code) if lang_code else ''}{''.join('.'+f for f in flags)}{original_extension}".replace('..','.'); new_name = fallback_name
        else:
            new_name = f"{new_video_stem}{original_extension}"; log.debug(f"Non-subtitle associated file, using simple name: '{new_name}'")

        # Apply final sanitization
        final_os_sanitized_name = sanitize_filename(new_name); log.debug(f"Final OS sanitized associated name: '{final_os_sanitized_name}'"); return final_os_sanitized_name


    def plan_rename(self, video_path: Path, associated_paths: List[Path], media_info: MediaInfo) -> RenamePlan:
        batch_id_suffix = uuid.uuid4().hex[:6]
        plan = RenamePlan(batch_id=f"plan-{video_path.stem[:10]}-{batch_id_suffix}", video_file=video_path)
        try:
            # Ensure base_target_dir comes from the actual args used for the run if available
            base_target_dir = self.cfg.args.directory.resolve() if hasattr(self.cfg, 'args') and hasattr(self.cfg.args, 'directory') else video_path.parent.resolve()
            original_video_path_resolved = video_path.resolve()
        except AttributeError: log.warning("Base directory not found in args, using video parent."); base_target_dir = video_path.parent.resolve(); original_video_path_resolved = video_path.resolve()
        except Exception as e: plan.status='failed'; plan.message=f"Cannot resolve base directory: {e}"; return plan

        log.debug(f"--- Planning Start: {video_path.name} ---"); log.debug(f"Original Video Path Resolved: {original_video_path_resolved}")

        try:
            # 1. Prepare Data & Type
            if not media_info.file_type or media_info.file_type == 'unknown': media_info.file_type = self._determine_file_type(media_info.guess_info)
            if media_info.file_type == 'unknown': plan.status = 'skipped'; plan.message = "Could not determine file type."; return plan
            # --- Prepare data ONCE ---
            format_data = self._prepare_format_data(media_info)
            media_info.data = format_data # Store prepared data back in media_info if needed elsewhere
            log.debug(f"Data for formatting in plan_rename: {format_data}") # Debug data used

            # 2. Format Video Name & Folder (Uses updated methods)
            new_video_stem = self._format_new_name(media_info, format_data);
            relative_folder = self._format_folder_path(media_info, format_data)
            target_dir = base_target_dir / relative_folder if relative_folder else original_video_path_resolved.parent
            plan.created_dir_path = target_dir.resolve() if relative_folder and target_dir.resolve() != original_video_path_resolved.parent.resolve() else None # Set created_dir only if truly different and exists/will exist
            new_video_filename = f"{new_video_stem}{video_path.suffix}"; final_video_path = target_dir / new_video_filename
            final_video_path_resolved = final_video_path.resolve(); log.debug(f"Calculated Final Video Path Resolved: {final_video_path_resolved}")

            # 3. Format Associated Files & Build Planned Actions
            planned_actions_dict: Dict[Path, RenameAction] = {}

            # Compare lowercased strings for case-insensitivity
            vid_paths_differ = str(final_video_path_resolved).lower() != str(original_video_path_resolved).lower()
            log.debug(f"Plan Check Video (case-insensitive): Differ={vid_paths_differ} ('{original_video_path_resolved}' vs '{final_video_path_resolved}')")
            if vid_paths_differ:
                 log.debug(f"   Adding video action: {video_path.name} -> {final_video_path.name}")
                 planned_actions_dict[original_video_path_resolved] = RenameAction(original_path=video_path, new_path=final_video_path, action_type='move' if target_dir.resolve() != original_video_path_resolved.parent.resolve() else 'rename')

            for assoc_path in associated_paths:
                 original_assoc_path_resolved = assoc_path.resolve(); log.debug(f"   Processing Assoc: {assoc_path.name}"); log.debug(f"   Original Assoc Path Resolved: {original_assoc_path_resolved}")
                 # --- Pass prepared format_data ---
                 new_assoc_filename = self._format_associated_name(assoc_path, final_video_path.stem, format_data)
                 final_assoc_path = target_dir / new_assoc_filename; final_assoc_path_resolved = final_assoc_path.resolve(); log.debug(f"   Calculated Final Assoc Path Resolved: {final_assoc_path_resolved}")
                 # Compare lowercased strings
                 assoc_paths_differ = str(final_assoc_path_resolved).lower() != str(original_assoc_path_resolved).lower(); log.debug(f"   Plan Check Assoc '{assoc_path.name}' (case-insensitive): Differ={assoc_paths_differ} ('{original_assoc_path_resolved}' vs '{final_assoc_path_resolved}')")
                 if assoc_paths_differ:
                      log.debug(f"      Adding assoc action: {assoc_path.name} -> {final_assoc_path.name}")
                      planned_actions_dict[original_assoc_path_resolved] = RenameAction(original_path=assoc_path, new_path=final_assoc_path, action_type='move' if target_dir.resolve() != original_assoc_path_resolved.parent.resolve() else 'rename')

            # 4. Check for No Change *after* attempting to plan all actions
            log.debug(f"Planned actions dict before 'no change' check ({len(planned_actions_dict)} actions): {planned_actions_dict}")
            if not planned_actions_dict and not plan.created_dir_path: # Check if dictionary is empty AND no new dir needed
                 log.info(f"No actions planned for '{video_path.name}', setting status to skipped.")
                 plan.status = 'skipped'; plan.message = "Path already correct."; return plan # Return *immediately*

            # 5. Preliminary Conflict Check (only if actions are planned)
            conflict_mode = self.cfg('on_conflict', 'skip')
            final_target_paths: Set[Path] = {action.new_path.resolve() for action in planned_actions_dict.values()}
            original_paths_in_plan: Set[Path] = set(planned_actions_dict.keys())
            # Check for internal plan collisions (multiple sources to one target)
            if len(final_target_paths) < len(planned_actions_dict):
                 # Find the colliding target
                 target_counts = defaultdict(int)
                 colliding_target = None
                 for action in planned_actions_dict.values():
                     target_res = action.new_path.resolve()
                     target_counts[target_res] += 1
                     if target_counts[target_res] > 1:
                         colliding_target = target_res
                         break
                 plan.status = 'conflict_unresolved'; plan.message = f"Multiple source files map to the same target path ('{colliding_target.name}' if found)."; return plan
            # Check for external collisions (target exists and isn't part of this rename batch)
            for target_path_resolved in final_target_paths:
                 if target_path_resolved.exists() and target_path_resolved not in original_paths_in_plan:
                      if conflict_mode in ['skip', 'fail']:
                           plan.status = 'conflict_unresolved'; plan.message = f"Target '{target_path_resolved.name}' exists (mode: {conflict_mode})."; return plan

            # If we reach here, the plan is successful at this stage
            plan.status = 'success'; plan.message = "Plan created successfully."; plan.actions = list(planned_actions_dict.values())

        except Exception as e:
            log.error(f"Error planning rename for '{video_path.name}': {e}", exc_info=True)
            plan.status = 'failed'; plan.message = f"Error during planning: {e}"; plan.actions = []

        log.debug(f"--- Planning End: {video_path.name} -> Status={plan.status}, Actions={len(plan.actions)} ---")
        return plan

# --- END OF FILE renamer_engine.py ---