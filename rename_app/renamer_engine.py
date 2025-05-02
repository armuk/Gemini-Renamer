# rename_app/renamer_engine.py

import logging
import re
import os
import uuid
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Any, Set

from .models import MediaInfo, MediaMetadata, RenamePlan, RenameAction
from .utils import (
    sanitize_filename, parse_subtitle_language, extract_scene_tags,
    sanitize_os_chars, LANGCODES_AVAILABLE # Import LANGCODES_AVAILABLE too
)
from .exceptions import RenamerError

try: from guessit import guessit; GUESSIT_AVAILABLE = True
except ImportError: GUESSIT_AVAILABLE = False

log = logging.getLogger(__name__)

# (sanitize_os_chars function unchanged)
# ...

class RenamerEngine:
    # (__init__, parse_filename, _determine_file_type remain the same)
    # ...
    def __init__(self, cfg_helper): self.cfg = cfg_helper
    def parse_filename(self, file_path: Path) -> Dict:
        if not GUESSIT_AVAILABLE: log.error("Guessit library not available."); return {}
        try: guess = guessit(str(file_path)); log.debug(f"Guessit: {guess}"); return guess
        except Exception as e: log.error(f"Guessit failed: {e}"); return {} # Return empty dict on failure
    def _determine_file_type(self, guess_info: Dict) -> str:
        # --- FIX: Add check if guess_info is a dict ---
        if not isinstance(guess_info, dict):
            log.warning(f"Cannot determine file type, guess_info is not a dictionary: {type(guess_info)}")
            return 'unknown'
        # --- End FIX ---
        file_type = guess_info.get('type');
        if file_type == 'episode': return 'series'
        if file_type == 'movie': return 'movie'
        if 'season' in guess_info and 'episode' in guess_info: return 'series'
        if guess_info.get('year') and 'title' in guess_info: return 'movie'
        log.debug(f"Could not determine type from guess: {guess_info}"); return 'unknown'

    def _prepare_format_data(self, media_info: MediaInfo) -> Dict[str, Any]:
        # --- FIX: Ensure guess_info is a dictionary ---
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
                # Add other essential keys used in formats with default values
                'season': 0,
                'episode': 0,
                'episode_list': [],
                'year': None,
                'movie_year': None,
                'show_year': None,
                'episode_title': 'Unknown Episode',
                'scene_tags': [],
                'scene_tags_dot': '',
            }
        # --- End FIX ---

        data = media_info.guess_info.copy(); metadata = media_info.metadata
        data['ext'] = media_info.original_path.suffix; data['original_filename'] = media_info.original_path.name; data['original_stem'] = media_info.original_path.stem
        tags_to_preserve = self.cfg.get_list('scene_tags_to_preserve', []); scene_tags_list, scene_tags_dot = [], ""
        # --- FIX: Ensure original_filename is not None before passing to extract_scene_tags ---
        original_filename_for_tags = data.get('original_filename', '')
        if tags_to_preserve and original_filename_for_tags:
             scene_tags_list, scene_tags_dot = extract_scene_tags(original_filename_for_tags, tuple(tags_to_preserve))
        # --- End FIX ---
        data['scene_tags'] = scene_tags_list; data['scene_tags_dot'] = scene_tags_dot

        # --- FIX: Add None checks before sanitize_os_chars ---
        if metadata:
             data['source_api'] = metadata.source_api; data['ids'] = metadata.ids
             if metadata.is_series:
                 show_title_raw = metadata.show_title if metadata.show_title else data.get('title', 'Unknown_Show')
                 data['show_title'] = sanitize_os_chars(show_title_raw) if show_title_raw else 'Unknown_Show' # Check raw before sanitizing
                 data['show_year'] = metadata.show_year; data['season'] = metadata.season; data['episode_list'] = metadata.episode_list or []
                 ep_list = sorted(data['episode_list'])
                 if len(ep_list) > 1:
                     data['episode_range'] = f"E{ep_list[0]:0>2}-E{ep_list[-1]:0>2}"
                     # Sanitize titles individually after retrieval
                     titles_raw = [metadata.episode_titles.get(ep, f'Ep_{ep}') for ep in ep_list]
                     titles = [sanitize_os_chars(t) if t else f'Ep_{ep}' for ep, t in zip(ep_list, titles_raw)]
                     specific_titles = [t for t in titles if not t.startswith("Ep_")]
                     data['episode_title'] = " & ".join(specific_titles if specific_titles else titles[:1]); data['episode'] = ep_list[0]
                 elif ep_list:
                     data['episode'] = ep_list[0]; ep_title_meta = metadata.episode_titles.get(data['episode'])
                     data['episode_title'] = sanitize_os_chars(ep_title_meta) if ep_title_meta else data.get('episode_title', f"Episode_{data['episode']}")
                 data['air_date'] = next(iter(metadata.air_dates.values()), data.get('date'))
             elif metadata.is_movie:
                 movie_title_raw = metadata.movie_title if metadata.movie_title else data.get('title', 'Unknown_Movie')
                 data['movie_title'] = sanitize_os_chars(movie_title_raw) if movie_title_raw else 'Unknown_Movie' # Check raw before sanitizing
                 data['movie_year'] = metadata.movie_year or data.get('year'); data['release_date'] = metadata.release_date
        else:
             # Fallback when no metadata - sanitize titles retrieved from guessit data
             show_title_guess = data.get('title', 'Unknown Show')
             data.setdefault('show_title', sanitize_os_chars(show_title_guess) if show_title_guess else 'Unknown Show')
             movie_title_guess = data.get('title', 'Unknown Movie')
             data.setdefault('movie_title', sanitize_os_chars(movie_title_guess) if movie_title_guess else 'Unknown Movie')
             episode_title_guess = data.get('episode_title', f"Episode_{data.get('episode', 0)}")
             data.setdefault('episode_title', sanitize_os_chars(episode_title_guess) if episode_title_guess else f"Episode_{data.get('episode', 0)}")
        # --- End FIX ---

        data.setdefault('season', 0); data.setdefault('episode', 0); data.setdefault('episode_list', [data['episode']])
        # Ensure episode_title exists if single episode
        if len(data['episode_list']) == 1 and 'episode_title' not in data:
             episode_title_guess = data.get('episode_title', f"Episode_{data.get('episode', 0)}") # Check if guessit had one
             data['episode_title'] = sanitize_os_chars(episode_title_guess) if episode_title_guess else f"Episode_{data.get('episode', 0)}"

        data.setdefault('movie_year', data.get('year')); data.setdefault('show_year', data.get('year'))
        if len(data['episode_list']) > 1 and 'episode_range' not in data: ep_list = sorted(data['episode_list']); data['episode_range'] = f"E{ep_list[0]:0>2}-E{ep_list[-1]:0>2}"
        for gk, gv in media_info.guess_info.items(): data.setdefault(gk, gv) # Add remaining guessit keys if not already set
        log.debug(f"Prepared format data: {data}"); return data
    
    def _format_new_name(self, media_info: MediaInfo, format_data: Dict) -> str:
        mode = media_info.file_type; format_str_key = 'series_format' if mode == 'series' else 'movie_format'; format_str = self.cfg(format_str_key, default_value='').replace("{ext}", "")
        if not format_str: fallback_format = "{original_stem}_renamed".replace("{ext}", ""); log.warning(f"Missing format '{format_str_key}'. Using fallback: '{fallback_format}'"); format_str = fallback_format
        try: new_stem = format_str.format(**defaultdict(str, format_data))
        except Exception as e: raise RenamerError(f"Failed formatting stem: {e}. Format='{format_str}', DataKeys={list(format_data.keys())}") from e
        scene_tags_dot = format_data.get('scene_tags_dot', '');
        if self.cfg('scene_tags_in_filename', True) and scene_tags_dot and scene_tags_dot not in new_stem: new_stem += scene_tags_dot
        final_stem = sanitize_os_chars(new_stem)
        if not final_stem: log.warning(f"Stem empty after OS sanitation for '{media_info.original_path.name}'. Using original."); return sanitize_os_chars(media_info.original_path.stem)
        return final_stem
    def _format_folder_path(self, media_info: MediaInfo, format_data: Dict) -> Optional[Path]:
        if not self.cfg('create_folders'): return None
        mode = media_info.file_type; folder_format_key = 'folder_format_series' if mode == 'series' else 'folder_format_movie'; folder_format = self.cfg(folder_format_key)
        if not folder_format: return None
        try:
            relative_str = folder_format.format(**defaultdict(str, format_data)); sanitized_parts = [sanitize_os_chars(part) for part in Path(relative_str).parts if part and part != '.']
            if not sanitized_parts: return None
            return Path(*sanitized_parts)
        except Exception as e: log.error(f"Failed formatting folder: {e}. Format='{folder_format}', DataKeys={list(format_data.keys())}"); return None


    # --- _format_associated_name with DEBUG logging ---
    def _format_associated_name(self, assoc_path: Path, new_video_stem: str, format_data: Dict) -> str:
        """Formats the name for an associated file (subs, nfo), applying OS sanitation."""
        original_extension = assoc_path.suffix
        # Use get_list helper and ensure defaults work
        subtitle_extensions = set(self.cfg.get_list('subtitle_extensions', default_value=['.srt', '.sub']))
        log.debug(f"Formatting associated file: '{assoc_path.name}' with video stem: '{new_video_stem}'")
        # --- Add Debugging Here ---
        log.debug(f"Checking extension '{original_extension.lower()}' against subtitle types: {subtitle_extensions}")
        # --- End Debugging ---

        if original_extension.lower() in subtitle_extensions:
            detect_enc = self.cfg('subtitle_encoding_detection', True)
            # Ensure LANGCODES_AVAILABLE is checked before calling parse_subtitle_language
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
                new_name_unclean = sub_format.format(**defaultdict(str, sub_data)); log.debug(f"Formatted subtitle stem (unclean): '{new_name_unclean}'"); new_name_cleaned = re.sub(r'\.+', '.', new_name_unclean).strip('._ '); log.debug(f"Formatted subtitle stem (cleaned): '{new_name_cleaned}'")
                new_name = f"{new_name_cleaned}{original_extension}"
                if not new_name_cleaned or new_name == original_extension: fallback_name = f"{new_video_stem}{sub_data['lang_dot']}{sub_data['flags_dot']}{original_extension}".replace('..','.'); log.warning(f"Formatted subtitle name was empty or unchanged ('{new_name}'), using fallback: '{fallback_name}'"); new_name = fallback_name
            except Exception as e: log.error(f"Failed formatting subtitle '{assoc_path.name}'. Format: '{sub_format}'. Error: {e}. Falling back."); fallback_name = f"{new_video_stem}{('.' + lang_code) if lang_code else ''}{''.join('.'+f for f in flags)}{original_extension}".replace('..','.'); new_name = fallback_name
        else:
            new_name = f"{new_video_stem}{original_extension}"; log.debug(f"Non-subtitle associated file, using simple name: '{new_name}'")

        final_os_sanitized_name = sanitize_os_chars(new_name); log.debug(f"Final OS sanitized associated name: '{final_os_sanitized_name}'"); return final_os_sanitized_name

    # --- plan_rename with DEBUG logging ---
    def plan_rename(self, video_path: Path, associated_paths: List[Path], media_info: MediaInfo) -> RenamePlan:
        """Creates a rename plan for a batch, handling no-change and conflicts."""
        batch_id_suffix = uuid.uuid4().hex[:6]
        plan = RenamePlan(batch_id=f"plan-{video_path.stem[:10]}-{batch_id_suffix}", video_file=video_path)
        try: base_target_dir = self.cfg.args.directory.resolve(); original_video_path_resolved = video_path.resolve()
        except AttributeError: log.warning("Base directory not found in args, using video parent."); base_target_dir = video_path.parent.resolve(); original_video_path_resolved = video_path.resolve()
        except Exception as e: plan.status='failed'; plan.message=f"Cannot resolve base directory: {e}"; return plan

        log.debug(f"--- Planning Start: {video_path.name} ---"); log.debug(f"Original Video Path Resolved: {original_video_path_resolved}")

        try:
            # 1. Prepare Data & Type
            if not media_info.file_type or media_info.file_type == 'unknown': media_info.file_type = self._determine_file_type(media_info.guess_info)
            if media_info.file_type == 'unknown': plan.status = 'skipped'; plan.message = "Could not determine file type."; return plan
            if not media_info.data: media_info.data = self._prepare_format_data(media_info)
            log.debug(f"Data for formatting in plan_rename: {media_info.data}") # Debug data used

            # 2. Format Video Name & Folder
            new_video_stem = self._format_new_name(media_info, media_info.data); relative_folder = self._format_folder_path(media_info, media_info.data)
            target_dir = base_target_dir / relative_folder if relative_folder else original_video_path_resolved.parent
            plan.created_dir_path = target_dir if relative_folder and target_dir.resolve() != original_video_path_resolved.parent else None # Set created_dir only if truly different
            new_video_filename = f"{new_video_stem}{video_path.suffix}"; final_video_path = target_dir / new_video_filename
            final_video_path_resolved = final_video_path.resolve(); log.debug(f"Calculated Final Video Path Resolved: {final_video_path_resolved}")

            # 3. Format Associated Files & Build Planned Actions
            planned_actions_dict: Dict[Path, RenameAction] = {}

            # Compare lowercased strings for case-insensitivity
            vid_paths_differ = str(final_video_path_resolved).lower() != str(original_video_path_resolved).lower()
            log.debug(f"Plan Check Video (case-insensitive): Differ={vid_paths_differ}")
            if vid_paths_differ:
                 log.debug(f"   Adding video action: {video_path.name} -> {final_video_path.name}")
                 planned_actions_dict[original_video_path_resolved] = RenameAction(original_path=video_path, new_path=final_video_path, action_type='move' if target_dir.resolve() != original_video_path_resolved.parent else 'rename')

            for assoc_path in associated_paths:
                 original_assoc_path_resolved = assoc_path.resolve(); log.debug(f"   Processing Assoc: {assoc_path.name}"); log.debug(f"   Original Assoc Path Resolved: {original_assoc_path_resolved}")
                 new_assoc_filename = self._format_associated_name(assoc_path, final_video_path.stem, media_info.data)
                 final_assoc_path = target_dir / new_assoc_filename; final_assoc_path_resolved = final_assoc_path.resolve(); log.debug(f"   Calculated Final Assoc Path Resolved: {final_assoc_path_resolved}")
                 # Compare lowercased strings
                 assoc_paths_differ = str(final_assoc_path_resolved).lower() != str(original_assoc_path_resolved).lower(); log.debug(f"   Plan Check Assoc '{assoc_path.name}' (case-insensitive): Differ={assoc_paths_differ}")
                 if assoc_paths_differ:
                      log.debug(f"      Adding assoc action: {assoc_path.name} -> {final_assoc_path.name}")
                      planned_actions_dict[original_assoc_path_resolved] = RenameAction(original_path=assoc_path, new_path=final_assoc_path, action_type='move' if target_dir.resolve() != original_assoc_path_resolved.parent else 'rename')

            # 4. Check for No Change *after* attempting to plan all actions
            log.debug(f"Planned actions dict before 'no change' check ({len(planned_actions_dict)} actions): {planned_actions_dict}")
            if not planned_actions_dict: # If dictionary is empty, no actions needed
                 log.info(f"No actions planned for '{video_path.name}', setting status to skipped.")
                 plan.status = 'skipped'; plan.message = "Path already correct."; return plan # Return *immediately*

            # 5. Preliminary Conflict Check (only if actions are planned)
            conflict_mode = self.cfg('on_conflict', 'skip'); final_target_paths: Set[Path] = {action.new_path.resolve() for action in planned_actions_dict.values()}; original_paths_in_plan: Set[Path] = set(planned_actions_dict.keys())
            if len(final_target_paths) < len(planned_actions_dict): plan.status = 'conflict_unresolved'; plan.message = "Multiple source files map to the same target path."; return plan
            for target_path_resolved in final_target_paths:
                 if target_path_resolved.exists() and target_path_resolved not in original_paths_in_plan:
                      if conflict_mode in ['skip', 'fail']: plan.status = 'conflict_unresolved'; plan.message = f"Target '{target_path_resolved.name}' exists (mode: {conflict_mode})."; return plan

            # If we reach here, the plan is successful at this stage
            plan.status = 'success'; plan.message = "Plan created successfully."; plan.actions = list(planned_actions_dict.values())

        except Exception as e:
            log.error(f"Error planning rename for '{video_path.name}': {e}", exc_info=True)
            plan.status = 'failed'; plan.message = f"Error during planning: {e}"; plan.actions = []

        log.debug(f"--- Planning End: {video_path.name} -> Status={plan.status}, Actions={len(plan.actions)} ---")
        return plan