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
    sanitize_os_chars, LANGCODES_AVAILABLE, extract_stream_info
)
from .exceptions import RenamerError
from .enums import ProcessingStatus # <--- IMPORT THE ENUM

try: from guessit import guessit; GUESSIT_AVAILABLE = True
except ImportError: GUESSIT_AVAILABLE = False

log = logging.getLogger(__name__)

# ... (Regex patterns remain the same) ...
MULTI_EP_RANGE_PATTERN = re.compile(
    r'[Ss](?P<snum>\d+)'
    r'[\s._-]*[Ee]'
    r'(?P<ep1>\d{2,})'
    r'(?:'
       r'\s*[-]\s*[Ee]?'
       r'|'
       r'\s*[._]\s*[Ee]'
    r')'
    r'(?P<ep2>\d{2,})'
    r'(?![\d])',
    re.IGNORECASE
)
MULTI_EP_CONSECUTIVE_PATTERN = re.compile(
    r'[Ss](?P<snum>\d+)'
    r'[Ee](?P<ep1>\d{2,})'
    r'[Ee](?P<ep2>\d{2,})'
    r'(?![\d])',
    re.IGNORECASE
)


class RenamerEngine:
    def __init__(self, cfg_helper): self.cfg = cfg_helper

    def parse_filename(self, file_path: Path) -> Dict:
        # (unchanged)
        if not GUESSIT_AVAILABLE: log.error("Guessit library not available."); return {}
        try: guess = guessit(str(file_path)); log.debug(f"Guessit: {guess}"); return guess
        except Exception as e: log.error(f"Guessit failed: {e}"); return {}

    def _determine_file_type(self, guess_info: Dict) -> str:
        # (unchanged)
        if not isinstance(guess_info, dict):
            log.warning(f"Cannot determine file type, guess_info is not a dictionary: {type(guess_info)}")
            return 'unknown'
        file_type = guess_info.get('type');
        if file_type == 'episode': return 'series'
        if file_type == 'movie': return 'movie'
        if 'season' in guess_info and ('episode' in guess_info or 'episode_list' in guess_info): return 'series'
        if guess_info.get('year') and 'title' in guess_info and file_type != 'episode': return 'movie'
        log.debug(f"Could not determine type from guess: {guess_info}"); return 'unknown'

    def _initialize_base_format_data(self, original_path: Path, guess_info: Optional[Dict]) -> Dict[str, Any]:
        if not isinstance(guess_info, dict):
            log.warning(f"Guess_info is not a dictionary for '{original_path.name}'. Using minimal fallbacks.")
            return {
                'ext': original_path.suffix, 'original_filename': original_path.name,
                'original_stem': original_path.stem, 'title': original_path.stem,
                'show_title': original_path.stem, 'movie_title': original_path.stem,
                'season': 0, 'episode': 0, 'episode_list': [], 'episode_range': '',
                'ep_identifier': 'E00', 'year': None, 'movie_year': None, 'show_year': None,
                'episode_title': 'Unknown Episode', 'scene_tags': [], 'scene_tags_dot': '',
                'resolution': '', 'vcodec': '', 'acodec': '', 'achannels': '',
                'collection': '', 'air_date': '', 'release_date': '', 'ids': {}, 'source_api': '',
            }
        data = guess_info.copy()
        data['ext'] = original_path.suffix; data['original_filename'] = original_path.name
        data['original_stem'] = original_path.stem
        return data
    def _add_scene_tags_to_format_data(self, data: Dict[str, Any]):
        tags_to_preserve = self.cfg.get_list('scene_tags_to_preserve', [])
        scene_tags_list, scene_tags_dot = [], ""
        original_filename_for_tags = data.get('original_filename')
        if tags_to_preserve and original_filename_for_tags:
            scene_tags_list, scene_tags_dot = extract_scene_tags(original_filename_for_tags, tuple(tags_to_preserve))
        data['scene_tags'] = scene_tags_list; data['scene_tags_dot'] = scene_tags_dot
    def _get_episode_list_from_guessit(self, guess_info: Dict, original_filename: str) -> List[int]:
        final_episode_list: List[int] = []; guess_ep_data = None
        if isinstance(guess_info.get('episode_list'), list): guess_ep_data = guess_info['episode_list']
        elif 'episode' in guess_info: guess_ep_data = guess_info['episode']
        elif 'episode_number' in guess_info: guess_ep_data = guess_info['episode_number']
        if guess_ep_data is not None:
            ep_data_list = guess_ep_data if isinstance(guess_ep_data, list) else [guess_ep_data]
            for ep in ep_data_list:
                try:
                    ep_int = int(str(ep))
                    if ep_int > 0: final_episode_list.append(ep_int)
                except (ValueError, TypeError): log.warning(f"Could not parse episode '{ep}' from guessit data for '{original_filename}'.")
        return sorted(list(set(final_episode_list)))
    def _get_episode_list_from_regex(self, original_stem: str, original_filename: str) -> Optional[List[int]]:
        range_match = MULTI_EP_RANGE_PATTERN.search(original_stem)
        if range_match:
            try:
                ep1 = int(range_match.group('ep1')); ep2 = int(range_match.group('ep2'))
                if ep1 < ep2: return list(range(ep1, ep2 + 1))
                else: log.warning(f"Regex range detection for '{original_filename}' skipped: {ep1} vs {ep2}.")
            except Exception as e: log.warning(f"Error parsing regex RANGE for '{original_filename}': {e}")
            return None
        consecutive_match = MULTI_EP_CONSECUTIVE_PATTERN.search(original_stem)
        if consecutive_match:
            try:
                ep1 = int(consecutive_match.group('ep1')); ep2 = int(consecutive_match.group('ep2'))
                if ep1 + 1 == ep2: return [ep1, ep2]
                else: log.warning(f"Regex CONSECUTIVE for '{original_filename}' not sequential: {ep1}->{ep2}.")
            except Exception as e: log.warning(f"Error parsing regex CONSECUTIVE for '{original_filename}': {e}")
        return None
    def _finalize_episode_data_for_formatting(self, data: Dict[str, Any], original_filename: str):
        guessit_ep_list = self._get_episode_list_from_guessit(data, original_filename)
        final_episode_list = guessit_ep_list
        if len(guessit_ep_list) <= 1:
            regex_ep_list = self._get_episode_list_from_regex(data.get('original_stem',''), original_filename)
            if regex_ep_list: final_episode_list = regex_ep_list
        final_episode_list = sorted(list(set(final_episode_list)))
        data['episode_list'] = final_episode_list
        data['episode'] = final_episode_list[0] if final_episode_list else 0
        data['episode_range'] = ''
        if len(final_episode_list) > 1:
            data['episode_range'] = f"E{final_episode_list[0]:0>2}-E{final_episode_list[-1]:0>2}"
            data['ep_identifier'] = data['episode_range']
        elif final_episode_list: data['ep_identifier'] = f"E{final_episode_list[0]:0>2}"
        else: data['ep_identifier'] = "E00"
    def _integrate_metadata_into_format_data(self, data: Dict[str, Any], metadata: Optional[MediaMetadata]):
        data.setdefault('collection', ''); data.setdefault('source_api', ''); data.setdefault('ids', {})
        data.setdefault('air_date', ''); data.setdefault('release_date', '')
        if not metadata: return
        data['source_api'] = metadata.source_api or ''; data['ids'] = metadata.ids or {}
        if metadata.is_movie:
            title_raw = metadata.movie_title or data.get('title', 'Unknown_Movie')
            data['movie_title'] = sanitize_os_chars(title_raw) if title_raw else 'Unknown_Movie'
            data['movie_year'] = metadata.movie_year or data.get('year')
            data['release_date'] = metadata.release_date or ''; data['collection'] = metadata.collection_name or ''
        elif metadata.is_series:
            title_raw = metadata.show_title or data.get('title', 'Unknown_Show')
            data['show_title'] = sanitize_os_chars(title_raw) if title_raw else 'Unknown_Show'
            data['show_year'] = metadata.show_year
            data['season'] = metadata.season if metadata.season is not None else data.get('season', 0)
            ep_list = data.get('episode_list', [])
            if len(ep_list) > 1:
                titles_r = [metadata.episode_titles.get(ep, f'Ep_{ep}') for ep in ep_list]
                titles_s = [sanitize_os_chars(t) if t else f'Ep_{ep}' for ep, t in zip(ep_list, titles_r)]
                specific = [t for t in titles_s if not t.startswith("Ep_")]
                data['episode_title'] = " & ".join(specific) if specific else (titles_s[0] if titles_s else 'Multiple Episodes')
            elif ep_list:
                ep_meta = metadata.episode_titles.get(ep_list[0])
                data['episode_title'] = sanitize_os_chars(ep_meta) if ep_meta else data.get('episode_title', f"Episode_{ep_list[0]}")
            else: data['episode_title'] = data.get('episode_title', 'Unknown Episode')
            if ep_list: data['air_date'] = metadata.air_dates.get(ep_list[0], data.get('date', ''))
    def _apply_format_data_fallbacks(self, data: Dict[str, Any]):
        stem = data.get('original_stem', 'Unknown')
        if not data.get('show_title'): data['show_title'] = sanitize_os_chars(data.get('title', stem + "_Show")) or 'Unknown Show'
        if not data.get('movie_title'): data['movie_title'] = sanitize_os_chars(data.get('title', stem + "_Movie")) or 'Unknown Movie'
        if not data.get('episode_title') or data['episode_title'] == 'Unknown Episode':
            ep_fb = data.get('episode',0); data['episode_title'] = sanitize_os_chars(data.get('episode_title_guessit', f"Episode_{ep_fb}")) or f"Episode_{ep_fb}"
        data.setdefault('season', 0); data.setdefault('movie_year', data.get('year')); data.setdefault('show_year', data.get('year'))
        data.setdefault('ep_identifier', f"E{data.get('episode', 0):0>2d}")
    def _extract_and_add_stream_info_to_format_data(self, data: Dict[str, Any], original_path: Path, file_type: str):
        data.update({'resolution': '', 'vcodec': '', 'acodec': '', 'achannels': ''}) # Ensure keys exist
        if not self.cfg('extract_stream_info', False): return
        stream_placeholders = {"{resolution}", "{vcodec}", "{acodec}", "{achannels}"}
        relevant_formats = []
        if file_type == 'series':
            relevant_formats.extend([self.cfg('series_format'), self.cfg('series_format_specials'), self.cfg('folder_format_series'), self.cfg('folder_format_specials')])
        elif file_type == 'movie':
            relevant_formats.extend([self.cfg('movie_format'), self.cfg('folder_format_movie')])
        relevant_formats.append(self.cfg('subtitle_format'))
        formats_to_check = [f for f in relevant_formats if f and isinstance(f, str)]
        should_extract = any(any(ph in fmt for ph in stream_placeholders) for fmt in formats_to_check)
        if not should_extract: log.debug(f"No stream placeholders for '{original_path.name}'. Skipping."); return
        try:
            stream_info = extract_stream_info(original_path)
            if stream_info: data.update({k:v for k,v in stream_info.items() if v and k in data})
        except Exception as e: log.error(f"Failed stream info for {original_path.name}: {e}")
    def _prepare_format_data(self, media_info: MediaInfo) -> Dict[str, Any]:
        data = self._initialize_base_format_data(media_info.original_path, media_info.guess_info)
        self._add_scene_tags_to_format_data(data)
        self._finalize_episode_data_for_formatting(data, media_info.original_path.name)
        self._integrate_metadata_into_format_data(data, media_info.metadata)
        self._apply_format_data_fallbacks(data)
        self._extract_and_add_stream_info_to_format_data(data, media_info.original_path, media_info.file_type)
        if isinstance(media_info.guess_info, dict):
            for gk, gv in media_info.guess_info.items(): data.setdefault(gk, gv)
        log.debug(f"Prepared format data for '{media_info.original_path.name}': {data}")
        return data

    def _format_new_name(self, media_info: MediaInfo, format_data: Dict) -> str:
        mode = media_info.file_type; format_str = None; fallback_format = "{original_stem}_renamed"
        if mode == 'series':
            is_special = format_data.get('season') == 0
            if is_special and self.cfg('series_format_specials'): format_str = self.cfg('series_format_specials').replace("{ext}", "")
            if not format_str: format_str = self.cfg('series_format', '').replace("{ext}", "")
            if not format_str: log.warning(f"Missing 'series_format'. Using fallback."); format_str = fallback_format
        elif mode == 'movie':
            format_str = self.cfg('movie_format', '').replace("{ext}", "")
            if not format_str: log.warning(f"Missing 'movie_format'. Using fallback."); format_str = fallback_format
        else: log.error(f"Unexpected type '{mode}'. Using fallback."); format_str = fallback_format
        try:
            new_stem = format_str.format(**defaultdict(str, format_data))
        except KeyError as e_key: 
            plan_message = f"[{ProcessingStatus.CONFIG_MISSING_FORMAT_STRING}] Failed formatting stem: Missing placeholder {e_key} in format '{format_str}'."
            log.error(plan_message + f" DataKeys={list(format_data.keys())}")
            raise RenamerError(plan_message) from e_key
        except Exception as e:
            plan_message = f"[{ProcessingStatus.INTERNAL_ERROR}] Failed formatting stem: {e}. Format='{format_str}'"
            log.error(plan_message + f" DataKeys={list(format_data.keys())}")
            raise RenamerError(plan_message) from e
        tags_dot = format_data.get('scene_tags_dot', '')
        if self.cfg('scene_tags_in_filename', True) and tags_dot and tags_dot not in new_stem: new_stem += tags_dot
        final_stem, _ = os.path.splitext(sanitize_filename(new_stem + format_data.get('ext', '')))
        if not final_stem:
            log.warning(f"Stem empty after sanitation for '{media_info.original_path.name}'. Using original.")
            return sanitize_filename(media_info.original_path.stem + format_data.get('ext', ''))
        return final_stem

    def _format_folder_path(self, media_info: MediaInfo, format_data: Dict) -> Optional[Path]:
        if not self.cfg('create_folders'): return None
        mode = media_info.file_type; folder_format = None
        if mode == 'series':
            is_special = format_data.get('season') == 0
            if is_special and self.cfg('folder_format_specials'): folder_format = self.cfg('folder_format_specials')
            if not folder_format: folder_format = self.cfg('folder_format_series')
        elif mode == 'movie': folder_format = self.cfg('folder_format_movie')
        if not folder_format: log.debug(f"No folder format for '{mode}'."); return None
        try:
            relative_str = folder_format.format(**defaultdict(str, format_data))
            parts = [sanitize_os_chars(p) for p in Path(relative_str).parts if p and p != '.']
            if not parts: log.warning(f"Folder path empty. Format: '{folder_format}'"); return None
            return Path(*parts)
        except KeyError as e_key:
            log.error(f"[{ProcessingStatus.CONFIG_MISSING_FORMAT_STRING}] Failed formatting folder: Missing placeholder {e_key} in format '{folder_format}'. DataKeys={list(format_data.keys())}")
            return None
        except Exception as e:
            log.error(f"Failed formatting folder: {e}. Format='{folder_format}', DataKeys={list(format_data.keys())}"); return None

    def _format_associated_name(self, assoc_path: Path, new_video_stem: str, format_data: Dict) -> str:
        original_extension = assoc_path.suffix
        sub_exts = {ext.lower() for ext in self.cfg.get_list('subtitle_extensions', ['.srt', '.sub'])}
        if original_extension.lower() in sub_exts:
            lang_code, flags, enc = (parse_subtitle_language(assoc_path.name, self.cfg('subtitle_encoding_detection', True), assoc_path) if LANGCODES_AVAILABLE else (None, [], None))
            sub_fmt = self.cfg('subtitle_format', "{stem}{lang_dot}{flags_dot}").replace("{ext}", "")
            sub_data = {'stem': new_video_stem, 'lang_code': lang_code or '', 'lang_dot': f".{lang_code}" if lang_code else '',
                        'flags': "".join(flags), 'flags_dot': "".join(f".{f}" for f in flags),
                        'encoding': enc or '', 'encoding_dot': f".{enc}" if enc else '',
                        'scene_tags_dot': format_data.get('scene_tags_dot', '')}
            try:
                name_u = sub_fmt.format(**defaultdict(str, sub_data))
                name_c = re.sub(r'\.+', '.', name_u).strip('._ ')
                new_name = f"{name_c}{original_extension}"
                if not name_c or new_name == original_extension:
                    fb_name = f"{new_video_stem}{sub_data['lang_dot']}{sub_data['flags_dot']}{original_extension}".replace('..','.')
                    log.warning(f"Sub name empty/unchanged ('{new_name}'), using fallback: '{fb_name}'"); new_name = fb_name
            except Exception as e:
                log.error(f"Failed formatting subtitle '{assoc_path.name}': {e}. Falling back.")
                new_name = f"{new_video_stem}{('.' + lang_code) if lang_code else ''}{''.join('.'+f for f in flags)}{original_extension}".replace('..','.')
        else: new_name = f"{new_video_stem}{original_extension}"
        return sanitize_filename(new_name)

    def plan_rename(self, video_path: Path, associated_paths: List[Path], media_info: MediaInfo) -> RenamePlan:
        batch_id_suffix = uuid.uuid4().hex[:6]
        plan = RenamePlan(batch_id=f"plan-{video_path.stem[:10]}-{batch_id_suffix}", video_file=video_path)
        original_video_path_resolved: Path
        base_target_dir: Path
        try:
            base_target_dir = self.cfg.args.directory.resolve() if hasattr(self.cfg, 'args') and hasattr(self.cfg.args, 'directory') else video_path.parent.resolve()
            original_video_path_resolved = video_path.resolve()
        except AttributeError: 
            log.warning("cfg.args.directory not found, using video parent for base_target_dir.")
            base_target_dir = video_path.parent.resolve()
            original_video_path_resolved = video_path.resolve()
        except Exception as e:
            plan.status = 'failed'
            plan.message = f"[{ProcessingStatus.INTERNAL_ERROR}] Cannot resolve base directory: {e}"
            return plan

        log.debug(f"PLAN_RENAME_ENTRY for '{video_path.name}':")
        log.debug(f"  Original Video Path: {video_path}")
        log.debug(f"  Original Video Path Resolved: {original_video_path_resolved}")
        log.debug(f"  Base Target Dir: {base_target_dir}")
        if media_info.metadata:
            log.debug(f"  MediaInfo Metadata Source: {media_info.metadata.source_api}, Title: {media_info.metadata.movie_title or media_info.metadata.show_title}, Year: {media_info.metadata.movie_year or media_info.metadata.show_year}, Score: {media_info.metadata.match_confidence}")
        else:
            log.debug(f"  MediaInfo Metadata: None")
        log.debug(f"  MediaInfo FileType: {media_info.file_type}")
        log.debug(f"--- Planning Start: {video_path.name} ---");
        log.debug(f"Original Video Path Resolved: {original_video_path_resolved}")

        try:
            if not media_info.file_type or media_info.file_type == 'unknown':
                media_info.file_type = self._determine_file_type(media_info.guess_info)
            
            if media_info.file_type == 'unknown':
                plan.status = 'skipped'
                plan.message = f"[{ProcessingStatus.FILE_TYPE_UNKNOWN}] Could not determine file type for '{video_path.name}'."
                return plan
            
            format_data = self._prepare_format_data(media_info)
            media_info.data = format_data 
            log.debug(f"Data for formatting in plan_rename for '{video_path.name}': {format_data}")

            new_video_stem = self._format_new_name(media_info, format_data) 
            if not new_video_stem or new_video_stem == video_path.stem and not self.cfg('create_folders'): 
                 plan.status = 'skipped'
                 plan.message = f"[{ProcessingStatus.PLAN_INVALID_GENERATED_NAME}] Generated video stem is empty or unchanged (and no folder change). Original: '{video_path.stem}'"
                 return plan


            relative_folder = self._format_folder_path(media_info, format_data)
            target_dir = base_target_dir / relative_folder if relative_folder else original_video_path_resolved.parent
            
            target_dir = target_dir.resolve()
            
            plan.created_dir_path = target_dir if relative_folder and target_dir != original_video_path_resolved.parent.resolve() else None
            
            new_video_filename = f"{new_video_stem}{video_path.suffix}"
            final_video_path = target_dir / new_video_filename
            final_video_path_resolved = final_video_path.resolve()
            log.debug(f"Calculated Final Video Path Resolved for '{video_path.name}': {final_video_path_resolved}")

            planned_actions_dict: Dict[Path, RenameAction] = {}

            vid_paths_differ = str(final_video_path_resolved).lower() != str(original_video_path_resolved).lower()
            vid_parent_dirs_differ = final_video_path_resolved.parent != original_video_path_resolved.parent
            
            if vid_paths_differ or vid_parent_dirs_differ: 
                 log.debug(f"Plan Check Video: Paths Differ={vid_paths_differ}, Parent Dirs Differ={vid_parent_dirs_differ}")
                 planned_actions_dict[original_video_path_resolved] = RenameAction(
                     original_path=video_path, new_path=final_video_path,
                     action_type='move' if vid_parent_dirs_differ else 'rename'
                 )

            for assoc_path in associated_paths:
                 original_assoc_path_resolved = assoc_path.resolve()
                 log.debug(f"   Processing Assoc: {assoc_path.name} (Resolved: {original_assoc_path_resolved})");
                 new_assoc_filename = self._format_associated_name(assoc_path, final_video_path.stem, format_data)
                 final_assoc_path = target_dir / new_assoc_filename
                 final_assoc_path_resolved = final_assoc_path.resolve()
                 
                 assoc_paths_differ = str(final_assoc_path_resolved).lower() != str(original_assoc_path_resolved).lower()
                 assoc_parent_dirs_differ = final_assoc_path_resolved.parent != original_assoc_path_resolved.parent

                 log.debug(f"   Plan Check Assoc '{assoc_path.name}': Final Path='{final_assoc_path_resolved}', Paths Differ={assoc_paths_differ}, Parent Dirs Differ={assoc_parent_dirs_differ}")
                 if assoc_paths_differ or assoc_parent_dirs_differ:
                      planned_actions_dict[original_assoc_path_resolved] = RenameAction(
                          original_path=assoc_path, new_path=final_assoc_path,
                          action_type='move' if assoc_parent_dirs_differ else 'rename'
                      )

            log.debug(f"Planned actions dict before 'no change' check for '{video_path.name}' ({len(planned_actions_dict)} actions): {planned_actions_dict}")
            
            if not planned_actions_dict:
                # If no file rename/move actions are planned, check if only a directory creation was planned.
                # If original video path is already IN the planned target directory, then it's truly "Path Already Correct".
                # Otherwise, if a directory creation IS planned, it means the file needs to move into that new dir,
                # even if its name doesn't change, which should have generated a 'move' action.
                # This means if created_dir_path is set, planned_actions_dict should not be empty if a move is needed.
                if not plan.created_dir_path or (plan.created_dir_path and original_video_path_resolved.parent == plan.created_dir_path.resolve()):
                    log.debug(f"PATH_ALREADY_CORRECT_CHECK for '{video_path.name}':")
                    log.debug(f"  Generated video_stem for new name: '{new_video_stem}' vs original stem: '{video_path.stem}'")
                    log.debug(f"  Calculated relative_folder: {relative_folder}")
                    log.debug(f"  Calculated target_dir: {target_dir}")
                    log.debug(f"  Calculated final_video_path: {final_video_path}")
                    log.debug(f"  Calculated final_video_path_resolved: {final_video_path_resolved}")
                    log.debug(f"  Original video_path_resolved: {original_video_path_resolved}")
                    log.debug(f"  Condition check: not plan.created_dir_path ({not plan.created_dir_path}) OR (plan.created_dir_path ({plan.created_dir_path is not None}) AND original_video_path_resolved.parent ({original_video_path_resolved.parent}) == plan.created_dir_path.resolve() ({plan.created_dir_path.resolve() if plan.created_dir_path else 'N/A'}))")
                    # ++++++++++++++++++++++++++++++
                    log.info(f"No actions planned for '{video_path.name}', path likely already correct.")
                    plan.status = 'skipped' 
                    plan.message = f"[{ProcessingStatus.PATH_ALREADY_CORRECT.name}] Path already correct for '{video_path.name}'."
                    # action_result['success'] will be handled by the caller based on this message/status
                    return plan
                elif plan.created_dir_path and original_video_path_resolved.parent == plan.created_dir_path.resolve():
                     # This means a dir was "planned" but the file is already in it, and names match.
                     log.info(f"No rename actions for '{video_path.name}', and it's already in the target directory structure. Path correct.")
                     plan.status = 'skipped'; plan.message = f"[{ProcessingStatus.PATH_ALREADY_CORRECT}] Path already correct for '{video_path.name}' (in target dir structure)."
                     return plan


            conflict_mode = self.cfg('on_conflict', 'skip')
            final_target_paths: Set[Path] = {action.new_path.resolve() for action in planned_actions_dict.values()}
            original_paths_in_plan: Set[Path] = set(planned_actions_dict.keys())

            if len(final_target_paths) < len(planned_actions_dict):
                 target_counts = defaultdict(list)
                 colliding_target_path_str = "UNKNOWN_TARGET"
                 for op, act in planned_actions_dict.items(): target_counts[act.new_path.resolve()].append(op)
                 for target_path_resolved_val, sources in target_counts.items(): # Renamed target_path to avoid conflict
                     if len(sources) > 1: colliding_target_path_str = target_path_resolved_val.name; break
                 plan.status = 'conflict_unresolved'
                 plan.message = f"[{ProcessingStatus.PLAN_MULTIPLE_SOURCES_TO_TARGET}] Multiple source files map to target '{colliding_target_path_str}'. Sources: {[p.name for p in sources]}"
                 return plan
            
            for target_path_resolved_val_check in final_target_paths: # Renamed target_path_resolved to avoid conflict
                 if target_path_resolved_val_check.exists() and target_path_resolved_val_check not in original_paths_in_plan:
                      if conflict_mode == 'skip':
                           plan.status = 'skipped' 
                           plan.message = f"[{ProcessingStatus.PLAN_TARGET_EXISTS_SKIP_MODE}] Target '{target_path_resolved_val_check.name}' exists (mode: skip). Batch skipped."
                           return plan
                      elif conflict_mode == 'fail':
                           plan.status = 'conflict_unresolved' 
                           plan.message = f"[{ProcessingStatus.PLAN_TARGET_EXISTS_FAIL_MODE}] Target '{target_path_resolved_val_check.name}' exists (mode: fail)."
                           return plan

            plan.status = 'success'; plan.message = "Plan created successfully."
            plan.actions = list(planned_actions_dict.values())

        except RenamerError as e: 
            log.error(f"RenamerError during planning for '{video_path.name}': {e}", exc_info=False) 
            plan.status = 'failed'; plan.message = str(e) 
            plan.actions = []
        except Exception as e:
            log.error(f"Unexpected error planning rename for '{video_path.name}': {e}", exc_info=True)
            plan.status = 'failed'; plan.message = f"[{ProcessingStatus.INTERNAL_ERROR}] Unexpected error during planning: {e}"
            plan.actions = []

        log.debug(f"--- Planning End: {video_path.name} -> Status={plan.status}, Message='{plan.message}' Actions={len(plan.actions)} ---")
        return plan