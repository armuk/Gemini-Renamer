# models.py
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Any

@dataclass
class MediaMetadata:
    """Holds combined metadata after fetching."""
    # Common fields
    source_api: Optional[str] = None # e.g., "tmdb", "tvdb"
    ids: Dict[str, Any] = field(default_factory=dict) # {'tmdb_id': 123, 'imdb_id': 'tt...', 'tvdb_id': 456}

    # Series specific
    is_series: bool = False # Flag for series
    show_title: Optional[str] = None # e.g., "The Office"
    show_year: Optional[int] = None # e.g., 2005
    season: Optional[int] = None # e.g., 1
    episode_list: List[int] = field(default_factory=list) # [1, 2, 3]
    episode_titles: Dict[int, str] = field(default_factory=dict) # {1: "Title A", 2: "Title B"}
    air_dates: Dict[int, str] = field(default_factory=dict) # {1: "YYYY-MM-DD"}

    # Movie specific
    is_movie: bool = False
    movie_title: Optional[str] = None
    movie_year: Optional[int] = None
    release_date: Optional[str] = None

    # --- NEW: Collection Info ---
    collection_name: Optional[str] = None
    collection_id: Optional[int] = None

    # Add other fields if needed (e.g., genres)

@dataclass
class RenameAction:
    """Represents a single proposed file system action."""
    original_path: Path
    new_path: Path
    action_type: str # 'rename', 'move', 'create_dir', 'delete_dir_revert'
    status: str = 'planned' # planned, success, failed, skipped
    message: Optional[str] = None
    is_temp_rename: bool = False # Flag for phase 1 of transactional rename

@dataclass
class RenamePlan:
    """Holds the overall plan for a batch."""
    batch_id: str # Unique ID for this processing attempt
    video_file: Path
    status: str = 'pending' # pending, success, partial_success, failed, skipped, conflict_unresolved
    message: Optional[str] = None
    actions: List[RenameAction] = field(default_factory=list)
    created_dir_path: Optional[Path] = None # Track directory to be created

    def get_final_map(self) -> Dict[Path, Path]:
        """Returns a map of original path to final intended new path."""
        # Excludes temp renames if transactional logic is separate
        return {a.original_path: a.new_path for a in self.actions if not a.is_temp_rename and a.action_type != 'create_dir'}

@dataclass
class MediaInfo:
     """Holds info derived from filename parsing and potential metadata."""
     original_path: Path
     guess_info: Dict[str, Any] = field(default_factory=dict)
     metadata: Optional[MediaMetadata] = None
     # Combine relevant fields after processing for easier access
     file_type: str = 'unknown' # series, movie, unknown
     data: Dict[str, Any] = field(default_factory=dict) # Merged data for formatting
     # --- NEW FIELD ---
     metadata_error_message: Optional[str] = None # To store specific API error messages
     # --- END NEW FIELD ---