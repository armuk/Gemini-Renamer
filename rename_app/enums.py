# rename_app/enums.py
from enum import Enum, auto

class ProcessingStatus(Enum):
    """
    Represents the status or reason for a particular outcome during processing.
    Used for standardized logging and potentially for more structured error/skip handling.
    """
    # General operational status
    SUCCESS = auto()
    SKIPPED = auto() # Generic skip, more specific reason might follow
    FAILED = auto()  # Generic failure, more specific reason might follow
    
    # Specific Skip/Fail Reasons
    # --- Pre-processing/Scanning ---
    FILE_TYPE_UNKNOWN = auto()
    AMBIGUOUS_VIDEO_FILES = auto()      # Multiple video files share the same base stem
    MISSING_VIDEO_FILE_IN_BATCH = auto()# Essential video file data missing for a batch

    # --- Metadata Phase ---
    METADATA_FETCH_API_ERROR = auto()   # General API communication error (timeout, connection)
    METADATA_NOT_FOUND_API = auto()     # API returned a definitive "not found" (e.g., 404)
    METADATA_NO_MATCH = auto()          # API searched, but no results met matching criteria (e.g., year, score)
    METADATA_INVALID_RESPONSE = auto()  # API response was unparseable or unexpected format
    METADATA_AUTH_ERROR = auto()        # API authentication/key error
    METADATA_CLIENT_UNAVAILABLE = auto() # TMDB/TVDB client not initialized

    # --- Planning Phase (after metadata or for guessit-only) ---
    CONFIG_MISSING_FORMAT_STRING = auto()# Required format string (e.g., series_format) not in config
    PATH_ALREADY_CORRECT = auto()       # Proposed new path is identical to original
    PLAN_TARGET_EXISTS_SKIP_MODE = auto() # on_conflict = skip, and target exists externally
    PLAN_TARGET_EXISTS_FAIL_MODE = auto() # on_conflict = fail, and target exists externally
    PLAN_MULTIPLE_SOURCES_TO_TARGET = auto() # Internal plan conflict: >1 original maps to same new_path
    PLAN_INVALID_GENERATED_NAME = auto()# Generated filename/foldername was empty or invalid after sanitization
    
    # --- Execution Phase (File System Operations) ---
    FILE_OPERATION_ERROR = auto()       # Generic OS/shutil error during file op (e.g., permissions)
    TRANSACTION_PHASE1_ERROR = auto()   # Error moving original to temporary path
    TRANSACTION_PHASE2_ERROR = auto()   # Error moving temporary to final path
    TRANSACTION_ROLLBACK_ERROR = auto() # Error during rollback of a transaction
    UNDO_INTEGRITY_FAILURE = auto()     # Undo integrity check failed (size/mtime/hash mismatch)
    
    # --- User Interaction ---
    USER_INTERACTIVE_SKIP = auto()      # User chose to skip the batch interactively
    USER_ABORTED_OPERATION = auto()     # User explicitly quit (e.g., Ctrl+C, 'q' in interactive)
    
    # --- Unknown File Handling Specific ---
    UNKNOWN_HANDLING_CONFIG_SKIP = auto() # Skipped due to unknown_file_handling='skip' setting
    UNKNOWN_HANDLING_MOVE_FAILED = auto() # 'move_to_unknown' operation failed
    UNKNOWN_HANDLING_GUESSIT_PLAN_FAILED = auto() # 'guessit_only' planning failed

    # --- Other ---
    INTERNAL_ERROR = auto()             # Catch-all for unexpected internal errors
    NOT_APPLICABLE = auto()             # When a reason code isn't relevant (e.g. for a successful op)

    def __str__(self):
        # Provides a more human-readable version of the enum member name
        return self.name.replace("_", " ").title()

    @property
    def description(self) -> str:
        # You can add more detailed descriptions here if needed for documentation
        # For now, __str__ is good enough for user messages.
        # Example:
        # if self == ProcessingStatus.FILE_TYPE_UNKNOWN:
        #     return "The type of the media (movie or series) could not be determined."
        return str(self)