class RenamerError(Exception):
    """Base class for application-specific errors."""
    pass

class ConfigError(RenamerError):
    """Errors related to configuration loading or validation."""
    pass

class MetadataError(RenamerError):
    """Errors related to fetching or processing metadata."""
    pass

class FileOperationError(RenamerError):
    """Errors during file system operations."""
    pass

class UserAbortError(RenamerError):
    """Error raised when user cancels an operation."""
    pass