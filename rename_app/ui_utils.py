# rename_app/ui_utils.py
import sys
import time
import builtins # Ensure builtins is imported for fallback print
from typing import Any, Optional, List, Dict, TYPE_CHECKING, Union # Added List for Prompt fallback type hint

# --- RICH IMPORT FOR UI COMPONENTS ---

# Flags to track individual Rich component availability
_RICH_CONSOLE_OK = False
_RICH_PROMPT_OK = False
_RICH_TABLE_OK = False
_RICH_TEXT_OK = False
_RICH_PANEL_OK = False
_RICH_PROGRESS_OK = False
_RICH_TASKID_OK = False # Though TaskID is often just an int or type alias

# Initialize Rich component variables to None
RichConsoleActual, RichConfirm, RichPrompt, RichInvalidResponse = None, None, None, None
RichTable, RichText, RichPanel, RichProgress = None, None, None, None
RichBarColumn, RichProgressTextColumn, RichTimeElapsedColumn, RichMofNCompleteColumn = None, None, None, None
RichTaskID_actual = None # Use a different name to avoid conflict if RichTaskID is a simple type

print("DEBUG ui_utils: Starting Rich component imports...")

try:
    from rich.console import Console as RichConsoleActual_
    RichConsoleActual = RichConsoleActual_
    _RICH_CONSOLE_OK = True
    print("DEBUG ui_utils: RichConsoleActual imported successfully.")
except ImportError:
    print("DEBUG ui_utils: Failed to import rich.console.Console.")
    pass

try:
    from rich.prompt import Confirm as RichConfirm_, Prompt as RichPrompt_, InvalidResponse as RichInvalidResponse_
    RichConfirm, RichPrompt, RichInvalidResponse = RichConfirm_, RichPrompt_, RichInvalidResponse_
    _RICH_PROMPT_OK = True
    print(f"DEBUG ui_utils: RichPrompt imported successfully: {RichPrompt}")
except ImportError:
    print("DEBUG ui_utils: Failed to import from rich.prompt (Confirm, Prompt, InvalidResponse).")
    pass

try:
    from rich.table import Table as RichTable_
    RichTable = RichTable_
    _RICH_TABLE_OK = True
    print("DEBUG ui_utils: RichTable imported successfully.")
except ImportError:
    print("DEBUG ui_utils: Failed to import rich.table.Table.")
    pass

try:
    from rich.text import Text as RichText_
    RichText = RichText_
    _RICH_TEXT_OK = True
    print("DEBUG ui_utils: RichText imported successfully.")
except ImportError:
    print("DEBUG ui_utils: Failed to import rich.text.Text.")
    pass

try:
    from rich.panel import Panel as RichPanel_
    RichPanel = RichPanel_
    _RICH_PANEL_OK = True
    print("DEBUG ui_utils: RichPanel imported successfully.")
except ImportError:
    print("DEBUG ui_utils: Failed to import rich.panel.Panel.")
    pass

try:
    from rich.progress import (
        Progress as RichProgress_,
        BarColumn as RichBarColumn_,
        TextColumn as RichProgressTextColumn_, # Keep alias to avoid conflict with fallback TextColumn
        TimeElapsedColumn as RichTimeElapsedColumn_,
        MofNCompleteColumn as RichMofNCompleteColumn_,
        TaskID as RichTaskID_ # Alias for TaskID from rich.progress
    )
    RichProgress, RichBarColumn, RichProgressTextColumn, RichTimeElapsedColumn, RichMofNCompleteColumn = \
        RichProgress_, RichBarColumn_, RichProgressTextColumn_, RichTimeElapsedColumn_, RichMofNCompleteColumn_
    RichTaskID_actual = RichTaskID_ # Store the imported TaskID type
    _RICH_PROGRESS_OK = True
    _RICH_TASKID_OK = True # If progress imports, TaskID should too
    print("DEBUG ui_utils: Rich Progress components imported successfully.")
except ImportError:
    print("DEBUG ui_utils: Failed to import from rich.progress.")
    pass


# Determine overall Rich availability based on *essential* components
# You can decide which ones are absolutely essential for RICH_AVAILABLE_UI
RICH_AVAILABLE_UI = _RICH_CONSOLE_OK and _RICH_PROMPT_OK and _RICH_TEXT_OK # Example: Console, Prompt, and Text are essential
print(f"DEBUG ui_utils: Overall RICH_AVAILABLE_UI set to: {RICH_AVAILABLE_UI}")
print(f"DEBUG ui_utils: _RICH_PROMPT_OK is: {_RICH_PROMPT_OK}")
if _RICH_PROMPT_OK and RichPrompt is not None:
    print(f"DEBUG ui_utils: RichPrompt (from rich library) is indeed: {RichPrompt}")
else:
    print("DEBUG ui_utils: RichPrompt (from rich library) is None or not imported.")


# --- Fallback Class Definitions (defined regardless, but used if Rich components are not OK) ---

class FallbackConsole:
    def __init__(self, quiet: bool = False, **kwargs: Any):
        self.quiet_mode = quiet
        # Other attributes for fallback if needed
        self.is_interactive: bool = False
        self.is_jupyter: bool = False
        self._live_display: Optional[Any] = None

    def print(self, *args: Any, **kwargs: Any) -> None:
        output_dest = kwargs.pop('file', sys.stdout)
        if self.quiet_mode and output_dest != sys.stderr: return
        processed_args = [
            (arg.plain if hasattr(arg, 'plain') and isinstance(getattr(arg, 'plain'), str)
             else arg.text if hasattr(arg, 'text') and isinstance(getattr(arg, 'text'), str) and not callable(getattr(arg, 'text'))
             else str(arg))
            for arg in args
        ]
        builtins.print(*processed_args, file=output_dest, **kwargs)

    def input(self, *args: Any, **kwargs: Any) -> str:
        return builtins.input(*args, **kwargs)

    def get_time(self) -> float: return time.monotonic()
    def rule(self, *args: Any, **kwargs: Any) -> None:
        if not self.quiet_mode: self.print("-" * (kwargs.get("characters", 70) if kwargs else 70))

    def status(self, *args: Any, **kwargs: Any) -> 'FallbackStatus': # Forward declaration
        return FallbackStatus(self, args[0] if args else "")

class FallbackStatus:
    def __init__(self, console_instance: FallbackConsole, message: str):
        self._console = console_instance
        self._message = message
    def start(self) -> None:
        if self._message and not self._console.quiet_mode : self._console.print(self._message)
    def stop(self) -> None: pass
    def __enter__(self) -> 'FallbackStatus': self.start(); return self
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None: self.stop()


class FallbackConfirm:
    @staticmethod
    def ask(prompt_text: str, default: bool = False, **kwargs: Any) -> bool:
        response = builtins.input(f"{prompt_text} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
        if not response: return default
        return response == 'y'

class FallbackPrompt:
    @staticmethod
    def ask(prompt_text: str, choices: Optional[List[str]] = None, default: Any = None, **kwargs: Any) -> str:
        full_prompt = prompt_text
        if choices:
            full_prompt += f" (choices: {', '.join(choices)})"
        if default is not None:
            full_prompt += f" [default: {default}]"
        full_prompt += ": "

        while True:
            response = builtins.input(full_prompt).strip()
            if not response and default is not None:
                return str(default)
            if choices and response not in choices:
                builtins.print(f"Invalid choice. Please select from: {', '.join(choices)}")
                continue
            return response

class FallbackInvalidResponse(Exception): pass

class FallbackTable:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.title = kwargs.get("title", "")
        self.rows: List[List[str]] = []
        self.columns: List[str] = []
        if self.title: builtins.print(f"\n--- {self.title} ---")

    def add_column(self, header: str, *args: Any, **kwargs: Any) -> None:
        self.columns.append(header)

    def add_row(self, *args: Any, **kwargs: Any) -> None:
        self.rows.append([str(arg.plain if hasattr(arg, 'plain') else arg) for arg in args])

    def __rich_console__(self, console: Any, options: Any) -> Any: # For compatibility if passed to Rich console
        if self.title: yield self.title
        if self.columns: yield " | ".join(self.columns)
        for row_data in self.rows: yield " | ".join(row_data)

    def _print_to_console(self, console_instance: FallbackConsole): # Actual print method for fallback
        if self.title and not console_instance.quiet_mode:
            console_instance.print(f"\n--- {self.title} ---")
        if self.columns and not console_instance.quiet_mode:
            # Simple column printing, adjust width as needed or use a more complex fallback
            console_instance.print(" | ".join(self.columns))
        if not console_instance.quiet_mode:
            for row_data in self.rows:
                console_instance.print(" | ".join(row_data))
# Note: The FallbackTable print needs to be called explicitly if not using Rich.
# For example, after populating, you might call console.print(table_instance)
# and the FallbackConsole would call table_instance.__str__ or similar.
# A better way for fallback table is to have its __str__ method format it.
# Or, for simplicity here, if ConsoleClass.print is called with a FallbackTable,
# FallbackConsole's print method should know how to render it.
# The above `_print_to_console` is a helper, actual rendering might need more logic
# in FallbackConsole or a __str__ method in FallbackTable.
# For now, the example relies on ConsoleClass.print iterating and calling str() on parts.


class FallbackText:
    def __init__(self, text_content: str = "", style: str = ""):
         self.text = text_content
         self.style = style # Style is ignored in fallback
    def __str__(self) -> str: return self.text
    @property
    def plain(self) -> str: return self.text
    @classmethod
    def assemble(cls, *parts: Any) -> 'FallbackText': # Basic assemble for fallback
        return FallbackText("".join(str(p) for p in parts))

class FallbackPanel:
     def __init__(self, content: Any, *args: Any, **kwargs: Any):
         self.content = content
         self.title = kwargs.get("title", "")
     def __str__(self) -> str:
         header = f"--- {self.title} ---\n" if self.title else ""
         return f"{header}{str(self.content)}\n--------------------"

class FallbackProgress:
    def __init__(self, *args: Any, **kwargs: Any):
        self.disable = kwargs.get('disable', False)
        self.console_instance = kwargs.get('console', FallbackConsole(quiet=self.disable))
        self.tasks: Dict[int, Dict[str, Any]] = {}
        self.task_id_counter = 0
        self.active = False

    def __enter__(self):
        self.active = True
        return self

    def __exit__(self, *args: Any):
        self.active = False
        if not self.disable and not self.console_instance.quiet_mode:
             self.console_instance.print("\nProgress finished.") # Simple notification

    def add_task(self, description: str, total: Optional[float] = None, start: bool = True, **fields: Any) -> int:
        self.task_id_counter += 1
        task_id = self.task_id_counter
        self.tasks[task_id] = {'description': description, 'total': total, 'completed': 0, 'fields': fields}
        if not self.disable and not self.console_instance.quiet_mode:
            msg = f"Starting: {description}"
            if total: msg += f" (0/{int(total) if total else '?'})"
            self.console_instance.print(msg)
        return task_id

    def update(self, task_id: int, advance: float = 1.0, description: Optional[str] = None, **fields: Any) -> None:
        if task_id not in self.tasks: return
        task = self.tasks[task_id]
        task['completed'] += advance
        if description: task['description'] = description
        task['fields'].update(fields)

        if not self.disable and not self.console_instance.quiet_mode:
            desc = task['description']
            item_name = task['fields'].get('item_name', '')
            progress_msg = f"  {desc}"
            if item_name : progress_msg += f": {item_name}"
            if task['total']:
                percent = (task['completed'] / task['total']) * 100 if task['total'] > 0 else 0
                progress_msg += f" [{int(task['completed'])}/{int(task['total'])} {percent:.0f}%]"
            # Only print updates periodically or on significant change for fallback
            if task.get('total') and (task['completed'] % (task['total'] // 10 or 1) == 0 or task['completed'] == task['total']):
                 self.console_instance.print(progress_msg)
            elif not task.get('total') and item_name: # For indeterminate tasks, print item name
                 self.console_instance.print(progress_msg)


    def stop(self):
        self.active = False

# Fallback for progress bar columns (mostly conceptual as they don't render directly in fallback)
class FallbackBarColumn: pass
class FallbackProgressTextColumn:
    def __init__(self, text_format: str, **kwargs: Any): pass # text_format used by Rich, ignored by fallback
class FallbackTimeElapsedColumn: pass
class FallbackMofNCompleteColumn: pass
FallbackTaskID = int # Simple integer for fallback TaskID type


# --- Assign the correct class based on individual component availability ---
if TYPE_CHECKING:
   # This block is only seen by type checkers like Pylance

    # Ensure RichConsoleActual and FallbackConsole are defined as classes before this
    _RichConsoleType = RichConsoleActual if RichConsoleActual else Any
    _FallbackConsoleType = FallbackConsole if FallbackConsole else Any
    ConsoleClass = Union[_RichConsoleType, _FallbackConsoleType] # <--- ADD/VERIFY THIS LINE

    # ... (other ...Class assignments for ProgressClass, TaskIDClass, etc.) ...
    _RichProgressType = RichProgress if RichProgress else Any
    _FallbackProgressType = FallbackProgress if FallbackProgress else Any
    ProgressClass = Union[_RichProgressType, _FallbackProgressType]

    _RichTaskIDType = RichTaskID_actual if RichTaskID_actual else Any
    _FallbackTaskIDType = FallbackTaskID
    TaskIDClass = Union[_RichTaskIDType, _FallbackTaskIDType]

    # ... (and for all other ...Class variables like TextClass, TableClass, etc.) ...
    _RichConfirmType = RichConfirm if RichConfirm else Any
    _FallbackConfirmType = FallbackConfirm if FallbackConfirm else Any
    ConfirmClass = Union[_RichConfirmType, _FallbackConfirmType]

    _RichPromptType = RichPrompt if RichPrompt else Any
    _FallbackPromptType = FallbackPrompt if FallbackPrompt else Any
    PromptClass = Union[_RichPromptType, _FallbackPromptType]

    # For exception classes, you can use type() or the class name directly if they are actual classes
    _RichInvalidResponseType = type(RichInvalidResponse) if RichInvalidResponse and isinstance(RichInvalidResponse, type) else Any # Check if it's a class type
    _FallbackInvalidResponseType = type(FallbackInvalidResponse) if FallbackInvalidResponse and isinstance(FallbackInvalidResponse, type) else Any
    InvalidResponseClass = Union[_RichInvalidResponseType, _FallbackInvalidResponseType]
    # A simpler way if they are always defined as classes:
    # InvalidResponseClass = Union[RichInvalidResponse, FallbackInvalidResponse]

    _RichTableType = RichTable if RichTable else Any
    _FallbackTableType = FallbackTable if FallbackTable else Any
    TableClass = Union[_RichTableType, _FallbackTableType]

    _RichTextType = RichText if RichText else Any
    _FallbackTextType = FallbackText if FallbackText else Any
    TextClass = Union[_RichTextType, _FallbackTextType]

    _RichPanelType = RichPanel if RichPanel else Any
    _FallbackPanelType = FallbackPanel if FallbackPanel else Any
    PanelClass = Union[_RichPanelType, _FallbackPanelType]
    
else:
    # In non-type-checking mode, we can directly assign the classes
    # based on the availability checks above.
    # This is more efficient and avoids unnecessary type hints.

    # Assign classes based on availability
    ConsoleClass = RichConsoleActual if _RICH_CONSOLE_OK and RichConsoleActual else FallbackConsole
    ConfirmClass = RichConfirm if _RICH_PROMPT_OK and RichConfirm else FallbackConfirm
    PromptClass = RichPrompt if _RICH_PROMPT_OK and RichPrompt else FallbackPrompt
    InvalidResponseClass = RichInvalidResponse if _RICH_PROMPT_OK and RichInvalidResponse else FallbackInvalidResponse
    TableClass = RichTable if _RICH_TABLE_OK and RichTable else FallbackTable
    TextClass = RichText if _RICH_TEXT_OK and RichText else FallbackText
    PanelClass = RichPanel if _RICH_PANEL_OK and RichPanel else FallbackPanel
    ProgressClass = RichProgress if _RICH_PROGRESS_OK and RichProgress else FallbackProgress

    BarColumnClass = RichBarColumn if _RICH_PROGRESS_OK and RichBarColumn else FallbackBarColumn
    ProgressTextColumnClass = RichProgressTextColumn if _RICH_PROGRESS_OK and RichProgressTextColumn else FallbackProgressTextColumn
    TimeElapsedColumnClass = RichTimeElapsedColumn if _RICH_PROGRESS_OK and RichTimeElapsedColumn else FallbackTimeElapsedColumn
    MofNCompleteColumnClass = RichMofNCompleteColumn if _RICH_PROGRESS_OK and RichMofNCompleteColumn else FallbackMofNCompleteColumn
    TaskIDClass = RichTaskID_actual if _RICH_TASKID_OK and RichTaskID_actual else FallbackTaskID

# print(f"DEBUG ui_utils: Final PromptClass is: {PromptClass}")