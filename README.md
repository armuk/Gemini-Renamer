# Gemini-Renamer

🚀 A modular and extensible file renaming tool powered by Python.  
It uses metadata and custom configurations to automate renaming tasks safely, with undo capabilities.

---

## Features
- Rename files intelligently based on metadata
- Undo and rollback renaming operations
- Configurable behavior with `config.toml` and `.env`
- CLI interface for easy batch operations
- Well-structured, extensible Python modules

---

## Installation

# Clone the repo
git clone https://github.com/yourusername/Gemini-Renamer.git
cd Gemini-Renamer

# (Optional) Create a virtual environment
python3 -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies

pip install -r requirements.txt

# Usage

python rename_main.py --input-folder /path/to/files --config config.toml

Example

python rename_main.py --dry-run --verbose

#Available CLI options:
     --input-folder: Path to the directory containing files to rename

    --dry-run: Show what would happen without making changes

    --verbose: Print detailed logs

    (more options configured via cli.py)

# Configuration

    config.toml: General app settings

    .env: Environment-specific variables (e.g., API keys)

# Testing

pytest tests/

Tests cover:

    File system operations

    Renaming logic

    Config and metadata handling

    Utilities

# ßßContributing

Pull requests are welcome!
For major changes, please open an issue first to discuss what you would like to change.
License

MIT License