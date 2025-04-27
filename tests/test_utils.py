# tests/test_utils.py
import pytest
from pathlib import Path
import langcodes # Keep this import
from unittest.mock import MagicMock, patch

# Import the module to test, assume it's in the parent directory structure
from rename_app import utils

# --- Test sanitize_filename ---
@pytest.mark.parametrize("input_str, expected", [
    ("Valid Filename.mkv", "Valid_Filename.mkv"),
    ("File with<>:\"/\\|?*chars.mp4", "File_with_chars.mp4"),
    ("  Leading and trailing spaces . .txt", "Leading_and_trailing_spaces_._.txt"),
    ("  Ends with spaces and dots . . ", "Ends_with_spaces_and_dots"),
    ("Multiple___Underscores___.srt", "Multiple_Underscores_.srt"),
    (".HiddenFile", ".HiddenFile"), # Should preserve leading dot
    ("", "_invalid_name_"),
    # Corrected expectation based on final logic (becomes '_', triggers invalid state)
    ("<>:\"/\\|?*", "_invalid_underscores_"),
    ("...", "_invalid_dots_"),
    (" . . . ", "_invalid_name_"), # Corrected expectation - becomes empty
    ("___", "_invalid_underscores_"),
])
def test_sanitize_filename(input_str, expected):
    """Tests the filename sanitation function."""
    assert utils.sanitize_filename(input_str) == expected

# --- Test extract_scene_tags ---
# Define sample tags list as a tuple for hashability with lru_cache
SAMPLE_TAGS_TUPLE = ("PROPER", "REPACK", "REAL", "RERIP", "LIMITED", "INTERNAL", "UNCUT", "DC")
@pytest.mark.parametrize("filename, tags_tuple, expected_list, expected_dot", [
    ("My.Show.S01E01.PROPER.mkv", SAMPLE_TAGS_TUPLE, ["PROPER"], ".PROPER"),
    ("Movie.Title.2023.REPACK.1080p.mkv", SAMPLE_TAGS_TUPLE, ["REPACK"], ".REPACK"),
    # Expect both tags now with improved regex, order based on tuple
    ("Another.S02E03.REAL.PROPER.mkv", SAMPLE_TAGS_TUPLE, ["PROPER", "REAL"], ".PROPER.REAL"),
    ("Show.S03E04.mkv", SAMPLE_TAGS_TUPLE, [], ""),
    ("File.With.internal.Source.mkv", SAMPLE_TAGS_TUPLE, ["INTERNAL"], ".INTERNAL"),
    ("No Tags Here", tuple(), [], ""), # Empty tags tuple test
    # Expect both tags now with improved regex, order based on tuple
    ("File.DC.LIMITED.mkv", SAMPLE_TAGS_TUPLE, ["LIMITED", "DC"], ".LIMITED.DC"),
    ("[Grp] Show.S01E01 (UNCUT) [1080p].mkv", SAMPLE_TAGS_TUPLE, ["UNCUT"], ".UNCUT"),
    ("Show.S01E01.CustomTag.mkv", SAMPLE_TAGS_TUPLE, [], ""), # Tag not in list
    ("File.proper.mkv", SAMPLE_TAGS_TUPLE, ["PROPER"], ".PROPER"), # Lowercase in filename match
    ("File.ends.with.REAL", SAMPLE_TAGS_TUPLE, ["REAL"], ".REAL"), # Tag at the very end
    ("LIMITED.file.starts.with.it.mkv", SAMPLE_TAGS_TUPLE, ["LIMITED"], ".LIMITED"), # Tag at the start
    ("Two.Tags.LIMITED.REPACK.File.mkv", SAMPLE_TAGS_TUPLE, ["REPACK","LIMITED"], ".REPACK.LIMITED"), # Check order again
])
def test_extract_scene_tags(filename, tags_tuple, expected_list, expected_dot):
    """Tests the scene tag extraction function."""
    # Pass the tuple directly
    tags, dot_str = utils.extract_scene_tags(filename, tags_tuple)
    assert tags == expected_list, f"Filename: {filename}"
    assert dot_str == expected_dot, f"Filename: {filename}"


# --- Test parse_subtitle_language ---
@pytest.mark.skipif(not utils.LANGCODES_AVAILABLE, reason="langcodes library not installed")
@pytest.mark.parametrize("filename, expected_lang, expected_flags", [
    ("sub.eng.srt", "eng", []),
    ("sub.en.srt", "eng", []),
    ("subtitle.fre.forced.sub", "fra", ["forced"]),
    ("mysub.spa.sdh.cc.vtt", "spa", ["sdh"]),
    ("NoLang.srt", None, []),
    ("Foreign.Name.German.Forced.srt", "deu", ["forced"]),
    ("Weird.Separator-jpn_sdh.ass", "jpn", ["sdh"]),
    ("Movie.Title.pt-BR.srt", "por", []),
    ("Movie.Title.pt.BR.srt", None, []),
    ("Movie.Title.pob.srt", "por", []),
    ("Movie.Title.BR.srt", None, []),
    ("My.Show.S01E01.720p.BluRay.x264-GRP.cze.forced.srt", "ces", ["forced"]),
    ("Show.S01.E01.FR.srt", "fra", []),
    ("Show Name S01E01 Español SDH.srt", "spa", ["sdh"]),
])
def test_parse_subtitle_language(filename, expected_lang, expected_flags, mocker):
    """Tests subtitle language and flag parsing, mocking guessit results."""
    # Mock guessit result specifically for this test run
    mock_guessit_instance = mocker.patch('rename_app.utils.guessit')
    mock_return = {'type': 'subtitle'} # Default mock: find nothing useful

    # Setup specific mocks needed (ensure lang obj mock has to_alpha3 if needed)
    # This structure ensures the correct mock is active for each parameter set
    if filename == "Movie.Title.pt-BR.srt":
        try:
            lang_obj = langcodes.get('pt-BR') # Try to get real object first
            lang_obj_mock = MagicMock(spec=lang_obj) # Create mock based on real obj if possible
            lang_obj_mock.to_alpha3.return_value = 'por'
            mock_return = {'language': lang_obj_mock, 'type': 'subtitle'}
        except LookupError: # If langcodes doesn't know pt-BR
             # Create a generic mock that still has the method
             lang_obj_mock = MagicMock(); lang_obj_mock.to_alpha3.return_value = 'por';
             mock_return = {'language': lang_obj_mock, 'type': 'subtitle'}
    elif filename == "Foreign.Name.German.Forced.srt":
         lang_obj_mock = MagicMock(); lang_obj_mock.to_alpha3.return_value = 'deu'
         mock_return = {'language': lang_obj_mock, 'type': 'subtitle', 'other': ['forced']}
    elif filename == "Show Name S01E01 Español SDH.srt":
         lang_obj_mock = MagicMock(); lang_obj_mock.to_alpha3.return_value = 'spa'
         mock_return = {'language': lang_obj_mock, 'type': 'subtitle', 'other': ['SDH']}
    elif filename in ["sub.eng.srt", "sub.en.srt"]:
        lang_obj_mock = MagicMock(); lang_obj_mock.to_alpha3.return_value = 'eng'
        mock_return = {'language': lang_obj_mock, 'type': 'subtitle'}
    elif filename == "subtitle.fre.forced.sub":
        lang_obj_mock = MagicMock(); lang_obj_mock.to_alpha3.return_value = 'fra'
        mock_return = {'language': lang_obj_mock, 'type': 'subtitle', 'subtitle_forced': True}
    elif filename == "mysub.spa.sdh.cc.vtt":
        lang_obj_mock = MagicMock(); lang_obj_mock.to_alpha3.return_value = 'spa'
        mock_return = {'language': lang_obj_mock, 'type': 'subtitle', 'other': ['sdh', 'cc']}
    elif filename == "Weird.Separator-jpn_sdh.ass":
        lang_obj_mock = MagicMock(); lang_obj_mock.to_alpha3.return_value = 'jpn'
        mock_return = {'language': lang_obj_mock, 'type': 'subtitle', 'other': ['sdh']}
    elif filename == "Movie.Title.pob.srt":
        # Let regex handle 'pob' -> 'por' mapping
        mock_return = {'type': 'subtitle'}
    elif filename == "My.Show.S01E01.720p.BluRay.x264-GRP.cze.forced.srt":
        lang_obj_mock = MagicMock(); lang_obj_mock.to_alpha3.return_value = 'ces'
        mock_return = {'language': lang_obj_mock, 'type': 'subtitle', 'subtitle_forced': True}
    elif filename == "Show.S01.E01.FR.srt":
        lang_obj_mock = MagicMock(); lang_obj_mock.to_alpha3.return_value = 'fra'
        mock_return = {'language': lang_obj_mock, 'type': 'subtitle'}

    # Set the return value for the patched guessit
    mock_guessit_instance.return_value = mock_return

    # Run the function under test
    lang, flags, encoding = utils.parse_subtitle_language(filename, detect_enc=False)

    # Assertions
    assert lang == expected_lang, f"Lang mismatch for: {filename}"
    assert set(flags) == set(expected_flags), f"Flags mismatch for: {filename}"
    assert encoding is None, f"Encoding unexpected for: {filename}"


# --- Test scan_media_files ---
def test_scan_media_files_simple(test_files, mock_cfg_helper):
    """Tests scanning a directory non-recursively."""
    # This test should now pass with the fixed scan logic using _get_base_stem
    mock_cfg_helper.manager._mock_values = {
        'recursive': False,
        'video_extensions': ['.mkv', '.mp4', '.avi'],
        'associated_extensions': ['.srt', '.nfo', '.sub'] # Ensure SUB is assoc
    }
    batches = utils.scan_media_files(test_files, mock_cfg_helper)
    expected_stems_root = {
        "Series.Title.S01E01.720p.WEBRip.x264-Grp",
        "My Awesome Movie (2022) [1080p] {imdb-tt12345}",
        "Ambiguous S02E05 File",
        "multi.episode.s03e01-e03",
        "some.file.PROPER",
        "another.file.REPACK",
        "File Without Pattern",
    }
    found_stems = set(batches.keys())
    print(f"Simple Scan Found Stems: {found_stems}")
    assert found_stems == expected_stems_root, "Mismatch in expected stems found at root level"
    assert "Nested.Show.S05E10" not in batches, "Nested file found in non-recursive scan"

    series_batch = batches.get("Series.Title.S01E01.720p.WEBRip.x264-Grp")
    assert series_batch is not None, "Series batch not found"
    assert series_batch['video'].name == "Series.Title.S01E01.720p.WEBRip.x264-Grp.mkv", "Incorrect video file"
    assoc_names = {f.name for f in series_batch['associated']}
    # Assert both srt and nfo are present
    assert "Series.Title.S01E01.720p.WEBRip.x264-Grp.eng.srt" in assoc_names, "SRT file missing from associated"
    assert "Series.Title.S01E01.720p.WEBRip.x264-Grp.nfo" in assoc_names, "NFO file missing from associated"

    multi_batch = batches.get("multi.episode.s03e01-e03")
    assert multi_batch is not None, "Multi-episode batch not found"
    assert multi_batch['video'].name == "multi.episode.s03e01-e03.mkv", "Incorrect multi-episode video file"
    assoc_names_multi = {f.name for f in multi_batch['associated']}
    # This assertion should now pass because _get_base_stem strips '.forced.ger'
    assert "multi.episode.s03e01-e03.forced.ger.sub" in assoc_names_multi, "SUB file missing from multi-episode associated"


def test_scan_media_files_recursive(test_files, mock_cfg_helper):
    """Tests scanning a directory recursively."""
    # (Test unchanged)
    mock_cfg_helper.manager._mock_values = { 'recursive': True, 'video_extensions': ['.mkv'], 'associated_extensions': [] }
    batches = utils.scan_media_files(test_files, mock_cfg_helper)
    found_stems = set(batches.keys())
    print(f"Recursive Scan Found Stems: {found_stems}")
    expected_mkv_stems = { "Series.Title.S01E01.720p.WEBRip.x264-Grp", "multi.episode.s03e01-e03", "File Without Pattern", "some.file.PROPER", "another.file.REPACK", "Nested.Show.S05E10" }
    assert found_stems == expected_mkv_stems
    assert "My Awesome Movie (2022) [1080p] {imdb-tt12345}" not in batches
    assert "Ambiguous S02E05 File" not in batches
    nested_batch = batches.get("Nested.Show.S05E10"); assert nested_batch['associated'] == []