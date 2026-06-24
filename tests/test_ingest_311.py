"""Smoke test for the 311 ingestion logic.

Runs fully offline by mocking the HTTP layer, so it verifies that pagination,
the short-page stop condition, and file writing all behave -- without needing
network access or the live city API.

    python -m pytest tests/ -v
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.ingest import ingest_311


def _fake_response(rows):
    """Build a stand-in for a requests Response object."""
    resp = MagicMock()
    resp.json.return_value = rows
    resp.raise_for_status.return_value = None
    return resp


def test_ingest_paginates_and_writes(tmp_path, monkeypatch):
    # Redirect bronze output into a temp folder for the test.
    monkeypatch.setattr(ingest_311, "BRONZE_DIR", tmp_path / "sr_311")
    # Shrink the page size so a small fake dataset still triggers paging.
    monkeypatch.setattr(ingest_311, "PAGE_SIZE", 2)

    # First call returns a FULL page (2 rows) -> keep paging.
    # Second call returns a SHORT page (1 row)  -> stop.
    pages = [
        _fake_response([{"sr_number": "A1"}, {"sr_number": "A2"}]),
        _fake_response([{"sr_number": "A3"}]),
    ]

    with patch("requests.Session.get", side_effect=pages) as mock_get:
        total = ingest_311.ingest("2024-01-01", "2024-02-01")

    assert total == 3
    assert mock_get.call_count == 2

    out_files = list((tmp_path / "sr_311").glob("*.ndjson"))
    assert len(out_files) == 1

    lines = out_files[0].read_text().strip().splitlines()
    assert len(lines) == 3
    assert json.loads(lines[0])["sr_number"] == "A1"
    assert json.loads(lines[2])["sr_number"] == "A3"


def test_incremental_cli_computes_window(monkeypatch):
    captured = {}

    def fake_ingest(start_date, end_date):
        captured["start"] = start_date
        captured["end"] = end_date
        return 0

    monkeypatch.setattr(ingest_311, "ingest", fake_ingest)
    ingest_311.main(["--incremental", "3"])

    # A 3-day incremental run should produce a 4-day-wide window
    # (N days back + today, end exclusive).
    start = captured["start"]
    end = captured["end"]
    assert start < end
    assert len(start) == 10 and len(end) == 10  # YYYY-MM-DD
