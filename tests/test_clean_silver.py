"""Tests for the silver cleaning logic.

Spins up a local Spark session and runs the real `clean` transform against the
sample bronze file, asserting that each tricky case is handled. Requires Java
(same as the silver job itself).

    python -m pytest tests/test_clean_silver.py -v
"""

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from src.transform.clean_silver import clean


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .appName("test_silver")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "America/Chicago")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def cleaned(spark):
    bronze = spark.read.json("tests/sample_bronze.ndjson")
    rows = {r["sr_number"]: r for r in clean(bronze).collect()}
    return rows


def test_deduplicates_on_sr_number(cleaned):
    # 6 bronze rows, one is a duplicate sr_number -> 5 unique rows out.
    assert len(cleaned) == 5


def test_dedupe_keeps_latest_modified_version(cleaned):
    # The duplicate's later version was "Completed", not the earlier "Open".
    assert cleaned["SR24-0003"]["status"] == "Completed"


def test_normal_resolution_is_computed(cleaned):
    # Created 01-02 08:00 -> closed 01-05 08:00 == 72 hours.
    assert cleaned["SR24-0001"]["resolution_hours"] == pytest.approx(72.0)
    assert cleaned["SR24-0001"]["has_valid_resolution"] is True


def test_negative_resolution_is_flagged_and_nulled(cleaned):
    # Closed BEFORE created -> invalid; value nulled, flag false.
    assert cleaned["SR24-0004"]["resolution_hours"] is None
    assert cleaned["SR24-0004"]["has_valid_resolution"] is False


def test_blank_community_area_becomes_null(cleaned):
    assert cleaned["SR24-0005"]["community_area"] is None


def test_missing_geo_is_flagged(cleaned):
    assert cleaned["SR24-0005"]["has_valid_geo"] is False
    assert cleaned["SR24-0001"]["has_valid_geo"] is True
