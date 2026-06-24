"""Pull Chicago 311 service requests from the Socrata API into the bronze layer."""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("ingest_311")

DATASET_ID = os.getenv("SOCRATA_DATASET_ID", "v6vf-nfxy")
BASE_URL = f"https://data.cityofchicago.org/resource/{DATASET_ID}.json"
DATE_FIELD = "created_date"
PAGE_SIZE = 50_000
APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
BRONZE_DIR = Path(os.getenv("BRONZE_DIR", "data/bronze/sr_311"))


def make_session():
    session = requests.Session()
    if APP_TOKEN:
        session.headers.update({"X-App-Token": APP_TOKEN})
    return session


def fetch_page(session, where_clause, offset, max_retries=5):
    params = {
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$order": DATE_FIELD,
        "$where": where_clause,
    }
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            wait = min(2 ** attempt, 30)
            log.warning("Request failed (%s/%s): %s -- retrying in %ss", attempt, max_retries, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch offset {offset} after {max_retries} attempts")


def ingest(start_date, end_date):
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    where = f"{DATE_FIELD} >= '{start_date}T00:00:00' AND {DATE_FIELD} < '{end_date}T00:00:00'"
    log.info("Ingesting where %s", where)

    session = make_session()
    out_path = BRONZE_DIR / f"sr_311_{start_date}_to_{end_date}.ndjson"

    offset = 0
    total = 0
    with out_path.open("w", encoding="utf-8") as f:
        while True:
            rows = fetch_page(session, where, offset)
            if not rows:
                break
            for row in rows:
                f.write(json.dumps(row) + "\n")
            total += len(rows)
            log.info("Fetched %s rows (offset %s), total %s", len(rows), offset, total)
            if len(rows) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

    log.info("Wrote %s rows to %s", total, out_path)
    return total


def main(argv=None):
    parser = argparse.ArgumentParser(description="Ingest Chicago 311 into the bronze layer")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (exclusive)")
    parser.add_argument("--incremental", type=int, metavar="DAYS", help="Ingest the last N days")
    args = parser.parse_args(argv)

    if args.incremental:
        end = datetime.now(timezone.utc).date() + timedelta(days=1)
        start = end - timedelta(days=args.incremental + 1)
        ingest(start.isoformat(), end.isoformat())
    elif args.start and args.end:
        ingest(args.start, args.end)
    else:
        parser.error("provide either --incremental N, or both --start and --end")


if __name__ == "__main__":
    main()
