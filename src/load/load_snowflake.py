"""Load the silver Parquet into the Snowflake raw table using key-pair auth."""

import argparse
import logging
import os
import sys

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("load_snowflake")

TABLE = "SR_311"
# These can hold nulls; Int64 keeps them as integers rather than floats.
NULLABLE_INT_COLS = ["community_area", "ward", "created_year", "created_month"]


def prepare_dataframe(input_path):
    df = pd.read_parquet(input_path)
    log.info("Read %s rows", len(df))
    for col in NULLABLE_INT_COLS:
        if col in df.columns:
            df[col] = df[col].astype("Int64")
    df.columns = [c.upper() for c in df.columns]
    return df


def get_connection():
    import snowflake.connector

    for var in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER"):
        if not os.getenv(var):
            sys.exit(f"Missing required env var: {var}")

    params = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "chi_311_wh"),
        database=os.getenv("SNOWFLAKE_DATABASE", "chi_311"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "raw"),
    )

    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        params["private_key_file"] = os.path.expanduser(key_path)
        passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        if passphrase:
            params["private_key_file_pwd"] = passphrase
    elif os.getenv("SNOWFLAKE_PASSWORD"):
        params["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    else:
        sys.exit("Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")

    return snowflake.connector.connect(**params)


def load(input_path, dry_run=False):
    df = prepare_dataframe(input_path)

    if dry_run:
        log.info("Dry run: would load %s rows into %s", len(df), TABLE)
        print(df.dtypes.to_string())
        return

    from snowflake.connector.pandas_tools import write_pandas

    conn = get_connection()
    try:
        conn.cursor().execute(f"TRUNCATE TABLE IF EXISTS {TABLE}")
        success, _, n_rows, _ = write_pandas(conn, df, TABLE)
        log.info("Loaded %s rows (success=%s)", n_rows, success)
        count = conn.cursor().execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        log.info("Row count in %s: %s", TABLE, count)
    finally:
        conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Load silver Parquet into Snowflake")
    parser.add_argument("--input", default="data/silver/sr_311")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    load(args.input, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
