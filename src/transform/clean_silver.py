"""Clean the raw bronze 311 data into a conformed, deduplicated silver dataset."""

import argparse
import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("clean_silver")

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"
CHI_LAT_MIN, CHI_LAT_MAX = 41.60, 42.10
CHI_LON_MIN, CHI_LON_MAX = -87.95, -87.50


def build_spark(app_name="silver_311"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "America/Chicago")
        .getOrCreate()
    )


def _ts(col):
    return F.to_timestamp(F.col(col), TS_FORMAT)


def _blank_to_null(col):
    c = F.trim(F.col(col))
    return F.when(c == "", None).otherwise(c)


def clean(df):
    df = (
        df
        .withColumn("created_ts", _ts("created_date"))
        .withColumn("closed_ts", _ts("closed_date"))
        .withColumn("last_modified_ts", _ts("last_modified_date"))
        .withColumn("community_area", _blank_to_null("community_area").cast("int"))
        .withColumn("ward", _blank_to_null("ward").cast("int"))
        .withColumn("latitude", _blank_to_null("latitude").cast("double"))
        .withColumn("longitude", _blank_to_null("longitude").cast("double"))
        .withColumn("zip_code", _blank_to_null("zip_code"))
    )

    # A request can appear more than once across pulls; keep the latest version.
    df = df.filter(F.col("sr_number").isNotNull())
    w = Window.partitionBy("sr_number").orderBy(F.col("last_modified_ts").desc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    hours = (F.col("closed_ts").cast("long") - F.col("created_ts").cast("long")) / 3600.0
    df = df.withColumn(
        "has_valid_resolution",
        F.col("closed_ts").isNotNull() & F.col("created_ts").isNotNull() & (hours >= 0),
    )
    # Negative time means closed-before-created, i.e. bad data: null it out.
    df = df.withColumn("resolution_hours", F.when(F.col("has_valid_resolution"), F.round(hours, 2)))
    df = df.withColumn("is_closed", F.col("closed_ts").isNotNull())

    df = df.withColumn(
        "has_valid_geo",
        F.coalesce(
            F.col("latitude").between(CHI_LAT_MIN, CHI_LAT_MAX)
            & F.col("longitude").between(CHI_LON_MIN, CHI_LON_MAX),
            F.lit(False),
        ),
    )

    df = (
        df
        .withColumn("created_date_only", F.to_date("created_ts"))
        .withColumn("created_year", F.year("created_ts"))
        .withColumn("created_month", F.month("created_ts"))
    )

    return df.select(
        "sr_number", "sr_type", "sr_short_code", "owner_department", "status", "origin",
        "created_ts", "closed_ts", "last_modified_ts",
        "resolution_hours", "is_closed", "has_valid_resolution",
        "community_area", "ward", "zip_code", "latitude", "longitude", "has_valid_geo",
        F.col("duplicate").alias("city_flagged_duplicate"), "legacy_record",
        "created_date_only", "created_year", "created_month",
    )


def run(input_path, output_path):
    spark = build_spark()
    bronze = spark.read.json(input_path)
    log.info("Bronze rows: %s", bronze.count())

    silver = clean(bronze)
    log.info("Silver rows after clean/dedupe: %s", silver.count())

    silver.write.mode("overwrite").partitionBy("created_year", "created_month").parquet(output_path)
    log.info("Wrote silver to %s", output_path)
    spark.stop()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Clean bronze 311 into the silver layer")
    parser.add_argument("--input", default="data/bronze/sr_311")
    parser.add_argument("--output", default="data/silver/sr_311")
    args = parser.parse_args(argv)
    run(args.input, args.output)


if __name__ == "__main__":
    main()
