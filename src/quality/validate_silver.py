"""Validate the silver layer against a Great Expectations suite.

Reads the silver Parquet and checks it against an explicit set of data-quality
expectations. Exits non-zero if any expectation fails, so it can act as a gate
between the silver and load steps.
"""

import argparse
import sys

import pandas as pd
import great_expectations as gx
from great_expectations import expectations as gxe


def build_suite(context):
    suite = context.suites.add(gx.ExpectationSuite(name="silver_311_suite"))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="sr_number"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="sr_number"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="status"))

    # Resolution time, where present, is never negative.
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="resolution_hours", min_value=0, mostly=1.0)
    )

    # Community areas are 1-77 (nulls allowed for info-only calls).
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="community_area", min_value=1, max_value=77)
    )

    # Coordinates, where present, fall within Chicago's bounds.
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="latitude", min_value=41.6, max_value=42.1)
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="longitude", min_value=-87.95, max_value=-87.5)
    )

    # Quality flags are real booleans.
    for col in ("has_valid_resolution", "has_valid_geo", "is_closed"):
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column=col, value_set=[True, False]))

    return suite


def validate(input_path):
    df = pd.read_parquet(input_path)
    print(f"Validating {len(df)} rows from {input_path}")

    context = gx.get_context()
    data_source = context.data_sources.add_pandas("silver")
    asset = data_source.add_dataframe_asset("sr_311")
    batch_definition = asset.add_batch_definition_whole_dataframe("batch")

    suite = build_suite(context)
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(name="silver_validation", data=batch_definition, suite=suite)
    )

    result = validation_definition.run(batch_parameters={"dataframe": df})

    print(f"\nSuccess: {result.success}")
    for r in result.results:
        status = "PASS" if r.success else "FAIL"
        exp = r.expectation_config
        col = exp.kwargs.get("column", "")
        print(f"  {status}  {exp.type}  {col}")

    return result.success


def main(argv=None):
    parser = argparse.ArgumentParser(description="Validate the silver layer with Great Expectations")
    parser.add_argument("--input", default="data/silver/sr_311")
    args = parser.parse_args(argv)

    if not validate(args.input):
        sys.exit(1)


if __name__ == "__main__":
    main()
