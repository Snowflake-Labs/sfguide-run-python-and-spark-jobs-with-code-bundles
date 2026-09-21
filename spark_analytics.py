import argparse
from functools import reduce
from snowflake.snowpark_connect import init_spark_session
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="Funnel analysis and product co-occurrence.")
    parser.add_argument("--events-table", required=True, help="Raw events table (fully qualified)")
    parser.add_argument("--funnel-output", required=True, help="Funnel metrics output table")
    parser.add_argument("--pairs-output", required=True, help="Product pairs output table")
    return parser.parse_args()


def build_funnel(events_df):
    """
    Compute a session-level conversion funnel per traffic_source x device.
    Counts how many sessions reached each step, with conversion rate and
    drop-off percentage from the previous step.
    """
    funnel_steps = ['page_view', 'product_view', 'add_to_cart', 'purchase']

    # For each session (approximated as user x traffic_source x device x date),
    # determine which funnel steps it reached
    session_steps = (
        events_df
        .withColumn("EVENT_DATE", F.to_date("EVENT_TIMESTAMP"))
        .groupBy("USER_ID", "TRAFFIC_SOURCE", "DEVICE", "EVENT_DATE")
        .agg(
            F.max(F.when(F.col("EVENT_TYPE") == "page_view", 1).otherwise(0)).alias("reached_page_view"),
            F.max(F.when(F.col("EVENT_TYPE") == "product_view", 1).otherwise(0)).alias("reached_product_view"),
            F.max(F.when(F.col("EVENT_TYPE") == "add_to_cart", 1).otherwise(0)).alias("reached_add_to_cart"),
            F.max(F.when(F.col("EVENT_TYPE") == "purchase", 1).otherwise(0)).alias("reached_purchase"),
        )
    )

    # Aggregate per traffic_source x device
    funnel_metrics = (
        session_steps
        .groupBy("TRAFFIC_SOURCE", "DEVICE")
        .agg(
            F.count("*").alias("total_sessions"),
            F.sum("reached_page_view").alias("page_view"),
            F.sum("reached_product_view").alias("product_view"),
            F.sum("reached_add_to_cart").alias("add_to_cart"),
            F.sum("reached_purchase").alias("purchase"),
        )
    )

    # Unpivot into rows with conversion rate and drop-off
    rows = []
    prev_step = None
    for step in funnel_steps:
        step_df = funnel_metrics.select(
            "TRAFFIC_SOURCE", "DEVICE",
            F.lit(step).alias("STEP"),
            F.col(step).alias("SESSIONS"),
            F.round(F.col(step) / F.col("total_sessions") * 100, 2).alias("CONVERSION_RATE"),
            # Drop-off from previous step
            F.round(
                (1 - F.col(step) / F.col(prev_step if prev_step else step)) * 100, 2
            ).alias("DROP_OFF_PCT"),
        )
        rows.append(step_df)
        prev_step = step

    funnel = reduce(lambda a, b: a.unionByName(b), rows)

    funnel = funnel.withColumn(
        "STEP_ORDER",
        F.when(F.col("STEP") == "page_view", 0)
         .when(F.col("STEP") == "product_view", 1)
         .when(F.col("STEP") == "add_to_cart", 2)
         .when(F.col("STEP") == "purchase", 3)
    )
    return funnel.orderBy("TRAFFIC_SOURCE", "DEVICE", "STEP_ORDER")


def build_product_pairs(events_df):
    """
    Find products frequently browsed together by the same user.
    Generates all product pairs per user, counts co-occurrence, computes lift.
    """
    product_views = (
        events_df
        .filter(F.col("EVENT_TYPE").isin("product_view", "add_to_cart", "purchase"))
        .filter(F.col("PRODUCT_ID").isNotNull())
        .select("USER_ID", "PRODUCT_ID", "CATEGORY")
        .distinct()
    )

    total_users = product_views.select("USER_ID").distinct().count()

    # Self-join to generate pairs (A < B to avoid duplicates)
    pairs = (
        product_views.alias("a")
        .join(product_views.alias("b"),
              (F.col("a.USER_ID") == F.col("b.USER_ID")) &
              (F.col("a.PRODUCT_ID") < F.col("b.PRODUCT_ID")))
        .select(
            F.col("a.PRODUCT_ID").alias("PRODUCT_A"),
            F.col("a.CATEGORY").alias("CATEGORY_A"),
            F.col("b.PRODUCT_ID").alias("PRODUCT_B"),
            F.col("b.CATEGORY").alias("CATEGORY_B"),
        )
    )

    # Count co-occurrences
    pair_counts = (
        pairs
        .groupBy("PRODUCT_A", "CATEGORY_A", "PRODUCT_B", "CATEGORY_B")
        .agg(F.count("*").alias("CO_OCCURRENCE_COUNT"))
    )

    # Individual product frequencies for lift
    product_freq = (
        product_views
        .groupBy("PRODUCT_ID")
        .agg((F.countDistinct("USER_ID") / F.lit(total_users)).alias("freq"))
    )

    # Join and compute lift
    result = (
        pair_counts
        .join(product_freq.alias("fa"), F.col("PRODUCT_A") == F.col("fa.PRODUCT_ID"))
        .join(product_freq.alias("fb"), F.col("PRODUCT_B") == F.col("fb.PRODUCT_ID"))
        .withColumn("LIFT", F.round(
            (F.col("CO_OCCURRENCE_COUNT") / F.lit(total_users)) /
            (F.col("fa.freq") * F.col("fb.freq")), 2))
        .select("PRODUCT_A", "CATEGORY_A", "PRODUCT_B", "CATEGORY_B",
                "CO_OCCURRENCE_COUNT", "LIFT")
        .orderBy(F.col("LIFT").desc())
    )
    return result


def main() -> None:
    args = parse_args()
    spark = init_spark_session()

    events = spark.table(args.events_table)
    print(f"Loaded {events.count()} events")

    # Funnel analysis
    print("Computing funnel metrics...")
    funnel = build_funnel(events)
    funnel.write.mode("overwrite").saveAsTable(args.funnel_output)
    print(f"Wrote funnel metrics to {args.funnel_output}")

    # Product co-occurrence
    print("Computing product co-occurrence pairs...")
    pairs = build_product_pairs(events)
    pairs.write.mode("overwrite").saveAsTable(args.pairs_output)
    print(f"Wrote product pairs to {args.pairs_output}")

    print("Done")


if __name__ == "__main__":
    main()
