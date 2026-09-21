import argparse
from helpers import get_session, log_step


def parse_args():
    parser = argparse.ArgumentParser(description="Sessionize raw clickstream events.")
    parser.add_argument("--source-table", required=True, help="Raw events table")
    parser.add_argument("--output-table", required=True, help="Output sessions table")
    parser.add_argument("--inactivity-minutes", type=int, default=30, help="Session timeout in minutes")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = get_session()

    log_step(f"Sessionizing {args.source_table} (timeout: {args.inactivity_minutes} min)")

    session.sql(f"""
        CREATE OR REPLACE TABLE {args.output_table} AS
        WITH ordered_events AS (
            SELECT *,
                LAG(EVENT_TIMESTAMP) OVER (
                    PARTITION BY USER_ID ORDER BY EVENT_TIMESTAMP
                ) AS prev_event_ts
            FROM {args.source_table}
        ),
        session_boundaries AS (
            SELECT *,
                CASE
                    WHEN prev_event_ts IS NULL THEN 1
                    WHEN DATEDIFF('minute', prev_event_ts, EVENT_TIMESTAMP) > {args.inactivity_minutes} THEN 1
                    ELSE 0
                END AS is_new_session
            FROM ordered_events
        ),
        session_ids AS (
            SELECT *,
                USER_ID || '-' || SUM(is_new_session) OVER (
                    PARTITION BY USER_ID ORDER BY EVENT_TIMESTAMP
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS SESSION_ID
            FROM session_boundaries
        ),
        first_events AS (
            SELECT SESSION_ID, DEVICE, TRAFFIC_SOURCE
            FROM (
                SELECT SESSION_ID, DEVICE, TRAFFIC_SOURCE,
                    ROW_NUMBER() OVER (PARTITION BY SESSION_ID ORDER BY EVENT_TIMESTAMP) AS rn
                FROM session_ids
            )
            WHERE rn = 1
        ),
        session_agg AS (
            SELECT
                SESSION_ID,
                USER_ID,
                MIN(EVENT_TIMESTAMP)                                          AS SESSION_START,
                MAX(EVENT_TIMESTAMP)                                          AS SESSION_END,
                DATEDIFF('second', MIN(EVENT_TIMESTAMP), MAX(EVENT_TIMESTAMP)) AS DURATION_SECONDS,
                COUNT(*)                                                       AS TOTAL_EVENTS,
                SUM(CASE WHEN EVENT_TYPE = 'page_view' THEN 1 ELSE 0 END)     AS PAGE_VIEWS,
                SUM(CASE WHEN EVENT_TYPE = 'product_view' THEN 1 ELSE 0 END)  AS PRODUCT_VIEWS,
                SUM(CASE WHEN EVENT_TYPE = 'add_to_cart' THEN 1 ELSE 0 END)   AS ADD_TO_CARTS,
                SUM(CASE WHEN EVENT_TYPE = 'purchase' THEN 1 ELSE 0 END)      AS PURCHASES,
                SUM(REVENUE)                                                   AS SESSION_REVENUE,
                MAX(CASE WHEN EVENT_TYPE = 'purchase' THEN 1 ELSE 0 END) = 1   AS CONVERTED
            FROM session_ids
            GROUP BY SESSION_ID, USER_ID
        )
        SELECT
            sa.*,
            fe.DEVICE,
            fe.TRAFFIC_SOURCE
        FROM session_agg sa
        JOIN first_events fe ON sa.SESSION_ID = fe.SESSION_ID
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {args.output_table}").collect()[0]['CNT']
    log_step(f"Created {count} sessions in {args.output_table}")

    # Quick summary
    summary = session.sql(f"""
        SELECT
            COUNT(*)                          AS total_sessions,
            SUM(CASE WHEN CONVERTED THEN 1 ELSE 0 END) AS converted_sessions,
            ROUND(AVG(DURATION_SECONDS), 1)   AS avg_duration_sec,
            ROUND(AVG(TOTAL_EVENTS), 1)       AS avg_events_per_session
        FROM {args.output_table}
    """).collect()[0]
    log_step(f"Summary: {summary['TOTAL_SESSIONS']} sessions, "
             f"{summary['CONVERTED_SESSIONS']} converted, "
             f"avg {summary['AVG_DURATION_SEC']}s duration")
    log_step("Done")


if __name__ == "__main__":
    main()
