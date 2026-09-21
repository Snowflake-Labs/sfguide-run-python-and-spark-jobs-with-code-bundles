from datetime import datetime, timezone
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session


def get_session() -> Session:
    try:
        return get_active_session()
    except Exception:
        raise RuntimeError(
            "No active Snowpark session found. "
            "Code Bundles automatically inject a session — "
            "ensure this script is executed via EXECUTE CODE BUNDLE."
        )


def log_step(message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)
