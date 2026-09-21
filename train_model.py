import argparse
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from snowflake.ml.registry import Registry
from snowflake.snowpark.context import get_active_session


def parse_args():
    parser = argparse.ArgumentParser(description="Train a purchase amount predictor.")
    parser.add_argument("--source-table", required=True, help="Fully qualified source table")
    parser.add_argument("--database", required=True, help="Database for the model registry")
    parser.add_argument("--schema", required=True, help="Schema for the model registry")
    parser.add_argument("--model-name", default="PURCHASE_AMOUNT_PREDICTOR", help="Model name")
    parser.add_argument("--version", default="v1", help="Model version")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = get_active_session()

    # Load purchase events
    rows = session.table(args.source_table).filter(
        "EVENT_TYPE = 'purchase'"
    ).select("PRODUCT_ID", "CATEGORY", "REVENUE").collect()
    df = pd.DataFrame([row.as_dict() for row in rows])

    X = df[["PRODUCT_ID", "CATEGORY"]]
    y = df["REVENUE"].astype(float)

    # One-hot encode category, pass through product_id, predict revenue
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), ["CATEGORY"]),
            ("num", "passthrough", ["PRODUCT_ID"])
        ]
    )
    model = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("regressor", Ridge(alpha=1.0))
    ])
    model.fit(X, y)

    score = model.score(X, y)
    print(f"Model R² on training data: {score:.4f}")

    # Log to Model Registry
    registry = Registry(session=session, database_name=args.database, schema_name=args.schema)
    registry.log_model(
        model,
        model_name=args.model_name,
        version_name=args.version,
        sample_input_data=X.head(),
    )
    print(f"Model logged to registry as {args.model_name} {args.version}")


if __name__ == "__main__":
    main()
