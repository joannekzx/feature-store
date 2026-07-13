"""
Step 7 -- Point-in-time (as-of) join + training dataset export.

Two things happen here:
  1. prove_pit()      -- a deterministic proof that the as-of join prevents label
                         leakage. Runs fully offline (no S3). This is the interview artifact.
  2. build_training() -- reads the real feature history written to S3 by the Flink job,
                         joins synthetic labels to the latest feature value at-or-before
                         each label's timestamp, and writes a training dataset.

Usage:
  python pit_join.py           # run the proof, then build a training set from S3
  python pit_join.py --demo    # run only the proof (no network / AWS needed)
"""
import argparse
import pandas as pd

BUCKET = "feature-store-joanne-demo"
FEATURES_URI = f"s3://{BUCKET}/features/"


def asof_join(labels: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    """Attach to each label the latest feature with feature_timestamp <= label_ts.

    This is a merge-on-nearest-backward join, keyed per entity. It is the whole point of
    the offline store: a label at time T must only see feature values that existed at or
    before T -- never a value computed afterwards.
    """
    labels = labels.sort_values("label_ts")
    features = features.sort_values("feature_timestamp")
    return pd.merge_asof(
        labels,
        features,
        left_on="label_ts",
        right_on="feature_timestamp",
        by="entity_id",
        direction="backward",  # nearest feature at-or-before the label time
    )


def prove_pit() -> None:
    print("=" * 64)
    print("POINT-IN-TIME CORRECTNESS PROOF")
    print("=" * 64)

    # user-1's feature changes from 10.0 -> 99.0 at t=200.
    features = pd.DataFrame({
        "entity_id":         ["user-1", "user-1"],
        "feature_timestamp": [100,       200],
        "total_amount":      [10.0,      99.0],
    })
    # Two labels: one just BEFORE the change, one just AFTER.
    labels = pd.DataFrame({
        "entity_id": ["user-1", "user-1"],
        "label_ts":  [150,       250],
        "label":     [0,         1],
    })

    asof = asof_join(labels, features)

    # The naive join = "use whatever the value is now" (latest feature) -- the SAME value
    # for every label, regardless of when the label happened.
    latest = features.sort_values("feature_timestamp").groupby("entity_id").tail(1)
    naive_value = latest.set_index("entity_id")["total_amount"]

    out = asof[["entity_id", "label_ts", "total_amount"]].rename(
        columns={"total_amount": "asof_value"})
    out["naive_value"] = out["entity_id"].map(naive_value)
    out["leaked?"] = out["asof_value"] != out["naive_value"]

    print(out.to_string(index=False))
    print()
    print("Feature history: user-1 total_amount = 10.0 at t=100, then 99.0 at t=200.")
    print("Label at t=150 must see 10.0 (the value that existed then), NOT 99.0.")
    print("  - naive 'use latest' join hands 99.0 to the t=150 label  -> FUTURE LEAKAGE")
    print("  - as-of join gives 10.0 at t=150 and 99.0 at t=250        -> correct")
    print()


def build_training() -> None:
    print("=" * 64)
    print(f"BUILDING TRAINING SET FROM {FEATURES_URI}")
    print("=" * 64)

    features = pd.read_parquet(FEATURES_URI, engine="pyarrow")
    features = features[["entity_id", "feature_timestamp", "total_amount", "event_type"]]
    print(f"Loaded {len(features)} feature records for "
          f"{features['entity_id'].nunique()} entities.")

    # Synthetic labels: one per entity, timestamped at the midpoint of that entity's
    # feature-time range -- so the as-of join has to reach back past later features.
    g = features.groupby("entity_id")["feature_timestamp"]
    mid = ((g.min() + g.max()) // 2).reset_index()
    mid.columns = ["entity_id", "label_ts"]
    mid["label"] = 1  # placeholder label

    training = asof_join(mid, features)
    training = training[["entity_id", "label_ts", "label",
                         "total_amount", "event_type", "feature_timestamp"]]
    print(training.to_string(index=False))

    out_path = "training.parquet"
    training.to_parquet(out_path, index=False)
    print(f"\nWrote {len(training)} training rows -> {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true",
                        help="run only the offline proof (no S3 / AWS needed)")
    args = parser.parse_args()

    prove_pit()
    if not args.demo:
        build_training()
