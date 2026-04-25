import time
import pandas as pd
from memory_profiler import profile
from download_dataset import get_instagram_dataframe


@profile
def analyze_instagram_cohorts(csv_path: str) -> pd.DataFrame:
    start_time = time.time()

    df = get_instagram_dataframe()

    df["last_login_date"] = pd.to_datetime(df["last_login_date"], errors="coerce")

    if "uses_premium_features" in df.columns:
        df["uses_premium_features"] = (
            df["uses_premium_features"]
            .astype(str)
            .str.strip()
            .str.lower()
            .map({"yes": True, "no": False, "true": True, "false": False})
        )

    numeric_cols = [
        "daily_active_minutes_instagram",
        "sessions_per_day",
        "user_engagement_score",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["heavy_usage"] = (df["daily_active_minutes_instagram"] >= 120) | (
        df["sessions_per_day"] >= 10
    )

    group_cols = ["country", "income_level"]

    cohort_table = (
        df.groupby(group_cols, dropna=False)
        .agg(
            users=("country", "size"),
            avg_minutes=("daily_active_minutes_instagram", "mean"),
            pct_premium=("uses_premium_features", "mean"),
            pct_heavy_usage=("heavy_usage", "mean"),
            avg_engagement=("user_engagement_score", "mean"),
        )
        .reset_index()
    )

    cohort_table = cohort_table.sort_values(by="avg_engagement", ascending=False)

    end_time = time.time()

    print(f"Execution time: {end_time - start_time:.2f} seconds\n")
    print("Top 10 Cohorts by Average Engagement:")
    print("-" * 80)
    print(cohort_table.head(10).to_string(index=False))

    return cohort_table


if __name__ == "__main__":
    file_path = "04Practice/completed_practice/instagram_users_lifestyle.csv"
    result_df = analyze_instagram_cohorts(file_path)
