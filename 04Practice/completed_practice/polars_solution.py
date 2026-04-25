import time
import polars as pl
from memory_profiler import profile
from download_dataset import get_instagram_dataframe


@profile
def analyze_instagram_cohorts_polars(csv_path: str) -> pl.DataFrame:

    start = time.time()

    pandas_df = get_instagram_dataframe()
    df = pl.from_pandas(pandas_df)

    df = df.with_columns(
        pl.col("last_login_date").str.to_date(strict=False).alias("last_login_date"),
        pl.when(
            pl.col("uses_premium_features").cast(pl.Utf8).str.to_lowercase() == "yes"
        )
        .then(True)
        .when(pl.col("uses_premium_features").cast(pl.Utf8).str.to_lowercase() == "no")
        .then(False)
        .otherwise(None)
        .alias("uses_premium_features"),
        (
            (
                pl.col("daily_active_minutes_instagram").cast(pl.Float64, strict=False)
                >= 120
            )
            | (pl.col("sessions_per_day").cast(pl.Float64, strict=False) >= 10)
        ).alias("heavy_usage"),
    )

    group_cols = ["country", "income_level"]

    cohort_table = df.group_by(group_cols).agg(
        pl.len().alias("users"),
        pl.col("daily_active_minutes_instagram")
        .cast(pl.Float64)
        .mean()
        .alias("avg_minutes"),
        pl.col("uses_premium_features").mean().alias("pct_premium"),
        pl.col("heavy_usage").mean().alias("pct_heavy_usage"),
        pl.col("user_engagement_score").cast(pl.Float64).mean().alias("avg_engagement"),
    )

    cohort_table = cohort_table.sort("avg_engagement", descending=True)

    end = time.time()

    print(f"Execution time (Polars): {end - start:.2f} seconds\n")
    print("Top 10 Cohorts by Average Engagement:")
    print("-" * 80)
    print(cohort_table.head(10))

    return cohort_table


if __name__ == "__main__":
    file_path = "04Practice/completed_practice/instagram_users_lifestyle.csv"
    analyze_instagram_cohorts_polars(file_path)
