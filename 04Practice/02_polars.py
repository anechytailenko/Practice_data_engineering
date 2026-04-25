# polars_analysis.py
import time
from memory_profiler import profile
import polars as pl


@profile
def analyze_instagram_users_polars(csv_path: str = "instagram_users.csv") -> pl.DataFrame:
    """
    Complex query (Polars) with same logic as the pandas version:
    - Parse/normalize types (dates + booleans + numerics)
    - Derived metrics (BP category, activity level, heavy usage flag)
    - Filter to recent users
    - Cohort aggregation by (country, gender, income_level)
    - Rank cohorts with a composite score

    Returns: aggregated cohort-level Polars DataFrame sorted by ranking.
    """
    start = time.time()

    df = pl.read_csv(csv_path, ignore_errors=True)

    # ---- Typing / normalization ----
    df = df.with_columns(
        # Date parsing
        pl.col("last_login_date").str.strptime(pl.Date, strict=False).alias("last_login_date"),

        # Booleans from Yes/No
        pl.col("uses_premium_features").cast(pl.Utf8).str.to_lowercase().map_elements(
            lambda x: True if x == "yes" else False if x == "no" else None,
            return_dtype=pl.Boolean,
        ).alias("uses_premium_features"),
        pl.col("two_factor_auth_enabled").cast(pl.Utf8).str.to_lowercase().map_elements(
            lambda x: True if x == "yes" else False if x == "no" else None,
            return_dtype=pl.Boolean,
        ).alias("two_factor_auth_enabled"),
        pl.col("biometric_login_used").cast(pl.Utf8).str.to_lowercase().map_elements(
            lambda x: True if x == "yes" else False if x == "no" else None,
            return_dtype=pl.Boolean,
        ).alias("biometric_login_used"),
        pl.col("has_children").cast(pl.Utf8).str.to_lowercase().map_elements(
            lambda x: True if x == "yes" else False if x == "no" else None,
            return_dtype=pl.Boolean,
        ).alias("has_children"),
    )

    # Numerics (cast where present; ignore missing columns gracefully)
    numeric_cols = [
        "age", "exercise_hours_per_week", "sleep_hours_per_night", "perceived_stress_score",
        "self_reported_happiness", "body_mass_index", "blood_pressure_systolic", "blood_pressure_diastolic",
        "daily_steps_count", "weekly_work_hours", "hobbies_count", "social_events_per_month",
        "books_read_per_year", "volunteer_hours_per_month", "travel_frequency_per_year",
        "daily_active_minutes_instagram", "sessions_per_day", "posts_created_per_week",
        "reels_watched_per_day", "stories_viewed_per_day", "likes_given_per_day",
        "comments_written_per_day", "dms_sent_per_week", "dms_received_per_week",
        "ads_viewed_per_day", "ads_clicked_per_day",
        "time_on_feed_per_day", "time_on_explore_per_day", "time_on_messages_per_day", "time_on_reels_per_day",
        "followers_count", "following_count", "notification_response_rate", "account_creation_year",
        "average_session_length_minutes", "linked_accounts_count", "user_engagement_score",
    ]
    existing_numeric_cols = [c for c in numeric_cols if c in df.columns]
    df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False).alias(c) for c in existing_numeric_cols])

    # ---- Derived fields (same logic) ----
    df = df.with_columns(
        (
            (pl.col("daily_active_minutes_instagram") >= 120)
            | (pl.col("sessions_per_day") >= 10)
            | (pl.col("reels_watched_per_day") >= 150)
        ).alias("heavy_usage"),

        pl.when(pl.col("daily_steps_count") <= 4999).then(pl.lit("low"))
        .when(pl.col("daily_steps_count") <= 9999).then(pl.lit("moderate"))
        .otherwise(pl.lit("high"))
        .alias("activity_level"),

        pl.when((pl.col("blood_pressure_systolic") >= 140) | (pl.col("blood_pressure_diastolic") >= 90))
        .then(pl.lit("high"))
        .when(
            ((pl.col("blood_pressure_systolic") >= 120) & (pl.col("blood_pressure_systolic") < 140))
            | ((pl.col("blood_pressure_diastolic") >= 80) & (pl.col("blood_pressure_diastolic") < 90))
        )
        .then(pl.lit("elevated"))
        .otherwise(pl.lit("normal"))
        .alias("bp_category"),

        (
            pl.col("user_engagement_score")
            - 0.15 * pl.col("perceived_stress_score").fill_null(0)
            - 0.10 * (pl.lit(7) - pl.col("sleep_hours_per_night")).clip(lower_bound=0).fill_null(0)
        ).alias("stress_adjusted_engagement"),
    )

    # ---- Filter: recent users + non-null cohort keys ----
    max_login = df.select(pl.col("last_login_date").max()).item()
    recent_cutoff = (max_login - pl.duration(days=365)) if max_login is not None else None

    df_f = df
    if recent_cutoff is not None:
        df_f = df_f.filter(pl.col("last_login_date") >= recent_cutoff)

    df_f = df_f.filter(
        pl.col("country").is_not_null()
        & pl.col("gender").is_not_null()
        & pl.col("income_level").is_not_null()
    )

    # ---- Cohort aggregation ----
    group_cols = ["country", "gender", "income_level"]

    agg = (
        df_f.group_by(group_cols)
        .agg(
            pl.len().alias("cohort_size"),
            pl.col("age").mean().alias("avg_age"),
            pl.col("uses_premium_features").mean().alias("pct_premium"),
            pl.col("daily_active_minutes_instagram").mean().alias("avg_daily_minutes"),
            pl.col("sessions_per_day").mean().alias("avg_sessions"),
            pl.col("heavy_usage").mean().alias("pct_heavy_usage"),
            pl.col("perceived_stress_score").mean().alias("avg_stress"),
            pl.col("self_reported_happiness").mean().alias("avg_happiness"),
            pl.col("body_mass_index").mean().alias("avg_bmi"),
            (pl.col("bp_category") == "high").mean().alias("pct_high_bp"),
            (pl.col("activity_level") == "low").mean().alias("pct_low_activity"),
            pl.col("user_engagement_score").mean().alias("avg_engagement"),
            pl.col("stress_adjusted_engagement").mean().alias("avg_stress_adjusted_engagement"),
            pl.col("followers_count").median().alias("median_followers"),
            pl.col("following_count").median().alias("median_following"),
            pl.col("ads_clicked_per_day").mean().alias("avg_ads_clicked"),
            pl.col("notification_response_rate").mean().alias("avg_notification_response"),
        )
        .filter(pl.col("cohort_size") >= 2)  # adjust for real dataset sizes
        .with_columns(
            (
                0.60 * pl.col("avg_stress_adjusted_engagement").fill_null(0)
                + 0.25 * pl.col("avg_engagement").fill_null(0)
                - 0.10 * pl.col("avg_stress").fill_null(0)
                + 0.05 * pl.col("avg_happiness").fill_null(0)
            ).alias("rank_score")
        )
        .sort(["rank_score", "cohort_size"], descending=[True, True])
    )

    end = time.time()
    print(f"Execution time (polars): {end - start:.2f} seconds")
    print(agg.head(20))

    agg.write_csv("output.csv")

    return agg


if __name__ == "__main__":
    analyze_instagram_users_polars("instagram_usage_lifestyle.csv")
