# pandas_analysis.py
import time
from memory_profiler import profile
import pandas as pd


@profile
def analyze_instagram_users_pandas(csv_path: str = "instagram_users.csv") -> pd.DataFrame:
    """
    Complex query (Pandas):
    - Clean/typed columns (dates + booleans + numerics)
    - Create derived metrics (BP category, activity level, heavy usage flag)
    - Segment users by (country, gender, income_level)
    - Filter to meaningful cohorts (min group size + recent logins)
    - Aggregate multiple health + usage KPIs
    - Rank cohorts by engagement score and stress-adjusted engagement

    Returns: aggregated cohort-level DataFrame sorted by ranking.
    """
    start = time.time()

    df = pd.read_csv(csv_path)

    # ---- Typing / normalization ----
    # Dates
    df["last_login_date"] = pd.to_datetime(df["last_login_date"], errors="coerce")

    # Booleans
    for col in ["uses_premium_features", "two_factor_auth_enabled", "biometric_login_used", "has_children"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.lower()
            .map({"yes": True, "no": False, "true": True, "false": False})
        )

    # Numerics (robust)
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
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # ---- Derived fields (complex logic) ----
    # Heavy usage: high daily minutes OR high sessions OR high reels watched
    df["heavy_usage"] = (
        (df["daily_active_minutes_instagram"] >= 120)
        | (df["sessions_per_day"] >= 10)
        | (df["reels_watched_per_day"] >= 150)
    )

    # Activity level: based on steps + active minutes
    df["activity_level"] = pd.cut(
        df["daily_steps_count"],
        bins=[-1, 4999, 9999, float("inf")],
        labels=["low", "moderate", "high"],
    )

    # Blood pressure category (simple clinical thresholding)
    df["bp_category"] = pd.Series(["normal"] * len(df), index=df.index)
    df.loc[
        (df["blood_pressure_systolic"] >= 140) | (df["blood_pressure_diastolic"] >= 90),
        "bp_category",
    ] = "high"
    df.loc[
        (df["blood_pressure_systolic"] < 140) & (df["blood_pressure_systolic"] >= 120)
        | ((df["blood_pressure_diastolic"] < 90) & (df["blood_pressure_diastolic"] >= 80)),
        "bp_category",
    ] = "elevated"

    # Stress-adjusted engagement: penalize engagement when stress is high and sleep is low
    df["stress_adjusted_engagement"] = (
        df["user_engagement_score"]
        - 0.15 * df["perceived_stress_score"].fillna(0)
        - 0.10 * (7 - df["sleep_hours_per_night"]).clip(lower=0).fillna(0)
    )

    # ---- Filter: cohorts with enough data + recent logins ----
    max_login = df["last_login_date"].max()
    recent_cutoff = max_login - pd.Timedelta(days=365) if pd.notna(max_login) else pd.Timestamp.min

    df_f = df[
        df["last_login_date"].ge(recent_cutoff)
        & df["country"].notna()
        & df["gender"].notna()
        & df["income_level"].notna()
    ].copy()

    # ---- Cohort aggregate: (country, gender, income_level) ----
    group_cols = ["country", "gender", "income_level"]

    agg = (
        df_f.groupby(group_cols, dropna=False)
        .agg(
            cohort_size=("user_id", "count"),
            avg_age=("age", "mean"),
            pct_premium=("uses_premium_features", "mean"),
            avg_daily_minutes=("daily_active_minutes_instagram", "mean"),
            avg_sessions=("sessions_per_day", "mean"),
            pct_heavy_usage=("heavy_usage", "mean"),
            avg_stress=("perceived_stress_score", "mean"),
            avg_happiness=("self_reported_happiness", "mean"),
            avg_bmi=("body_mass_index", "mean"),
            pct_high_bp=("bp_category", lambda s: (s == "high").mean()),
            pct_low_activity=("activity_level", lambda s: (s == "low").mean()),
            avg_engagement=("user_engagement_score", "mean"),
            avg_stress_adjusted_engagement=("stress_adjusted_engagement", "mean"),
            median_followers=("followers_count", "median"),
            median_following=("following_count", "median"),
            avg_ads_clicked=("ads_clicked_per_day", "mean"),
            avg_notification_response=("notification_response_rate", "mean"),
        )
        .reset_index()
    )

    # Keep only meaningful cohorts
    agg = agg[agg["cohort_size"] >= 2].copy()  # adjust for real dataset sizes

    # Ranking: prioritize high engagement but penalize high stress + low happiness
    agg["rank_score"] = (
        0.60 * agg["avg_stress_adjusted_engagement"].fillna(0)
        + 0.25 * agg["avg_engagement"].fillna(0)
        - 0.10 * agg["avg_stress"].fillna(0)
        + 0.05 * agg["avg_happiness"].fillna(0)
    )

    agg = agg.sort_values(["rank_score", "cohort_size"], ascending=[False, False])

    end = time.time()
    print(f"Execution time (pandas): {end - start:.2f} seconds")
    print(agg.head(20).to_string(index=False))

    df.to_csv("02_pandas_output.csv", index=False)

    return agg


if __name__ == "__main__":
    analyze_instagram_users_pandas("instagram_usage_lifestyle.csv")
