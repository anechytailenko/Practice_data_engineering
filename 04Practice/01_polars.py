import time
from memory_profiler import profile
import polars as pl
import matplotlib.pyplot as plt


@profile
def police_analysis_polars():
    start = time.time()

    # -----------------------------
    # Load dataset (Polars)
    # -----------------------------
    df = pl.read_csv("police.csv")

    # -----------------------------
    # Feature engineering
    # -----------------------------
    df = df.with_columns(
        (
            pl.col("stop_date") + " " + pl.col("stop_time")
        )
        .str.strptime(pl.Datetime, strict=False)
        .alias("stop_datetime")
    ).with_columns(
        pl.col("stop_datetime").dt.hour().alias("stop_hour")
    )

    # -----------------------------
    # Query 1: Most common violations
    # -----------------------------
    violations_count = (
        df.group_by("violation")
        .len()
        .sort("len", descending=True)
    )

    print("\nTop violations:")
    print(violations_count)

    violations_pd = violations_count.to_pandas()

    violations_pd.plot(
        kind="bar",
        x="violation",
        y="len",
        title="Most Common Traffic Violations",
        legend=False
    )
    plt.tight_layout()
    plt.show()

    # -----------------------------
    # Query 2: Arrest rate by race
    # -----------------------------
    arrest_rate_by_race = (
        df.group_by("driver_race")
        .agg(pl.col("is_arrested").mean().alias("arrest_rate"))
        .sort("arrest_rate", descending=True)
    )

    print("\nArrest rate by race:")
    print(arrest_rate_by_race)

    arrest_rate_pd = arrest_rate_by_race.to_pandas()

    arrest_rate_pd.plot(
        kind="bar",
        x="driver_race",
        y="arrest_rate",
        title="Arrest Rate by Driver Race",
        legend=False
    )
    plt.tight_layout()
    plt.show()

    # -----------------------------
    # Query 3: Stops by hour of day
    # -----------------------------
    stops_by_hour = (
        df.group_by("stop_hour")
        .len()
        .sort("stop_hour")
    )

    print("\nStops by hour:")
    print(stops_by_hour)

    stops_by_hour_pd = stops_by_hour.to_pandas()

    stops_by_hour_pd.plot(
        kind="line",
        x="stop_hour",
        y="len",
        marker="o",
        title="Traffic Stops by Hour of Day",
        legend=False
    )
    plt.xlabel("Hour of Day")
    plt.ylabel("Number of Stops")
    plt.tight_layout()
    plt.show()

    end = time.time()
    print(f"\nExecution time: {end - start:.2f} seconds")


if __name__ == "__main__":
    police_analysis_polars()
