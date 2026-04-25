import time
from memory_profiler import profile
import pandas as pd
import matplotlib.pyplot as plt


@profile
def police_analysis():
    start = time.time()

    # Load dataset
    df = pd.read_csv("police.csv")

    # -----------------------------
    # Basic preparation
    # -----------------------------
    df["stop_datetime"] = pd.to_datetime(
        df["stop_date"] + " " + df["stop_time"],
        errors="coerce"
    )
    df["stop_hour"] = df["stop_datetime"].dt.hour

    # -----------------------------
    # Query 1: Most common violations
    # -----------------------------
    violations_count = (
        df["violation"]
        .value_counts()
        .sort_values(ascending=False)
    )

    print("\nTop violations:")
    print(violations_count)

    violations_count.plot(
        kind="bar",
        title="Most Common Traffic Violations"
    )
    plt.tight_layout()
    plt.show()

    # -----------------------------
    # Query 2: Arrest rate by race
    # -----------------------------
    arrest_rate_by_race = (
        df.groupby("driver_race")["is_arrested"]
        .mean()
        .sort_values(ascending=False)
    )

    print("\nArrest rate by race:")
    print(arrest_rate_by_race)

    arrest_rate_by_race.plot(
        kind="bar",
        title="Arrest Rate by Driver Race"
    )
    plt.tight_layout()
    plt.show()

    # -----------------------------
    # Query 3: Stops by hour of day
    # -----------------------------
    stops_by_hour = (
        df["stop_hour"]
        .value_counts()
        .sort_index()
    )

    print("\nStops by hour:")
    print(stops_by_hour)

    stops_by_hour.plot(
        kind="line",
        marker="o",
        title="Traffic Stops by Hour of Day"
    )
    plt.xlabel("Hour of Day")
    plt.ylabel("Number of Stops")
    plt.tight_layout()
    plt.show()

    end = time.time()
    print(f"\nExecution time: {end - start:.2f} seconds")


if __name__ == "__main__":
    police_analysis()
