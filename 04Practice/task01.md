## Student Task: Cohort Analysis with Pandas and Polars

**_1 point**_

Use this dataset [Social Media User Analysis](https://www.kaggle.com/datasets/rockyt07/social-media-user-analysis)

Your goal is to analyze the Instagram users dataset using **both pandas and polars**, applying the same logic in each implementation.

### Steps

1. **Read the CSV file** into a DataFrame.

2. **Convert column types**
   - Convert `last_login_date` to a date type.
   - Convert `uses_premium_features` from `Yes/No` values to boolean (`True/False`).

3. **Create a derived column**
   - Create `heavy_usage`:
     - `True` if `daily_active_minutes_instagram >= 120`
       **OR**
       `sessions_per_day >= 10`
     - Otherwise `False`.

4. **Build a cohort table**
   - Group the data by:
     - `country`
     - `income_level`
   - Compute the following metrics:
     - `users` — count of users
     - `avg_minutes` — mean of `daily_active_minutes_instagram`
     - `pct_premium` — mean of `uses_premium_features`
     - `pct_heavy_usage` — mean of `heavy_usage`
     - `avg_engagement` — mean of `user_engagement_score`

5. **Sort and display results**
   - Sort the cohort table by `avg_engagement` in descending order.
   - Print the top **10** cohorts.

6. **Memory profiling**
   - Wrap your analysis function with `@profile` from the `memory_profiler` package.

### Notes
- The logic and output should be **identical** for pandas and polars.
