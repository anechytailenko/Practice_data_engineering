import pandas as pd
import kagglehub
from kagglehub import KaggleDatasetAdapter



def get_instagram_dataframe() -> pd.DataFrame:
    file_path = "instagram_users_lifestyle.csv"

    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "rockyt07/social-media-user-analysis",
        file_path,
    )

    return df


if __name__ == "__main__":
    test_df = get_instagram_dataframe()
    print(test_df.head())
