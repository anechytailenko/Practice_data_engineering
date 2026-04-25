import os
import csv
import glob


def verify_and_print(directory_path):
    print(f"Searching for CSV files in:\n{directory_path}\n")

    search_pattern = os.path.join(directory_path, "city_paid_metrics_*.csv")
    matching_files = glob.glob(search_pattern)

    if not matching_files:
        print(f"Error: No files matching the pattern were found in {directory_path}")
        return

    target_file = sorted(matching_files)[-1]

    print(f"File found successfully: {os.path.basename(target_file)}")
    print("Reading contents...\n")
    print("-" * 60)

    try:
        with open(target_file, mode="r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row in reader:
                print(",".join(row))

        print("-" * 60)
        print("Verification complete.")

    except Exception as e:
        print(f"An error occurred while attempting to read the file: {e}")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    target_directory = os.path.join(current_dir, "airflow_polars_project", "data")
    verify_and_print(target_directory)
