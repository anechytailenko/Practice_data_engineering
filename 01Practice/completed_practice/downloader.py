import kagglehub
import shutil
import os

dataset_path = kagglehub.dataset_download("matepapava/nike-discounts-dataset")

source_file = os.path.join(dataset_path, "nike_discounts.json")
destination_file = "nike_discounts.json"

shutil.copy(source_file, destination_file)

print("File successfully downloaded and saved to the current directory.")
