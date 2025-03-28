import os
import glob
import shutil

# Set paths
DATA_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/data"
CACHE_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/cache"
OUTPUT_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/output"

def remove_cached_and_csv_files():
    """Delete all .csv and .parquet files from cache, data, and output directories."""

    # Remove CSVs from data dir
    csv_files = glob.glob(f"{DATA_DIR}/*.csv")
    for file in csv_files:
        os.remove(file)
        print(f"🗑️ Removed CSV: {file}")

    # Remove Parquet files from cache dir
    parquet_files = glob.glob(f"{CACHE_DIR}/*.parquet")
    for file in parquet_files:
        if os.path.isdir(file):
            shutil.rmtree(file)
        else:
            os.remove(file)
        print(f"🗑️ Removed Parquet: {file}")

    # Remove output query result files
    output_files = glob.glob(f"{OUTPUT_DIR}/*.csv")
    for file in output_files:
        os.remove(file)
        print(f"🗑️ Removed Query Output: {file}")

if __name__ == "__main__":
    remove_cached_and_csv_files()
