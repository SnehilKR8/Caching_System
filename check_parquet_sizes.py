import os
import dask.dataframe as dd

# Define your root cache directory
CACHE_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/cache/"

def scan_parquet_folder(base_dir):
    print(f"\n🔍 Scanning Parquet files in: {base_dir}\n")

    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".parquet"):
                full_path = os.path.join(root, file)
                try:
                    df = dd.read_parquet(full_path)
                    rows, cols = df.shape
                    print(f"📦 {os.path.relpath(full_path, CACHE_DIR)} → {rows.compute()} rows × {cols} columns")
                except Exception as e:
                    print(f"⚠️ Could not read {full_path}: {e}")

if __name__ == "__main__":
    scan_parquet_folder(CACHE_DIR)
