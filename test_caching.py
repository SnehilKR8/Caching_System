import os
import time
import pandas as pd
import numpy as np
import dask.dataframe as dd

# Define cache directory
CACHE_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING/cache"
DATA_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING/data"

# Ensure cache directory exists
os.makedirs(CACHE_DIR, exist_ok=True)

# Define test file paths
csv_file = f"{DATA_DIR}/test.csv"
parquet_file = f"{CACHE_DIR}/test.parquet"

def test_caching():
    # Step 1: Create a small test DataFrame
    num_rows = 100000
    df = pd.DataFrame({
    "id": np.arange(1, num_rows + 1),
    "name": np.random.choice(["Alice", "Bob", "Charlie", "David", "Eve"], num_rows),
    "score": np.random.randint(50, 100, num_rows)  # Random scores between 50-100
    })

    print("📌 Writing test DataFrame to CSV & Parquet...")

    # Save to CSV (for speed comparison)
    df.to_csv(csv_file, index=False)

    # Convert to Dask DataFrame & write to Parquet
    dask_df = dd.from_pandas(df, npartitions=1)
    dask_df.to_parquet(parquet_file, engine="pyarrow", write_index=False, overwrite=True)

    print(f"✅ Cached test DataFrame to {parquet_file}")

def test_cache_speed():
    """Compare loading speed of CSV vs. Parquet."""
    
    print("\n⏳ Testing Cache Speed...")

    # Measure time to load from CSV
    start = time.time()
    df_csv = pd.read_csv(csv_file)
    csv_time = time.time() - start
    print(f"⏳ Loading from CSV took: {csv_time:.4f} seconds")

    # Measure time to load from Parquet
    start = time.time()
    df_parquet = pd.read_parquet(parquet_file, engine="pyarrow")
    parquet_time = time.time() - start
    print(f"🚀 Loading from Parquet took: {parquet_time:.4f} seconds")

    # Compare Results
    improvement = csv_time / parquet_time if parquet_time > 0 else float("inf")
    print(f"\n📊 Parquet is {improvement:.2f}x faster than CSV!\n")

if __name__ == "__main__":
    test_caching()  # Write test data
    test_cache_speed()  # Measure speed
