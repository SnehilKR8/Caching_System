import dask.dataframe as dd
from scripts.caching import load_cached_data

# Load TPC-H dataset from cached Parquet
def load_tpch_data():
    tables = load_cached_data()
    return tables["orders"], tables["customer"]  # Return orders & customers as Dask DataFrames
