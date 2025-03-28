


# from dask_sql import Context
# from sql_metadata import Parser
# from scripts.caching import COLUMN_DTYPES, convert_tbl_to_csv, COLUMN_NAMES, load_selected_tables
# from scripts.queries import TOP_CUSTOMERS_QUERY
# import dask.dataframe as dd
# import os
# import hashlib

# # Define paths
# DATA_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/data"
# OUTPUT_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/output"
# CACHE_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/cache"
# os.makedirs(OUTPUT_DIR, exist_ok=True)
# os.makedirs(CACHE_DIR, exist_ok=True)

# def run_query_cached():
#     """Run a SQL query on cached data — caches only filtered data per table (dynamically hashed)."""
#     query = TOP_CUSTOMERS_QUERY
#     query_hash = hashlib.md5(query.encode("utf-8")).hexdigest()

#     result_file = f"{OUTPUT_DIR}/query_result_cached_{query_hash}.csv"
#     query_cache_dir = f"{CACHE_DIR}/query_cache/{query_hash}"
#     os.makedirs(query_cache_dir, exist_ok=True)

#     if os.path.exists(result_file):
#         print(f"✅ Query result cache found: {result_file}. Loading from cache...")
#         return dd.read_csv(result_file).compute()

#     required_tables = Parser(query).tables
#     tables = load_selected_tables(required_tables)
#     ctx = Context()

#     for table_name, ddf in tables.items():
#         ctx.create_table(table_name, ddf)
#         print(f" => Registered {table_name} in DaskDB.")

#     print("\n🚀 Running Query Using Cached Parquet Data...")
#     result = ctx.sql(query).persist().compute()

#     result.to_csv(result_file, index=False)
#     print(f"📁 Query result saved to: {result_file}")

#     for table_name in required_tables:
#         try:
#             full_ddf = tables[table_name]
#             common_keys = set(full_ddf.columns) & set(result.columns)

#             if not common_keys:
#                 print(f"⚠️ No matching columns to filter {table_name}, caching first 1000 rows.")
#                 filtered_ddf = full_ddf.head(1000, compute=False)
#             else:
#                 match_col = list(common_keys)[0]
#                 relevant_keys = result[match_col].dropna().unique().compute().tolist()
#                 filtered_ddf = full_ddf[full_ddf[match_col].isin(relevant_keys)].persist()
#                 print(f"🔍 Filtering {table_name} using {match_col} ({len(relevant_keys)} keys)")

#             cache_path = f"{query_cache_dir}/{table_name}_filtered.parquet"
#             filtered_ddf.to_parquet(cache_path, engine="pyarrow", write_index=False, overwrite=True)
#             print(f"📦 Cached filtered {table_name} to: {cache_path}")

#         except Exception as e:
#             print(f"⚠️ Failed to cache filtered {table_name}: {e}")

#     return result

# def run_query_non_cached():
#     """Run a SQL query using CSV files (no caching involved)."""
#     query = TOP_CUSTOMERS_QUERY
#     required_tables = Parser(query).tables

#     ctx = Context()
#     print("\n🟡 Running Query Using CSV Files (No Caching)...")

#     for table in required_tables:
#         csv_file = f"{DATA_DIR}/{table}.csv"
#         tbl_file = f"{DATA_DIR}/{table}.tbl"

#         if not os.path.exists(csv_file):
#             print(f"❗ CSV missing for {table}. Generating from .tbl...")
#             convert_tbl_to_csv(tbl_file, csv_file, table)

#         ddf = dd.read_csv(
#             csv_file,
#             names=COLUMN_NAMES[table],
#             delimiter=",",
#             assume_missing=True,
#             dtype=COLUMN_DTYPES.get(table, str),
#             header=0
#         )
#         ctx.create_table(table, ddf)
#         print(f" => Registered {table} from CSV")

#     print("🚀 Running Query Using CSV...")
#     result = ctx.sql(query).compute()

#     result_file = f"{OUTPUT_DIR}/query_result_non_cached.csv"
#     result.to_csv(result_file, index=False)
#     print(f"📁 Query result saved to: {result_file}")

#     return result

from dask_sql import Context
from sql_metadata import Parser
from scripts.caching import COLUMN_DTYPES, convert_tbl_to_csv, COLUMN_NAMES, load_selected_tables
from scripts.queries import TOP_CUSTOMERS_QUERY
import dask.dataframe as dd
import os
import hashlib

# Define paths
DATA_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/data"
OUTPUT_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/output"
CACHE_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/cache"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

def run_query_cached():
    """Run a SQL query on cached data — caches only filtered data per table (dynamically hashed)."""
    query = TOP_CUSTOMERS_QUERY
    query_hash = hashlib.md5(query.encode("utf-8")).hexdigest()

    result_file = f"{OUTPUT_DIR}/query_result_cached_{query_hash}.csv"
    query_cache_dir = f"{CACHE_DIR}/query_cache/{query_hash}"
    os.makedirs(query_cache_dir, exist_ok=True)

    if os.path.exists(result_file):
        print(f"✅ Query result cache found: {result_file}. Loading from cache...")
        return dd.read_csv(result_file).compute()

    required_tables = Parser(query).tables
    tables = load_selected_tables(required_tables)
    ctx = Context()

    for table_name, ddf in tables.items():
        ctx.create_table(table_name, ddf)
        print(f" => Registered {table_name} in DaskDB.")

    print("\n🚀 Running Query Using Cached Parquet Data...")
    result = ctx.sql(query).persist().compute()

    result.to_csv(result_file, index=False)
    print(f"📁 Query result saved to: {result_file}")

    for table_name in required_tables:
        try:
            full_ddf = tables[table_name]

            # Check for known primary/foreign key columns in this table
            key_candidates = {"custkey", "suppkey", "partkey", "nationkey", "regionkey"}
            match_keys = key_candidates & set(full_ddf.columns) & set(result.columns)

            if not match_keys:
                print(f"⚠️ No relevant keys found to filter {table_name}, skipping caching.")
                continue

            match_col = list(match_keys)[0]
            relevant_keys = result[match_col].dropna().unique().compute().tolist()
            filtered_ddf = full_ddf[full_ddf[match_col].isin(relevant_keys)].persist()
            print(f"🔍 Filtering {table_name} using {match_col} ({len(relevant_keys)} keys)")

            cache_path = f"{query_cache_dir}/{table_name}_filtered.parquet"
            filtered_ddf.to_parquet(cache_path, engine="pyarrow", write_index=False, overwrite=True)
            print(f"📦 Cached filtered {table_name} to: {cache_path}")

        except Exception as e:
            print(f"⚠️ Failed to cache filtered {table_name}: {e}")

    return result

def run_query_non_cached():
    """Run a SQL query using CSV files (no caching involved)."""
    query = TOP_CUSTOMERS_QUERY
    required_tables = Parser(query).tables

    ctx = Context()
    print("\n🟡 Running Query Using CSV Files (No Caching)...")

    for table in required_tables:
        csv_file = f"{DATA_DIR}/{table}.csv"
        tbl_file = f"{DATA_DIR}/{table}.tbl"

        if not os.path.exists(csv_file):
            print(f"❗ CSV missing for {table}. Generating from .tbl...")
            convert_tbl_to_csv(tbl_file, csv_file, table)

        ddf = dd.read_csv(
            csv_file,
            names=COLUMN_NAMES[table],
            delimiter=",",
            assume_missing=True,
            dtype=COLUMN_DTYPES.get(table, str),
            header=0
        )
        ctx.create_table(table, ddf)
        print(f" => Registered {table} from CSV")

    print("🚀 Running Query Using CSV...")
    result = ctx.sql(query).compute()

    result_file = f"{OUTPUT_DIR}/query_result_non_cached.csv"
    result.to_csv(result_file, index=False)
    print(f"📁 Query result saved to: {result_file}")

    return result
