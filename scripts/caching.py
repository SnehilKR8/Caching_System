import csv
import os
import dask.dataframe as dd

# Define paths
DATA_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/data"
CACHE_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/cache"
OUTPUT_DIR = "C:/Users/snehi/OneDrive/Desktop/DASKDB/NEWCACHING_TEST/output"

TABLES = ["customer", "orders", "lineitem", "supplier", "nation", "region", "part", "partsupp"]

COLUMN_NAMES = {
    "customer": ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"],
    "orders": ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment"],
    "lineitem": ["orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice", "discount", "tax", "returnflag",
                 "linestatus", "shipdate", "commitdate", "receiptdate", "shipinstruct", "shipmode", "comment"],
    "supplier": ["suppkey", "name", "address", "nationkey", "phone", "acctbal", "comment"],
    "nation": ["nationkey", "name", "regionkey", "comment"],
    "region": ["regionkey", "name", "comment"],
    "part": ["partkey", "name", "mfgr", "brand", "type", "size", "container", "retailprice", "comment"],
    "partsupp": ["partkey", "suppkey", "availqty", "supplycost", "comment"]
}

COLUMN_DTYPES = {
    "customer": {
        "custkey": "int64",
        "nationkey": "int64",
        "acctbal": "float64"
    },
    "orders": {
        "orderkey": "int64",
        "custkey": "int64",
        "totalprice": "float64",
        "shippriority": "int64"
    },
    "lineitem": {
        "orderkey": "int64",
        "partkey": "int64",
        "suppkey": "int64",
        "linenumber": "int64",
        "quantity": "float64",
        "extendedprice": "float64",
        "discount": "float64",
        "tax": "float64"
    },
    "supplier": {
        "suppkey": "int64",
        "nationkey": "int64",
        "acctbal": "float64"
    },
    "nation": {
        "nationkey": "int64",
        "regionkey": "int64"
    },
    "region": {
        "regionkey": "int64"
    },
    "part": {
        "partkey": "int64",
        "size": "int64",
        "retailprice": "float64"
    },
    "partsupp": {
        "partkey": "int64",
        "suppkey": "int64",
        "availqty": "int64",
        "supplycost": "float64"
    }
}



# Define dtypes for numeric conversion
DASK_DTYPES = {
    "customer": {"custkey": int, "acctbal": float},
    "orders": {"orderkey": int, "custkey": int, "totalprice": float},
    "lineitem": {"orderkey": int, "partkey": int, "suppkey": int, "linenumber": int, "quantity": float, "extendedprice": float,
                  "discount": float, "tax": float},
    "supplier": {"suppkey": int, "nationkey": int, "acctbal": float},
    "nation": {"nationkey": int, "regionkey": int},
    "region": {"regionkey": int},
    "part": {"partkey": int, "size": int, "retailprice": float},
    "partsupp": {"partkey": int, "suppkey": int, "availqty": int, "supplycost": float}
}

# Ensure necessary directories exist
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def convert_tbl_to_csv(tbl_file, csv_file, table):
    """Convert .tbl file to CSV with headers and validate rows."""
    if os.path.exists(csv_file):
        print(f"CSV already exists: {csv_file}")
        return  # Skip if CSV already exists

    if not os.path.exists(tbl_file):
        print(f" ERROR: .tbl file not found: {tbl_file}")
        return

    print(f" Converting {tbl_file} to CSV...")

    with open(tbl_file, "r", encoding="utf-8") as infile, open(csv_file, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        # Write column headers first
        writer.writerow(COLUMN_NAMES[table])

        invalid_rows = 0  # Counter for malformed rows
        total_rows = 0  # Counter for total rows

        for line in infile:
            cleaned_line = line.strip().rstrip("|")  # Remove trailing '|'
            fields = cleaned_line.split("|")

            if len(fields) != len(COLUMN_NAMES[table]):
                print(f" Skipping malformed line in {tbl_file}: {line.strip()}")
                invalid_rows += 1
                continue

            writer.writerow(fields)
            total_rows += 1

    print(f" Converted {tbl_file} to {csv_file}. Skipped {invalid_rows} invalid rows. Total rows: {total_rows}")

def cache_selected_tables(required_tables):
    """Cache only selected tables from TPC-H."""
    for table in required_tables:
        tbl_file = f"{DATA_DIR}/{table}.tbl"
        csv_file = f"{DATA_DIR}/{table}.csv"
        parquet_file = f"{CACHE_DIR}/{table}.parquet"

        convert_tbl_to_csv(tbl_file, csv_file, table)

        if os.path.exists(parquet_file):
            print(f"Parquet cache exists: {parquet_file}")
            continue

        try:
            print(f" Creating Parquet for {table}...")
            df = dd.read_csv(
                csv_file,
                names=COLUMN_NAMES[table],
                delimiter=",",
                assume_missing=True,
                dtype=DASK_DTYPES.get(table, str),
                header=0
            )
            df.to_parquet(parquet_file, engine="pyarrow", write_index=False, overwrite=True)
            print(f" Cached {table} as Parquet.")
        except Exception as e:
            print(f" Error processing {table}: {e}")

def load_selected_tables(required_tables):
    """Load only selected Parquet tables, convert and cache if needed."""
    tables = {}
    for table in required_tables:
        parquet_file = f"{CACHE_DIR}/{table}.parquet"
        if not os.path.exists(parquet_file):
            print(f" Cache missing for {table}, regenerating...")
            cache_selected_tables([table])

        tables[table] = dd.read_parquet(parquet_file)
    return tables


