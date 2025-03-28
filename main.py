

from scripts.query_data import run_query_cached, run_query_non_cached
from benchmarking import compare_performance

if __name__ == "__main__":
    print("Comparing Query Performance With and Without Caching...\n")
    compare_performance(run_query_cached, run_query_non_cached)
   

