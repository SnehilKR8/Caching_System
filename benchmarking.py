import time
import psutil
import os

def measure_execution_time(func, *args, **kwargs):
    """Measure execution time of a function."""
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    return result, elapsed_time

def measure_resource_usage():
    """Measure CPU and Memory usage."""
    process = psutil.Process(os.getpid())
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = process.memory_info().rss / (1024 * 1024)  # Convert to MB
    return cpu_usage, memory_usage

def compare_performance(run_query_cached, run_query_non_cached):
    """Compare the performance of cached vs. non-cached queries."""
    
    print("\n🔍 Running Query Without Caching...")
    _, exec_time_no_cache = measure_execution_time(run_query_non_cached)
    cpu_no_cache, mem_no_cache = measure_resource_usage()

    print("\nRunning Query With Caching...")
    _, exec_time_cached = measure_execution_time(run_query_cached)
    cpu_cached, mem_cached = measure_resource_usage()

    print("\n Performance Comparison")
    print(f" Without Caching: {exec_time_no_cache:.4f} sec | CPU: {cpu_no_cache}% | Memory: {mem_no_cache:.2f} MB")
    print(f" With Caching: {exec_time_cached:.4f} sec | CPU: {cpu_cached}% | Memory: {mem_cached:.2f} MB")

    improvement = exec_time_no_cache / exec_time_cached if exec_time_cached > 0 else float("inf")
    print(f"\nSpeed Improvement with Caching: {improvement:.2f}x faster!\n")
