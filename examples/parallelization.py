"""
Example of serial vs parallel processing using concurrent.futures
"""

import time
from concurrent.futures import ProcessPoolExecutor


def task(n):
    time.sleep(1)
    return n * 2


# serial processing using a for loop
serial_start_time = time.time()
serial_results = []

for i in range(5):
    serial_results.append(task(i))

serial_end_time = time.time()
print(f"Serial execution results: {serial_results}")
print(f"Serial execution time: {serial_end_time - serial_start_time:.2f} seconds")



# parallel processing using concurrent.futures ProcessPoolExecutor

parallel_start_time = time.time()

with ProcessPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(task, i) for i in range(5)]
    parallel_results = [future.result() for future in futures]

parallel_end_time = time.time()
print(f"Parallel execution results: {parallel_results}")
print(f"Parallel execution time: {parallel_end_time - parallel_start_time:.2f} seconds")
