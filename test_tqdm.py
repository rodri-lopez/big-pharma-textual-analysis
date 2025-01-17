from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Dummy function for demonstration
def dummy_task(x):
    import time
    time.sleep(1)  # Simulating a task taking time
    return x * x

tasks = [1, 2, 3, 4, 5]  # Example tasks
futures = []

with ThreadPoolExecutor() as executor:
    for task in tasks:
        futures.append(executor.submit(dummy_task, task))

# Correct usage of tqdm
for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Tasks"):
    result = future.result()  # Retrieve the result if neede