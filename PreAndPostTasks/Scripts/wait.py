import time
import sys

total_wait: int = 300
interval: int = 5

if len(sys.argv) > 1:
    total_wait = int(sys.argv[1])

if len(sys.argv) > 2:
    interval = int(sys.argv[2])

start_time = time.time()
while time.time() - start_time < total_wait: 
    time.sleep(interval)
    print("Current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))