#!/usr/bin/env python
import ydb
import time
import os
import sys

def wait_for_ydb(timeout):
    endpoint = os.getenv("YDB_ENDPOINT", "localhost:2136")
    database = os.getenv("YDB_DATABASE", "/local")

    print(f"Waiting for YDB at {endpoint}{database}...")

    driver_config = ydb.DriverConfig(
        endpoint=endpoint,
        database=database,
    )
    driver = ydb.Driver(driver_config)

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            driver.wait(timeout=5, fail_fast=True)
            print("YDB is ready.")
            driver.stop()
            return
        except Exception as e:
            print(f"Waiting for YDB... ({e})", file=sys.stderr)
            time.sleep(5)

    driver.stop()
    raise TimeoutError("YDB did not become ready in time.")


if __name__ == "__main__":
    wait_for_ydb(180)