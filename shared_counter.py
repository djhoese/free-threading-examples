"""Simple multi-threaded count example that can show race conditions.
"""
from __future__ import annotations

import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
import threading
import time

COUNT = 0


def do_some_counting(max_count: int) -> int:
    global COUNT
    for _ in range(max_count):
        COUNT += 1
    return 0


def locked_counting(lock: threading.Lock | multiprocessing.Lock, max_count: int) -> int:
    global COUNT
    for _ in range(max_count):
        with lock:
            COUNT += 1
    return 0


def dict_counting(shared_dict: dict, max_count: int) -> int:
    for _ in range(max_count):
        shared_dict["count"] += 1
    return 0


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "--num-workers",
        type=int,
        default=10,
        help="Number of threads or processes to use in pool.",
    )
    parser.add_argument(
        "--use-processes",
        action="store_true",
        help="Use a process pool instead of a thread pool.",
    )
    parser.add_argument(
        "--use-locking",
        action="store_true",
        help="Use a threading or multiprocess lock to control access to the shared count",
    )
    parser.add_argument(
        "--use-dict",
        action="store_true",
        help="Use a dictionary as the shared storage for the counter instead of a global int.",
    )
    parser.add_argument(
        "--max-count", type=int, default=10000, help="Each thread will increment this many times."
    )
    args = parser.parse_args()

    executor_cls = ThreadPoolExecutor
    if args.use_processes:
        executor_cls = ProcessPoolExecutor

    max_counts = [args.max_count] * args.num_workers
    exp_count = args.max_count * args.num_workers

    thread_func = do_some_counting
    shared_dict = {"count": 0}
    if args.use_locking:
        lock = threading.Lock() if not args.use_processes else multiprocessing.Lock()
        thread_func = partial(locked_counting, lock)
    elif args.use_dict:
        thread_func = partial(dict_counting, shared_dict)

    start = time.perf_counter()
    with executor_cls(max_workers=args.num_workers) as executor:
        list(executor.map(thread_func, max_counts))
    stop = time.perf_counter()

    final_count = COUNT if not args.use_dict else shared_dict["count"]
    status = "SUCCESS" if final_count == exp_count else "FAILURE"
    print(f"{status}: Got {final_count} | Expected {exp_count} | {(stop - start):0.04f} seconds")
