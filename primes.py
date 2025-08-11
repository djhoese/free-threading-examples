"""Simple multi-threaded prime generator.
"""

import math
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial

import numpy as np


def process_primes(chunk_size: int, start: int) -> int:
    count = 0
    for num_to_check in range(start, start + chunk_size):
        if _is_prime(num_to_check):
            count += 1

    return count


def _is_prime(val: int) -> bool:
    if val <= 1:
        return False
    if val <= 3:
        return True
    if val % 2 == 0 or val % 3 == 0:
        return False
    i = 5
    while i * i <= val:
        if val % i == 0 or val % (i + 2) == 0:
            return False
        i += 6

    return True


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
    # parser.add_argument("--block-size", type=int, default=None,
    #                     help="Number of bytes to process at a time. Defaults to ~1GB.")
    parser.add_argument(
        "--max-check", type=int, default=1000000, help="Check for primes up to this value"
    )
    args = parser.parse_args()

    executor_cls = ThreadPoolExecutor
    if args.use_processes:
        executor_cls = ProcessPoolExecutor

    chunk_size = args.max_check // args.num_workers
    starts = list(range(1, args.max_check + 1, chunk_size))
    process_func = partial(process_primes, chunk_size)
    with executor_cls(max_workers=args.num_workers) as executor:
        result_iter = executor.map(process_func, starts)
        print(f"Total primes: {sum(result_iter)}")
