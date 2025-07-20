"""Simple multi-threaded sleep script."""

import random
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time


def process_sleep(input_sleep: int) -> int:
    mask = random.randint(1, 4)
    sleep_times = sorted([1, 2, 2, 4, 4, input_sleep], key=lambda x: x & mask)
    count = 0
    for sleep_time in sleep_times:
        time.sleep(sleep_time)
        count += sleep_time
    return count


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "--num-workers",
        type=int,
        default=8,
        help="Number of threads or processes to use in pool.",
    )
    parser.add_argument(
        "--use-processes",
        action="store_true",
        help="Use a process pool instead of a thread pool.",
    )
    args = parser.parse_args()

    executor_cls = ThreadPoolExecutor
    if args.use_processes:
        executor_cls = ProcessPoolExecutor

    with executor_cls(max_workers=args.num_workers) as executor:
        result_iter = executor.map(process_sleep, [1, 2] * 4)
        print(list(result_iter))
