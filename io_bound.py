"""Simple multi-threaded I/O bound script.

Create fake data with::

    dd if=/dev/urandom of=my_data.dat bs=128M count=80 iflag=fullblock

"""

import math
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial


def process_binary(filename: str, operation: str, num_bytes: int, offset: int) -> int:
    with open(filename, mode="rb") as bin_file:
        print(f"File descriptor: {bin_file.fileno()=}")
        bin_file.seek(offset)
        data = bin_file.read(num_bytes)
        print(f"Reading data done on file descriptor {bin_file.fileno()}")

        if operation == "index_middle":
            return data[len(data) // 2]
        if operation == "max":
            return max(data)
        raise ValueError(f"Unexpected operation: {operation}")


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
    # parser.add_argument("--block-size", type=int, default=None,
    #                     help="Number of bytes to process at a time. Defaults to ~1GB.")
    parser.add_argument(
        "--operation",
        default="index_middle",
        choices=["index_middle", "max"],
        help="Operation or calculation to perform on the loaded chunk of file data.",
    )
    parser.add_argument(
        "in_file", nargs="?", default="my_data.dat", help="File to process."
    )
    args = parser.parse_args()

    file_size = os.stat(args.in_file).st_size
    num_bytes = min(
        math.ceil(file_size / args.num_workers), 1024 * 1024 * 1024
    )  # Maximum of ~1000MB chunk sizes
    offsets = list(range(0, file_size, num_bytes))
    print(f"File size: {file_size} @ {num_bytes} bytes")
    print("Offsets: ", offsets)
    print("Operation: ", args.operation)

    executor_cls = ThreadPoolExecutor
    if args.use_processes:
        executor_cls = ProcessPoolExecutor

    process_func = partial(process_binary, args.in_file, args.operation, num_bytes)
    with executor_cls(max_workers=args.num_workers) as executor:
        result_iter = executor.map(process_func, offsets)
        print("Results: ", list(result_iter))
