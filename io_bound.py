"""Simple multi-threaded I/O bound script.

Create fake data with::

    dd if=/dev/urandom of=my_data.dat bs=128M count=80 iflag=fullblock

"""

import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial


def process_binary(filename: str, num_bytes: int, offset: int) -> int:
    with open(filename, mode="rb") as bin_file:
        bin_file.seek(offset)
        data = bin_file.read(num_bytes)
        return max(data)


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("--num-workers", type=int, default=8,
                        help="Number of threads or processes to use in pool.")
    parser.add_argument("--use-processes", type=bool, action="store_true",
                        help="Use a process pool instead of a thread pool.")
    # parser.add_argument("--block-size", type=int, default=None,
    #                     help="Number of bytes to process at a time. Defaults to ~1GB.")
    parser.add_argument("in_file", nargs="?", default="my_data.dat",
                        help="File to process.")
    args = parser.parse_args()

    file_size = os.stat(args.in_file).st_size
    num_bytes = min(file_size // args.num_workers, 1024 * 1024 * 1024)  # Maximum of ~1000MB chunk sizes
    #num_bytes = min(file_size // max_workers, 1024 * 1024 * 500)  # Maximum of ~500MB chunk sizes
    offsets = list(range(0, file_size, num_bytes))
    print(f"File size: {file_size} @ {num_bytes} bytes")
    print("Offsets: ", offsets)

    executor_cls = ThreadPoolExecutor
    if args.use_processes:
        executor_cls = ProcessPoolExecutor

    process_func = partial(process_binary, args.in_file, num_bytes)
    with executor_cls(max_workers=args.num_workers) as executor:
        result_iter = executor.map(process_func, offsets)
        print(list(result_iter))

