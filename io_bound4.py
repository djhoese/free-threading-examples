"""Simple multi-threaded I/O bound script.

Create fake data with::

    for i in $(seq -f "%02.0f" 1 10); do dd if=/dev/urandom of=my_data_$i.dat bs=128M count=8 iflag=fullblock; done

"""

import os
import random
from functools import partial
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from glob import glob


def process_binary(block_size: int, filename: str) -> int:
    total_bytes = os.stat(filename).st_size
    mask = random.randint(0, 255)
    jump_size = int(total_bytes * 0.5)
    with open(filename, mode="r+b") as bin_file:
        for offset in range(0, total_bytes, max(total_bytes // 10000, block_size)):
            bin_file.seek(offset, os.SEEK_SET)
            data = bin_file.read(block_size)
            sorted_data = data
            # sorted_data = bytes(sorted(data, key=lambda x: x & mask))
            dst_offset = (bin_file.tell() + jump_size) % total_bytes
            if dst_offset + block_size > total_bytes:
                dst_offset -= (dst_offset + block_size) - total_bytes
            bin_file.seek(dst_offset, os.SEEK_SET)
            bin_file.write(sorted_data)
    return jump_size


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("--num-workers", type=int, default=8,
                        help="Number of threads or processes to use in pool.")
    parser.add_argument("--use-processes", action="store_true",
                        help="Use a process pool instead of a thread pool.")
    parser.add_argument("--block-size", type=int, default=1024 * 1024,
                        help="Number of bytes to process at a time")
    parser.add_argument("glob_pat", nargs="+",
                        help="Glob pattern to use for files to process. Can also be a list of files.")
    args = parser.parse_args()

    files = []
    for glob_pat in args.glob_pat:
        files.extend(glob(glob_pat))

    executor_cls = ThreadPoolExecutor
    if args.use_processes:
        executor_cls = ProcessPoolExecutor

    process_func = partial(process_binary, args.block_size)
    with executor_cls(max_workers=args.num_workers) as executor:
        result_iter = executor.map(process_func, files)
        print(list(result_iter))

