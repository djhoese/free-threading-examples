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
        max_val = max(data)
        with open(filename.replace(".dat", f"_{offset}.dat"), mode="wb") as out_file:
            out_file.write(data)
            out_file.write(bytes([max_val]))
        return max_val


if __name__ == "__main__":
    in_file = "my_data.dat"
    max_workers = 8

    file_size = os.stat(in_file).st_size
    num_bytes = min(file_size // max_workers, 1024 * 1024 * 500)  # Maximum of ~500MB chunk sizes
    offsets = list(range(0, file_size, num_bytes))
    print(f"File size: {file_size} @ {num_bytes} bytes")
    print("Offsets: ", offsets)

    # executor_cls = ProcessPoolExecutor
    executor_cls = ThreadPoolExecutor
    process_func = partial(process_binary, in_file, num_bytes)
    with executor_cls(max_workers=max_workers) as executor:
        result_iter = executor.map(process_func, offsets)
        print(list(result_iter))

