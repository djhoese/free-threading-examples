"""A CPU-bound multi-threaded script."""

import random
import string
from functools import partial
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


def process_letters(dst_dict: dict[str, dict | str], max_level: int, curr_level: int, letters: str) -> int:
    if curr_level == max_level:
        count = 0
        for letter in letters:
            for i in range(1000):
                count += random.choice([i, i ** 2, i ** 3])
                # count += i
            dst_dict[letter] = string.ascii_letters.split(letter, 1)[0]
        return 0
    for letter in letters:
        sub_dict = {}
        process_letters(sub_dict, max_level, curr_level + 1, letters)
        dst_dict[letter] = sub_dict
    return len(letters)


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("--num-workers", type=int, default=8,
                        help="Number of threads or processes to use in pool.")
    parser.add_argument("--use-processes", action="store_true",
                        help="Use a process pool instead of a thread pool.")
    # parser.add_argument("--block-size", type=int, default=1024 * 1024,
    #                     help="Number of bytes to process at a time")
    # parser.add_argument("glob_pat", nargs="+",
    #                     help="Glob pattern to use for files to process. Can also be a list of files.")
    args = parser.parse_args()

    executor_cls = ThreadPoolExecutor
    if args.use_processes:
        executor_cls = ProcessPoolExecutor

    result = {}
    all_letters = string.ascii_lowercase
    group_size = len(all_letters) // args.num_workers
    letter_groups = [all_letters[idx:idx + group_size] for idx in range(0, len(all_letters), group_size)]
    process_func = partial(process_letters, result, 5, 0)
    with executor_cls(max_workers=args.num_workers) as executor:
        result_iter = executor.map(process_func, letter_groups)
        print(list(result_iter))
    print(list(result.keys()))


