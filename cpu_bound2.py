"""A CPU-bound multi-threaded script.

Download and unzip: http://www.gwicks.net/textlists/english3.zip
Remove the last 4 lines (unknown encoding)

"""
import math
import os
from functools import partial
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Iterator


def process_words(dictionary_file: str, num_bytes: int, letters: list[str], offset: int) -> list:
    with open(dictionary_file, mode="rt", encoding="IBM852") as dict_file:
        dict_file.seek(offset)
        words_str = dict_file.read(num_bytes + 15)

    words_list = words_str[:-15].splitlines()
    if offset != 0 or words_str[0] != r"\n":
        words_list = words_list[1:]
    if words_str[-15] != r"\n":
        words_list[-1] = words_list[-1] + words_str[-15:].splitlines()[0]

    return list(_filter_words_list(words_list, letters))


def _filter_words_list(words_list: list[str], letters: list[str]) -> Iterator[str]:
    for word in words_list:
        if len(word) < 4:
            continue
        if len(set(word)) > 7:
            continue
        if not all(word_letter in letters for word_letter in word):
            continue
        if letters[0] not in word:
            continue
        yield word


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("--num-workers", type=int, default=8,
                        help="Number of threads or processes to use in pool.")
    parser.add_argument("--use-processes", action="store_true",
                        help="Use a process pool instead of a thread pool.")
    parser.add_argument("letters",
                        help="Letters to solve for. Primary letter must be first.")
    args = parser.parse_args()

    executor_cls = ThreadPoolExecutor
    if args.use_processes:
        executor_cls = ProcessPoolExecutor

    dictionary_file = "english3.txt"
    dict_size = os.stat(dictionary_file).st_size
    num_bytes = math.ceil(dict_size / args.num_workers)
    offsets = list(range(0, dict_size, num_bytes))
    process_func = partial(process_words, dictionary_file, num_bytes, args.letters)
    with executor_cls(max_workers=args.num_workers) as executor:
        result_iter = executor.map(process_func, offsets)
        results = list(result_iter)
        print([word for words in results for word in words])


