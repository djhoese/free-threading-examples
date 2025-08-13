"""Simple multi-threaded I/O bound script."""

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from urllib.request import urlopen


def process_binary(url: str) -> int:
    response = urlopen(url)
    data = response.read()
    data_len = len(data)
    print(data_len)
    return data_len


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
    urls = [
        "https://sample-videos.com/video321/mp4/720/big_buck_bunny_720p_1mb.mp4",
        "https://sample-videos.com/video321/mp4/720/big_buck_bunny_720p_2mb.mp4",
        "https://sample-videos.com/video321/mp4/720/big_buck_bunny_720p_5mb.mp4",
        "https://sample-videos.com/video321/mp4/480/big_buck_bunny_480p_1mb.mp4",
        "https://sample-videos.com/video321/mp4/480/big_buck_bunny_480p_2mb.mp4",
        "https://sample-videos.com/video321/mp4/480/big_buck_bunny_480p_5mb.mp4",
    ]

    executor_cls = ThreadPoolExecutor
    if args.use_processes:
        executor_cls = ProcessPoolExecutor

    with executor_cls(max_workers=args.num_workers) as executor:
        result_iter = executor.map(process_binary, urls)
        print(list(result_iter))
