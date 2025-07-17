"""Simple multi-threaded I/O bound script."""

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from urllib.request import urlopen


def process_binary(url: str) -> int:
    # response = urlopen("http://example.com/")
    # response = urlopen("https://bin.ssec.wisc.edu/pub/incoming/SSMIS_AMSR2_comparison.tar.gz")
    # response = urlopen("https://bin.ssec.wisc.edu/pub/incoming/THORPEX_2008_Dropsonde_txt.zip")
    response = urlopen(url)
    data = response.read()
    print(len(data))
    return max(data)


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("--num-workers", type=int, default=8,
                        help="Number of threads or processes to use in pool.")
    parser.add_argument("--use-processes", action="store_true",
                        help="Use a process pool instead of a thread pool.")
    args = parser.parse_args()
    urls = [
        "https://www.ssec.wisc.edu/~davidh/polar2grid/scmi_grids/scmi_grid_GOES_EAST.png",
        "https://www.ssec.wisc.edu/~davidh/polar2grid/scmi_grids/scmi_grid_GOES_STORE.png",
        "https://www.ssec.wisc.edu/~davidh/polar2grid/scmi_grids/scmi_grid_GOES_TEST.png",
        "https://www.ssec.wisc.edu/~davidh/polar2grid/scmi_grids/scmi_grid_GOES_WEST.png",
        "https://www.ssec.wisc.edu/~davidh/polar2grid/scmi_grids/scmi_grid_LCC.png",
        "https://www.ssec.wisc.edu/~davidh/polar2grid/scmi_grids/scmi_grid_Mercator.png",
        "https://www.ssec.wisc.edu/~davidh/polar2grid/scmi_grids/scmi_grid_Pacific.png",
        "https://www.ssec.wisc.edu/~davidh/polar2grid/scmi_grids/scmi_grid_Polar.png",
    ]

    executor_cls = ThreadPoolExecutor
    if args.use_processes:
        executor_cls = ProcessPoolExecutor

    with executor_cls(max_workers=args.num_workers) as executor:
        result_iter = executor.map(process_binary, urls * 3)
        print(list(result_iter))

