import logging
from pathlib import Path


from geopy import Point


from utils import find_points_in_polygon

from engines.gosom_scraper import process, crawler


def main():
    # save logs to file
    logging.basicConfig(
        filename="crawler.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # HOANKIEM_CORNERS = [
    #     Point(21.019655, 105.86402),
    #     Point(21.020777, 105.84170),
    #     Point(21.039603, 105.84630),
    #     Point(21.042487, 105.85754),
    # ]

    DISTANCE_POINTS_KMS = 0.5

    crawler.crawl(
        input_file=Path("./queries/arounds.txt"),
        output_file=Path("./datasets/arounds.csv"),
        coordinates="21.033,105.853",
        zoom=18,
        timeout=1,
        depth=10,
    )

    # df = process.prepare(Path("./datasets/arounds.csv"))
    # print(df)
    # df.write_csv(Path("./datasets/results/arounds.csv"))


if __name__ == "__main__":
    main()
