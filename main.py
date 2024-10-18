import logging
from pathlib import Path

from geopy.point import Point

from engines.gosom_scraper import crawler
from utils import find_points_in_polygon


def hoankiem():
    output_file = Path("./datasets/area.csv")
    query_file = Path("./queries/atm.txt")

    HOANKIEM_CORNERS = [
        Point(21.019655, 105.86402),
        Point(21.020777, 105.84170),
        Point(21.039603, 105.84630),
        Point(21.042487, 105.85754),
    ]

    DISTANCE_POINTS_KMS = 1.0
    points = find_points_in_polygon(HOANKIEM_CORNERS, DISTANCE_POINTS_KMS)

    crawler.crawl_in_area(output_file=output_file, query_file=query_file, points=points)


def main():
    hoankiem()


if __name__ == "__main__":
    logging.basicConfig(
        filename=Path("crawling.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
