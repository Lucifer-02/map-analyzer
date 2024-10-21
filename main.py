import logging
from pathlib import Path

from geopy.point import Point

from engines.gosom_scraper import crawler
from utils import find_points_in_polygon, draw_circle


def hoankiem():
    query_file = QUERY_DIR / Path("atm.txt")

    HOANKIEM_CORNERS = [
        Point(21.019655, 105.86402),
        Point(21.020777, 105.84170),
        Point(21.039603, 105.84630),
        Point(21.042487, 105.85754),
    ]

    DISTANCE_POINTS_KMS = 1.0
    points = find_points_in_polygon(HOANKIEM_CORNERS, DISTANCE_POINTS_KMS)

    output_files = [RAW_DATA_DIR / Path(f"area_{i}.csv") for i in range(len(points))]

    crawler.crawl_in_area(
        output_files=output_files, query_file=query_file, points=points
    )


def around_poi():
    poi = Point(21.019430, 105.836551)

    DISTANCE_POINTS_KMS = 0.8
    circle = draw_circle(center=poi, radius_km=2.0)
    points = find_points_in_polygon(circle, DISTANCE_POINTS_KMS)

    query_file = QUERY_DIR / Path("arounds.txt")
    output_files = [RAW_DATA_DIR / Path(f"around_{i}.csv") for i in range(len(points))]
    crawler.crawl_in_area(
        output_files=output_files, query_file=query_file, points=points
    )


def main():
    # hoankiem()
    around_poi()


if __name__ == "__main__":
    logging.basicConfig(
        filename=Path("crawling.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    RAW_DATA_DIR = Path("./datasets/raw")
    QUERY_DIR = Path("./queries")
    main()
