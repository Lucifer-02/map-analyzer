import logging
from pathlib import Path

from geopy.point import Point
import polars as pl

from engines.gosom_scraper import crawler
from utils import find_points_in_polygon, draw_circle


def test_hoankiem():
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


def test_around_point():
    poi = Point(21.019430, 105.836551)

    DISTANCE_SAMPLE_POINTS_KMS = 0.9
    circle = draw_circle(center=poi, radius_km=2.0, num_points=4)
    sample_points = find_points_in_polygon(circle, DISTANCE_SAMPLE_POINTS_KMS)

    query_file = QUERY_DIR / Path("arounds.txt")
    output_files = [
        RAW_DATA_DIR / Path(f"around_{i}.csv") for i in range(len(sample_points))
    ]
    crawler.crawl_in_area(
        output_files=output_files, query_file=query_file, points=sample_points
    )


def test_places_within_radius():
    df = pl.read_excel("./datasets/results/poi_with_coordinates_full.xlsx")
    hanoi_poi = df.filter(pl.col("Address Line 1").str.contains(r"(Ha Noi)|(Hanoi)"))

    # make a list of dictionaries with keys are name,lat,lon
    pois = hanoi_poi.to_dicts()

    for poi in pois:
        places_within_radius(
            name=poi["Unique Identifier"],
            center=Point(poi["lat"], poi["lon"]),
            radius_km=2.0,
        )


def test_around_points():
    df = pl.read_excel("./datasets/results/poi_with_coordinates_full.xlsx")
    # not contain "100","101","102","103","105","106","108","109","110","111","200","202","203","204","205"
    hanoi_poi = df.filter(
        pl.col("Address Line 1").str.contains(r"(Ha Noi)|(Hanoi)"),
    )
    # print(hanoi_poi)
    # logging.info(f"Found {len(hanoi_poi)} POIs in Hanoi")

    # make a list of dictionaries with keys are name,lat,lon
    pois = hanoi_poi.to_dicts()

    query_file = QUERY_DIR / Path("arounds.txt")
    for poi in pois:
        output_file = RAW_DATA_DIR / Path(f"around_{poi['Unique Identifier']}.csv")
        crawler.crawl_around_point(
            point=Point(poi["lat"], poi["lon"]),
            query_file=query_file,
            output_file=output_file,
        )


def places_within_radius(
    name: str,
    center: Point,
    radius_km: float,
    DISTANCE_SAMPLE_POINTS_KMS: float = 0.9,
):

    points_on_circle = draw_circle(center=center, radius_km=radius_km)
    sample_points = find_points_in_polygon(points_on_circle, DISTANCE_SAMPLE_POINTS_KMS)

    query_file = QUERY_DIR / Path("arounds.txt")
    output_files = [
        RAW_DATA_DIR / Path(f"around_{name}_{i}.csv") for i in range(len(sample_points))
    ]
    crawler.crawl_in_area(
        output_files=output_files, query_file=query_file, points=sample_points
    )


def main():
    # hoankiem()
    # places_within_radius(center=Point(21.019430, 105.836551), radius_km=2.0)
    # test_around_point()
    # test_places_within_radius()
    test_around_points()


if __name__ == "__main__":
    logging.basicConfig(
        filename=Path("crawling.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    RAW_DATA_DIR = Path("./datasets/raw")
    QUERY_DIR = Path("./queries")
    main()
