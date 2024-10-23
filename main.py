import logging
from pathlib import Path

from geopy.point import Point
import polars as pl
import rasterio

from engines.gosom_scraper import crawler
from lib.utils import find_points_in_polygon, draw_circle, create_cover_from_points
from lib.population import pop_in_radius, _get_pop


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


def test_population():
    # COVER = Path("./queries/long_bien.geojson")
    # aoi = gpd.read_file(COVER)

    coordinates = [
        Point(21.081652, 105.841987),
        Point(21.079282, 105.842232),
        Point(21.051409, 105.850300),
        Point(21.006545, 105.874504),
        Point(21.004943, 105.876736),
        Point(20.995167, 105.899567),
        Point(21.004462, 105.910897),
        Point(21.004622, 105.920510),
        Point(21.039554, 105.938191),
        Point(21.070473, 105.925660),
        Point(21.078321, 105.905575),
        Point(21.078642, 105.891327),
        Point(21.066948, 105.866308),
    ]

    cover_area = create_cover_from_points(points=coordinates)

    POPULATION_DATASET = Path("./datasets/population/vnm_general_2020.tif")
    with rasterio.open(POPULATION_DATASET) as src:
        total_population = _get_pop(src=src, aoi=cover_area)

    print(f"Total population within the area of interest: {total_population}")


def test_pop_in_radius():
    POPULATION_DATASET = Path("./datasets/population/vnm_general_2020.tif")
    with rasterio.open(POPULATION_DATASET) as src:
        total_population = pop_in_radius(
            center=Point(21.0197031, 105.8459557), radius_km=2.0, dataset=src
        )

    print(f"Total population within the area of interest: {total_population}")


def add_pop_around_poi():
    hanoi_poi = pl.read_excel("./datasets/temp/around_poi_with_distance.xlsx")

    # make a list of dictionaries with keys are name,lat,lon
    pois = hanoi_poi.to_dicts()

    new_pois = []
    POPULATION_DATASET = Path("./datasets/population/vnm_general_2020.tif")
    with rasterio.open(POPULATION_DATASET) as src:
        for poi in pois:
            total_population = pop_in_radius(
                center=Point(poi["lat"], poi["lon"]), radius_km=2.0, dataset=src
            )
            poi["population"] = total_population
            new_pois.append(poi)

    new_df = pl.DataFrame(new_pois)
    new_df.write_excel("./datasets/temp/around_poi_with_population.xlsx")


def main():
    # hoankiem()
    # places_within_radius(center=Point(21.019430, 105.836551), radius_km=2.0)
    # test_around_point()
    # test_places_within_radius()
    # test_around_points()

    # test_population()

    test_pop_in_radius()

    # add_pop_around_poi()


if __name__ == "__main__":
    logging.basicConfig(
        filename=Path("crawling.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    RAW_DATA_DIR = Path("./datasets/raw")
    QUERY_DIR = Path("./queries")
    main()
