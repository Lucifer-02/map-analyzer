import logging
from pathlib import Path
from pprint import pprint
from typing import Dict, List
import json

import shapely
from geopy.point import Point
import polars as pl
import rasterio
import googlemaps
from tqdm import tqdm
import geopandas as gpd

from engines.gosom_scraper import crawler
from engines.google_api import places_api
from mylib.utils import (
    distance,
    find_points_in_polygon,
    draw_circle,
    points_to_polygon,
    geojson_to_polygon,
    city_mapping,
)
from mylib.population import pop_in_radius, _get_pop


def test_hoankiem():
    query_file = QUERY_DIR / Path("atm.txt")

    HOANKIEM_CORNERS = [
        Point(21.019655, 105.86402),
        Point(21.020777, 105.84170),
        Point(21.039603, 105.84630),
        Point(21.042487, 105.85754),
    ]
    DISTANCE_POINTS_KMS = 1.0
    points = find_points_in_polygon(
        points_to_polygon(HOANKIEM_CORNERS), DISTANCE_POINTS_KMS
    )

    output_files = [RAW_DATA_DIR / Path(f"area_{i}.csv") for i in range(len(points))]

    crawler.crawl_in_area(
        output_files=output_files, query_file=query_file, points=points
    )


def test_around_point():

    poi = Point(21.019430, 105.836551)

    logging.info(f"Starting crawl all places around {poi}")

    DISTANCE_SAMPLE_POINTS_KMS = 0.8
    circle = draw_circle(center=poi, radius_meters=2000, num_points=4)
    sample_points = find_points_in_polygon(
        points_to_polygon(circle), DISTANCE_SAMPLE_POINTS_KMS
    )

    query_file = QUERY_DIR / Path("arounds.txt")
    output_files = [
        RAW_DATA_DIR / Path(f"around_{i}.csv") for i in range(len(sample_points))
    ]
    crawler.crawl_in_area(
        output_files=output_files, query_file=query_file, points=sample_points
    )

    logging.info(f"finished crawl all places around {poi}")


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
    logging.info(f"Found {len(hanoi_poi)} POIs in Hanoi")

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
    points_on_circle = draw_circle(center=center, radius_meters=radius_km * 1000)
    sample_points = find_points_in_polygon(
        points_to_polygon(points_on_circle), DISTANCE_SAMPLE_POINTS_KMS
    )

    query_file = QUERY_DIR / Path("arounds.txt")
    output_files = [
        RAW_DATA_DIR / Path(f"around_{name}_{i}.csv") for i in range(len(sample_points))
    ]
    crawler.crawl_in_area(
        output_files=output_files, query_file=query_file, points=sample_points
    )


def test_population():
    COVER = Path("./queries/ha_noi.geojson")
    cover_area = gpd.read_file(COVER)

    # coordinates = [
    #     Point(21.081652, 105.841987),
    #     Point(21.079282, 105.842232),
    #     Point(21.051409, 105.850300),
    #     Point(21.006545, 105.874504),
    #     Point(21.004943, 105.876736),
    #     Point(20.995167, 105.899567),
    #     Point(21.004462, 105.910897),
    #     Point(21.004622, 105.920510),
    #     Point(21.039554, 105.938191),
    #     Point(21.070473, 105.925660),
    #     Point(21.078321, 105.905575),
    #     Point(21.078642, 105.891327),
    #     Point(21.066948, 105.866308),
    # ]

    # cover_area = create_cover_from_points(points=coordinates)

    POPULATION_DATASET = Path(
        "./datasets/population/GHS_POP_E2025_GLOBE_R2023A_4326_3ss_V1_0.tif"
        # "./datasets/population/GHS_POP_E2030_GLOBE_R2023A_4326_3ss_V1_0.tif"
        # "./datasets/population/vnm_general_2020.tif"
    )
    with rasterio.open(POPULATION_DATASET) as src:
        total_population = _get_pop(src=src, aoi=cover_area)

    print(f"Total population within the area of interest: {total_population}")


def test_vietnam_population():

    # Specify the directory path
    directory = Path("/media/lucifer/STORAGE/IMPORTANT/map-analyzer/queries/")

    META_POPULATION_DATASET = Path("./datasets/population/vnm_general_2020.tif")

    GHS_POPULATION_DATASET = Path(
        "./datasets/population/GHS_POP_E2025_GLOBE_R2023A_4326_3ss_V1_0.tif"
    )
    CITY_MAP = city_mapping()
    # List all files (excluding directories)
    files = [file for file in directory.glob("*.geojson")]  # Lists only .txt files

    result = []

    for file in tqdm(files):
        cover_area = gpd.read_file(file)
        try:
            with rasterio.open(META_POPULATION_DATASET) as src:
                total_population = _get_pop(src=src, aoi=cover_area)
                result.append(
                    {
                        "area": CITY_MAP[file.name],
                        "source": "meta",
                        "population": total_population,
                    }
                )
        except ValueError:
            result.append(
                {"city": CITY_MAP[file.name], "source": "meta", "population": -1}
            )

        try:
            with rasterio.open(GHS_POPULATION_DATASET) as src:
                total_population = _get_pop(src=src, aoi=cover_area)
                result.append(
                    {
                        "area": CITY_MAP[file.name],
                        "source": "ghs",
                        "population": total_population,
                    }
                )
        except ValueError:
            result.append(
                {"area": CITY_MAP[file.name], "source": "ghs", "population": -1}
            )

    TCTK = (
        pl.read_csv(Path("./datasets/population/tctk.csv"))
        .with_columns(pl.lit("tctk").alias("source"))
        .to_dicts()
    )

    result.extend(TCTK)

    df = pl.DataFrame(result)
    print(df)
    df.write_csv("pop.csv")


def test_pop_in_radius():
    POPULATION_DATASET = Path(
        "./datasets/population/GHS_POP_E2025_GLOBE_R2023A_4326_3ss_V1_0_R7_C29.tif"
    )
    with rasterio.open(POPULATION_DATASET) as src:
        total_population = pop_in_radius(
            center=Point(21.0197031, 105.8459557), radius_km=2.0, dataset=src
        )

    print(f"Total population within the area of interest: {total_population}")


def add_pop_around_poi():
    hanoi_poi = pl.read_excel("./datasets/temp/around_poi_with_population.xlsx")

    # make a list of dictionaries with keys are name,lat,lon
    pois = hanoi_poi.to_dicts()

    new_pois = []
    POPULATION_DATASET = Path("./datasets/population/vnm_general_2020.tif")
    with rasterio.open(POPULATION_DATASET) as src:
        for poi in pois:
            print(f"Processing {poi}")
            total_population = pop_in_radius(
                center=Point(poi["lat"], poi["lon"]), radius_km=2.0, dataset=src
            )
            poi["population"] = total_population
            new_pois.append(poi)

    new_df = pl.DataFrame(new_pois)
    new_df.write_excel("./datasets/temp/around_poi_with_population.xlsx")


def test_google_api():
    gmaps = googlemaps.Client(key="AIzaSyASSHrsakND-N8dCFji0KkESaeyLoWq87Y")

    # Define search parameters
    location = Point(latitude=21.0212062, longitude=105.8343646)
    radius = 2000
    place_type = "school"
    # keyword = "vietcombank"

    results = places_api.nearby_search(
        client=gmaps,
        location=location,
        radius=radius,
        place_type=place_type,
        # keyword=keyword,
    )

    # Process the results
    print(len(results))
    for result in results:
        pprint(result)
        print("---")


def test_near_api():
    # --------setup--------------
    RADIUS = 2000
    POI_TYPES = ["atm", "bank", "cafe", "hospital", "school"]

    gmaps = googlemaps.Client(key="AIzaSyASSHrsakND-N8dCFji0KkESaeyLoWq87Y")

    df = pl.read_excel(Path("./datasets/original/atm.xlsx"))
    valid_df = df.filter(
        pl.col("LONGITUDE").str.contains(r"\d+\.\d+"),
        pl.col("LATITUDE").str.contains(r"\d+\.\d+"),
    )
    hanoi_atms = valid_df.filter(pl.col("CITY").str.contains(r"(HANOI)|(HA NOI)"))

    # --------start--------------
    num_places = 0
    records: List[Dict] = []

    for i in tqdm(range(len(hanoi_atms[:]))):
        logging.info(f"Progress {i}/{len(hanoi_atms)}")
        try:
            atm_center = Point(
                latitude=hanoi_atms["LATITUDE"][i],
                longitude=hanoi_atms["LONGITUDE"][i],
            )

            places_around: List = []
            for poi_type in POI_TYPES:
                logging.info(f"Nearby searching for type {poi_type}")
                places_around.extend(
                    places_api.nearby_search(
                        client=gmaps,
                        location=atm_center,
                        radius=RADIUS,
                        place_type=poi_type,
                    )
                )

            num_places += len(places_around)

            # filter all place outside radius
            for place in places_around:
                if (
                    distance(atm_center, Point(latitude=place.lat, longitude=place.lon))
                    <= RADIUS
                ):
                    records.append(
                        {
                            "atm_center_id": hanoi_atms["ATM_ID"][i],
                            "id": place.id,
                            "lat": place.lat,
                            "lon": place.lon,
                            "name": place.name,
                            "categories": ",".join(place.categories),
                        }
                    )

        except ValueError as e:
            logging.error(f"{e} with record {hanoi_atms.select(pl.all())[i]}")

    around = pl.DataFrame(records)
    print(num_places)
    print(around)

    # save
    around.write_parquet("./datasets/raw/arounds_atm.parquet")


def test_point_radius_api():
    # --------setup--------------
    gmaps = googlemaps.Client(key="AIzaSyASSHrsakND-N8dCFji0KkESaeyLoWq87Y")

    RADIUS = 2000
    POI_TYPES = ["atm", "bank", "cafe", "school"]

    poi = Point(21.019430, 105.836551)
    DISTANCE_SAMPLE_POINTS_KMS = 0.8
    circle = draw_circle(center=poi, radius_meters=RADIUS, num_points=4)
    sample_points = find_points_in_polygon(
        points_to_polygon(circle), DISTANCE_SAMPLE_POINTS_KMS
    )

    # --------start--------------
    num_places = 0
    records: List[Dict] = []

    for point in tqdm(sample_points):
        places_around: List[places_api.DacPlace] = []
        for poi_type in POI_TYPES:
            logging.info(f"Nearby searching for type {poi_type} on {point}")
            places_around.extend(
                places_api.nearby_search(
                    client=gmaps,
                    location=point,
                    radius=RADIUS,
                    place_type=poi_type,
                )
            )

        num_places += len(places_around)

        # remove all place outside the circle
        for place in places_around:
            if (
                distance(Point(latitude=place.lat, longitude=place.lon), point).meters
                <= RADIUS
            ):
                records.append(
                    {
                        "id": place.id,
                        "lat": place.lat,
                        "lon": place.lon,
                        "name": place.name,
                        "categories": ",".join(place.categories),
                    }
                )

    pois = pl.DataFrame(records)
    print(num_places)
    print(pois)

    # save
    # pois.write_parquet("./datasets/raw/area_pois.parquet")


def test_area_api():
    # --------setup--------------
    RADIUS = 2000
    POI_TYPES = ["atm", "bank", "cafe", "hospital", "school"]
    COVER = Path("./queries/hanoi_city.geojson")
    with open(COVER, "r") as f:
        data = json.load(f)
    poly = geojson_to_polygon(data)
    points = find_points_in_polygon(polygon=poly, distance_points_kms=0.8)

    gmaps = googlemaps.Client(key="AIzaSyASSHrsakND-N8dCFji0KkESaeyLoWq87Y")

    # --------start--------------
    num_places = 0
    records: List[Dict] = []

    for point in tqdm(points):
        places_around: List = []
        for poi_type in POI_TYPES:
            logging.info(f"Nearby searching for type {poi_type} on {point}")
            places_around.extend(
                places_api.nearby_search(
                    client=gmaps,
                    location=point,
                    radius=RADIUS,
                    place_type=poi_type,
                )
            )

        num_places += len(places_around)

        # remove all place outside the polygon
        for place in places_around:
            if poly.contains(shapely.geometry.Point(place.lon, place.lat)):
                records.append(
                    {
                        "id": place.id,
                        "lat": place.lat,
                        "lon": place.lon,
                        "name": place.name,
                        "categories": ",".join(place.categories),
                    }
                )

    pois = pl.DataFrame(records)
    print(num_places)
    print(pois)

    # save
    pois.write_parquet("./datasets/raw/area_pois.parquet")


def main():
    # test_hoankiem()
    # places_within_radius(center=Point(21.019430, 105.836551), radius_km=2.0)
    # test_around_point()
    # test_places_within_radius()
    # test_around_points()
    # test_population()
    # test_pop_in_radius()
    # add_pop_around_poi()
    # test_google_api()
    # test_near_api()
    # test_area_api()
    # test_point_radius_api()
    test_vietnam_population()


if __name__ == "__main__":
    logging.basicConfig(
        filename=Path("crawling.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    RAW_DATA_DIR = Path("./datasets/raw")
    QUERY_DIR = Path("./queries")
    main()
