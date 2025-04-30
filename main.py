import logging
from pathlib import Path
from pprint import pprint
from typing import Dict, List, Set
import json

import shapely
from geopy.point import Point
import polars as pl
import rasterio
import googlemaps
from tqdm import tqdm
import geopandas as gpd
import click

from engines.gosom_scraper import crawler
from engines.google_api import places_api
from mylib.population import pop_in_radius, _get_pop
from mylib import ALL_TYPES, utils, POI_GROUPS, viz


def test_hoankiem():
    HOANKIEM_CORNERS = [
        Point(21.019655, 105.86402),
        Point(21.020777, 105.84170),
        Point(21.039603, 105.84630),
        Point(21.042487, 105.85754),
    ]
    DISTANCE_POINTS_KMS = 1.0
    points = utils.find_points_in_polygon(
        utils.points_to_polygon(HOANKIEM_CORNERS), DISTANCE_POINTS_KMS
    )

    crawler.crawl_in_area(points=points, keywords={"cafe"})


def test_around_point():

    poi = Point(21.019430, 105.836551)

    logging.info(f"Starting crawl all places around {poi}")

    circle = utils.draw_circle(center=poi, radius_meters=2000, num_points=4)

    for dist in range(500, 1501, 500):
        sample_points = utils.find_points_in_polygon(circle, dist)

        viz.map_points(sample_points)
        df = crawler.crawl_in_area(points=sample_points, keywords={"atm", "school"})
        logging.info(
            f"Result length: {len(df)}, dedupliced length: {len(df.unique())}, ratio: {1-(len(df) / len(df.unique()))}."
        )

    logging.info(f"Finished crawl all places around {poi}")


def test_crawl_area():

    poi = Point(21.019430, 105.836551)

    logging.info(f"Starting crawl all places around {poi}")

    circle = utils.draw_circle(center=poi, radius_meters=2000, num_points=4)

    DISTANCE_SAMPLE_POINTS_MS = 500
    sample_points = utils.find_points_in_polygon(circle, DISTANCE_SAMPLE_POINTS_MS)

    viz.map_points(sample_points)
    df = crawler.crawl_in_area(points=sample_points, keywords={"atm", "school"})
    logging.info(
        f"Result length: {len(df)}, dedupliced length: {len(df.unique())}, ratio: {1-(len(df) / len(df.unique()))}."
    )

    logging.info(f"Finished crawl all places around {poi}")


def test_crawl_atm_places_within_radius():
    # PDG
    # df = pl.read_excel("./datasets/results/poi_with_coordinates_full.xlsx")
    # hanoi_poi = df.filter(pl.col("Address Line 1").str.contains(r"(Ha Noi)|(Hanoi)"))
    # pois = hanoi_poi.to_dicts()

    # ATM
    df = pl.read_excel("./datasets/original/Mẫu 3 Pool ATM Data.xlsx")

    valid_df = df.filter(
        pl.col("LONGITUDE").str.contains(r"\d+\.\d+"),
        pl.col("LATITUDE").str.contains(r"\d+\.\d+"),
    )
    filted = valid_df.filter(pl.col("CITY").str.contains(r"(HANOI)|(HA NOI)"))
    atms = filted.to_dicts()

    logging.info(f"There are {len(atms)} ATMs.")

    START_IDX = 0
    # make a list of dictionaries with keys are name,lat,lon
    for i, poi in enumerate(atms[START_IDX:]):
        output = Path(
            f'./datasets/raw/oss/atms/ha_noi/atm_{poi["ATM_ID"]}_{START_IDX + i}.parquet'
        )
        if output.exists() == False:
            result = crawl_places_within_radius(
                center=Point(latitude=poi["LATITUDE"], longitude=poi["LONGITUDE"]),
                radius_m=2000,
                categories={"atm"},
            )
            result.write_parquet(output)
            logging.info(
                f'Written result for ATM id {poi["ATM_ID"]} with lat: {poi["LATITUDE"]} and lon: {poi["LONGITUDE"]} to {output}.'
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

    for poi in pois:
        crawler.crawl(center=Point(poi["lat"], poi["lon"]), keywords={"atm"})


def crawl_places_within_radius(
    center: Point,
    radius_m: float,
    categories: Set[str],
    DISTANCE_SAMPLE_POINTS_MS: float = 1000,
):
    points_on_circle = utils.draw_circle(center=center, radius_meters=radius_m)
    sample_points = utils.find_points_in_polygon(
        polygon=points_on_circle,
        distance_points_ms=DISTANCE_SAMPLE_POINTS_MS,
    )
    viz.map_points(sample_points)
    logging.info(
        f"Found {len(sample_points)} sample points around {center.format_decimal()}"
    )

    df = crawler.crawl_in_area(points=sample_points, keywords=categories)

    return utils.filter_within_radius(
        df,
        lat_col="latitude",
        lon_col="longitude",
        radius_m=radius_m,
        center=center,
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
    directory = Path(
        "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/queries/with_ocean/"
    )

    META_POPULATION_DATASET = Path("./datasets/population/vnm_general_2020.tif")

    GHS_POPULATION_DATASET = Path(
        "./datasets/population/GHS_POP_E2025_GLOBE_R2023A_4326_3ss_V1_0.tif"
    )
    WORLDPOP_POPULATION_DATASET = Path(
        "./datasets/population/vnm_pop_2024_CN_100m_R2024B_v1.tif"
    )

    CITY_MAP = utils.city_mapping()
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
            with rasterio.open(GHS_POPULATION_DATASET) as src:
                total_population = _get_pop(src=src, aoi=cover_area)
                result.append(
                    {
                        "area": CITY_MAP[file.name],
                        "source": "ghs",
                        "population": total_population,
                    }
                )
            with rasterio.open(WORLDPOP_POPULATION_DATASET) as src:
                # total_population = _get_pop_2(
                #     geojson_path=file, raster_path=WORLDPOP_POPULATION_DATASET
                # )
                total_population = _get_pop(src=src, aoi=cover_area)
                result.append(
                    {
                        "area": CITY_MAP[file.name],
                        "source": "worldpop",
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

    df = pl.DataFrame(result).sort("area")
    print(df)
    df.write_csv("pop.csv")


def test_pop_in_radius():
    POPULATION_DATASET = Path(
        "./datasets/population/GHS_POP_E2025_GLOBE_R2023A_4326_3ss_V1_0_R7_C29.tif"
    )
    with rasterio.open(POPULATION_DATASET) as src:
        total_population = pop_in_radius(
            center=Point(21.0197031, 105.8459557), radius_meters=2.0, dataset=src
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
                center=Point(poi["lat"], poi["lon"]), radius_meters=2000, dataset=src
            )
            poi["population"] = total_population
            new_pois.append(poi)

    new_df = pl.DataFrame(new_pois)
    new_df.write_excel("./datasets/temp/around_poi_with_population.xlsx")


def test_api():
    gmaps = googlemaps.Client(key="AIzaSyCDPST-2Vz3DBukf4sfkPZwUIUfJdHvwLQ")

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


def test_api_near():
    # --------setup--------------
    RADIUS = 1000
    POI_TYPES = ["atm", "school"]

    gmaps = googlemaps.Client(key="AIzaSyCDPST-2Vz3DBukf4sfkPZwUIUfJdHvwLQ")

    # df = pl.read_excel(Path("./datasets/original/atm.xlsx"))
    df = pl.read_excel(Path("./datasets/original/Mẫu 3 Pool ATM Data.xlsx"))
    valid_df = df.filter(
        pl.col("LONGITUDE").str.contains(r"\d+\.\d+"),
        pl.col("LATITUDE").str.contains(r"\d+\.\d+"),
    )
    hanoi_atms = valid_df.filter(pl.col("CITY").str.contains(r"(HANOI)|(HA NOI)"))

    # --------start--------------
    num_places = 0
    records: List[Dict] = []

    for i in tqdm(range(len(hanoi_atms[:10]))):
        logging.info(f"Progress {i}/{len(hanoi_atms)}")
        try:
            atm_center = Point(
                latitude=hanoi_atms["LATITUDE"][i],
                longitude=hanoi_atms["LONGITUDE"][i],
            )

            places_around: List[places_api.DacPlace] = []
            for poi_type in POI_TYPES:
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
                    utils.distance(
                        atm_center, Point(latitude=place.lat, longitude=place.lon)
                    )
                    <= RADIUS
                ):
                    records.append(
                        {
                            "atm_center_id": hanoi_atms["ATM_ID"][i],
                            "id": place.place_id,
                            "lat": place.lat,
                            "lon": place.lon,
                            "name": place.name,
                            "categories": ",".join(place.categories),
                            "address": place.address,
                        }
                    )

        except ValueError as e:
            logging.error(f"{e} with record {hanoi_atms.select(pl.all())[i]}")

    around = pl.DataFrame(records)
    print(num_places)
    print(around)

    # save
    around.write_parquet("./datasets/raw/arounds_hanoi_atm.parquet")


def test_api_point_radius():
    # --------setup--------------
    gmaps = googlemaps.Client(key="AIzaSyCDPST-2Vz3DBukf4sfkPZwUIUfJdHvwLQ")

    RADIUS = 2000
    POI_TYPES = ["atm", "bank", "cafe", "school"]

    poi = Point(21.019430, 105.836551)
    DISTANCE_SAMPLE_POINTS_KMS = 0.8
    circle = utils.draw_circle(center=poi, radius_meters=RADIUS, num_points=4)
    sample_points = utils.find_points_in_polygon(circle, DISTANCE_SAMPLE_POINTS_KMS)

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
                utils.distance(
                    Point(latitude=place.lat, longitude=place.lon), point
                ).meters
                <= RADIUS
            ):
                records.append(
                    {
                        "id": place.place_id,
                        "lat": place.lat,
                        "lon": place.lon,
                        "name": place.name,
                        "categories": ",".join(place.categories),
                        "address": place.address,
                    }
                )

    pois = pl.DataFrame(records)
    print(num_places)
    print(pois)

    # save
    # pois.write_parquet("./datasets/raw/area_pois.parquet")


def test_api_area():
    # --------setup--------------
    RADIUS = 1000
    POI_TYPES = ["atm", "bank", "cafe", "hospital", "school"]
    # POI_TYPES = ["hospital", "school"]
    COVER = Path("./queries/ha_noi.geojson")
    with open(COVER, "r", encoding="utf8") as f:
        data = json.load(f)
    poly = utils.geojson_to_polygon(data)
    cover_points = utils.find_points_in_polygon(polygon=poly, distance_points_ms=1500)

    gmaps = googlemaps.Client(key="AIzaSyCDPST-2Vz3DBukf4sfkPZwUIUfJdHvwLQ")

    # --------start--------------
    num_places = 0
    records: List[Dict] = []

    for i, point in enumerate(cover_points):
        logging.info(f"Progress: {i+1}/{len(cover_points)}")

        places_around: List[places_api.DacPlace] = []
        for poi_type in POI_TYPES:
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
                        "id": place.place_id,
                        "lat": place.lat,
                        "lon": place.lon,
                        "name": place.name,
                        "categories": ",".join(place.categories),
                        "address": place.address,
                    }
                )

        logging.info(pl.DataFrame(records))

    pois = pl.DataFrame(records)
    print(num_places)
    print(pois)
    print(pois.unique())

    # save
    pois.write_parquet(f"{COVER.stem}.parquet")


def test_area_crawl():
    logging.info("Start crawl...")
    # --------setup--------------
    # POI_TYPES = ["atm", "bank", "cafe", "hospital", "school", "restaurant", "park"]
    COVER = Path("./queries/with_ocean/vinh_phuc.geojson")
    with open(COVER, "r", encoding="utf8") as f:
        data = json.load(f)
    poly = utils.geojson_to_polygon(data)
    DISTANCE_POINTS_MS = 2000
    points = utils.find_points_in_polygon(
        polygon=poly, distance_points_ms=DISTANCE_POINTS_MS
    )  # DONT CHANGE DISTANCE
    # map_points(points)

    # pois = crawler.crawl_in_area(points=points, keywords=list(ALL_TYPES))

    FROM_IDX = 0
    for i, point in enumerate(points[FROM_IDX:]):
        logging.info(
            f"Crawling {i+1}/{len(points[FROM_IDX:])} with distane of sample points is {DISTANCE_POINTS_MS} meters from area {COVER}..."
        )
        save_path = Path(f"./datasets/raw/oss/{COVER.stem}_{i+FROM_IDX}.parquet")
        if save_path.exists() == False:
            try:
                pois = crawler.crawl(center=point, keywords=ALL_TYPES, ncores=8)
                result = utils.filter_within_polygon1(df=pois, poly=poly)
                logging.info(f"Result after filted all outside the area: {result}")
                result.write_parquet(save_path)
            except Exception as e:
                logging.error(f"Error for point {point}: {e}, skipping...")
        else:
            logging.info(f"The dataset {save_path} already exists, skipping...")


def test_area_crawl2(
    cover: Path,
    radius: float = 2000,
    factor: float = 1,
    base_distance_points_ms: float = 2500,
    ncores: int = 4,
):
    logging.info("Start crawl...")
    # --------setup--------------
    with open(cover, "r", encoding="utf8") as f:
        data = json.load(f)
    polys = utils.geojson_to_polygons(data)
    DISTANCE_POINTS_MS = base_distance_points_ms * factor

    # print(polys)

    for poly in polys:
        points = utils.find_points_in_polygon(
            polygon=poly, distance_points_ms=DISTANCE_POINTS_MS
        )

        if len(points) == 0:
            continue

        viz.map_points(points)

        FROM_IDX = 0
        for i, point in enumerate(points[FROM_IDX:]):
            logging.info(
                f"Crawling {i+1}/{len(points[FROM_IDX:])} with distane of sample points is {DISTANCE_POINTS_MS} meters from area {cover}..."
            )
            save_path = Path(f"./datasets/raw/oss/{cover.stem}_{i+FROM_IDX}.parquet")
            if save_path.exists() == False:
                try:
                    pois = crawler.crawl(
                        center=point,
                        keywords=ALL_TYPES,
                        ncores=ncores,
                        radius=radius,
                        area=cover.stem.replace("_", " "), # extract area name
                    )
                    result = utils.filter_within_polygon1(df=pois, poly=poly)
                    logging.info(f"Result after filted all outside the area: {result}")
                    result.write_parquet(save_path)
                except Exception as e:
                    logging.error(f"Error for point {point}: {e}, skipping...")
            else:
                logging.info(f"The dataset {save_path} already exists, skipping...")


def classify_group2(category: str, group_dict: Dict[str, Set]) -> List[str]:

    result = set()
    for key in group_dict.keys():
        if category.lower() in group_dict[key]:
            result.add(key)

    assert len(result) != 0
    return list(result)


# for new dataset
def post_process_atm2():
    # ATM
    df = pl.read_excel("./datasets/original/Mẫu 3 Pool ATM Data.xlsx")
    POPULATION_DATASET = Path("./datasets/population/vnm_general_2020.tif")

    valid_df = df.filter(
        pl.col("LONGITUDE").str.contains(r"\d+\.\d+"),
        pl.col("LATITUDE").str.contains(r"\d+\.\d+"),
    )
    # filted = valid_df.filter(pl.col("CITY").str.contains(r"(HANOI)|(HA NOI)"))
    # filted = valid_df.filter(pl.col("CITY").str.contains(r"(HCM)|(HO CHI MINH)"))
    filted = valid_df.filter(
        pl.col("CITY").str.contains(r"(HCM)|(HO CHI MINH)|(HANOI)|(HA NOI)")
    )
    atms = filted.to_dicts()

    # print(atms)
    files = []

    files.append(list(Path("./datasets/raw/oss/").glob("ha_noi_*.parquet")))
    files.append(list(Path("./datasets/raw/oss/").glob("hcm_*.parquet")))

    dfs = [pl.read_parquet(file) for file in files]

    places = pl.concat(dfs).unique(subset=["link"])

    places_group = places.with_columns(
        pl.col("query")
        .map_elements(
            lambda x: classify_group2(category=x, group_dict=POI_GROUPS),
            return_dtype=pl.DataType.from_python(list[str]),
        )
        .alias("group")
    )
    print(places_group)

    result = pl.DataFrame()

    for atm in tqdm(atms):
        logging.info(f'Adding groups count for ATM {atm["ATM_ID"]}')
        try:
            center = Point(
                latitude=atm["LATITUDE"],
                longitude=atm["LONGITUDE"],
            )

            pois = utils.filter_within_radius(
                places_group,
                lat_col="latitude",
                lon_col="longitude",
                radius_m=1000,
                center=center,
            )
            count_pois = pois.explode("group").group_by("group").len().drop_nulls()

            atm.update(
                {
                    count_pois.filter(pl.col("group").eq("group1"))
                    .row(0)[0]: count_pois.filter(pl.col("group").eq("group1"))
                    .row(0)[1],
                    count_pois.filter(pl.col("group").eq("group2"))
                    .row(0)[0]: count_pois.filter(pl.col("group").eq("group2"))
                    .row(0)[1],
                    count_pois.filter(pl.col("group").eq("group3"))
                    .row(0)[0]: count_pois.filter(pl.col("group").eq("group3"))
                    .row(0)[1],
                    count_pois.filter(pl.col("group").eq("group4"))
                    .row(0)[0]: count_pois.filter(pl.col("group").eq("group4"))
                    .row(0)[1],
                }
            )
            with_groups = pl.DataFrame(atm)
            # print(with_groups)

            with rasterio.open(POPULATION_DATASET) as src:
                total_population = pop_in_radius(
                    center=center, radius_meters=1000, dataset=src
                )
                with_groups_pop = with_groups.hstack(
                    [pl.Series("population(radius 1 kms)", [total_population])]
                )

            result.vstack(with_groups_pop, in_place=True)

        except ValueError as e:
            logging.error(
                f'Error: {e}, ATM id: {atm["ATM_ID"]}, point: {atm["LATITUDE"], atm["LONGITUDE"]}',
                stack_info=True,
            )
        except pl.exceptions.OutOfBoundsError as e:
            logging.error(
                f'Not found groups in ATM id: {atm["ATM_ID"]}, point: {atm["LATITUDE"], atm["LONGITUDE"]}, Error: {e}',
                stack_info=True,
            )

    print(result)

    logging.info(f"Finished adding to {len(result)} ATMs")

    result.write_excel("./datasets/results/atms.xlsx")


# according pop density of Tong cuc thong ke
def scale(x: float) -> float:
    # f(x) = ax+b, f in [0.5;6] and x in [0.57;39.93], f(0.57) = 0.5, f(39.93)=6
    a = 0.14
    b = 0.42
    return a * x + b


def factor(densities: pl.DataFrame, area: Path) -> float:
    BASE_DENSITY = 2555.8  # hanoi

    target_density = densities.filter(pl.col("area").eq(area.stem))[0, "density"]

    return scale(
        BASE_DENSITY / target_density
    )  # (hanoi pop density) / (district pop density)


@click.command()
@click.argument("area")
@click.option("--ncores", default=2, help="number of cores to use")
def cli(area, ncores):
    COVER = Path(area)
    FACTOR = factor(
        densities=pl.read_csv("./datasets/population/V02.01.csv"), area=COVER
    )
    logging.info(f"factor for sample point: {FACTOR}")
    test_area_crawl2(
        cover=COVER,
        factor=FACTOR,
        base_distance_points_ms=3000,
        ncores=ncores,
        radius=5000,
    )


def main():
    # COVER = Path("./queries/nghe_an.geojson")
    # FACTOR = factor(
    #     densities=pl.read_csv("./datasets/population/V02.01.csv"), area=COVER
    # )
    # logging.info(f"factor for sample point: {FACTOR}")
    # test_area_crawl2(cover=COVER, factor=FACTOR, base_distance_points_ms=2500, ncores=4)
    cli()


if __name__ == "__main__":
    logging.basicConfig(
        filename=Path("crawling.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    RAW_DATA_DIR = Path("./datasets/raw")
    QUERY_DIR = Path("./queries")

    main()
