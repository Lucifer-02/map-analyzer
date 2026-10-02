#! uv run

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import click
import geopandas as gpd
import polars as pl
import rasterio
from geopy.point import Point
from map_miner import scrape_google_maps
from tqdm import tqdm

from mylib import ALL_TYPES, AREAS, POI_GROUPS, utils
from mylib.population import _get_pop, pop_in_radius
from mylib.utils import point_to_string


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


def test_area_crawl2(
    cover: Path,
    factor: float = 1,
    base_distance_points_ms: float = 2500,
    output_dir: Path = Path("./datasets/raw/oss/"),
):
    logger.info("Start crawl...")
    # --------setup--------------
    with open(cover, "r", encoding="utf8") as f:
        data = json.load(f)
    polys = utils.geojson_to_polygons(data)
    assert len(polys) >= 1

    logger.info(f"Found {len(polys)} polygons.")
    DISTANCE_POINTS_MS = base_distance_points_ms * factor

    # print(polys)

    for poly_idx, poly in enumerate(polys):
        points = utils.find_points_in_polygon(
            polygon=poly, distance_points_ms=DISTANCE_POINTS_MS
        )

        if len(points) == 0:
            continue

        # viz.map_points(points)

        for i, point in enumerate(points):
            logger.info(
                f"Crawling {i + 1}/{len(points)} with distane of sample points is {DISTANCE_POINTS_MS} meters from area {cover}, at {point_to_string(point)}..."
            )
            save_path = output_dir / Path(f"{cover.stem}_{poly_idx}_{i}.parquet")
            if not save_path.exists():
                try:
                    logging.getLogger("main.scraper").setLevel(logging.INFO)
                    pois = asyncio.run(
                        scrape_google_maps(
                            queries=ALL_TYPES,
                            max_places=50,
                            lang="en",
                            geo_coordinates=point,
                            zoom=18,
                            # headless=False,
                            # proxy={
                            #     # "server": "http://gate.decodo.com:10000",
                            #     # "username": "spp86iv7zu",
                            #     # "password": "6yoqpXiuaF5bT_83sV",
                            #     "server": "socks5://127.0.0.1:9050",
                            #     "bypass": DEFAULT_PROXY_BYPASS,
                            # },
                        )
                    )
                    logger.info("Done crawling, starting preprocess...")
                    print(f"pois: {pois}")
                    result = utils.filter_within_polygon1(df=pois, poly=poly)
                    logger.info(f"Result after filted all outside the area: {result}")
                    result.write_parquet(save_path)
                except Exception as e:
                    logger.error(f"Error for point {point}: {e}, skipping...")
            else:
                logger.info(f"The dataset {save_path} already exists, skipping...")


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


def summary():
    # =================== summry each area =========================

    for area in AREAS:
        df = pl.read_parquet(f"./datasets/raw/oss/{area}_*.parquet").unique()
        new_df = df.with_columns(pl.lit(area).alias("province"))
        new_df.write_parquet(f"./datasets/raw/oss/summary/{area}.parquet")
        print(area, len(df))

    # =================== summry areas =========================

    df = pl.read_parquet(
        "./datasets/raw/oss/summary/*.parquet", allow_missing_columns=True
    )

    new_df = (
        df.unique()
        .with_columns(
            is_poi_transport=pl.col("query").is_in(POI_GROUPS["group1"]).cast(pl.Int64)
        )
        .with_columns(
            is_poi_ecom=pl.col("query").is_in(POI_GROUPS["group2"]).cast(pl.Int64)
        )
        .with_columns(
            is_poi_popu=pl.col("query").is_in(POI_GROUPS["group3"]).cast(pl.Int64)
        )
        .with_columns(
            is_ATM=(
                pl.col("categories").str.contains("(atm)|(ATM)")
                | pl.col("query").eq("atm")
            ).cast(pl.Int64)
        )
        .with_columns([pl.arange(0, df.height).alias("id")])
        .with_columns(pl.lit(datetime.now(UTC)).alias("created_date"))
        .with_columns(pl.lit(datetime.now(UTC)).alias("updated_date"))
    )

    new_df.write_parquet("./datasets/results/vietnam.parquet")


@click.command()
@click.argument("area")
@click.option("--ncores", default=2, help="number of cores to use")
@click.option(
    "--base_distance_points_ms",
    default=3500,
    help="base distance(meter) between sample points",
)
@click.option("--radius", default=5000, help="radius to filter around a point")
def cli(area, base_distance_points_ms, radius):
    COVER = Path(area)
    FACTOR = factor(
        densities=pl.read_csv("./datasets/population/V02.01.csv"), area=COVER
    )

    logger.info(
        f"factor for sample point: {FACTOR}, radius: {radius}, base_distance_points_ms: {base_distance_points_ms}."
    )
    test_area_crawl2(
        cover=COVER,
        factor=FACTOR,
        base_distance_points_ms=base_distance_points_ms,
    )


def filter_vcb(pois: pl.DataFrame) -> pl.DataFrame:
    return pois.filter(
        pl.col("name").str.to_lowercase().str.contains("(vcb)|(vietcombank)")
    )


def filter_pgd(pois: pl.DataFrame) -> pl.DataFrame:
    return pois.filter(
        pl.col("name").str.to_lowercase().str.contains(r"(pgd)|(phòng giao dịch)"),
        pl.col("category").str.contains(r"(Bank)|(ATM)"),
    )


def post_process_pgd():
    POPULATION_DATASET = Path(
        "./datasets/population/vnm_pop_2024_CN_100m_R2024B_v1.tif"
    )

    pois = pl.read_parquet("./vietnam_pois.parquet")

    # TODO
    pgds = pl.read_excel("./datasets/original/dim_region.xlsx").to_dicts()

    results = []
    for pgd in tqdm(pgds[:]):
        # for pgd in pgds[:]:
        try:
            #         # print(atm["LATITUDE"], atm["LONGITUDE"])
            center = Point(latitude=pgd["NewLatitude"], longitude=pgd["NewLongitude"])
            # print(center)
            pois_in_radius = utils.filter_within_radius(
                df=pois,
                lat_col="latitude",
                lon_col="longitude",
                radius_m=1000,
                center=center,
            )
            # print(pois_in_radius)
            poi_transport_radius1 = pois_in_radius.select(
                pl.col("is_poi_transport").sum()
            ).item()
            poi_pop_radius1 = pois_in_radius.select(pl.col("is_poi_popu").sum()).item()
            poi_ecom_radius1 = pois_in_radius.select(pl.col("is_poi_ecom").sum()).item()
            # count_atm = len(pois_in_radius.filter(pl.col("categories").str.contains(r'(Bank)'))

            count_pgds = len(filter_pgd(pois=pois_in_radius))
            # print("pgd num: ", count_pgds)

            vcb_pgd = filter_vcb(pois=filter_pgd(pois_in_radius))
            pgd_vcb_radius1 = len(vcb_pgd)
            # print("pgd vcb num: ", pgd_vcb_radius1)
            pgd_competitor_radius1 = count_pgds - pgd_vcb_radius1

            result = {}
            result.update(
                {
                    "poi_transport_radius1": poi_transport_radius1,
                    "poi_ecom_radius1": poi_ecom_radius1,
                    "poi_pop_radius1": poi_pop_radius1,
                    "pgd_vcb_radius1": pgd_vcb_radius1,
                    "pgd_competitor_radius1": pgd_competitor_radius1,
                    "created_dated": datetime.now(UTC),
                    "pgd_id": pgd["DVGS"],
                    # "province": atm["CITY"],
                    "latitude": pgd["NewLatitude"],
                    "longitude": pgd["NewLongitude"],
                }
            )

            with rasterio.open(POPULATION_DATASET) as src:
                total_population = pop_in_radius(
                    center=center, radius_meters=1000, dataset=src
                )
                result.update({"population_radius1": total_population})

            results.append(result)

        except Exception as e:
            logger.error(f"Failed: {e} for {pgd}.")

    results_df = pl.DataFrame(results)
    print(results_df)
    results_df.write_parquet("count_pgds.parquet")


def post_process_points(points: list[Point], output="points.xlsx"):
    POPULATION_DATASET = Path(
        "./datasets/population/vnm_pop_2024_CN_100m_R2024B_v1.tif"
    )
    pois = pl.read_parquet("../map_data/vietnam.parquet").rename({"title": "name"})

    results = []

    with rasterio.open(POPULATION_DATASET) as src:
        for point in points:
            try:
                center = point

                pois_in_radius = utils.filter_within_radius(
                    df=pois,
                    lat_col="latitude",
                    lon_col="longitude",
                    radius_m=1000,
                    center=center,
                )

                pgds = filter_pgd(pois_in_radius)
                vcb_pgds = filter_vcb(pgds)

                results.append(
                    {
                        "poi_transport_radius1": pois_in_radius[
                            "is_poi_transport"
                        ].sum(),
                        "poi_ecom_radius1": pois_in_radius["is_poi_ecom"].sum(),
                        "poi_pop_radius1": pois_in_radius["is_poi_popu"].sum(),
                        "pgd_vcb_radius1": len(vcb_pgds),
                        "pgd_competitor_radius1": len(pgds) - len(vcb_pgds),
                        "population_radius1": pop_in_radius(
                            center=center,
                            radius_meters=1000,
                            dataset=src,
                        ),
                        "latitude": center.latitude,
                        "longitude": center.longitude,
                        "created_dated": datetime.now(),
                    }
                )

            except Exception as e:
                logger.error(f"Failed for {point}: {e}")

    df = pl.DataFrame(results)
    df.write_excel(output)
    return df


def main():

    COVER = Path("./queries/thanh_hoa.geojson")
    FACTOR = factor(
        densities=pl.read_csv("./datasets/population/V02.01.csv"), area=COVER
    )
    logger.info(f"factor for sample point: {FACTOR}")
    test_area_crawl2(
        cover=COVER,
        factor=FACTOR,
        base_distance_points_ms=5000,
    )

    # cli()

    # points = [
    #     Point(10.9597855, 106.8550636),
    #     Point(10.9558213, 106.8651273),
    # ]
    #
    # df = post_process_points(points, "pois.xlsx")
    # print(df)
    # df = pl.read_parquet("./counts.parquet")
    # print(df)
    # result = add_areas(df).drop("latitude", "longitude").rename({"area": "province"})
    # print(result)
    # result.write_parquet("./atm_pois_summary.parquet")

    # for PGD
    # post_process_pgd()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        filename=Path("crawling.log"),
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] [%(name)s:%(lineno)d] %(message)s",
    )
    RAW_DATA_DIR = Path("./datasets/raw")
    QUERY_DIR = Path("./queries")

    main()
