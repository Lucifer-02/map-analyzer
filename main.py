import json
import logging
from datetime import datetime
from pathlib import Path

import click
import geopandas as gpd
import polars as pl
import rasterio
from geopy.point import Point
from tqdm import tqdm

from engines.gosom_scraper import crawler
from mylib import ALL_TYPES, AREAS, POI_GROUPS, utils, viz
from mylib.population import _get_pop, pop_in_radius


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


def test_area_crawl(
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
    assert len(polys) >= 1

    logging.info(f"Found {len(polys)} polygons.")
    DISTANCE_POINTS_MS = base_distance_points_ms * factor

    # print(polys)

    for pyly_idx, poly in enumerate(polys):
        points = utils.find_points_in_polygon(
            polygon=poly, distance_points_ms=DISTANCE_POINTS_MS
        )

        if len(points) == 0:
            continue

        viz.map_points(points)

        for i, point in enumerate(points):
            logging.info(
                f"Crawling {i + 1}/{len(points)} with distane of sample points is {DISTANCE_POINTS_MS} meters from area {cover}..."
            )
            save_path = Path(f"./datasets/raw/oss/{cover.stem}_{pyly_idx}_{i}.parquet")
            if not save_path.exists():
                try:
                    pois = crawler.crawl(
                        center=point,
                        keywords=ALL_TYPES,
                        ncores=ncores,
                        radius=radius,
                    )
                    result = utils.filter_within_polygon1(df=pois, poly=poly)
                    logging.info(f"Result after filted all outside the area: {result}")
                    result.write_parquet(save_path)
                except Exception as e:
                    logging.error(f"Error for point {point}: {e}, skipping...")
            else:
                logging.info(f"The dataset {save_path} already exists, skipping...")


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
        .with_columns(pl.lit(datetime.now()).alias("created_date"))
        .with_columns(pl.lit(datetime.now()).alias("updated_date"))
    )

    new_df.write_parquet("./datasets/results/vietnam.parquet")


def final_result():
    df = pl.read_parquet("./datasets/results/vietnam.parquet")
    result = (
        df.drop("query", "link", "categories", "complete_address")
        .rename({"title": "name"})
        .unique()
    )
    print(result)
    result.write_parquet("./vietnam_pois.parquet")


@click.command()
@click.argument("area")
@click.option("--ncores", default=2, help="number of cores to use")
@click.option(
    "--base_distance_points_ms",
    default=3500,
    help="base distance(meter) between sample points",
)
@click.option("--radius", default=5000, help="radius to filter around a point")
def cli(area, ncores, base_distance_points_ms, radius):
    COVER = Path(area)
    FACTOR = factor(
        densities=pl.read_csv("./datasets/population/V02.01.csv"), area=COVER
    )

    logging.info(
        f"factor for sample point: {FACTOR}, radius: {radius}, base_distance_points_ms: {base_distance_points_ms}."
    )
    test_area_crawl(
        cover=COVER,
        factor=FACTOR,
        base_distance_points_ms=base_distance_points_ms,
        ncores=ncores,
        radius=radius,
    )


def filter_vcb_atm(pois: pl.DataFrame) -> pl.DataFrame:
    return pois.filter(pl.col("is_ATM").eq(1)).filter(
        pl.col("name").str.to_lowercase().str.contains("(vcb)|(vietcombank)")
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


def atm_excel_preprocess() -> pl.DataFrame:
    df1 = pl.read_excel("./datasets/original/Mẫu 3 Pool ATM Data.xlsx").select(
        pl.col("ATM_ID", "LATITUDE", "LONGITUDE")
    )
    df2 = pl.read_csv("./datasets/original/more_atms.csv").select(
        pl.col("ATM_ID", "LATITUDE", "LONGITUDE").cast(pl.String),
    )

    table = pl.concat([df1, df2])

    return (
        table.unique(subset=["ATM_ID"])
        .with_columns(
            pl.col("LATITUDE").str.strip_chars().alias("LATITUDE"),
            pl.col("LONGITUDE").str.strip_chars().alias("LONGITUDE"),
        )
        .filter(
            pl.col("LONGITUDE").str.contains(r"\d+\.\d+"),
            pl.col("LATITUDE").str.contains(r"\d+\.\d+"),
        )
    )


def post_process_pgd():
    POPULATION_DATASET = Path(
        "./datasets/population/vnm_pop_2024_CN_100m_R2024B_v1.tif"
    )

    pois = pl.read_parquet("./vietnam_pois.parquet")
    # print(pois)
    # print(pois.schema)
    # print(valid_df)

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
                    "created_dated": datetime.now(),
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
            logging.error(f"Failed: {e} for {pgd}.")

    results_df = pl.DataFrame(results)
    print(results_df)
    results_df.write_parquet("count_pgds.parquet")


def post_process_atm():
    POPULATION_DATASET = Path(
        "./datasets/population/vnm_pop_2024_CN_100m_R2024B_v1.tif"
    )

    pois = pl.read_parquet("./vietnam_pois.parquet")
    # print(pois)
    # print(pois.schema)
    # print(valid_df)
    atms = atm_excel_preprocess().to_dicts()

    results = []
    for atm in tqdm(atms[:]):
        try:
            # print(atm["LATITUDE"], atm["LONGITUDE"])
            center = Point(latitude=atm["LATITUDE"], longitude=atm["LONGITUDE"])
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
            count_atm = len(pois_in_radius.filter(pl.col("is_ATM").eq(1)))
            vcb_atm = filter_vcb_atm(pois=pois_in_radius)
            atm_vcb_radius1 = len(vcb_atm)
            atm_competitor_radius1 = count_atm - atm_vcb_radius1

            result = {}
            result.update(
                {
                    "poi_transport_radius1": poi_transport_radius1,
                    "poi_ecom_radius1": poi_ecom_radius1,
                    "poi_pop_radius1": poi_pop_radius1,
                    "atm_vcb_radius1": atm_vcb_radius1,
                    "atm_competitor_radius1": atm_competitor_radius1,
                    "created_dated": datetime.now(),
                    "amt_id": atm["ATM_ID"],
                    # "province": atm["CITY"],
                    "latitude": atm["LATITUDE"],
                    "longitude": atm["LONGITUDE"],
                }
            )

            with rasterio.open(POPULATION_DATASET) as src:
                total_population = pop_in_radius(
                    center=center, radius_meters=1000, dataset=src
                )
                result.update({"population_radius1": total_population})
            results.append(result)

        except Exception as e:
            logging.error(f"Failed: {e} for {atm}.")

    results_df = pl.DataFrame(results)
    results_df.write_parquet("count_atms.parquet")


def add_areas(df: pl.DataFrame) -> pl.DataFrame:
    areas = [file for file in Path("./queries/temp").iterdir()]
    results = []
    for area in tqdm(areas):
        with open(area, "r", encoding="utf8") as f:
            data = json.load(f)
        polygon = utils.geojson_to_polygons(data)[0]
        area_df = utils.add_area_col(df=df, poly=polygon, name=area.stem)
        results.append(area_df)

    result_df = pl.concat(results, how="vertical")
    return result_df


# adhoc fix excel atm original "CITY" col
def refine_area(df: pl.DataFrame):
    df = df.with_columns(
        pl.col("province").str.replace_all("LAMDONG", "LAM DONG").alias("province")
    )
    df = df.with_columns(
        pl.col("province")
        .str.replace_all(
            "(BR - VT)|(BR - VUNG TAU)|(BR - VUNGTAU)|(BR VUNGTAU)", "BR-VT"
        )
        .alias("province")
    )
    df = df.with_columns(
        pl.col("province")
        .str.replace_all("(002407)|(HO CHI MINH)|(HOCHIMINH)", "HCM")
        .alias("province")
    )

    df = df.with_columns(
        pl.col("province")
        .str.replace_all("(TT HUE)|(TP HUE)|(HOCHIMINH)", "HCM")
        .alias("province")
    )

    df = df.with_columns(
        pl.col("province").str.replace_all("(DAKLAK)", "DAK LAK").alias("province")
    )

    df = df.with_columns(
        pl.col("province").str.replace_all("(BACNINH)", "BAC NINH").alias("province")
    )

    df = df.with_columns(
        pl.col("province").str.replace_all("(SOCTRANG)", "SOC TRANG").alias("province")
    )

    df = df.with_columns(
        pl.col("province").str.replace_all("(Quang Nam)", "QUANG NAM").alias("province")
    )

    df = df.with_columns(
        pl.col("province").str.replace_all("(RACH GIA)", "KIEN GIANG").alias("province")
    )
    df = df.with_columns(
        pl.col("province")
        .str.replace_all("(DUNG QUAT)", "QUANG NGAI")
        .alias("province")
    )
    df = df.with_columns(
        pl.col("province").str.replace_all("(CAM RANH)", "KHANH HOA").alias("province")
    )

    return df.filter(pl.col("province") != "SONG THAN")


def main():
    # COVER = Path("./queries/nghe_an.geojson")
    # FACTOR = factor(
    #     densities=pl.read_csv("./datasets/population/V02.01.csv"), area=COVER
    # )
    # logging.info(f"factor for sample point: {FACTOR}")
    # test_area_crawl2(cover=COVER, factor=FACTOR, base_distance_points_ms=2500, ncores=4)
    cli()

    # summary()
    # final_result()

    # for ATM
    # post_process_atm()
    # df = pl.read_parquet("./counts.parquet")
    # print(df)
    # result = add_areas(df).drop("latitude", "longitude").rename({"area": "province"})
    # print(result)
    # result.write_parquet("./atm_pois_summary.parquet")

    # for PGD
    # post_process_pgd()


if __name__ == "__main__":
    logging.basicConfig(
        filename=Path("crawling.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    RAW_DATA_DIR = Path("./datasets/raw")
    QUERY_DIR = Path("./queries")

    main()
