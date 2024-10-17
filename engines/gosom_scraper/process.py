from pathlib import Path
import logging

import polars as pl
import pandas as pd
from geopy import Point

from engines.gosom_scraper import crawler, utils


def prepare(file: Path) -> pl.DataFrame:
    df = pd.read_csv(file, index_col=0)
    df = df[["link", "title", "category", "latitude", "longitude", "address"]]
    return pl.DataFrame(df)


def crawl_around_point(
    point: Point,
    query_file: Path,
    output_file: Path,
    zoom: int = 18,
    timeout: float = 1,
    depth: int = 10,
):
    crawler.crawl(
        input_file=query_file,
        output_file=output_file,
        coordinates=utils.point_to_string(point),
        zoom=zoom,
        timeout=timeout,
        depth=depth,
    )

    df = prepare(output_file)
    df.write_csv(output_file)


def crawl_in_area(output_file: Path, query_file: Path):
    HOANKIEM_CORNERS = [
        Point(21.019655, 105.86402),
        Point(21.020777, 105.84170),
        Point(21.039603, 105.84630),
        Point(21.042487, 105.85754),
    ]
    DISTANCE_POINTS_KMS = 0.8
    points = utils.find_points_in_polygon(HOANKIEM_CORNERS, DISTANCE_POINTS_KMS)

    logging.info(f"Found {len(points)} points in the polygon")

    for i, point in enumerate(points):
        logging.info(f"Processing point {i+1}/{len(points)}")

        crawl_around_point(
            point=point,
            query_file=query_file,
            output_file=output_file.parent / f"area_{i}.csv",
        )
