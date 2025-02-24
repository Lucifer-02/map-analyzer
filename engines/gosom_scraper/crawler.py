from subprocess import call
from pathlib import Path
import logging
from datetime import datetime
from typing import List

from geopy import Point
import polars as pl

from .process import prepare


def point_to_string(point: Point) -> str:
    return f"{point.latitude},{point.longitude}"


def crawl(
    keywords: List[str],
    coordinates: str,
    zoom: int = 19,
    timeout: float = 1,
    depth: int = 5,
    ncores: int = 8,
) -> pl.DataFrame:
    logging.info(f"Crawling around the coordinates: {coordinates} with zoom {zoom}")

    # try to eliminate IO by this hack :)
    ts = datetime.now().timestamp()
    input_path = Path(__file__).parent / f"input_{ts}.txt"
    with open(input_path, "w") as f:
        for keyword in keywords:
            f.write(keyword + "\n")

    output_path = Path(__file__).parent / f"output_{ts}.csv"

    call(
        [
            "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/engines/gosom_scraper/the_scraper",
            "-input",
            str(input_path),
            "-results",
            str(output_path),
            "-geo",
            coordinates,
            "-zoom",
            str(zoom),
            "-exit-on-inactivity",
            str(timeout) + "m",
            "-depth",
            str(depth),
            "-c",
            str(ncores),
        ]
    )

    df = prepare(output_path)
    logging.info(f"Collected {len(df)} places")

    return df


def crawl_around_point(keywords: List[str], point: Point) -> pl.DataFrame:
    logging.info(f"Crawling around the point: {point}")
    return crawl(
        keywords=keywords,
        coordinates=point_to_string(point),
    )


def crawl_in_area(points: list[Point], keywords: List[str]) -> pl.DataFrame:
    logging.info(f"Found {len(points)} points in the polygon")

    df = pl.DataFrame()
    for i, point in enumerate(points):
        logging.info(f"Processing point {i+1}/{len(points)}")

        df.vstack(
            crawl_around_point(
                point=point,
                keywords=keywords,
            )
        )

    return df
