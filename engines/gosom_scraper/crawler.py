import logging
import os
import platform
import time
from datetime import datetime
from pathlib import Path
from subprocess import call
from typing import List, Set

import polars as pl
from geopy import Point

from .process import prepare


def point_to_string(point: Point) -> str:
    return f"{point.latitude},{point.longitude}"


# https://developers.google.com/maps/documentation/urls/get-started
def crawl(
    keywords: Set[str],
    center: Point,
    radius: float = 10000,
    zoom: int = 18,
    timeout: str = "10s",
    depth: int = 2,
    ncores: int = 8,
) -> pl.DataFrame:
    logging.info(
        f"Crawling around {center.format_decimal()} with zoom {zoom}, timeout: {timeout}, scroll depth: {depth}, cores: {ncores}"
    )

    # try to eliminate IO by this hack :)
    ts = datetime.now().timestamp()
    input_path = Path(__file__).parent / Path(f"input_{ts}.txt")
    with open(input_path, "w") as f:
        for keyword in keywords:
            query_input = f"{keyword.replace('_', ' ')} #!#{keyword}\n"  # '#!#' for custom input id, see the repo doc
            logging.debug(f"Actual query input for engine: {query_input}.")
            f.write(query_input)

    output_path = Path(__file__).parent / Path(f"output_{ts}.csv")

    exe: str = ""
    OS = platform.system()

    if OS == "Windows":
        # exe = f"{Path(__file__).parent}/google_maps_scraper-1.7.10-windows-amd64.exe"
        exe = f"{Path(__file__).parent}/google_maps_scraper-1.8.5-windows-amd64.exe"

    if OS == "Linux":
        # exe = f"{Path(__file__).parent}/google_maps_scraper-1.7.10-linux-amd64"
        exe = f"{Path(__file__).parent}/google_maps_scraper-1.8.5-linux-amd64"

    if OS == "Darwin":
        # exe = f"{Path(__file__).parent}/google_maps_scraper-1.7.10-darwin-amd64"
        exe = f"{Path(__file__).parent}/google_maps_scraper-1.8.5-darwin-amd64"

    logging.info("Start crawling...")
    start = time.time()
    # use the crawling tool
    call(
        [
            exe,
            "-input",
            str(input_path),
            "-results",
            str(output_path),
            "-geo",
            point_to_string(center),
            "-zoom",
            str(zoom),
            "-exit-on-inactivity",
            timeout,
            "-depth",
            str(depth),
            "-c",
            str(ncores),
            "-radius",
            str(radius),
            # "-debug",
        ]
    )
    end = time.time()
    logging.info("Done crawling.")

    df = prepare(output_path)
    logging.info(f"Collected {len(df)} places in {end - start} seconds.")

    # clean up
    os.remove(input_path)
    os.remove(output_path)

    return df


def crawl_in_area(points: List[Point], keywords: Set[str]) -> pl.DataFrame:
    df = pl.DataFrame()
    for i, point in enumerate(points):
        logging.info(f"Processing point {i + 1}/{len(points)}")

        try:
            result = crawl(
                center=point,
                keywords=keywords,
            )
            df.vstack(result, in_place=True)
        except Exception as e:
            logging.error(e, stack_info=True)

    assert len(df) > 0
    return df
