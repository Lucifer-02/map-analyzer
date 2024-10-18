from subprocess import call
from pathlib import Path
import logging

from geopy import Point


def point_to_string(point: Point) -> str:
    return f"{point.latitude},{point.longitude}"


def crawl(
    input_file: Path,
    output_file: Path,
    coordinates: str,
    zoom: int = 18,
    timeout: float = 1,
    depth: int = 10,
):
    logging.info(f"Crawling around the coordinates: {coordinates} with zoom {zoom}")
    call(
        [
            "/media/lucifer/STORAGE/IMPORTANT/map-analyzer/engines/gosom_scraper/google-maps-scraper",
            "-input",
            str(input_file),
            "-results",
            str(output_file),
            "-geo",
            coordinates,
            "-zoom",
            str(zoom),
            "-exit-on-inactivity",
            str(timeout) + "m",
            "-depth",
            str(depth),
        ]
    )


def crawl_around_point(
    point: Point,
    query_file: Path,
    output_file: Path,
):
    logging.info(f"Crawling around the point: {point}")
    crawl(
        input_file=query_file,
        output_file=output_file,
        coordinates=point_to_string(point),
    )


def crawl_in_area(points: list[Point], output_file: Path, query_file: Path):

    logging.info(f"Found {len(points)} points in the polygon")

    for i, point in enumerate(points):
        logging.info(f"Processing point {i+1}/{len(points)}")

        crawl_around_point(
            point=point,
            query_file=query_file,
            output_file=output_file.parent / f"area_{i}.csv",
        )
