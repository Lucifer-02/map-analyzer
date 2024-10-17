from subprocess import call
from pathlib import Path
import logging


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
