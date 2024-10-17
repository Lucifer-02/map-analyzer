import logging
from pathlib import Path


from engines.gosom_scraper import process


def main():
    output_file = Path("./datasets/area.csv")
    query_file = Path("./queries/atm.txt")
    process.crawl_in_area(output_file=output_file, query_file=query_file)


if __name__ == "__main__":
    logging.basicConfig(
        filename=Path("crawling.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
