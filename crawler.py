import sys
import json
import logging
import concurrent.futures
from time import time


sys.path.append("./engines/omkarcloud-scraper/google-maps-scraper/")

from geopy import Point

from src.scraper import scrape_places


from utils import find_points_in_polygon


def fast_crawl_area(
    query: str, corners: list[Point], distance_points_kms: float
) -> list[dict]:
    points = find_points_in_polygon(
        corners=corners, distance_points_kms=distance_points_kms
    )
    logging.info(f"Number of points: {len(points)}")
    logging.info(f"Points: {points}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        worker_results = list(
            executor.map(crawl_by_point, [query] * len(points), points)
        )

    results = [item for sublist in worker_results for item in sublist]

    logging.info("Number of results: {len(results)}")

    return results


def crawl_area(
    query: str, corners: list[Point], distance_points_kms: float
) -> list[dict]:
    points = find_points_in_polygon(
        corners=corners, distance_points_kms=distance_points_kms
    )
    logging.info(f"Number of points: {len(points)}")
    logging.info(f"Points: {points}")

    worker_results = []
    for point in points:
        worker_result = crawl_by_point(query, point)
        print("Number of results: ", len(worker_result))
        worker_results.append(worker_result)

    results = [item for sublist in worker_results for item in sublist]

    logging.info("Number of results: {len(results)}")

    return results


def crawl_by_point(query: str, point: Point) -> list[dict]:
    try:
        result = scrape_places(
            {
                "query": query,
                "max": None,
                "lang": "en",
                "geo_coordinates": ",".join(
                    [str(point.latitude), str(point.longitude)]
                ),
                "zoom": 18,
                "links": [],
            }
        )
    except Exception as e:
        logging.exception(f"Error: {e}")
        raise e
    return result["places"]


def main():
    # save logs to file
    logging.basicConfig(
        filename="crawler.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    hanoi_corners = [
        Point(21.051418, 105.72756),
        Point(20.991982, 105.73963),
        Point(20.946243, 105.79256),
        Point(20.937046, 105.82785),
        Point(20.956840, 105.88069),
        Point(20.998715, 105.95790),
        Point(21.027230, 105.95907),
        Point(21.072989, 105.92062),
        Point(21.096554, 105.86728),
        Point(21.089600, 105.81340),
        Point(21.088461, 105.76863),
    ]

    HOANKIEM_CORNERS = [
        Point(21.019655, 105.86402),
        Point(21.020777, 105.84170),
        Point(21.039603, 105.84630),
        Point(21.042487, 105.85754),
    ]

    ATM = Point(21.0207823, 105.8523434)

    DISTANCE_POINTS_KMS = 0.5
    QUERY = "ATM Vietcombank"

    # start = time()
    # results = crawl_area(
    #     query=QUERY, corners=HOANKIEM_CORNERS, distance_points_kms=DISTANCE_POINTS_KMS
    # )
    # print("Normal Time elapsed: ", time() - start)
    #
    # # save the results
    # with open(f"results_{DISTANCE_POINTS_KMS}.json", "w") as file:
    #     json.dump(results, file)

    QUERY = "school"
    start = time()
    result = crawl_by_point(QUERY, ATM)
    print("Time elapsed: ", time() - start)

    # save the results
    with open(f"{QUERY}_around_atm.json", "w") as file:
        json.dump(result, file)


if __name__ == "__main__":
    main()
