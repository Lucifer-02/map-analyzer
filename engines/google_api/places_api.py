import time
from dataclasses import dataclass
from pprint import pprint
from typing import List

from geopy.point import Point
from googlemaps.client import places_nearby
import googlemaps
from googlemaps.convert import latlng
from torch import long


@dataclass(frozen=True)
class DacPlace:
    name: str
    categories: List[str]
    lat: float
    lon: float
    id: str


def parse_place(response) -> List[DacPlace]:
    return [
        DacPlace(
            name=place["name"],
            categories=place["types"],
            lat=place["geometry"]["location"]["lat"],
            lon=place["geometry"]["location"]["lng"],
            id=place["place_id"],
        )
        for place in response
    ]


def nearby_search(
    client: googlemaps.Client,
    location: Point,
    radius: int,  # meters
    place_type: str,
    keyword: str = "",
) -> List[DacPlace]:
    # Perform nearby search
    page_token = None
    all: list[DacPlace] = []

    while True:
        response = places_nearby(
            location=(location.latitude, location.longitude),
            radius=radius,
            type=place_type,
            keyword=keyword,
            client=client,
            page_token=page_token,
        )

        all.extend(parse_place(response["results"]))

        if "next_page_token" in response.keys():
            page_token = response["next_page_token"]
        else:
            break
        time.sleep(2)

    return all


if __name__ == "__main__":
    gmaps = googlemaps.Client(key="AIzaSyASSHrsakND-N8dCFji0KkESaeyLoWq87Y")

    # Define search parameters
    location = (21.0212062, 105.8343646)
    radius = 1000
    place_type = "atm"
    keyword = "vietcombank"

    results = nearby_search(
        client=gmaps,
        location=location,
        radius=radius,
        place_type=place_type,
        keyword=keyword,
    )

    # Process the results
    print(len(results))
    for result in results:
        pprint(result)
        print("---")
