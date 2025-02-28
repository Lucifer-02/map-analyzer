import time
from dataclasses import dataclass
from pprint import pprint
from typing import List, Dict
import logging
from pprint import pprint

from geopy.point import Point
from googlemaps.client import places_nearby
import googlemaps


@dataclass(frozen=True)
class DacPlace:
    name: str
    categories: List[str]
    lat: float
    lon: float
    place_id: str
    address: str


def to_dict(self: DacPlace) -> Dict:
    return {
        "place_id": self.place_id,
        "name": self.name,
        "categories": self.categories,
        "lat": self.lat,
        "lon": self.lon,
        "address": self.address,
    }


def parse_place(places) -> List[DacPlace]:
    result = []
    for place in places:
        address = ""
        if "vicinity" in place:
            address = (
                place["vicinity"] + "," + place["plus_code"]["compound_code"]
                if "plus_code" in place
                else place["vicinity"]
            )

        else:
            logging.warning(f"Not contain vicinity, place: {place}")

        result.append(
            DacPlace(
                name=place["name"],
                categories=place["types"],
                lat=place["geometry"]["location"]["lat"],
                lon=place["geometry"]["location"]["lng"],
                place_id=place["place_id"],
                address=address,
            )
        )
    return result


# TODO: will by much more data details when use rank_by in places_nearby() but slow
def nearby_search(
    client: googlemaps.Client,
    location: Point,
    radius: int,  # meters
    place_type: str,
    keyword: str = "",
) -> List[DacPlace]:
    # Perform nearby search
    page_token = None
    all_places: list[DacPlace] = []

    logging.info(
        f"Nearby searching for location: {location.format_decimal()}, radius: {radius}, type: {place_type}, keyword: {keyword}"
    )
    # logging.info(
    #     f"Nearby searching for location: {location.format_decimal()}, type: {place_type}, keyword: {keyword}"
    # )

    while True:
        try:
            response = places_nearby(
                client=client,
                location=(location.latitude, location.longitude),
                radius=radius,
                type=place_type,
                keyword=keyword,
                page_token=page_token,
                # rank_by="distance",
            )

            all_places.extend(parse_place(places=response["results"]))

            if "next_page_token" in response.keys():
                page_token = response["next_page_token"]
            else:
                break
            time.sleep(2)

        except googlemaps.exceptions.TransportError as e:
            logging.error(e, stack_info=True)
            return all_places

    return all_places


if __name__ == "__main__":
    gmaps = googlemaps.Client(key="AIzaSyASSHrsakND-N8dCFji0KkESaeyLoWq87Y")

    # Define search parameters
    location = Point(21.0212062, 105.8343646)
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
