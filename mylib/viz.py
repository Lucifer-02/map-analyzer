from pathlib import Path
from typing import List

import matplotlib.pyplot as plt

import folium
import geopandas as gpd
from geopy.point import Point
from shapely.geometry import Polygon


def map_points(points: List[Point]):

    m = folium.Map(location=[points[0].latitude, points[0].longitude], zoom_start=15)

    # add points to the map
    for point in points:
        folium.Marker([point.latitude, point.longitude]).add_to(m)

    OUTPUT = Path("out.html")
    m.save(OUTPUT)
    print(f"Open {OUTPUT}")


def polygon(polygon: Polygon):
    x, y = polygon.exterior.xy
    plt.plot(x, y, color="green")
    plt.show()


def plot_points(points: List[Point]):

    plt.plot(
        [point.longitude for point in points],
        [point.latitude for point in points],
        "ro",
    )
    plt.show()


if __name__ == "__main__":

    points = gpd.read_file("./hoan_kiem.geojson")

    m = folium.Map(location=[21.019655, 105.86402], zoom_start=15)

    folium.GeoJson(points).add_to(m)

    m.save("index.html")
