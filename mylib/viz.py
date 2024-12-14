import matplotlib.pyplot as plt

import folium
import geopandas as gpd
from geopy.point import Point
from shapely.geometry import Polygon


def viz_circle(center: Point, points: list[Point]):

    m = folium.Map(location=[center.latitude, center.longitude], zoom_start=15)

    # add points to the map
    for point in points:
        folium.Marker([point.latitude, point.longitude]).add_to(m)

    m.save("index.html")


def viz_polygon(polygon: Polygon):
    x, y = polygon.exterior.xy
    plt.plot(x, y, color="green")
    plt.show()


if __name__ == "__main__":

    points = gpd.read_file("./hoan_kiem.geojson")

    m = folium.Map(location=[21.019655, 105.86402], zoom_start=15)

    folium.GeoJson(points).add_to(m)

    m.save("index.html")
