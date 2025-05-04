import json
import matplotlib.pyplot as plt
from pathlib import Path
from typing import List, Dict

from geopy.point import Point
from geopy.distance import Distance, geodesic
import geopy.distance
import shapely
import geopy
import polars as pl
from shapely.geometry import Polygon
import geopandas as gpd


import polars as pl
import numpy as np


def haversine(lat1, lon1, lat2, lon2):
    """Compute the Haversine distance between two points in meters."""
    R = 6371000  # Earth radius in meters
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    return R * c


# custom algorithm to speed up
def filter_within_radius(
    df: pl.DataFrame,
    lat_col: str,
    lon_col: str,
    center: Point,
    radius_m: float,
) -> pl.DataFrame:
    """Filter rows in a Polars DataFrame that are within the given radius (meters)."""

    if len(df) == 0:
        return df  # Return early if DataFrame is empty

    # Vectorized computation using Polars expressions
    filtered_df = (
        df.with_columns(
            haversine(
                center.latitude, center.longitude, pl.col(lat_col), pl.col(lon_col)
            ).alias("distance")
        )
        .filter(pl.col("distance") <= radius_m)
        .drop("distance")
    )

    return filtered_df


# another version but using other lib
def filter_within_radius1(
    df: pl.DataFrame,
    lat_col: str,
    lon_col: str,
    center: Point,
    radius_m: float,
) -> pl.DataFrame:
    assert len(df) > 0

    new_df = df.with_columns(
        pl.struct([lat_col, lon_col])
        .map_elements(
            lambda x: distance(center, Point(x[lat_col], x[lon_col])).meters,
            return_dtype=pl.Float64,
        )
        .alias("distance")
    )

    filtered = new_df.filter(pl.col("distance") <= radius_m).drop("distance")

    assert len(filtered) >= 0
    return filtered


def filter_within_radius2(
    df: pl.DataFrame, lat_col: str, lon_col: str, center: Point, radius_m: float
) -> pl.DataFrame:
    """Efficiently filter rows within a radius using GeoPandas spatial operations."""
    circle = draw_circle(center=center, radius_meters=radius_m, num_points=64)

    return filter_within_polygon1(df, poly=circle, lat_col=lat_col, lon_col=lon_col)


def filter_within_polygon1(
    df: pl.DataFrame,
    poly: Polygon,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
) -> pl.DataFrame:
    """Efficiently filter rows within a polygon using GeoPandas spatial join."""
    if len(df) == 0:
        return df

    # Convert Polars DataFrame to GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df.to_pandas(),
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs="EPSG:4326",
    )
    polygon_gdf = gpd.GeoDataFrame(geometry=[poly], crs="EPSG:4326")

    # Spatial join (inner join keeps only matching points)
    joined_gdf = gpd.sjoin(gdf, polygon_gdf, predicate="within", how="inner")
    return pl.from_pandas(joined_gdf.drop(columns=["geometry", "index_right"]))


def filter_within_polygon(
    df: pl.DataFrame,
    poly: Polygon,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
) -> pl.DataFrame:
    assert len(df) > 0

    new_df = df.with_columns(
        pl.struct([lat_col, lon_col])
        .map_elements(
            lambda x: poly.contains(shapely.geometry.Point(x[lon_col], x[lat_col])),
            return_dtype=pl.Boolean,
        )
        .alias("is_inside")
    )

    filtered = new_df.filter(pl.col("is_inside")).drop("is_inside")

    assert len(filtered) >= 0
    assert len(filtered) <= len(df)
    return filtered


def polygon_to_points(polygon: Polygon) -> List[Point]:
    # Convert the polygon vertices to geopy Point objects
    return [Point(lat, lon) for lon, lat in polygon.exterior.coords]


def geojson_to_polygon(data: Dict) -> Polygon:
    assert all(
        item in data.keys() for item in ["features", "type"]
    ), "check valid geojson input"
    assert geojson.FeatureCollection(data).is_valid

    if data.get("type") != "FeatureCollection":
        raise ValueError("The provided GeoJSON does not contain a FeatureCollection.")

    features = data.get("features", [])
    assert len(features) == 1, "currently support parse only one feature"

    geometry = features[0].get("geometry", {})
    if geometry.get("type") != "Polygon":
        raise ValueError("This is not a polygon")

    # 'coordinates' for Polygon is an array of linear rings
    # The first ring is the outer boundary, subsequent ones (if any) are holes
    # Coordinates are in the format [[lon, lat], [lon, lat], ...]
    polygon_coords = geometry.get("coordinates", [])

    # polygon_coords[0] should be the outer ring
    # Shapely expects (x, y) = (longitude, latitude)
    outer_ring = polygon_coords[0]

    # Create the shapely Polygon
    poly = Polygon(outer_ring)
    return poly


import geojson


# This is a trick because geojson only contain feature
def geojson_to_polygons(data: Dict) -> List[Polygon]:
    assert geojson.GeoJSON(data).is_valid

    polygons = []

    match data.get("type"):

        case "Polygon":
            polygon_coords = data.get("coordinates", [])

            # polygon_coords[0] should be the outer ring
            outer_ring = polygon_coords[0]
            poly = Polygon(outer_ring)
            polygons.append(poly)

        case "MultiPolygon":
            # 'coordinates' for MultiPolygon is an array of polygons
            # Each polygon is an array of linear rings
            multipolygon_coords = data.get("coordinates", [])

            for polygon_coords in multipolygon_coords:
                # polygon_coords[0] should be the outer ring
                outer_ring = polygon_coords[0]
                poly = Polygon(outer_ring)
                polygons.append(poly)

        case "FeatureCollection":

            features = data.get("features", [])
            assert len(features) == 1, "currently support parse only one feature"

            geometry = features[0].get("geometry", {})
            if geometry.get("type") != "Polygon":
                raise ValueError("This is not a polygon")

            # 'coordinates' for Polygon is an array of linear rings
            # The first ring is the outer boundary, subsequent ones (if any) are holes
            # Coordinates are in the format [[lon, lat], [lon, lat], ...]
            polygon_coords = geometry.get("coordinates", [])

            # polygon_coords[0] should be the outer ring
            # Shapely expects (x, y) = (longitude, latitude)
            outer_ring = polygon_coords[0]

            # Create the shapely Polygon
            poly = Polygon(outer_ring)

            polygons.append(poly)

    return polygons


def distance(point1: Point, point2: Point) -> Distance:
    return geodesic(point1, point2)


# shift point by distance to the directions
def shift_location(point: Point, distance_meters: float, degree: float) -> Point:
    return geopy.distance.distance(kilometers=distance_meters / 1000).destination(
        point=point, bearing=degree
    )


def point_to_string(point: Point) -> str:
    return f"{point.latitude},{point.longitude}"


def point_to_tuple(point: Point) -> tuple[float, float]:
    return (point.latitude, point.longitude)


def extract_coordinates(table: pl.DataFrame, link_col: str) -> pl.DataFrame:
    return table.with_columns(
        pl.col(link_col).str.extract(r"@(\d+\.\d+),(\d+\.\d+)", 1).alias("lat"),
        pl.col(link_col).str.extract(r"@(\d+\.\d+),(\d+\.\d+)", 2).alias("lon"),
    )


def points_to_polygon(corners: List[Point]) -> Polygon:
    return shapely.geometry.Polygon(
        [(corner.longitude, corner.latitude) for corner in corners]
    )


def find_points_in_polygons(
    polygons: List[Polygon], distance_points_ms: float, is_include_corners: bool = False
) -> List[Point]:

    result = []

    for poly in polygons:
        points = find_points_in_polygon(
            polygon=poly,
            distance_points_ms=distance_points_ms,
            is_include_corners=is_include_corners,
        )
        result.extend(points)

    return result


# input is a list of corners of a polygon and distance of each other points, find the points inside the polygon by calculating evenly spaced points inside the rectangle that contains the polygon and check if the point is inside the polygon
def find_points_in_polygon(
    polygon: Polygon, distance_points_ms: float, is_include_corners: bool = False
) -> List[Point]:

    # calculate the bounding box of the polygon
    min_lon, min_lat, max_lon, max_lat = polygon.bounds

    # calculate the number of points in each direction
    points_distance = geopy.distance.distance(kilometers=distance_points_ms / 1000)
    distance_lat = (
        points_distance.destination(
            point=geopy.Point(min_lat, min_lon), bearing=0
        ).latitude
        - min_lat
    )
    distance_lon = (
        points_distance.destination(
            point=geopy.Point(min_lat, min_lon), bearing=90
        ).longitude
        - min_lon
    )
    num_points_lat = int((max_lat - min_lat) / distance_lat) + 1
    num_points_lon = int((max_lon - min_lon) / distance_lon) + 1

    # calculate the points inside the bounding box
    points = []
    for i in range(num_points_lat):
        for j in range(num_points_lon):
            point = geopy.Point(min_lat + i * distance_lat, min_lon + j * distance_lon)
            if polygon.contains(
                shapely.geometry.Point(point.longitude, point.latitude)
            ):
                points.append(point)

    if is_include_corners:
        # add the corners to the polygon
        points.extend(polygon_to_points(polygon))

    return points


def aoi_to_geojson(aoi: gpd.GeoDataFrame, output_file: Path) -> None:
    with open(output_file, "w") as f:
        json.dump(aoi.to_geo_dict(), f)


def draw_circle(center: Point, radius_meters: float, num_points: int = 4) -> Polygon:
    assert num_points >= 3, "Must be a polygon"

    points = []
    for i in range(num_points):
        degree = 360 * i / num_points
        point = shift_location(
            point=center, distance_meters=radius_meters, degree=degree
        )
        points.append(point)

    return points_to_polygon(points)


def test_polygon():
    corners = [
        geopy.Point(21.019655, 105.86402),
        geopy.Point(21.020777, 105.84170),
        geopy.Point(21.039603, 105.84630),
        geopy.Point(21.042487, 105.85754),
    ]
    points = find_points_in_polygon(points_to_polygon(corners), distance_points_ms=200)
    print(points)

    # plot the points
    import matplotlib.pyplot as plt

    plt.plot(
        [point.longitude for point in points],
        [point.latitude for point in points],
        "ro",
    )
    plt.show()


def create_cover_from_polygon(
    poly: Polygon, name: str = "New AOI", crs: str = "EPSG:4326"
) -> gpd.GeoDataFrame:
    """
    Creates a GeoDataFrame representing an Area of Interest (AOI)
    from a list of geopy.point.Point objects.

    :param poly: polygon
    :param name: The name of the AOI feature.
    :param crs: Coordinate reference system for the GeoDataFrame.
    :return: A GeoDataFrame containing the AOI polygon.
    """

    # Create a GeoDataFrame with the polygon
    return gpd.GeoDataFrame([{"name": name}], geometry=[poly], crs=crs)


def test_circle():
    center = geopy.Point(21.025206, 105.848712)
    radius = 2.0
    num_points = 15
    points = draw_circle(center=center, radius_meters=radius, num_points=num_points)
    print(points)

    # plot the points
    import matplotlib.pyplot as plt

    plt.plot(
        [point.longitude for point in polygon_to_points(points)],
        [point.latitude for point in polygon_to_points(points)],
        "ro",
    )
    plt.show()


def city_mapping() -> Dict[str, str]:
    # IO
    with open(f"{Path(__file__).parent}/geo_map.json", "r", encoding="utf-8") as file:
        data = json.load(file)  # Load JSON data as a Python dictionary or list
    return data


def test_filter_within_polygon():
    # df = pl.DataFrame(
    #     {"latitude": [20.331948, 21.017354], "longitude": [106.196018, 105.814087]}
    # )

    df = pl.read_parquet("../datasets/raw/oss/ha_noi_100.parquet")
    print(len(df))

    with open("../queries/HN/hoan_kiem.geojson", "r") as f:
        data = json.load(f)

    poly = geojson_to_polygon(data)

    # print(filter_within_polygon1(df=df, poly=poly))
    import time

    start = time.time()
    print(len(filter_within_polygon(df=df, poly=poly)))
    end = time.time()
    print(end - start)

    start = time.time()
    print(len(filter_within_polygon1(df=df, poly=poly)))
    end = time.time()
    print(end - start)


def test_filter_within_radius():

    df = pl.read_parquet("../datasets/raw/oss/ha_noi_99.parquet")
    print(len(df))

    # print(filter_within_polygon1(df=df, poly=poly))
    import time

    start = time.time()
    print(
        len(
            filter_within_radius(
                df,
                center=Point(21.024958, 105.828912),
                lat_col="latitude",
                lon_col="longitude",
                radius_m=2000,
            )
        )
    )
    end = time.time()
    print(end - start)

    # ----------------

    start = time.time()
    print(
        len(
            filter_within_radius1(
                df,
                center=Point(21.024958, 105.828912),
                lat_col="latitude",
                lon_col="longitude",
                radius_m=2000,
            )
        )
    )

    end = time.time()
    print(end - start)

    # -------------
    start = time.time()
    print(
        len(
            filter_within_radius2(
                df,
                center=Point(21.024958, 105.828912),
                lat_col="latitude",
                lon_col="longitude",
                radius_m=2000,
            )
        )
    )
    end = time.time()
    print(end - start)


def main():
    # point1 = Point(21.025206, 105.848712)
    # point2 = Point(21.0253751, 105.8512529)
    # print(distance(point1, point2))
    # test_polygon()
    # test_circle()
    # circle = draw_circle(
    #     center=geopy.Point(21.025206, 105.848712), radius_meters=2.0, num_points=4
    # )
    # points = find_points_in_polygon(
    #     polygon=points_to_polygon(circle),
    #     distance_points_kms=0.9,
    # )
    # points.extend(circle)
    # # display the points
    #
    # plt.plot(
    #     [point.longitude for point in points],
    #     [point.latitude for point in points],
    #     "ro",
    # )
    # plt.show()

    # print(city_mapping())

    test_filter_within_polygon()
    # test_filter_within_radius()


if __name__ == "__main__":
    main()
