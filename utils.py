from geopy.point import Point
from geopy.distance import Distance, geodesic
import geopy.distance
import shapely
import geopy


def distance(point1: Point, point2: Point) -> Distance:
    return geodesic(point1, point2)


# shift point by distance to the directions
def shift_localtion(point: Point, distance_kms: float, degree: float) -> Point:
    return geopy.distance.distance(kilometers=distance_kms).destination(
        point=point, bearing=degree
    )


def point_to_string(point: Point) -> str:
    return f"{point.latitude},{point.longitude}"


def point_to_tuple(point: Point) -> tuple[float, float]:
    return (point.latitude, point.longitude)


# input is a list of corners of a polygon and distance of each other points, find the points inside the polygon by calculating evenly spaced points inside the rectangle that contains the polygon and check if the point is inside the polygon
def find_points_in_polygon(
    corners: list[Point], distance_points_kms: float
) -> list[Point]:
    polygon = shapely.geometry.Polygon(
        [(corner.longitude, corner.latitude) for corner in corners]
    )

    # calculate the bounding box of the polygon
    min_lon, min_lat, max_lon, max_lat = polygon.bounds

    # calculate the number of points in each direction
    distance_points = geopy.distance.distance(kilometers=distance_points_kms)
    distance_lat = (
        distance_points.destination(
            point=geopy.Point(min_lat, min_lon), bearing=0
        ).latitude
        - min_lat
    )
    distance_lon = (
        distance_points.destination(
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

    # add the corners to the polygon
    points.extend(corners)

    return points


def draw_circle(center: Point, radius_km: float, num_points: int = 15) -> list[Point]:
    points = []
    for i in range(num_points):
        degree = 360 * i / num_points
        point = shift_localtion(point=center, distance_kms=radius_km, degree=degree)
        points.append(point)

    return points


def test_polygon():
    corners = [
        geopy.Point(21.019655, 105.86402),
        geopy.Point(21.020777, 105.84170),
        geopy.Point(21.039603, 105.84630),
        geopy.Point(21.042487, 105.85754),
    ]
    points = find_points_in_polygon(corners, distance_points_kms=0.2)
    print(points)

    # plot the points
    import matplotlib.pyplot as plt

    plt.plot(
        [point.longitude for point in points],
        [point.latitude for point in points],
        "ro",
    )
    plt.show()


def test_circle():
    center = geopy.Point(21.025206, 105.848712)
    radius = 2.0
    num_points = 15
    points = draw_circle(center=center, radius_km=radius, num_points=num_points)
    print(points)

    # plot the points
    import matplotlib.pyplot as plt

    plt.plot(
        [point.longitude for point in points],
        [point.latitude for point in points],
        "ro",
    )
    plt.show()


def main():
    # point1 = Point(21.025206, 105.848712)
    # point2 = Point(21.0253751, 105.8512529)
    # print(distance(point1, point2))
    # test_polygon()
    # test_circle()
    circle = draw_circle(geopy.Point(21.025206, 105.848712), 2.0, 20)
    points = find_points_in_polygon(
        corners=circle,
        distance_points_kms=0.5,
    )
    points.extend(circle)
    # display the points
    import matplotlib.pyplot as plt

    plt.plot(
        [point.longitude for point in points],
        [point.latitude for point in points],
        "ro",
    )
    plt.show()


if __name__ == "__main__":
    main()
