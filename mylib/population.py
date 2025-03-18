from pathlib import Path
import rasterio
from rasterio.mask import mask
import geopandas as gpd
import numpy as np

from geopy.point import Point

from .utils import draw_circle, create_cover_from_polygon


def pop_in_radius(
    center: Point, radius_meters: float, dataset: rasterio.DatasetReader
) -> float:
    circle = draw_circle(center=center, radius_meters=radius_meters, num_points=16)

    cover_area = create_cover_from_polygon(poly=circle)

    return _get_pop(src=dataset, aoi=cover_area)


def _get_pop(src: rasterio.DatasetReader, aoi: gpd.GeoDataFrame) -> float:
    # Reproject the AOI to match the CRS of the population raster
    new_aoi = aoi.to_crs(src.crs, epsg=4326)
    assert new_aoi is not None, "Failed to reproject the AOI"

    # Mask the raster with the GeoJSON geometry
    out_image, _ = mask(src, new_aoi.geometry, crop=True)

    # Extract valid (non-NaN) values
    population_data = out_image[0]  # First band
    population_data = population_data[
        population_data > 0
    ]  # Remove zero or nodata values

    # Compute the total population
    total_population = np.sum(population_data)
    return total_population


def main():
    COVER = Path("./queries/long_bien.geojson")
    aoi = gpd.read_file(COVER)

    POPULATION_DATASET = Path("./datasets/population/vnm_general_2020.tif")
    with rasterio.open(POPULATION_DATASET) as src:
        total_population = _get_pop(src, aoi)

    print(f"Total population within the area of interest: {total_population}")


if __name__ == "__main__":
    main()
