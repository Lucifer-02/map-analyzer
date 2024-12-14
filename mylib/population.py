from pathlib import Path
import rasterio
from rasterio.mask import mask
import geopandas as gpd
import numpy as np

from geopy.point import Point

from .utils import draw_circle, create_cover_from_points


def pop_in_radius(
    center: Point, radius_km: float, dataset: rasterio.DatasetReader
) -> float:
    points = draw_circle(center=center, radius_km=radius_km, num_points=8)

    cover_area = create_cover_from_points(points=points)

    return _get_pop(src=dataset, aoi=cover_area)


def _get_pop(src: rasterio.DatasetReader, aoi: gpd.GeoDataFrame) -> float:
    # Reproject the AOI to match the CRS of the population raster
    new_aoi = aoi.to_crs(src.crs)
    assert new_aoi is not None, "Failed to reproject the AOI"

    # Mask the raster using the AOI geometry
    out_image, _ = mask(src, new_aoi.geometry, crop=True)
    out_image = out_image[0]  # Extract the masked population data band

    # Replace NaN values (masked areas) with 0 for summation
    out_image = np.nan_to_num(out_image)

    # Calculate the sum of the population within the masked area
    total_population = np.sum(out_image)

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
