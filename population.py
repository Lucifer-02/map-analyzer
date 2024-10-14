from pathlib import Path

import rasterio
from rasterio.mask import mask
import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon


def get_pop(src: rasterio.DatasetReader, aoi: gpd.GeoDataFrame) -> float:
    pop_meta = src.meta

    # Reproject the AOI to match the CRS of the population raster
    aoi.to_crs(pop_meta["crs"])

    # Step 3: Mask the Raster with AOI
    # Mask the raster using the AOI geometry
    out_image, _ = mask(src, aoi.geometry, crop=True)
    out_image = out_image[0]  # Extract the masked population data band

    # Step 4: Calculate the Total Population
    # Replace NaN values (masked areas) with 0 for summation
    out_image = np.nan_to_num(out_image)

    # Calculate the sum of the population within the masked area
    total_population = np.sum(out_image)

    return total_population


def main():
    COVER = Path("./long_bien.geojson")
    aoi = gpd.read_file(COVER)
    # assert aoi is not None, "AOI is None"
    # aoi = gpd.GeoDataFrame(
    #     {
    #         "geometry": [
    #             Polygon(
    #                 [
    #                     (105.841987, 21.081652),
    #                     (105.925660, 21.070473),
    #                     (105.866308, 21.066948),
    #                     (105.841987, 21.081652),
    #                 ]
    #             )
    #         ]
    #     },
    #     crs="EPSG:4326",
    # )
    POPULATION_DATASET = Path(
        # "./datasets/population/GHS_POP_E2025_GLOBE_R2023A_4326_3ss_V1_0_R7_C29.tif"
        "./datasets/population/vnm_general_2020_geotiff/vnm_general_2020.tif"
    )
    with rasterio.open(POPULATION_DATASET) as src:
        total_population = get_pop(src, aoi)

    print(src.bounds)
    print(f"Total population within the area of interest: {total_population}")


if __name__ == "__main__":
    main()
