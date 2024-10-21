from pathlib import Path
import rasterio
from rasterio.mask import mask
import geopandas as gpd
import numpy as np

def get_pop(src: rasterio.DatasetReader, aoi: gpd.GeoDataFrame) -> float:
    # Reproject the AOI to match the CRS of the population raster
    aoi = aoi.to_crs(src.crs)

    # Mask the raster using the AOI geometry
    out_image, _ = mask(src, aoi.geometry, crop=True)
    out_image = out_image[0]  # Extract the masked population data band

    # Replace NaN values (masked areas) with 0 for summation
    out_image = np.nan_to_num(out_image)

    # Calculate the sum of the population within the masked area
    total_population = np.sum(out_image)

    return total_population

def main():
    COVER = Path("./long_bien.geojson")
    aoi = gpd.read_file(COVER)

    POPULATION_DATASET = Path("./datasets/population/vnm_general_2020.tif")
    with rasterio.open(POPULATION_DATASET) as src:
        total_population = get_pop(src, aoi)

    print(f"Total population within the area of interest: {total_population}")

if __name__ == "__main__":
    main()
