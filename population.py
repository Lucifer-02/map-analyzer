import rasterio
from rasterio.mask import mask
import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon

# Step 1: Load the Population Raster
population_raster_path = (
    "./datasets/population/GHS_POP_E2025_GLOBE_R2023A_54009_100_V1_0_R7_C29.tif"
)
with rasterio.open(population_raster_path) as src:
    pop_raster = src.read(
        1
    )  # Read the population data band (assume it’s the first band)
    pop_meta = src.meta

# Step 2: Define the Area of Interest (AOI)
aoi = gpd.GeoDataFrame(
    {
        "geometry": [
            Polygon(
                [
                    (105.844191, 21.034608),
                    (105.827711, 21.022831),
                    (105.857752, 21.013937),
                    (105.844191, 21.034608),
                ]
            )
        ]
    },
    crs="EPSG:4326",  # Specify the CRS of the AOI (WGS84)
)

# Reproject the AOI to match the CRS of the population raster
aoi = aoi.to_crs(pop_meta["crs"])
assert aoi is not None, "AOI is None"

# Step 3: Mask the Raster with AOI
with rasterio.open(population_raster_path) as src:
    # Mask the raster using the AOI geometry
    out_image, out_transform = mask(src, aoi.geometry, crop=True)
    out_image = out_image[0]  # Extract the masked population data band

# Step 4: Calculate the Total Population
# Replace NaN values (masked areas) with 0 for summation
out_image = np.nan_to_num(out_image)

# Calculate the sum of the population within the masked area
total_population = np.sum(out_image)

print(f"Total population within the area of interest: {total_population}")
