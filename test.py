from pathlib import Path

import polars as pl
from geopy.point import Point

from mylib import utils


def main():
    files = list(Path("./datasets/raw/oss/").glob("bac_ninh_*.parquet"))

    dfs = [pl.read_parquet(file) for file in files]

    df = pl.concat(dfs).unique(subset=["link"])
    # df = pl.concat(dfs)

    # print(df["link"][0])
    print(df)

    # result = utils.filter_within_radius(
    #     df,
    #     lat_col="latitude",
    #     lon_col="longitude",
    #     center=Point(20.913986, 105.879145),
    #     radius_m=2000,
    # )
    #
    # print(result)


if __name__ == "__main__":
    main()
