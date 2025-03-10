from pathlib import Path
import functools

import polars as pl
from geopy.point import Point

from mylib import utils


def main():
    files = list(Path("./datasets/raw/oss/").glob("ha_noi*.parquet"))

    dfs = [pl.read_parquet(file) for file in files]

    df = pl.concat(dfs).unique(subset=["link"])
    # df = pl.concat(dfs)

    # print(df["link"][0])
    print(df)
    # temp = df.with_columns(pl.col("categories").str.split(", ").alias("splited"))
    # print(temp.filter(pl.col("splited").is_null()))

    temp = (
        df.select(pl.col("categories").drop_nulls().str.split(", "))
        .to_series()
        .to_list()
    )
    categories = set(functools.reduce(lambda x, y: x + y, temp))

    print(categories)
    print(len(categories))

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

