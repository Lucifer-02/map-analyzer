from pathlib import Path

import polars as pl


def main():

    df = pl.read_excel("./datasets/results/poi_with_coordinates_full.xlsx")

    # hanoi_poi = df.filter(pl.col("Address Line 1").str.contains(r"(Ha Noi)|(Hanoi)"))

    # extract link from "Address Line 1" column to get latitude and longitude
    new_df = df.with_columns(
        pl.col("link").str.extract(r"@(\d+\.\d+),(\d+\.\d+)", 1).alias("lat"),
        pl.col("link").str.extract(r"@(\d+\.\d+),(\d+\.\d+)", 2).alias("lon"),
    )

    # print(hanoi_poi.select(pl.col("Address Line 1")).to_series().to_list())

    new_df.write_excel("poi_with_coordinates_full.xlsx")


if __name__ == "__main__":
    main()
