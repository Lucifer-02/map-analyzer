from os.path import join
from pathlib import Path
import logging

import polars as pl
import pandas as pd
from geopy.point import Point

from utils import distance


def prepare(file: Path) -> pl.DataFrame:
    df = pd.read_csv(file, index_col=0)
    df = df[["link", "title", "category", "latitude", "longitude", "address"]]
    logging.info(f"Read {len(df)} rows from {file}")
    return pl.DataFrame(df)


def hanoi_around_poi_process():
    logging.basicConfig(level=logging.INFO)

    hanoi_poi_id = [
        207,
        4903,
        6905,
        100,
        101,
        102,
        103,
        105,
        106,
        108,
        109,
        110,
        111,
        1202,
        200,
        202,
        203,
        204,
        205,
        206,
        208,
        210,
        3000,
        3001,
        3002,
        3003,
        3004,
        4500,
        4501,
        4502,
        4503,
        4504,
        4505,
        4507,
        4900,
        4901,
        4902,
        4904,
        4905,
        5400,
        5401,
        5402,
        5403,
        5404,
        5405,
        6100,
        6101,
        6102,
        6104,
        6900,
        6901,
        6902,
        6903,
        6904,
        7100,
        7101,
        7102,
        7103,
        7104,
        7105,
        8500,
        8501,
        8502,
        9300,
        9301,
        9302,
        9400,
        9401,
        9402,
        9403,
        9600,
        9601,
        9602,
        9603,
        9700,
        9701,
        9702,
        9900,
        9901,
        9902,
    ]
    around_df = pl.DataFrame()
    for id in hanoi_poi_id:
        file = Path(f"./datasets/raw/around_{id}.csv")
        df = prepare(file)
        # in type i64
        new_df = df.with_columns(poi_id=id)
        new_df = new_df.with_columns(pl.col("poi_id").cast(pl.Int64))
        around_df = around_df.vstack(new_df.unique())

    poi_df = pl.read_excel(
        "./datasets/results/poi_with_coordinates_full.xlsx",
        columns=["Unique Identifier", "lat", "lon"],
    )
    # print(poi_df.columns)

    # right join with around_df on poi_id and "Unique Identifier"
    joined_df = around_df.join(poi_df, left_on="poi_id", right_on="Unique Identifier")

    rows = joined_df.to_dicts()
    new_rows = []

    for row in rows:
        # add distance column
        row["distance"] = distance(
            Point(row["latitude"], row["longitude"]),
            Point(row["lat"], row["lon"]),
        ).km
        if row["distance"] <= 2.0:
            new_rows.append(row)

    new_df = pl.DataFrame(new_rows)
    # print(new_df)

    # save to excel
    new_df.write_excel("./datasets/temp/around_poi_with_distance.xlsx")


def main():
    hanoi_around_poi_process()


if __name__ == "__main__":
    main()
