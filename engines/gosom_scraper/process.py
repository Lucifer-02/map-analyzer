from pathlib import Path

import polars as pl


def prepare(file: Path) -> pl.DataFrame:
    df = pl.read_csv(file)
    selected = df.select(
        pl.col(
            "input_id",
            "link",
            "title",
            "category",
            "categories",
            "latitude",
            "longitude",
            "address",
        )
    ).rename({"input_id": "query"})
    return selected
