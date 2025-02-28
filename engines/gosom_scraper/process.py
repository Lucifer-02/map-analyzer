from pathlib import Path
import logging

import polars as pl
import pandas as pd


def prepare(file: Path) -> pl.DataFrame:
    df = pd.read_csv(file, index_col=0)
    df = df[
        ["link", "title", "category", "categories", "latitude", "longitude", "address"]
    ]
    logging.info(f"Read {len(df)} rows from {file}")
    return pl.DataFrame(df)
