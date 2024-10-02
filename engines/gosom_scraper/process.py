from pathlib import Path

import polars as pl
import pandas as pd


def prepare(file: Path) -> pl.DataFrame:
    df = pd.read_csv(file)
    return pl.DataFrame(
        df[["link", "title", "category", "latitude", "longitude", "address"]]
    )
