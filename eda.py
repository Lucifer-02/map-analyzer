from pathlib import Path

import polars as pl


def main():
    files = list(Path("./datasets/").glob("area*.csv"))
    print(files)

    dfs = pl.DataFrame()

    for file in files:
        df = pl.read_csv(file)
        dfs = dfs.vstack(df)

    # print(dfs)

    df = dfs.unique()
    df = df.filter(
        # pl.col("category").eq("ATM"),
        pl.col("title").str.contains("(?i)vietcombank"),
        pl.col("address").str.contains("(?i)Hoàn Kiếm"),
    )
    print(df)


if __name__ == "__main__":
    main()
