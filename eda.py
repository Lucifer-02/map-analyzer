from pathlib import Path

import polars as pl


def main():
    df = pl.read_excel(Path("./datasets/original/atm.xlsx"))
    # print(df.columns)

    # print(hanoi_atms)
    # coordinates = hanoi_atms.select(pl.col("LATITUDE", "LONGITUDE"))
    # print(df.filter(pl.col("LONGITUDE").is_null()))
    # print(df.filter(pl.col("LATITUDE").eq("")))
    # print(df.filter(pl.col("ATM_ID").eq("10800236")).select(pl.col("LATITUDE", "LONGITUDE")).to_series().to_list())

    valid_df = df.filter(
        pl.col("LONGITUDE").str.contains(r"\d+\.\d+"),
        pl.col("LATITUDE").str.contains(r"\d+\.\d+"),
    )
    hanoi_atms = valid_df.filter(pl.col("CITY").str.contains(r"(HANOI)|(HA NOI)"))
    # print(hanoi_atms)

    for i in range(len(hanoi_atms)):
        print(hanoi_atms["CITY"][i])


if __name__ == "__main__":
    main()
