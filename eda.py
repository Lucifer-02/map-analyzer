import polars as pl


def test1():
    df = pl.read_json("./results_0.5.json")
    print(len(df))
    df = df.filter(
        pl.col("main_category").eq("ATM"),
        pl.col("name").str.contains("(?i)vietcombank"),
        pl.col("address").str.contains("(?i)Hoàn Kiếm"),
    )

    print(df.unique("place_id"))

    for add in df["address"].unique().to_list():
        print(add)


def test2():
    df = pl.read_json("./school_around_atm.json")
    print(df["main_category"])


def main():
    test2()


if __name__ == "__main__":
    main()
