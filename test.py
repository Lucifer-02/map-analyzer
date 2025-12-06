import polars as pl
from geopy import Point

from mylib import utils

# pl.Config.set_tbl_formatting("MARKDOWN")
pl.Config.set_tbl_rows(-1)
df = pl.read_excel("~/Documents/Map_khoangcach_PGD.xlsx")
# print(
#     df.select(
#         pl.col(
#             "Vị trí PGD (longtitude)",
#             "Vị trí PGD (lattitude)",
#         )
#     )
# )


temp = df.select(
    "DVGS",
    "Tên CN",
    "Mã CN",
    "Tên ĐVGS",
    "Vị trí PGD (longtitude)",
    "Vị trí PGD (lattitude)",
).with_columns(
    [
        pl.col("Vị trí PGD (longtitude)")
        .filter(pl.col("Tên ĐVGS") == "Trụ sở Chi nhánh")
        .first()
        .over("Mã CN")
        .alias("base lon"),
        pl.col("Vị trí PGD (lattitude)")
        .filter(pl.col("Tên ĐVGS") == "Trụ sở Chi nhánh")
        .first()
        .over("Mã CN")
        .alias("base lat"),
    ]
)


def cal(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None

    if lat1 == 0.0 or lon1 == 0.0 or lat2 == 0.0 or lon2 == 0.0:
        return None

    return utils.distance(point1=Point(lat1, lon1), point2=Point(lat2, lon2)).meters


result = temp.with_columns(
    pl.struct(
        [
            "Vị trí PGD (longtitude)",
            "Vị trí PGD (lattitude)",
            "base lon",
            "base lat",
        ]
    )
    .map_elements(  # <-- FIX 1: Use map_elements for row-by-row operations
        lambda row: cal(  # <-- FIX 2: Lambda now receives a dict `row`
            lon1=row["Vị trí PGD (longtitude)"],
            lat1=row["Vị trí PGD (lattitude)"],
            lon2=row["base lon"],
            lat2=row["base lat"],
        ),
        return_dtype=pl.Float64,
    )
    .alias("distance(meters)")
)
print(result)
result.write_excel("distance.xlsx")
