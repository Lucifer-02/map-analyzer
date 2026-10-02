#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path
import re

import polars as pl


def read_parquet_polars(
    dir_path: str | Path = ".", pattern: str | re.Pattern | None = None
) -> pl.DataFrame:
    """
    Đọc và gộp tất cả file .parquet trong thư mục bằng polars.
    Hỗ trợ lọc file theo regex pattern.
    Sử dụng how='diagonal_relaxed' để xử lý schema linh hoạt giữa các file.
    """
    folder = Path(dir_path)
    parquet_files = sorted(folder.glob("*.parquet"))

    if pattern:
        regex = re.compile(pattern) if isinstance(pattern, str) else pattern
        parquet_files = [f for f in parquet_files if regex.search(f.name)]

    if not parquet_files:
        msg = (
            f"Không tìm thấy file .parquet nào khớp với pattern '{pattern}' trong: {folder.resolve()}"
            if pattern
            else f"Không tìm thấy file .parquet nào trong: {folder.resolve()}"
        )
        raise FileNotFoundError(msg)

    print(
        f"⚡ Đang đọc {len(parquet_files)} file parquet"
        + (f" khớp với pattern '{pattern}'" if pattern else "")
        + " bằng Polars..."
    )
    dfs = [pl.read_parquet(f) for f in parquet_files]
    combined_df = pl.concat(dfs, how="diagonal_relaxed")
    return combined_df


def main():
    parser = argparse.ArgumentParser(
        description="Đọc và tổng hợp các file Parquet trong thư mục."
    )
    parser.add_argument(
        "--dir",
        type=str,
        default=".",
        help="Đường dẫn thư mục chứa các file .parquet (mặc định: thư mục hiện tại)",
    )
    parser.add_argument(
        "--pattern",
        "-p",
        type=str,
        default=None,
        help="Regex pattern để chọn các file .parquet cụ thể (ví dụ: '^hcm_0_1.*\\.parquet$')",
    )
    args = parser.parse_args()

    data_dir = Path(args.dir)

    df = read_parquet_polars(data_dir, pattern=args.pattern)
    print(f"  • Danh sách các cột ({len(df.columns)} cột): {', '.join(df.columns)}")
    print("Unique df:\n", df.unique("place_id"))


if __name__ == "__main__":
    main()
