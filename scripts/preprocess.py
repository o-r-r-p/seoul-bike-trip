import pandas as pd
import polars as pl
import numpy as np
import datetime as dt


def reduce_mem_usage(df: pl.LazyFrame) -> pl.LazyFrame:
    """iterate through all numeric columns of a dataframe
    and modify the data type to reduce memory usage.
    """

    for col in df.columns:
        col_type = df.select(col).dtypeｓ

        if str(col_type)[1:4] == "Int":
            df = df.with_columns(pl.col(col).cast(pl.UInt16))

    return df


def feature_engineering(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        (
            "2023"
            + "-"
            + (pl.col("Pmonth").cast(str))
            + "-"
            + (pl.col("Pday").cast(str))
            + " "
            + (pl.col("Phour").cast(str))
            + ":"
            + (pl.col("Pmin").cast(str))
        )
        .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M")
        .alias("Pdatetime"),
        pl.when(
            (pl.col("Pmonth") == 12)
            & (pl.col("Pday") == 31)
            & (pl.col("Dmonth") == 1)
            & (pl.col("Dday") == 1)
        )
        .then(
            (
                "2024"
                + "-"
                + (pl.col("Dmonth").cast(str))
                + "-"
                + (pl.col("Dday").cast(str))
                + " "
                + (pl.col("Dhour").cast(str))
                + ":"
                + (pl.col("Dmin").cast(str))
            ).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M")
        )
        .otherwise(
            (
                "2023"
                + "-"
                + (pl.col("Dmonth").cast(str))
                + "-"
                + (pl.col("Dday").cast(str))
                + " "
                + (pl.col("Dhour").cast(str))
                + ":"
                + (pl.col("Dmin").cast(str))
            ).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M")
        )
        .alias("Ddatetime"),
    )
    # df = df.filter(~(pl.col("Pdatetime") > pl.col("Ddatetime")))

    # exp2 利用時間カラム(Usage_time)を追加
    df = df.with_columns((df["Ddatetime"] - df["Pdatetime"]).alias("duration"))
    df = df.with_columns(
        pl.when(pl.col("duration") < 0)
        .then(0)
        .otherwise((pl.col("duration") / (60 * 10**6)))
        .cast(pl.Int16)
        .alias("Usage_time")
    )
    df = df.drop(["duration", "Ddatetime", "Pdatetime"])

    return df
