import pandas as pd
import polars as pl
import numpy as np
import datetime as dt

from typing import Dict, List, Tuple


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

    # exp4: add column(Usage_time / Distance)
    df = df.with_columns(
        (pl.col("Distance") / pl.col("Usage_time")).alias("Velocity")  # exp4
    )

    return df


def feature_agg_temp(
    train: pd.DataFrame, test: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # exp3 Tempの統計情報を追加
    agg_df = train.groupby(["Pmonth", "Phour"])["Temp"].agg({"mean", "min", "max"})
    agg_df = agg_df.reset_index().rename(
        columns={"max": "Temp_max", "min": "Temp_min", "mean": "Temp_mean"}
    )
    train = pd.merge(train, agg_df, on=["Pmonth", "Phour"], how="inner")
    test = pd.merge(test, agg_df, on=["Pmonth", "Phour"], how="inner")

    # exp5 出発・終着点のDuration平均と中央値を追加
    agg_df = (
        train[["Duration", "PLatd", "PLong", "DLatd", "DLong"]]
        .groupby(["PLatd", "PLong", "DLatd", "DLong"])["Duration"]
        .agg({"mean", "median"})
    )
    agg_df = agg_df.reset_index().rename(
        columns={"mean": "Duratioin_mean", "median": "Duration_meadian"}
    )

    train = pd.merge(
        train, agg_df, on=["PLatd", "PLong", "DLatd", "DLong"], how="inner"
    )
    test = pd.merge(test, agg_df, on=["PLatd", "PLong", "DLatd", "DLong"], how="inner")

    return train, test
