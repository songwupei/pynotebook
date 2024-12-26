#!/bin/python3

import polars as pl

df = pl.DataFrame(
    {
        "str_int": ["1", "2"],
        "str_date": ["2024-1-1", "2024-2-1"],
        "str_datetime": ["2024-3-1 12:22:33", "2024-4-1 19:00:11"],
    },
)
df = df.with_columns(pl.col("str_datetime").str.to_datetime())
###print(
##    df.cast(
##        {"str_date": pl.Date, "str_int": pl.Int8, "str_datetime": pl.Datetime},
##    )
##)
print(df.cast({"str_date": pl.Date, "str_int": pl.Int8}))
