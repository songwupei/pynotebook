#!/bin/python3
"""
1.altair
"""
import altair as alt


def get_heatmap(sourcedf, X, Y, Z):
    from altair import datum

    _brush = alt.selection_interval()
    heatmap = (
        alt.Chart(sourcedf)
        # .mark_point()
        .mark_rect()  # 热力图/HEATMAP
        .encode(
            y=f"{Y}:O",
            x=f"{X}:T",
            color=f"{Z}:Q",
            # size="交易当日每小时累计金额:Q",
            # color=alt.Color("mean(交易当日每小时累计笔数):Q").legend(None),
        )
        .add_params(_brush)
    ).interactive()

    alt.data_transformers.disable_max_rows()
    # Pay_hour_count_chart = alt.layer(point1).interactive()i
    return heatmap


def get_dual_chart(sourcedf, X, Y, Z_size, Z_color):
    brush = alt.selection_interval()

    bar = (
        alt.Chart(PayDate_count_df)
        .mark_bar()
        .encode(
            x=f"{X}:T",
            y=f"{Y}:Q",
        )
        .add_params(brush)
    )
    point = (
        alt.Chart(PayDate_count_df)
        .mark_point()
        .encode(
            x=f"{X}:T",
            y=f"{Y}:Q",
            size=f"{Z_Size}:Q",
            color=alt.Color(f"mean({Z_color}):Q").legend(None),
        )
        .add_params(brush)
    )
    line = (
        alt.Chart(PayDate_count_df)
        .mark_line()
        .encode(x=f"{X}:T", y=f"{Y}:Q", color=alt.value("green"))
        .add_params(brush)
    )
    alt.data_transformers.disable_max_rows()
    Pay_count_dual_chart = (
        alt.layer(point, line).resolve_scale(y="independent").interactive()
    )
    # duckdb.query(sql_WhoPayToMax).pl()
    # alt.JupyterChart(Pay_count_dual_chart)
    # Pay_count_dual_chart.save("SKCF_v2.html", inline=True)
    # Pay_count_dual_chart.save("SKCF.png", ppi=400)
    return Pay_count_dual_chart
