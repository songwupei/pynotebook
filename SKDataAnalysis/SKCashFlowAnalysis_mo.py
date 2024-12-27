import marimo

__generated_with = "0.10.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(alt):
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
    return (get_heatmap,)


@app.cell
def _(PayDate_count_df, Z_Size, alt):
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
    return (get_dual_chart,)


@app.cell
def _(mo):
    mo.md(
        """
        with open("./SKCashFlowAnalysis_nvim.py", "r") as file:
            code_txt = file.read()
            file.close()
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
        editor = mo.ui.code_editor(code_txt)
        editor
        """
    )
    return


@app.cell
def _():
    from pathlib import Path

    import altair as alt
    import duckdb
    import polars as pl


    fb_findata_initialpathstr = "/home/song/NutstoreFiles/8-MyData/FinData/"
    fb_findata_initialpathstrV2 = (
        "/home/song/Documents/上报国资委司库专班_parquetV2/"
    )
    data_parquet_pathstr = (
        Path(fb_findata_initialpathstr)
        / "006_中国兵器工业集团有限公司_资金结算_20200101-20240930.parquet"
    )
    data_parquet_pathstrV2 = (
        Path(fb_findata_initialpathstrV2)
        / "006_中国兵器工业集团有限公司_资金结算_20200101-20240930.parquet"
    )
    金额dict = r"regexp_extract(交易金额,'(\d+).(\d)',['金额元','金额分'])"
    交易时间dt = r"strptime(交易时间,'%Y-%m-%d %H:%M:%S')"
    交易时间dt_add8 = (
        r"date_add(strptime(交易时间,'%Y-%m-%d %H:%M:%S'),INTERVAL 8 Hours)"
    )
    交易时间dict = f"regexp_extract(交易时间,'(.*) (.*)',['交易日期','交易时间'])"
    交易日期时间dict = (
        f"regexp_extract(交易日期时间,'(.*) (.*)',['交易日期','交易时间'])"
    )
    交易年份 = r"year(交易日期时间) :: BIGINT AS 交易年份"
    交易月份 = r"month(交易日期时间) :: BIGINT AS 交易月份"
    交易日份 = r"day(交易日期时间) :: BIGINT AS 交易月份"
    交易小时 = r"hour(交易日期时间) :: BIGINT AS 交易小时"
    交易分钟 = r"minute(交易日期时间) :: BIGINT AS 交易分钟"
    当年累计交易金额 = "SUM(交易金额) OVER (ORDER BY 交易年份 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT)"
    当年累计交易笔数 = "SUM(交易笔数) OVER (ORDER BY 交易年份 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT)"
    sql_GroupBlacklist = f"""
    SELECT 供应商名称
    FROM read_csv_auto('{fb_findata_initialpathstr}/供应商风险信息_20241112.csv')
    WHERE 单位名称 = '中国兵器工业集团有限公司（合并）'
    """
    sql_SubGroupBlacklist = f"""
    SELECT 单位名称,供应商名称
    FROM '供应商风险信息_20241112.csv'
    WHERE 单位名称 != '中国兵器工业集团有限公司（合并）'
    """
    sql_PayToBlacklist = f"""
    SELECT 本方单位名称,对方单位账户户名,
    regexp_extract(交易金额,'(\d+).(\d+)',['金额元','金额分'])['金额元'] :: BIGINT as 交易金额,
    date_add(strptime(交易时间,'%Y-%m-%d %H:%M:%S'),INTERVAL 8 Hours) AS 交易时间,
    摘要
    FROM '{data_parquet_pathstr}'
    where 对方单位账户户名 in ({sql_GroupBlacklist})
    order by 交易时间 desc,交易金额 desc;
    """
    sql_getAmount = f"""
    SELECT 
    regexp_extract(交易金额,'(\d+).(\d+)',['金额元','金额分'])['金额元'] :: BIGINT) as 交易金额,
    FROM '{data_parquet_pathstr}'
    """
    sql_MaxAmount = f"""
    SELECT 
    max(regexp_extract(交易金额,'(\d+).(\d+)',['金额元','金额分'])['金额元'] :: BIGINT) as max交易金额,
    FROM '{data_parquet_pathstr}'
    """
    sql_WhoPayToMax = f"""
    Select *
    From PayView
    where 交易金额 =  ({sql_MaxAmount});
    """
    sql_ViewPay_split_dt = f"""
    CREATE or REPLACE VIEW PayView_split_dt AS
    Select
    本方单位名称,对方单位账户户名,
    {金额dict}['金额元'] :: BIGINT as 交易金额,
    {交易时间dict}['交易日期'] :: DATE AS 交易日期,
    {交易时间dict}['交易时间'] :: TIME AS 交易时间,
    摘要
    FROM '{data_parquet_pathstr}';
    """
    sql_FromPayViewPlotAll = """
    SELECT *
    FROM PayView;
    """
    SQL_DROPVIEW = """
    DROP table IF EXISTS v1;
    """
    sql_selectcolumns = f"""
    SELECT  COLUMNS('(.*(时间|金额?))') AS '\\1'
    FROM '{data_parquet_pathstr}'
    LIMIT 1;
    """

    sql_PayView = f"""
    CREATE or REPLACE VIEW PayView AS
    Select
    本方单位名称,
    对方单位账户户名,
    {金额dict}['金额元'] :: BIGINT as 交易金额,
    {交易时间dt} :: DATETIME AS 交易日期时间,
    {交易时间dict}['交易日期'] :: DATE AS 交易日期,
    {交易时间dict}['交易时间'] :: TIME AS 交易时间,
    摘要
    FROM '{data_parquet_pathstr}';
    """

    sql_PayView_ByTime = """
    CREATE OR REPLACE VIEW PayView_ByTime AS
    SELECT 
    交易日期时间,
    ANY_VALUE(交易日期) AS 交易日期,
    sum(交易金额) as 交易每个时间金额,
    count(交易日期时间) as 交易每个时间笔数,
    FROM PayView
    GROUP BY 交易日期时间
    ORDER BY 交易日期时间;
    """
    sql_PayView_AtTime = """
    CREATE OR REPLACE VIEW PayView_AtTime AS
    SELECT 
    交易日期时间,
    交易日期,
    交易每个时间金额,
    交易每个时间笔数,
    year(交易日期时间) :: BIGINT AS 交易年份,
    month(交易日期时间) :: BIGINT AS 交易月份,
    day(交易日期时间) :: BIGINT AS 交易日份,
    hour(交易日期时间) :: BIGINT AS 交易小时,
    minute(交易日期时间) :: BIGINT AS 交易分钟,
    FROM PayView_ByTime;
    """

    sql_PayView_ByDay = """
    CREATE OR REPLACE VIEW PayView_ByDay AS
    SELECT 
    交易日期,
    sum(交易每个时间金额) as 交易当日累计金额,
    sum(交易每个时间笔数) as 交易当日累计笔数,
    FROM PayView_AtTime
    GROUP BY 交易日期;
    """
    sql_FromPayView_count = """
    SELECT 
    交易日期,
    交易当日累计笔数 :: BIGINT AS 交易当日累计笔数,
    交易当日累计金额 :: BIGINT AS 交易当日累计金额,
    SUM(交易当日累计金额) OVER (PARTITION BY year(交易日期) ORDER BY 交易日期 ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING)  :: BIGINT AS 当年累计交易金额,
    SUM(交易当日累计笔数) OVER (PARTITION BY year(交易日期) ORDER BY 交易日期 ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING)  :: BIGINT AS 当年累计交易笔数,
    year(交易日期) :: BIGINT AS 交易年份,
    month(交易日期) :: BIGINT AS 交易月份,
    FROM PayView_ByDay;
    """
    return (
        Path,
        SQL_DROPVIEW,
        alt,
        data_parquet_pathstr,
        data_parquet_pathstrV2,
        duckdb,
        fb_findata_initialpathstr,
        fb_findata_initialpathstrV2,
        pl,
        sql_FromPayViewPlotAll,
        sql_FromPayView_count,
        sql_GroupBlacklist,
        sql_MaxAmount,
        sql_PayToBlacklist,
        sql_PayView,
        sql_PayView_AtTime,
        sql_PayView_ByDay,
        sql_PayView_ByTime,
        sql_SubGroupBlacklist,
        sql_ViewPay_split_dt,
        sql_WhoPayToMax,
        sql_getAmount,
        sql_selectcolumns,
        交易分钟,
        交易小时,
        交易年份,
        交易日份,
        交易日期时间dict,
        交易时间dict,
        交易时间dt,
        交易时间dt_add8,
        交易月份,
        当年累计交易笔数,
        当年累计交易金额,
        金额dict,
    )


@app.cell
def _():
    sql_PayView_ByHour = """
    CREATE OR REPLACE VIEW PayView_ByHour AS
    SELECT 
    交易日期,
    year(交易日期) :: BIGINT AS 交易年份,
    交易小时,
    sum(交易每个时间金额) :: BIGINT as 交易当日每小时累计金额,
    sum(交易每个时间笔数) :: BIGINT as 交易当日每小时累计笔数,
    FROM PayView_AtTime
    GROUP BY 交易小时,交易日期
    ORDER BY 交易小时,交易日期;
    """

    sql_FromPayViewByHour_count = """
    SELECT 
    *
    FROM PayView_ByHour;
    """
    sql_FromPayViewByTime_show = """
    SELECT 
    *
    FROM PayView_ByTime;
    """
    return (
        sql_FromPayViewByHour_count,
        sql_FromPayViewByTime_show,
        sql_PayView_ByHour,
    )


@app.cell
def _(data_parquet_pathstr, duckdb):
    duckdb.query(f"select count(*) from '{data_parquet_pathstr}' ")
    return


@app.cell
def _(data_parquet_pathstr, duckdb):
    duckdb.query(f"select * from '{data_parquet_pathstr}'  limit 5 ")
    return


@app.cell
def _(data_parquet_pathstrV2, duckdb):
    duckdb.query(f"select count(*) from '{data_parquet_pathstrV2}' ")
    return


@app.cell
def _(data_parquet_pathstrV2, duckdb):
    duckdb.query(
        f"select 交易金额,交易时间,数据日期 from '{data_parquet_pathstrV2}'  limit 5 "
    )
    return


@app.cell
def _(
    duckdb,
    sql_PayView,
    sql_PayView_AtTime,
    sql_PayView_ByDay,
    sql_PayView_ByHour,
    sql_PayView_ByTime,
):
    duckdb.query(sql_PayView)
    duckdb.query(sql_PayView_ByTime)
    duckdb.query(sql_PayView_AtTime)
    duckdb.query(sql_PayView_ByDay)
    duckdb.query(sql_PayView_ByHour)
    return


@app.cell
def _(mo):
    mo.md(
        """
        '''
        PayDate_df = duckdb.query(sql_FromPayView_amount).pl()
        sql_1 = f\"""
        SELECT 本方单位名称,对方单位账户户名,交易金额,交易时间,摘要
        FROM '{data_parquet_pathstr}'
        WHERE 摘要 = '购物卡'
        \"""
        sql_2 = f\"""
        SELECT 本方单位名称,count(本方单位名称) AS 本单位交易笔数
        FROM '{data_parquet_pathstr}'
        Group by 本方单位名称
        Having 本单位交易笔数 < 10
        order by 本单位交易笔数 desc;
        \"""
        sql_2l = f\"""
        SELECT 本方单位名称
        FROM '{data_parquet_pathstr}'
        Group by 本方单位名称
        Having count(本方单位名称) < 10
        \"""
        sql_3 = f\"""
        SELECT 本方单位名称,对方单位账户户名,交易金额,交易时间,摘要
        FROM '{data_parquet_pathstr}'
        where 本方单位名称 in ({sql_2l})
        order by 交易时间 desc,交易金额 desc;
        \"""
        sql_4 = f\"""
        SELECT 本方单位名称,count(本方单位名称) AS 本单位交易笔数
        FROM '{data_parquet_pathstr}'
        where 本方单位名称 like '%北方工业有限%'
        Group by 本方单位名称
        order by 本单位交易笔数 desc;
        \"""
        sql2 = f\"""
        SELECT * FROM parquet_metadata("{data_parquet_pathstr}");
        \"""
        sql3 = f'DESCRIBE SELECT * FROM "{data_parquet_pathstr}";'
        \"""
        '''
        """
    )
    return


@app.cell
def _(duckdb, pl, sql_FromPayView_count):
    PayDate_count_df = duckdb.query(sql_FromPayView_count).pl()
    PayDate_count_df = PayDate_count_df.filter(pl.col("交易年份") == 2024)
    return (PayDate_count_df,)


@app.cell
def _(duckdb, sql_FromPayViewByHour_count):
    PayHour_count_df = duckdb.query(sql_FromPayViewByHour_count).pl()
    # PayHour_count_df = PayHour_count_df.filter(pl.col("交易年份") == 2024)
    return (PayHour_count_df,)


@app.cell
def _(PayHour_count_df, pl):
    PayHour_count_df.filter(pl.col("交易小时") == 0)
    return


@app.cell
def _(duckdb, sql_FromPayViewByTime_show):
    PayTime_df = duckdb.query(sql_FromPayViewByTime_show).pl()
    return (PayTime_df,)


@app.cell
def _(PayTime_df):
    PayTime_df.head(100)
    return


@app.cell
def _(PayTime_df, pl):
    PayTime_groupby30m_df = (
        PayTime_df.filter(
            pl.col("交易日期时间").dt.hour().is_in(list(range(6, 21)))
        )
        .group_by_dynamic("交易日期时间", every="30m")
        .agg(
            pl.col("交易每个时间笔数").sum().alias("交易笔数"),
            pl.col("交易每个时间金额").sum().cast(pl.Int64).alias("交易金额"),
        )
        .with_columns(
            pl.col("交易日期时间").dt.date().alias("交易日期"),
            pl.col("交易日期时间").dt.to_string("%H:%M").alias("交易时间"),
        )
    )
    return (PayTime_groupby30m_df,)


@app.cell
def _(PayTime_groupby30m_df):
    PayTime_groupby30m_df.tail()
    return


@app.cell
def _(PayTime_groupby30m_df, alt, get_heatmap):
    JScount_heatmap = get_heatmap(
        PayTime_groupby30m_df, X="交易日期", Y="交易时间", Z="交易笔数"
    )
    JSsum_heatmap = get_heatmap(
        PayTime_groupby30m_df, X="交易日期", Y="交易时间", Z="交易金额"
    )
    alt.hconcat(JScount_heatmap, JSsum_heatmap).resolve_scale(color="independent")
    return JScount_heatmap, JSsum_heatmap


@app.cell
def _(Pay_count_dual_chart):
    Pay_count_dual_chart
    return


if __name__ == "__main__":
    app.run()
