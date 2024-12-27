import marimo

__generated_with = "0.9.31"
app = marimo.App(width="medium", app_title="SKDataAnalysis")


@app.cell
def __():
    import altair as alt
    return (alt,)


@app.cell
def __(mo):
    mo.md(r"""# 司库数据分析（2024年10月）""")
    return


@app.cell(hide_code=True)
def __():
    import itertools
    from collections import OrderedDict, defaultdict
    from datetime import datetime
    from pathlib import Path

    import polars as pl

    from helpers import searchconditions
    from play_text import (
        contains_CJK,
        play_csv,
        split_OnePath_names,
        split_rPath_names,
    )
    return (
        OrderedDict,
        Path,
        contains_CJK,
        datetime,
        defaultdict,
        itertools,
        pl,
        play_csv,
        searchconditions,
        split_OnePath_names,
        split_rPath_names,
    )


@app.cell
def __(mo):
    mo.md(r"""## 一、资金流水分析""")
    return


@app.cell
def __():
    ## 常量清单
    return


@app.cell
def __():
    colnames = """1、银行账户
    2、资金结算
    3、银行贷款
    4、应付债券
    5、应付票据
    6、应收票据
    7、担保
    8、信用证
    9、保函
    10、金融投资业务
    11、供应链金融
    12、财务公司附表
    13、PPP附表
    14、金融衍生业务附表
    15、增值税发票
    16、合同
    17、应收账款 
    18、应付账款  
    19、客商信息"""
    return (colnames,)


@app.cell
def __(colnames):
    sheetnames_dict = dict(
        [colname.strip().split("、")[::-1] for colname in colnames.split("\n")]
    )
    print(sheetnames_dict)
    return (sheetnames_dict,)


@app.cell(hide_code=True)
def __():
    # filter_conditions =  "(pl.col('摘要').str.contains('购物卡') | pl.col('备注').str.contains('购物卡'))"
    资金结算1_selcolnames_list = "本方单位名称,本方单位账户户名,对方单位账户户名,交易金额,交易时间,摘要,备注".split(
        ","
    )
    return (资金结算1_selcolnames_list,)


@app.cell
def __():
    ## 函数清单
    return


@app.cell
def __(mo):
    fb_initialpathstr = "/home/song/Documents/上报国资委司库专班/"
    fb_csvpath = mo.ui.file_browser(
        initial_path=fb_initialpathstr,
        selection_mode="directory",
        restrict_navigation=True,
    )
    return fb_csvpath, fb_initialpathstr


@app.cell
def __(fb_csvpath, fb_initialpathstr, mo):
    data_csvpathstr = fb_csvpath.path() if fb_csvpath.path() else fb_initialpathstr
    mo.vstack([fb_csvpath, mo.md(f"选取的路径是：{data_csvpathstr}")])
    return (data_csvpathstr,)


@app.cell
def __(mo):
    mo.md("""### 1. 上报数据文件""")
    return


@app.cell
def __(Path, data_csvpathstr, mo, sheetnames_dict, split_rPath_names):
    csv_paths = Path(data_csvpathstr).rglob(r"*.csv")
    ## 文件中所有字段
    # csv_names_list = []
    SplitPathNamesCLS = split_rPath_names(csv_paths)
    # csv_path_stem = #[csv_names_list.extend(csv_path.stem.split("_")) for csv_path in csv_paths]
    csv_names_CJK_list = SplitPathNamesCLS.get_CJK_list()  # [csv_name for csv_name in csv_names_list if contains_CJK(csv_name).contains_chinese()]
    csv_names_ONLYNUM_list = SplitPathNamesCLS.get_ONLYNUM_list()  # [csv_name for csv_name in csv_names_list if csv_name.isdigit() and len(csv_name) > 3]

    csv_names_TABLENAME_set = sorted(
        set(csv_names_CJK_list) - set(["中国兵器工业集团有限公司"]),
        key=lambda x: int(sheetnames_dict[x]),
    )  ## 上报表名（查找规律包含汉字）

    csv_names_DATANSTR_set = sorted(
        set(csv_names_ONLYNUM_list)
    )  ## 上报数据日期（查找规律都是数字）
    csv_default_list = ["资金结算"]
    csv_TABLENAME_ui_ms = mo.ui.multiselect(
        csv_names_TABLENAME_set, value=csv_default_list, max_selections=1
    )
    csv_DATASTR_ui_ms = mo.ui.multiselect(
        csv_names_DATANSTR_set, value=csv_names_DATANSTR_set
    )
    return (
        SplitPathNamesCLS,
        csv_DATASTR_ui_ms,
        csv_TABLENAME_ui_ms,
        csv_default_list,
        csv_names_CJK_list,
        csv_names_DATANSTR_set,
        csv_names_ONLYNUM_list,
        csv_names_TABLENAME_set,
        csv_paths,
    )


@app.cell
def __(csv_names_TABLENAME_set):
    print(csv_names_TABLENAME_set)
    return


@app.cell(hide_code=True)
def __(csv_DATASTR_ui_ms, csv_TABLENAME_ui_ms, mo):
    mo.vstack(
        [
            mo.hstack([csv_TABLENAME_ui_ms, csv_DATASTR_ui_ms]),
        ]
    )
    return


@app.cell(hide_code=True)
def __(csv_DATASTR_ui_ms, csv_TABLENAME_ui_ms, mo, sheetnames_dict):
    mo.hstack(
        [
            mo.md(
                f"选择的工作表是：\n【{sheetnames_dict[csv_TABLENAME_ui_ms.value[0]]}-{csv_TABLENAME_ui_ms.value[0]}】"
            ),
            mo.md(f"数据日期是：{csv_DATASTR_ui_ms.value}"),
        ]
    )
    return


@app.cell
def __(csv_DATASTR_ui_ms, csv_TABLENAME_ui_ms):
    csv_TABLENAME_keyword_list = csv_TABLENAME_ui_ms.value
    csv_DATASTR_keyword_list = csv_DATASTR_ui_ms.value
    TABLENAME = (
        csv_TABLENAME_keyword_list[0] if csv_TABLENAME_keyword_list else None
    )
    DATASTR_LIST = csv_DATASTR_keyword_list if csv_DATASTR_keyword_list else None
    return (
        DATASTR_LIST,
        TABLENAME,
        csv_DATASTR_keyword_list,
        csv_TABLENAME_keyword_list,
    )


@app.cell
def __(Path, TABLENAME, data_csvpathstr, pl, play_csv):
    csv_paths_selected_iterator = (
        Path(data_csvpathstr).rglob(f"*{TABLENAME}*") if TABLENAME else None
    )
    PlayCsvCLS = play_csv(csv_paths_selected_iterator)

    TABLENAME_colnames_list = PlayCsvCLS.get_colnames_union()
    pop_item_list = ["所属集团编码", "所属集团名称"]  # ,'对方单位编码' ]
    for pop_item in pop_item_list:
        TABLENAME_colnames_list.remove(pop_item)

    TABLENAME_schema = PlayCsvCLS.get_schema_union()
    update_dict = {
        "本方单位账号": pl.Utf8,
        "交易流水号": pl.Utf8,
        "交易金额": pl.Utf8,
        "对方单位编码": pl.Utf8,
    }

    TABLENAME_schema.update(update_dict)
    return (
        PlayCsvCLS,
        TABLENAME_colnames_list,
        TABLENAME_schema,
        csv_paths_selected_iterator,
        pop_item,
        pop_item_list,
        update_dict,
    )


@app.cell
def __(mo):
    slider = mo.ui.slider(1, 10, 1)
    return (slider,)


@app.cell
def __():
    import yaml

    with open(".config/keyword.yml") as file:
        keywordss = yaml.safe_load(file)
    Blacklist_company = keywordss["Blacklist_company"]
    Blacklist_keyword = keywordss["PayBlackKeywords"]
    print(Blacklist_company)
    print(Blacklist_keyword)
    return Blacklist_company, Blacklist_keyword, file, keywordss, yaml


@app.cell
def __(Path):
    import spacy

    nlp = spacy.blank("zh")
    # nlp = spacy.load('zh_core_web_sm')
    nlp = spacy.load("zh_core_web_trf")
    # from spacy.tokens import Doc, Span


    txtfilepath1 = (
        r"/home/song/NutstoreFiles/1-MyWork/4-相关制度/gzwzd/穿透式监管.txt"
    )

    txtfilepath2 = r"/home/song/NutstoreFiles/8-MyData/report/2024年1-11月经济运行动态信息情况-refine.txt"
    txtfilepath = txtfilepath2
    with open(txtfilepath) as txtfile:
        text = txtfile.read()
        txtfile.close()

    doc = nlp(text)

    doc_token = [token.text for token in doc]

    with open(
        f"{Path(txtfilepath).parents[0]}/{Path(txtfilepath).stem}_token{Path(txtfilepath).suffix}",
        "w+",
    ) as outtxtfile:
        outtxtfile.write(" ".join(doc_token))
        outtxtfile.close()
    return (
        doc,
        doc_token,
        nlp,
        outtxtfile,
        spacy,
        text,
        txtfile,
        txtfilepath,
        txtfilepath1,
        txtfilepath2,
    )


@app.cell
def __(TABLENAME_colnames_list, mo, searchconditions, slider):
    selcolnames_list = TABLENAME_colnames_list
    searchconditions_cls = searchconditions(selcolnames_list)
    search_array = searchconditions_cls.creat_array(slider.value)
    mo.vstack([slider, search_array])
    return search_array, searchconditions_cls, selcolnames_list


@app.cell
def __(mo, search_array, searchconditions_cls):
    manylines_searchresult = searchconditions_cls.manylines_searchcondition(
        search_array.value
    )
    condition_textarea = mo.ui.text_area(
        manylines_searchresult, placeholder="filter condition...."
    )
    # condition_code = mo.ui.code_editor(value=manylines_searchresult,label="filter condition",min_height=30, language="python")
    mo.vstack([condition_textarea])
    return condition_textarea, manylines_searchresult


@app.cell
def __(
    Path,
    TABLENAME,
    TABLENAME_colnames_list,
    TABLENAME_schema,
    condition_textarea,
    data_csvpathstr,
    play_csv,
    selcolnames_list,
):
    search_conditions = condition_textarea.value

    csv_paths_selected_iterator2 = (
        Path(data_csvpathstr).rglob(f"*{TABLENAME}*") if TABLENAME else None
    )
    PlayCsvCLS2 = play_csv(csv_paths_selected_iterator2)

    result_df = PlayCsvCLS2.get_searchresult(
        TABLENAME_schema,
        selcolnames_list,
        search_conditions,
        TABLENAME_colnames_list[-1],
    )
    result_df
    return (
        PlayCsvCLS2,
        csv_paths_selected_iterator2,
        result_df,
        search_conditions,
    )


@app.cell
def __(mo):
    stop_button1 = mo.ui.run_button()
    stop_button1
    return (stop_button1,)


@app.cell
def __(mo, stop_button1):
    mo.stop(not stop_button1.value)
    return


@app.cell
def __(result_df):
    rdf_nunique = result_df.group_by("数据日期").n_unique()
    rdf_nunique
    return (rdf_nunique,)


@app.cell
def __(result_df):
    result_df.group_by("数据日期").first()
    return


@app.cell
def __(result_df):
    result_df.group_by("数据日期").last()
    return


@app.cell
def __(mo):
    mo.md("""### 自动计入历史文件""")
    return


@app.cell
def __(Path, datetime, manylines_searchresult):
    historyfile_pathstr = "./temp/SKDataAnalysis_history"

    if not Path(historyfile_pathstr).exists():
        with open(historyfile_pathstr, mode="w", newline="\n") as f:
            f.writelines("时间,polars.filter语句\n")
            f.writelines(f"{datetime.now()},{manylines_searchresult}\n")
            f.close()
    else:
        with open("./temp/SKDataAnalysis_history", mode="a", newline="\n") as f:
            f.writelines(f"{datetime.now()},{manylines_searchresult}\n")
            f.close()
    return f, historyfile_pathstr


@app.cell
def __(mo):
    mo.md(r"""## 三、账户专题分析""")
    return


@app.cell
def __(mo):
    mo.md("""### 1. 账户的分布图""")
    return


@app.cell
def __(TABLENAME):
    if TABLENAME != "银行账户":
        print("此模块仅适用于账户模块！")
    else:
        print("Welcome!")
        pass
    return


@app.cell
def __(mo):
    stop_buttonZH = mo.ui.run_button()
    stop_buttonZH
    return (stop_buttonZH,)


@app.cell
def __(mo, pl, stop_buttonZH):
    mo.stop(not stop_buttonZH.value)
    print("Come on!")
    pl.read_database()
    return


@app.cell
def __(mo):
    mo.md("""## 1-sqlite格式""")
    return


@app.cell
def __(mo):
    fb_findata_initialpathstr = "/home/song/NutstoreFiles/8-MyData/FinData/"
    fb_bankinfo_path = mo.ui.file_browser(
        initial_path=fb_findata_initialpathstr,
        selection_mode="file",
        restrict_navigation=True,
        filetypes=[".db"],
    )
    return fb_bankinfo_path, fb_findata_initialpathstr


@app.cell
def __(fb_bankinfo_path, fb_findata_initialpathstr, mo):
    datadb_pathstr = (
        fb_bankinfo_path.path()
        if fb_bankinfo_path.path()
        else fb_findata_initialpathstr
    )
    mo.vstack([fb_bankinfo_path, mo.md(f"选取的路径是：{datadb_pathstr}")])
    return (datadb_pathstr,)


@app.cell
def __(mo):
    stop_button_duckdb = mo.ui.run_button()
    stop_button_duckdb
    return (stop_button_duckdb,)


@app.cell
def __(mo, stop_button_duckdb):
    mo.stop(not stop_button_duckdb.value)
    return


@app.cell
def __(Path, datadb_pathstr):
    import duckdb

    ## 安装插件
    con = duckdb.connect()
    con.install_extension("sqlite")
    con.install_extension("parquet")
    match Path(datadb_pathstr).suffix:
        case ".db":
            con = duckdb.connect(datadb_pathstr)
            # con.sql('alter table BANKINFO rename to BankBasicInfo;')
            bankinfodb_tablesnames_fetchall = con.sql("show tables;").fetchall()
            bankinfodb_tablesnames_list = list(
                i[0] for i in bankinfodb_tablesnames_fetchall
            )
            print(bankinfodb_tablesnames_list)
    return (
        bankinfodb_tablesnames_fetchall,
        bankinfodb_tablesnames_list,
        con,
        duckdb,
    )


@app.cell
def __(bankinfodb_tablesnames_list, con, defaultdict):
    tableAndColnames = defaultdict(list)
    for tbname in bankinfodb_tablesnames_list:
        tableAndColnames[tbname] = [
            i[0]
            for i in con.sql(
                f"SELECT column_name FROM duckdb_columns() where table_name = '{tbname}';"
            ).fetchall()
        ]
    return tableAndColnames, tbname


@app.cell
def __(Path, datadb_pathstr, tableAndColnames):
    with open(
        Path(datadb_pathstr).parent / "bankinfodb_table_colnames", "w+"
    ) as datadb_tbl_col_f:
        for k, v in tableAndColnames.items():
            datadb_tbl_col_f.writelines(["".join([k, ".", j, "\n"]) for j in v])
        datadb_tbl_col_f.close()
    return datadb_tbl_col_f, k, v


@app.cell
def __(Path, datadb_pathstr):
    with open(
        Path(datadb_pathstr).parent / "select_bankinfodb_colnames", "r"
    ) as select_colnames_f:
        select_bankinfodb_colnames = select_colnames_f.readlines()
        select_bankinfodb_colnames = [
            i.rstrip() for i in select_bankinfodb_colnames
        ]
        select_colnames_f.close()
    return select_bankinfodb_colnames, select_colnames_f


@app.cell
def __(Path, datadb_pathstr):
    with open(
        Path(datadb_pathstr).parent / "select_bankinfodb_table_colnames", "r"
    ) as select_table_colnames_f:
        select_bankinfodb_table_colnames = select_table_colnames_f.readlines()
        select_bankinfodb_table_colnames = [
            i.rstrip() for i in select_bankinfodb_table_colnames
        ]
        select_table_colnames_f.close()
    return select_bankinfodb_table_colnames, select_table_colnames_f


@app.cell
def __(con):
    bankinfodb_fetchfull = con.sql(
        """select * from BankBasicInfo Join CityInfo ON BankBasicInfo.CCPC = CityInfo.城市代码 JOIN BankTypeInfo ON BankBasicInfo.BankType = BankTypeInfo.行别代码"""
    ).pl()
    return (bankinfodb_fetchfull,)


@app.cell
def __(con, select_bankinfodb_table_colnames):
    sqlstr = ",".join(select_bankinfodb_table_colnames)
    bankinfodb_fetchpart = con.sql(
        f"""select {sqlstr} from BankBasicInfo Join CityInfo ON BankBasicInfo.CCPC = CityInfo.城市代码 JOIN BankTypeInfo ON BankBasicInfo.BankType = BankTypeInfo.行别代码"""
    ).pl()
    return bankinfodb_fetchpart, sqlstr


@app.cell
def __(bankinfodb_fetchpart):
    bankinfodb_fetchpart
    return


@app.cell
def __(Path, bankinfodb_fetchfull, datadb_pathstr):
    with open(
        Path(datadb_pathstr).parent / "bankinfodb_only_colnames", "w+"
    ) as datadb_f:
        datadb_f.writelines(
            ["".join([v, "\n"]) for v in bankinfodb_fetchfull.columns]
        )
        datadb_f.close()
    return (datadb_f,)


@app.cell
def __():
    ## 数据库中所有表存成pl。df格式
    bankinfo_dfs_dict = {}
    return (bankinfo_dfs_dict,)


@app.cell
def __(bankinfo_dfs_dict, bankinfodb_tablesnames_list, con):
    for tablename in bankinfodb_tablesnames_list:
        bankinfo_dfs_dict[tablename] = con.sql(f"select * from {tablename}").pl()
    return (tablename,)


@app.cell
def __(mo):
    mo.md("""### 查询模块""")
    return


@app.cell
def __(bankinfodb_tablesnames_list, mo):
    bankinfodb_tablesnames_ms = mo.ui.multiselect(
        bankinfodb_tablesnames_list,
        value=[bankinfodb_tablesnames_list[0]],
        max_selections=1,
    )
    bankinfodb_tablesnames_ms
    return (bankinfodb_tablesnames_ms,)


@app.cell
def __(mo):
    bankinfo_slider = mo.ui.slider(1, 10, 1)
    return (bankinfo_slider,)


@app.cell
def __(
    bankinfo_dfs_dict,
    bankinfo_slider,
    bankinfodb_fetchfull,
    bankinfodb_tablesnames_ms,
    mo,
    searchconditions,
):
    bankinfo_table_df = bankinfo_dfs_dict[bankinfodb_tablesnames_ms.value[0]]
    bankinfo_df = bankinfodb_fetchfull
    bankinfo_selcolnames_list = bankinfo_table_df.columns
    bankinfo_searchconditions_cls = searchconditions(bankinfo_selcolnames_list)

    bankinfo_search_array = bankinfo_searchconditions_cls.creat_array(
        bankinfo_slider.value
    )
    mo.vstack([bankinfo_slider, bankinfo_search_array])
    return (
        bankinfo_df,
        bankinfo_search_array,
        bankinfo_searchconditions_cls,
        bankinfo_selcolnames_list,
        bankinfo_table_df,
    )


@app.cell
def __(bankinfo_search_array, bankinfo_searchconditions_cls, mo):
    bankinfo_manylines_searchresult = (
        bankinfo_searchconditions_cls.manylines_searchcondition(
            bankinfo_search_array.value
        )
    )
    bankinfo_condition_textarea = mo.ui.text_area(
        bankinfo_manylines_searchresult, placeholder="filter condition...."
    )
    mo.vstack([bankinfo_condition_textarea])
    return bankinfo_condition_textarea, bankinfo_manylines_searchresult


@app.cell
def __(mo):
    mo.md("""### 方法1:输入的是完整（full)df，在df阶段再筛选；""")
    return


@app.cell
def __(bankinfo_condition_textarea, bankinfo_df):
    bankinfo_search_conditions = bankinfo_condition_textarea.value
    bankinfo_select_colnames_list = [
        "BANKCODE",
        "BANKNAME",
        "城市代码",
        "城市名称",
        "行别代码",
        "行别名称",
    ]
    bankinfo_search_result_df = bankinfo_df.select(
        bankinfo_select_colnames_list
    ).filter(eval(bankinfo_search_conditions))
    bankinfo_search_result_df
    return (
        bankinfo_search_conditions,
        bankinfo_search_result_df,
        bankinfo_select_colnames_list,
    )


@app.cell
def __(mo):
    mo.md("""### 方法2:输入的是筛选后(part)df；""")
    return


@app.cell
def __(bankinfo_search_conditions, bankinfodb_fetchpart):
    bankinfo_search_result_df2 = (
        bankinfodb_fetchpart
        # .select(bankinfo_select_colnames_list)
        .filter(eval(bankinfo_search_conditions))
    )
    bankinfo_search_result_df2
    return (bankinfo_search_result_df2,)


@app.cell
def __():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
