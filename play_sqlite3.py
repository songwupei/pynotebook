# -*- encoding: utf-8 -*-
"""
@File    :   exec_sqlite.py
@Time    :   2022/12/13 22:55
@Author  :   SxS
@Version :   1.0
====
@File    :   play_sqlite3.py
@Time    :   2024/09/04 14:27
@Version :   1.1
@Contact :   songwupei@163.com
"""

from pathlib import Path
from datetime import datetime

import sqlite3
import pandas as pd
from sqlalchemy import create_engine

# MyPkg
from similarity_of_words import Similarity_word
from pd_io_excel import get_shts_to_df  # 读取一个workbook中所有sheets


class xls_convert_db(object):
    """
    My first autorun_project
    LAZY WORK
    """

    def __init__(self):
        import platform

        self.OS_NAME = platform.system()

    def get_path(self, filetype):
        """
        Parameters
        ----------
        filetype : str
        e.g.:xls|xlsx, db
        以后excel数据库
        db数据库都要放到指定位置.

        Returns
        -------
        str:dirPath
        """
        match self.OS_NAME:
            case "Linux":
                match filetype:
                    case "xls" | "xlsx":
                        return "/home/song/NutstoreFiles/1-MyWork/8-MyData/"
                    case "db":
                        return "/home/song/NutstoreFiles/1-MyWork/8-MyData/"
            case "Windows":
                match filetype:
                    case "xls" | "xlsx":
                        return "D:\\宋吴培\\国务院国资委（央企信息）\\"

    def get_file(self, filename):
        """
        get full_file_PATH, filetype
        """
        filetype = filename.split(".")[-1]
        return Path(self.get_path(filetype) + filename), filetype

    def file_exist(self, filepath):
        if not filepath.exists():
            with open(filepath, "w", encoding="utf-8") as fp:
                fp.close()

    def show_tablenames(self, dbfile):
        return operate_sqlite3(dbfile).get_tablenames()

    def df_to_xls(self, df, xlsfile):
        # 如果不存在outfile, 新建file。
        self.file_exist(xlsfile)
        df.to_excel(xlsfile)

    def df_to_db(self, df, tablenames, dbfile):
        # 如果不存在outfile, 新建file。
        self.file_exist(dbfile)
        engine = create_engine("sqlite:///" + dbfile, echo=False)
        df.to_sql(tablenames, con=engine)

    def xls_to_db(self, xlsfile, dbfile):
        dfs, sheetnames, _, _ = get_shts_to_df(xlsfile, skiprows=1, index_col=0)
        tablenames = operate_sqlite3(dbfile).get_tablenames()
        # 取需要新建的tablename
        sheetnames = set(sheetnames).difference(set(tablenames))
        if len(sheetnames) == 0:
            return print("已有的sheetname都在dbfile中.")
        for df, sheetname in zip(dfs, list(sheetnames)):
            self.df_to_db(df, sheetname, dbfile)


class operate_sqlite3(object):
    """
    operate the database by sqlite3
    """

    def __init__(self, dbfile, sqlstr=None):
        print("init method called")
        self.conn = sqlite3.connect(dbfile)
        self.cur = self.conn.cursor()
        self.sql = sqlstr
        self.dbfile_stem = Path(dbfile).stem
        self.dbfile_parent = Path(dbfile).parent
        self.res = self.cur.execute(self.sql)

    def fetchone(self):
        return self.res.fetchone()

    def fetchall(self):
        return self.res.fetchall()

    def get_colnames(self):
        return tuple([d[0] for d in self.res.description])

    def __exit__(self):
        self.conn.commit()
        self.conn.close()
        print("exit method called")

    def pandas_read_sql(self):
        return pd.read_sql_query(self.sql, self.conn)

    def get_tablenames(self):
        # 根据sheetname 决定 tablename。
        self.sql = "SELECT name FROM sqlite_master WHERE type='table'"
        return [list(i)[0] for i in self.fetchall()]

    def sql_to_excel(self):
        colnames = self.get_colnames()
        fetchalldata = self.fetchall()
        df = pd.DataFrame(data=fetchalldata, columns=colnames)
        df.to_excel(
            f"{self.dbfile_parent}/{self.dbfile_stem}_query_{datetime.today().date()}.xlsx"
        )
        print(
            f"输出{self.dbfile_parent}/{self.dbfile_stem}_query_{datetime.today().date()}.xlsx"
        )


class exec_SL(object):
    """
    固化函数
    1-将企查查的分类【700类】与国资委【10类+】的组织类型字典相匹配
    2-生成SQL语句，用于查找一张表,主键相同，但是字段赋值不同的情况。
    """

    def __init__(self):
        self.xlsname = "econkind.xlsx"
        self.column_names = ["国资委字典", "企查查字典"]

        self.dirpath = "/home/song/NutstoreFiles/1-MyWork/8-MyData/"

    def get_sqlite_duplicate_str(self, checkcolname, tablename):
        ## 生成sql语句，
        ## 用于寻找一张表重复的字段
        sqlquery_str = f"""
        SELECT * FROM CompanyList WHERE {checkcolname} IN 
        (SELECT {checkcolname} FROM ( SELECT {checkcolname},count({checkcolname}) FROM {tablename} GROUP By {checkcolname} HAVING count({checkcolname}) >1));
        """
        return sqlquery_str

    def SLdf_to_db(self):
        # 使用SL 函数取得 match_df
        SL = Similarity_word(self.dirpath, self.xlsname, self.column_names)
        match_df = SL.Contrast()
        xls_convert_db().df_to_db(match_df, "ECONKIND", "FMDM.db")


if __name__ == "__main__":
    # 调用自制函数basic_project
    xlsname1 = "SKPXList-ZW.xlsx"
    xlsname2 = "五类企业代码库.xlsx"
    dbname1 = "FMDM.db"
    dbname2 = "CONTACT.db"
    dbname3 = "五类企业2022年.db"
    sql1 = "SELECT * FROM COMPANY WHERE NAME = 'SONG'"
    sql2 = "SELECT * FROM CONTACT"
    """
    # 选择语句
    # infileNAME, sql = infileNAME1, sql1
    # infileNAME, sql = infileNAME2, sql2
    """
    xlsname, dbname = xlsname2, dbname3
    """
    et_sql = xls_convert_db(infileNAME)
    dirpath = et_sql.get_path(et_sql.inTYPE)
    dbfile = Path(dirpath + infileNAME)
    xlsfile = Path(dirpath + xlsname)
    """
    """
    # show tablenames
    et_sql.show_tablenames()
    """
    # xls_convert_db
    et_to_sql = xls_convert_db()
    xlsfile, _ = et_to_sql.get_file(xlsname)
    dbfile, _ = et_to_sql.get_file(dbname)
    et_to_sql.xls_to_db(xlsfile, dbfile)
    print(et_to_sql.show_tablenames(dbfile))


def sqlite_exec(dbfilepath, sqlquery_str):
    """
    exec database
    """
    dbfile_stem = Path(dbfilepath).stem
    dbfile_parent = Path(dbfilepath).parent

    print(dbfile_stem)
    conn = sqlite3.connect(dbfilepath)
    c = conn.cursor()

    c.execute(sqlquery_str)
    ## 打印标题
    colnames = [d[0] for d in c.description]
    fetchalldata = [f for f in c.fetchall()]
    print(colnames)
    print(fetchalldata)
    conn.commit()
    conn.close()
    df = pd.DataFrame(data=fetchalldata, columns=colnames)
    df.to_excel(f"{dbfile_parent}/{dbfile_stem}_query_{datetime.today().date()}.xlsx")


dbpath = (
    "/home/song/NutstoreFiles/1-MyWork/2024年司库/20240101-vizdata/SK_AccountInfo.db"
)
sqlquery_str = get_sqlite_duplicate_str("开户单位编码", "CompanyList")
sqlite_exec(dbpath, sqlquery_str)
