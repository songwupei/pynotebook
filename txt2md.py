#!/bin/python3

import sys
import polars as pl
from tabulate import tabulate

def get_csv(n_rows=33, n_cols=2):
    '''
    该函数用于读取CSV文件，清洗数据，并将数据拆分为多个表格后保存为Markdown文件。
    :param n_cols: 每页表格的列数。
    :param n_rows: 每页表格的行数。
    '''
    # 检查是否提供了文件路径参数
    if len(sys.argv) < 2:
        print("Usage: python lpr.py <datafilepath>")
        sys.exit(1)

    # 获取文件路径
    datafilepath = sys.argv[1]
    
    # 第一步：读取txt文档
    df = pl.read_csv(datafilepath, separator="\t")  # 假设txt文件是以制表符分隔的

    # 第二步：清洗数据
    for col in df.columns:
        # 1. 如果列名包含“日期”，将其转换为日期格式 "YYYY-MM-DD"
        if "日期" in col:
            df = df.with_columns(pl.col(col).str.strptime(pl.Date, "%Y/%m/%d").dt.strftime("%Y-%m-%d"))
        
        # 2. 如果数值中包含“%”，则删除“%”，并在列标题中添加“%”，并将数值类型转换为浮点类型
        if df[col].dtype == pl.Utf8 and df[col].str.contains("%").any():
            df = df.with_columns(pl.col(col).str.replace("%", "").cast(pl.Float64))
            df = df.rename({col: f"{col}%"})

    # 第三步：将df拆分成多个表格并保存为markdown文件
    # 读取模板文件
    with open("../config/output_normal.md", "r") as template_file:
        template_content = template_file.read()

        # 打开输出文件并写入模板内容
    with open("./output.md", "w") as f:
        f.write(template_content)
        f.write("\n\n")  # 在表格前添加一些空白

        # 将DataFrame按n_rows拆分成多个块
        chunks = [df.slice(start, n_rows) for start in range(0, df.height, n_rows)]
        for i,chunk in enumerate(chunks):
            for col in chunk.columns:
                chunk = chunk.rename({col: f"{col}_{i}"})
            chunks[i] = chunk

            # 每n_cols个块水平拼接
        for idx in range(0, len(chunks), n_cols):
            joined_chunk = pl.concat([chunks[idx + j] for j in range(n_cols) if idx + j < len(chunks)], how="horizontal")
                
                # 将拼接后的块转换为Markdown表格
            markdown_table = tabulate(joined_chunk.to_pandas(), headers='keys', tablefmt='github', showindex=False)
                
                # 将Markdown表格写入文件
            f.write(markdown_table)
            f.write("\n\n")  # 在每个表格后添加一些空白

if __name__ == "__main__":
    get_csv()
