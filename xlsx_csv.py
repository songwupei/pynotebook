from pathlib import Path
from pd_io_excel import xlsxs_to_csv
pathstr = '/home/song/Documents/temp/关于进一步做好报送三季度司库数据的通知10.10v3/20240701-20240930国资委上报数据_各子集团'
filepaths1 =Path(pathstr).rglob('*.xlsx')
filepaths =Path(pathstr).rglob('*.xlsx')
root_dir = "/home/song/Documents/temp/上报数据"
xlsxs_to_csv(filepaths, pathstr, root_dir,'SK')
