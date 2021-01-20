import datetime
import re

import requests
import xlrd

url = 'https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities?sc_lang=zh-HK'


class EliStockSpider(object):
    # 滬股通股票/中華通股票名單之更改(2020年12月)
    change_url = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities_xls/Change_of_SSE_Securities_Lists_c.xls?la=zh-HK'
    change_file = 'Change_of_SSE_Securities_Lists_c.xls'

    def refresh_xls_file(self):
        resp = requests.get(self.change_url)
        if resp.status_code == 200:
            with open(self.change_file, 'wb') as f:
                f.write(resp.content)

    def perse_xls_datas(self):
        workbook = xlrd.open_workbook(self.change_file)
        # 通过sheet索引获得sheet对象
        worksheet = workbook.sheet_by_index(0)
        nrows = worksheet.nrows  # 获取该表总行数
        # ncols = worksheet.ncols  # 获取该表总列数
        # for i in range(nrows):  # 循环打印每一行
        #     print(worksheet.row_values(i))

        # 发布时间
        pubdate = worksheet.row_values(2)[0]
        y, m, d = re.findall(r"更新日期：(\d{4})年(\d{1,2})月(\d{1,2})日", pubdate)[0]
        pubdate = datetime.datetime(int(y), int(m), int(d))

        chi_headers = worksheet.row_values(3)
        # ['數目', '上交所股份編號', '股票名稱', '更改', '備註', '生效日期']
        headers = ['OrderID', 'SecuCode', 'SecuAbbr', 'ChangeType', 'Remarks', 'EffectiveDate']
        for i in range(4, nrows):  # 循环打印每一行
            row_values = worksheet.row_values(i)
            item = dict(zip(headers, row_values))
            item.update({"PubDate": pubdate})
            print(item)






if __name__ == '__main__':
    eli = EliStockSpider()
    # eli.refresh_xls_file()
    eli.perse_xls_datas()
