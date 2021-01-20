import datetime
import re

import requests
import xlrd
from xlrd import xldate_as_tuple

from hkland_configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_USER, SPIDER_MYSQL_PORT, SPIDER_MYSQL_PASSWORD,
                            SPIDER_MYSQL_DB)
from sql_base import Connection

url = 'https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities?sc_lang=zh-HK'


class EliStockSpider(object):
    spider_conn = Connection(  # 爬虫库
        host=SPIDER_MYSQL_HOST,
        port=SPIDER_MYSQL_PORT,
        user=SPIDER_MYSQL_USER,
        password=SPIDER_MYSQL_PASSWORD,
        database=SPIDER_MYSQL_DB,
    )
    # 滬股通股票/中華通股票名單之更改(2020年12月)
    change_url = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities_xls/Change_of_SSE_Securities_Lists_c.xls?la=zh-HK'
    change_file = 'Change_of_SSE_Securities_Lists_c.xls'

    def create_table(self):
        sql = '''
        CREATE TABLE IF NOT EXISTS `Change_of_SSE_Securities_Lists` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `OrderID` int unsigned NOT NULL COMMENT '网站爬取序号',
          `EffectiveDate` date NOT NULL COMMENT '变更生效日期',
          `SecuCode` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '股票代码',
          `SecuAbbr` varchar(200) COLLATE utf8_bin DEFAULT NULL COMMENT '股票名称',
          `ChangeType` varchar(200) COLLATE utf8_bin DEFAULT NULL COMMENT '变更类型',
          `PubDate` date DEFAULT NULL COMMENT '网站发布更新日期',
          `Remarks` longtext COLLATE utf8_bin COMMENT '变更说明',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `change_record` (`PubDate`, `OrderID`), 
          KEY `update_time` (`UPDATETIMEJZ`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='滬股通股票/中華通股票名單之更改'; 
        '''
        self.spider_conn.execute(sql)

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
        items = []
        for i in range(4, nrows):  # 循环打印每一行
            row_values = worksheet.row_values(i)
            item = dict(zip(headers, row_values))
            item.update({"PubDate": pubdate, 'EffectiveDate': datetime.datetime(*xldate_as_tuple(row_values[-1], 0))})
            items.append(item)
        self.spider_conn.batch_insert(items, 'Change_of_SSE_Securities_Lists', headers)


if __name__ == '__main__':
    eli = EliStockSpider()
    # eli.create_table()
    # eli.refresh_xls_file()
    eli.perse_xls_datas()
