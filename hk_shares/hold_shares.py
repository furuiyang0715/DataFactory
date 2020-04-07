import datetime
import hashlib
import logging
import re
import sys
import traceback
import requests
import opencc
from lxml import html

from hk_shares.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER, SPIDER_MYSQL_PASSWORD,
                               SPIDER_MYSQL_DB, PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_PORT, PRODUCT_MYSQL_USER,
                               PRODUCT_MYSQL_PASSWORD, PRODUCT_MYSQL_DB)
from hk_shares.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HoldShares(object):
    """滬股通及深股通持股紀錄按日查詢"""
    spider_cfg = {   # 爬虫库
        "host": SPIDER_MYSQL_HOST,
        "port": SPIDER_MYSQL_PORT,
        "user": SPIDER_MYSQL_USER,
        "password": SPIDER_MYSQL_PASSWORD,
        "db": SPIDER_MYSQL_DB,
    }

    product_cfg = {    # 正式库
        "host": PRODUCT_MYSQL_HOST,
        "port": PRODUCT_MYSQL_PORT,
        "user": PRODUCT_MYSQL_USER,
        "password": PRODUCT_MYSQL_PASSWORD,
        "db": PRODUCT_MYSQL_DB,
    }

    def __init__(self, type):
        self.type = type
        self.url = 'http://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t={}'.format(type)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        }
        self.today = datetime.date.today()
        # 当前只能查询之前一天的记录
        self.check_day = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y/%m/%d")   # 2020/03/29
        self.converter = opencc.OpenCC('t2s')  # 中文繁体转简体
        _type_map = {
            'sh': '沪股通',
            'sz': '深股通',
            'hk': '港股通',
        }
        self.type_name = _type_map.get(self.type)
        _percent_comment_map = {
            'sh': '占于上交所上市及交易的A股总数的百分比(%)',
            'sz': '占于深交所上市及交易的A股总数的百分比(%)',
            'hk': '占已发行股份的百分比(%)',
        }
        self.percent_comment = _percent_comment_map.get(self.type)

        self.table = 'hold_shares_{}'.format(self.type)

    @property
    def post_params(self):
        data = {
            '__VIEWSTATE': '/wEPDwUJNjIxMTYzMDAwZGQ79IjpLOM+JXdffc28A8BMMA9+yg==',
            '__VIEWSTATEGENERATOR': 'EC4ACD6F',
            '__EVENTVALIDATION': '/wEdAAdtFULLXu4cXg1Ju23kPkBZVobCVrNyCM2j+bEk3ygqmn1KZjrCXCJtWs9HrcHg6Q64ro36uTSn/Z2SUlkm9HsG7WOv0RDD9teZWjlyl84iRMtpPncyBi1FXkZsaSW6dwqO1N1XNFmfsMXJasjxX85jz8PxJxwgNJLTNVe2Bh/bcg5jDf8=',
            'today': '{}'.format(self.today.strftime("%Y%m%d")),
            'sortBy': 'stockcode',
            'sortDirection': 'asc',
            'alertMsg': '',
            'txtShareholdingDate': '{}'.format(self.check_day),
            'btnSearch': '搜尋',
        }
        return data

    def _init_pool(self, cfg):
        pool = PyMysqlPoolBase(**cfg)
        return pool

    def _create_table(self):
        # SHOUGANG CONCORD INTERNATIONAL ENTERPRISES CO LTD-TEMPORARY COUNTER    # length = 67 50-->100
        sql = '''
        CREATE TABLE IF NOT EXISTS `hold_shares_{}` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `SecuCode` varchar(10) COLLATE utf8_bin NOT NULL COMMENT '股票交易代码',
          `SecuName` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '股票名称',
          `Holding` decimal(19,2) DEFAULT NULL COMMENT '于中央结算系统的持股量',
          `Percent` decimal(9,4) DEFAULT NULL COMMENT '{}',
          `Date` date DEFAULT NULL COMMENT '日期',
          `ItemID` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT 'itemid',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `unique_key` (`SecuCode`,`Date`,`ItemID`),
          KEY `SecuCode` (`SecuCode`),
          KEY `update_time` (`UPDATETIMEJZ`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='{}持股记录（HKEX）'; 
        '''.format(self.type, self.percent_comment, self.type_name)
        spider = self._init_pool(self.spider_cfg)
        spider.insert(sql)
        spider.dispose()

    # def contract_sql(self, to_insert: dict, table: str):
    #     ks = []
    #     vs = []
    #     for k in to_insert:
    #         ks.append(k)
    #         vs.append(to_insert.get(k))
    #     fields_str = "(" + ",".join(ks) + ")"
    #     values_str = "(" + "%s," * (len(vs) - 1) + "%s" + ")"
    #     base_sql = '''REPLACE INTO `{}` '''.format(table) + fields_str + ''' values ''' + values_str + ''';'''
    #     return base_sql, tuple(vs)

    def contract_sql(self, to_insert: dict, table: str, update_fields: list):
        ks = []
        vs = []
        for k in to_insert:
            ks.append(k)
            vs.append(to_insert.get(k))
        fields_str = "(" + ",".join(ks) + ")"
        values_str = "(" + "%s," * (len(vs) - 1) + "%s" + ")"
        base_sql = '''INSERT INTO `{}` '''.format(table) + fields_str + ''' values ''' + values_str
        on_update_sql = ''' ON DUPLICATE KEY UPDATE '''
        update_vs = []
        for update_field in update_fields:
            on_update_sql += '{}=%s,'.format(update_field)
            update_vs.append(to_insert.get(update_field))
        on_update_sql = on_update_sql.rstrip(",")
        sql = base_sql + on_update_sql + """;"""
        vs.extend(update_vs)
        return sql, tuple(vs)

    def _save(self, sql_pool, to_insert, table, update_fields):
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            count = sql_pool.insert(insert_sql, values)
        except:
            traceback.print_exc()
            logger.warning("失败")
        else:
            if count == 1:
                logger.info("更入新数据 {}".format(to_insert))
            sql_pool.end()
            return count

    def _start(self):
        self._create_table()
        resp = requests.post(self.url, data=self.post_params)
        if resp.status_code == 200:
            body = resp.text
            doc = html.fromstring(body)
            date = doc.xpath('//*[@id="pnlResult"]/h2/span/text()')[0]
            # print(date)    # 持股日期: 2020/03/28
            date = re.findall(r"持股日期: (\d{4}/\d{2}/\d{2})", date)[0]
            # print(date)    # 2020/03/28
            trs = doc.xpath('//*[@id="mutualmarket-result"]/tbody/tr')
            item = {}
            for tr in trs:
                # 股份代码
                secu_code = tr.xpath('./td[1]/div[2]/text()')[0].strip()
                item['SecuCode'] = secu_code
                # 股票名称
                secu_name = tr.xpath('./td[2]/div[2]/text()')[0].strip()
                simple_secu_name = self.converter.convert(secu_name)
                item['SecuName'] = simple_secu_name
                # 於中央結算系統的持股量
                holding = tr.xpath('./td[3]/div[2]/text()')[0]
                if holding:
                    holding = int(holding.replace(',', ''))
                else:
                    holding = 0
                item['Holding'] = holding
                # 占股的百分比
                POAShares = tr.xpath('./td[4]/div[2]/text()')
                if POAShares:
                    POAShares = float(POAShares[0].replace('%', ''))
                else:
                    POAShares = float(0)
                item['Percent'] = POAShares
                # # 类别
                # item['category'] = self.type_name
                # 时间是连续的今天的时间
                # item['']

                # 时间
                # item['Date'] = date.replace("/", "-")
                # # 类别+代码+时间 存成一个 hashID
                # d = date.replace('/', '')
                # content = self.type_name + item['SecuCode'] + d
                # m2 = hashlib.md5()
                # m2.update(content.encode('utf-8'))
                # item_id = m2.hexdigest()
                # item['ItemID'] = item_id
                spider = self._init_pool(self.spider_cfg)
                update_fields = ['SecuCode', 'SecuName', 'Holding', 'Percent', 'Date']
                print(item)
                # self._save(spider, item, self.table, update_fields)
                # 将其存入爬虫数据库 hold_shares_sh hold_shares_sz hold_shares_hk

    def start(self):
        try:
            self._start()
        except:
            traceback.print_exc()

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 数据加工处理分界线 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # 生成正式库中的两个 hkland_shares hkland_hkshares
    def _create_product_table(self):
        sql = '''
        CREATE TABLE IF NOT EXISTS `hkland_shares` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `SecuCode` varchar(16) COLLATE utf8_bin NOT NULL COMMENT '股票交易代码',
          `InnerCode` int(11) NOT NULL COMMENT '内部编码',
          `SecuAbbr` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '股票简称',
          `Date` datetime NOT NULL COMMENT '自然日',
          `HKTradeDay` datetime NOT NULL COMMENT '港交所交易日',
          `Percent` decimal(20,4) DEFAULT NULL COMMENT '占A股总股本的比例（%）',
          `ShareNum` decimal(20,0) DEFAULT NULL COMMENT '股票数量(股)',
          `HashID` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT 'HashID',
          `CMFTime` datetime NOT NULL COMMENT '日期',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un` (`Date`,`HKTradeDay`,`SecuCode`),
          UNIQUE KEY `un2` (`InnerCode`,`Date`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='沪/深股通持股记录'; 
        '''

        sql2 = '''
        CREATE TABLE IF NOT EXISTS `hkland_hkshares` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `SecuCode` varchar(16) COLLATE utf8_bin NOT NULL COMMENT '股票交易代码',
          `InnerCode` int(11) NOT NULL COMMENT '内部编码',
          `SecuAbbr` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '股票简称',
          `Date` datetime NOT NULL COMMENT '日期',
          `Percent` decimal(20,4) DEFAULT NULL COMMENT '占已发行港股的比例（%）',
          `ShareNum` decimal(20,0) DEFAULT NULL COMMENT '股票数量（股）',
          `HashID` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT 'HashID',
          `CMFTime` datetime NOT NULL COMMENT '日期',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un` (`Date`,`SecuCode`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='港股通持股记录-港股'; 
        '''

    def sync(self):
        pass


if __name__ == "__main__":
    # 可开多线程 不要求太实时 就顺序执行
    for type in ("sh", "sz", "hk"):
        h = HoldShares(type)
        h.start()
