import datetime
import hashlib
import json
import logging
import re
import sys
import traceback

import pandas as pd
import requests
import opencc
from lxml import html

from hk_shares.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER, SPIDER_MYSQL_PASSWORD,
                               SPIDER_MYSQL_DB, PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_PORT, PRODUCT_MYSQL_USER,
                               PRODUCT_MYSQL_PASSWORD, PRODUCT_MYSQL_DB, JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD,
                               JUY_DB)
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

    juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,
    }

    def __init__(self, type, offset=1):
        self.type = type
        self.url = 'http://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t={}'.format(type)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        }
        self.today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        self.offset = offset
        # 当前只能查询之前一天的记录
        self.check_day = (datetime.date.today() - datetime.timedelta(days=self.offset)).strftime("%Y/%m/%d")
        self.converter = opencc.OpenCC('t2s')  # 中文繁体转简体
        _type_map = {
            'sh': '沪股通',
            'sz': '深股通',
            'hk': '港股通',
        }

        _market_map = {
            "sh": 83,
            "sz": 90,
            'hk': 72,

        }
        self.market = _market_map.get(self.type)

        self.type_name = _type_map.get(self.type)
        _percent_comment_map = {
            'sh': '占于上交所上市及交易的A股总数的百分比(%)',
            'sz': '占于深交所上市及交易的A股总数的百分比(%)',
            'hk': '占已发行股份的百分比(%)',
        }
        self.percent_comment = _percent_comment_map.get(self.type)

        # 敏仪的爬虫表名是 hold_shares_sh hold_shares_sz hold_shares_hk
        # 我这边更新后的表名是 hoding_ .. 区别是跟正式表的字段保持一致
        self.spider_table = 'holding_shares_{}'.format(self.type)
        # 生成正式库中的两个 hkland_shares hkland_hkshares
        if self.type in ("sh", "sz"):
            self.table_name = 'hkland_shares'
        elif self.type == "sz":
            self.table_name = 'hkland_hkshares'
        else:
            raise

        #  FIXME 运行内存
        self.inner_code_map = self.get_inner_code_map()

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
        # ['SecuCode', 'InnerCode', 'SecuAbbr', 'Date', 'Percent', 'ShareNum']
        sql = '''
         CREATE TABLE IF NOT EXISTS `{}` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `SecuCode` varchar(16) COLLATE utf8_bin NOT NULL COMMENT '股票交易代码',
          `InnerCode` int(11) NOT NULL COMMENT '内部编码',
          `SecuAbbr` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '股票简称',
          `Date` datetime NOT NULL COMMENT '自然日',
          `Percent` decimal(20,4) DEFAULT NULL COMMENT '占A股总股本的比例（%）',
          `ShareNum` decimal(20,0) DEFAULT NULL COMMENT '股票数量(股)',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un2` (`InnerCode`,`Date`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='沪/深股通持股记录'; 
        '''.format(self.spider_table)
        spider = self._init_pool(self.spider_cfg)
        spider.insert(sql)
        spider.dispose()

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

    def get_inner_code_map(self):
        """https://dd.gildata.com/#/tableShow/27/column///
           https://dd.gildata.com/#/tableShow/718/column///
        """
        juyuan = self._init_pool(self.juyuan_cfg)
        if self.type in ("sh", "sz"):
            sql = 'SELECT SecuCode,InnerCode from SecuMain WHERE SecuCategory in (1, 2) and SecuMarket in (83, 90) and ListedSector in (1, 2, 6, 7);'
        else:
            sql = '''SELECT SecuCode,InnerCode from hk_secumain WHERE SecuCategory in (51, 3, 53, 78) and SecuMarket in (72) and ListedSector in (1, 2, 6, 7);'''
        ret = juyuan.select_all(sql)
        juyuan.dispose()
        info = {}
        for r in ret:
            key = r.get("SecuCode")
            value = r.get('InnerCode')
            info[key] = value
        return info

    def _trans_secucode(self, secu_code: str):
        """香港 大陆证券代码转换
        规则: 沪: 60-> 9
            深: 000-> 70, 001-> 71, 002-> 72, 003-> 73, 300-> 77

        FIXME: 科创板  688 在港股无对应的代码
        """
        if self.type == "sh":
            if secu_code.startswith("9"):
                secu_code = "60" + secu_code[1:]
            else:
                logger.warning("{}无对应的大陆编码".format(secu_code))
                raise
        elif self.type == 'sz':
            if secu_code.startswith("70"):
                secu_code = "000" + secu_code[2:]
            elif secu_code.startswith("71"):
                secu_code = "001" + secu_code[2:]
            elif secu_code.startswith("72"):
                secu_code = "002" + secu_code[2:]
            elif secu_code.startswith("73"):
                secu_code = "003" + secu_code[2:]
            elif secu_code.startswith("77"):
                secu_code = "300" + secu_code[2:]
            else:
                logger.warning("{} 无对应的大陆编码".format(secu_code))
                raise
        elif self.type == 'hk':
            # 补上 0
            if len(secu_code) != 5:
                secu_code = "0"*(5-len(secu_code)) + secu_code
        else:
            raise

        return secu_code

    def get_inner_code(self, secu_code):
        ret = self.inner_code_map.get(secu_code)
        if not ret:
            logger.warning("{} 不存在内部编码".format(secu_code))
            raise
        return ret

    def get_secu_name(self, secu_code):
        """网站显示的名称过长 就使用数据库中查询出来的名称 """
        juyuan = self._init_pool(self.juyuan_cfg)
        if self.type in ("sh", "sz"):
            sql = 'SELECT ChiNameAbbr from SecuMain WHERE SecuCode ="{}" and SecuCategory in (1, 2) and SecuMarket in (83, 90) and ListedSector in (1, 2, 6, 7);'.format(secu_code)
        else:
            sql = '''SELECT ChiNameAbbr from hk_secumain WHERE SecuCode ="{}" and  SecuCategory in (51, 3, 53, 78) and SecuMarket in (72) and ListedSector in (1, 2, 6, 7);'''.format(secu_code)
        ret = juyuan.select_one(sql).get("ChiNameAbbr")
        juyuan.dispose()
        return ret

    def _start(self):
        self._create_table()
        resp = requests.post(self.url, data=self.post_params)
        if resp.status_code == 200:
            body = resp.text
            doc = html.fromstring(body)
            # 查询较早数据可能长时间加载不出来
            date = doc.xpath('//*[@id="pnlResult"]/h2/span/text()')[0]
            date = re.findall(r"持股日期: (\d{4}/\d{2}/\d{2})", date)[0]
            trs = doc.xpath('//*[@id="mutualmarket-result"]/tbody/tr')
            item = {}
            for tr in trs:
                # 股份代码
                secu_code = tr.xpath('./td[1]/div[2]/text()')[0].strip()
                item['SecuCode'] = secu_code
                # 聚源内部编码
                _secu_code = self._trans_secucode(secu_code)
                item['InnerCode'] = self.get_inner_code(_secu_code)
                # 股票简称
                secu_name = tr.xpath('./td[2]/div[2]/text()')[0].strip()
                simple_secu_name = self.converter.convert(secu_name)
                if len(simple_secu_name) > 50:
                    simple_secu_name = self.get_secu_name(_secu_code)
                item['SecuAbbr'] = simple_secu_name

                # 时间 在数据处理的时候进行控制 这里统一用网页上的时间
                item['Date'] = date.replace("/", "-")
                # item['Date'] = self.check_day
                # 判断是否是港交所交易日 在数据处理的时间进行判断
                # item['HKTradeDay'] =

                # 於中央結算系統的持股量
                holding = tr.xpath('./td[3]/div[2]/text()')[0]
                if holding:
                    holding = int(holding.replace(',', ''))
                else:
                    holding = 0
                item['ShareNum'] = holding

                # 占股的百分比
                POAShares = tr.xpath('./td[4]/div[2]/text()')
                if POAShares:
                    POAShares = float(POAShares[0].replace('%', ''))
                else:
                    POAShares = float(0)
                item['Percent'] = POAShares

                spider = self._init_pool(self.spider_cfg)
                update_fields = ['SecuCode', 'InnerCode', 'SecuAbbr', 'Date', 'Percent', 'ShareNum']
                self._save(spider, item, self.spider_table, update_fields)

    def start(self):
        try:
            self._start()
        except:
            traceback.print_exc()

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 数据加工处理分界线 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    def _create_product_table(self):
        sql1 = '''
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
        product = self._init_pool(self.product_cfg)
        product.insert(sql1)
        product.insert(sql2)
        product.dispose()

    def _sync(self):
        # 首先创建正式表
        self._create_product_table()
        # 获取爬虫数据库中最近 7 天的数据
        start_dt = self.today - datetime.timedelta(days=8)
        end_dt = self.today - datetime.timedelta(days=1)
        sql = '''select * from {} where Date >=  '{}'  and Date <= '{}'; '''.format(self.spider_table, start_dt, end_dt)
        spider = self._init_pool(self.spider_cfg)
        datas = spider.select_all(sql)
        print(datas)


        pass


if __name__ == "__main__":
    # 可开多线程 不要求太实时 就顺序执行
    for _type in ("sh", "sz", "hk"):
        h = HoldShares(_type)
        h.start()

    # h = HoldShares("hk")
    # ret = h.inner_code_map
    # h._start()

    # h = HoldShares("sh")
    # h._sync()


