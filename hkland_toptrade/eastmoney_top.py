# -*- coding: utf-8 -*-
import datetime
import json
import logging
import re
import requests

import utils
from sql_base import Connection
from hkland_configs import (PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_USER, PRODUCT_MYSQL_PASSWORD,
                            PRODUCT_MYSQL_DB, PRODUCT_MYSQL_PORT, JUY_HOST, JUY_PORT, JUY_DB,
                            JUY_PASSWD, JUY_USER)

logger = logging.getLogger()


class EastMoneyTop10(object):
    """十大成交股 东财数据源 """
    def __init__(self, day: datetime.datetime):
        self.headers = {
            'Referer': 'http://data.eastmoney.com/hsgt/top10.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        }
        self.dt = day
        self.day = day.strftime("%Y-%m-%d")
        self.url = 'http://data.eastmoney.com/hsgt/top10/{}.html'.format(day)
        self.table_name = 'hkland_toptrade'
        self.fields = ['Date', 'SecuCode', 'InnerCode', 'SecuAbbr', 'Close', 'ChangePercent',
                       'TJME', 'TMRJE', 'TCJJE', 'CategoryCode']

        self.product_conn = Connection(
            host=PRODUCT_MYSQL_HOST,
            database=PRODUCT_MYSQL_DB,
            user=PRODUCT_MYSQL_USER,
            password=PRODUCT_MYSQL_PASSWORD,
            port=PRODUCT_MYSQL_PORT,
        )

        self.juyuan_conn = Connection(
            host=JUY_HOST,
            port=JUY_PORT,
            user=JUY_USER,
            password=JUY_PASSWD,
            database=JUY_DB,
        )

    def _get_inner_code_map(self, market_type):
        """https://dd.gildata.com/#/tableShow/27/column///
           https://dd.gildata.com/#/tableShow/718/column///
        """
        if market_type in ("sh", "sz"):
            sql = 'SELECT SecuCode,InnerCode from SecuMain WHERE SecuCategory in (1, 2) and SecuMarket in (83, 90) and ListedSector in (1, 2, 6, 7);'
        else:
            sql = '''SELECT SecuCode,InnerCode from hk_secumain WHERE SecuCategory in (51, 3, 53, 78) and SecuMarket in (72) and ListedSector in (1, 2, 6, 7);'''
        ret = self.juyuan_conn.query(sql)
        info = {}
        for r in ret:
            key = r.get("SecuCode")
            value = r.get('InnerCode')
            info[key] = value
        return info

    def crawl(self):
        resp = requests.get(self.url, headers=self.headers)
        if resp.status_code == 200:
            body = resp.text
            # 沪股通十大成交股
            data1 = re.findall('var DATA1 = (.*);', body)[0]
            # 深股通十大成交股
            data2 = re.findall('var DATA2 = (.*);', body)[0]
            # 港股通(沪)十大成交股
            data3 = re.findall('var DATA3 = (.*);', body)[0]
            # 港股通(深)十大成交股
            data4 = re.findall('var DATA4 = (.*);', body)[0]

            sh_innercode_map = self._get_inner_code_map("sh")
            sz_innercode_map = self._get_inner_code_map("sz")
            hk_innercode_map = self._get_inner_code_map("hk")

            jishu = []
            for data in [data1, data2, data3, data4]:
                data = json.loads(data)
                top_datas = data.get("data")
                logger.debug(top_datas)
                for top_data in top_datas:
                    item = dict()
                    item['Date'] = self.day  # 时间
                    secu_code = top_data.get("Code")
                    item['SecuCode'] = secu_code  # 证券代码
                    item['SecuAbbr'] = top_data.get("Name")  # 证券简称
                    item['Close'] = top_data.get('Close')  # 收盘价
                    item['ChangePercent'] = top_data.get('ChangePercent')  # 涨跌幅
                    item['CMFID'] = 1  # 兼容之前的程序 写死
                    item['CMFTime'] = datetime.datetime.now()  # 兼容和之前的程序 用当前的时间代替
                    # '类别代码:GGh: 港股通(沪), GGs: 港股通(深), HG: 沪股通, SG: 深股通',
                    if top_data['MarketType'] == 1.0:
                        item['CategoryCode'] = 'HG'
                        # item['Category'] = '沪股通'
                        # 净买额
                        item['TJME'] = top_data['HGTJME']
                        # 买入金额
                        item['TMRJE'] = top_data['HGTMRJE']
                        # # 卖出金额
                        # item['TMCJE'] = top_data['HGTMCJE']
                        # 成交金额
                        item['TCJJE'] = top_data['HGTCJJE']
                        item['InnerCode'] = sh_innercode_map.get(secu_code)

                    elif top_data['MarketType'] == 2.0:
                        item['CategoryCode'] = 'GGh'
                        # item['Category'] = '港股通(沪)'
                        # 港股通(沪)净买额(港元）
                        item['TJME'] = top_data['GGTHJME']
                        # 港股通(沪)买入金额(港元）
                        item['TMRJE'] = top_data['GGTHMRJE']
                        # # 港股通(沪)卖出金额(港元）
                        # item['TMCJE'] = top_data['GGTHMCJE']
                        # 港股通(沪)成交金额(港元）
                        item['TCJJE'] = top_data['GGTHCJJE']
                        item['InnerCode'] = hk_innercode_map.get(secu_code)

                    elif top_data['MarketType'] == 3.0:
                        item['CategoryCode'] = 'SG'
                        # item['Category'] = '深股通'
                        # 净买额
                        item['TJME'] = top_data['SGTJME']
                        # 买入金额
                        item['TMRJE'] = top_data['SGTMRJE']
                        # # 卖出金额
                        # item['TMCJE'] = top_data['SGTMCJE']
                        # 成交金额
                        item['TCJJE'] = top_data['SGTCJJE']
                        item['InnerCode'] = sz_innercode_map.get(secu_code)

                    elif top_data['MarketType'] == 4.0:
                        item['CategoryCode'] = 'GGs'
                        # item['Category'] = '港股通(深)'
                        # 港股通(沪)净买额(港元）
                        item['TJME'] = top_data['GGTSJME']
                        # 港股通(沪)买入金额(港元）
                        item['TMRJE'] = top_data['GGTSMRJE']
                        # # 港股通(沪)卖出金额(港元）
                        # item['TMCJE'] = top_data['GGTSMCJE']
                        # 港股通(沪)成交金额(港元）
                        item['TCJJE'] = top_data['GGTSCJJE']
                        item['InnerCode'] = hk_innercode_map.get(secu_code)

                    else:
                        raise
                    ret = self.product_conn.table_insert(self.table_name, item, self.fields)
                    if ret == 1:
                        jishu.append(ret)
            if len(jishu) != 0:
                utils.ding_msg("【datacenter】当前的时间是{}, 数据库 {} 更入了 {} 条新数据".format(
                    datetime.datetime.now(), self.table_name, len(jishu)))
        pass

    def start(self):
        # 检查当前是否是交易日
        is_trading_day = utils.check_iftradingday('n', self.dt) and utils.check_iftradingday('s', self.dt)
        if is_trading_day is False:
            logger.info(f"{self.dt} 南北均不交易")
            return

        self.crawl()

        self.refresh_updatetime()

    def refresh_updatetime(self):
        sql = '''select max(UPDATETIMEJZ) as max_dt from {}; '''.format(self.table_name)
        max_dt = self.product_conn.get(sql).get("max_dt")
        logger.info(f"{self.table_name} 最新的更新时间是{max_dt}")
        self.product_conn.table_update('base_table_updatetime', {'LastUpdateTime': max_dt}, 'TableName', self.table_name)
