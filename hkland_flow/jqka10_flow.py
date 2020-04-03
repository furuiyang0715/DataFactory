import datetime
import logging
import re

import execjs
import requests

from hkland_flow.configs import DC_HOST, DC_PORT, DC_USER, DC_DB, DC_PASSWD, SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, \
    SPIDER_MYSQL_USER, SPIDER_MYSQL_PASSWORD, SPIDER_MYSQL_DB
from hkland_flow.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SFLgthisdataspiderSpider(object):
    dc_cfg = {
        "host": DC_HOST,
        "port": DC_PORT,
        "user": DC_USER,
        "password": DC_PASSWD,
        "db": DC_DB,
    }

    spider_cfg = {
        "host": SPIDER_MYSQL_HOST,
        "port": SPIDER_MYSQL_PORT,
        "user": SPIDER_MYSQL_USER,
        "password": SPIDER_MYSQL_PASSWORD,
        "db": SPIDER_MYSQL_DB,
    }

    def __init__(self):
        self.headers = {
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            # 'hexin-v': '',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
            'Accept': 'text/html, */*; q=0.01',
            'Referer': 'http://data.10jqka.com.cn/hgt/ggtb/',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
        }
        self.base_url = 'http://data.10jqka.com.cn/hgt/{}/'
        # 1-沪股通 2-港股通（沪）3-深股通，4-港股通（深）5-港股通（沪深）
        self.category_map = {
            'hgtb': ('沪股通', 1),
            'ggtb': ('港股通(沪)', 2),
            'sgtb': ('深股通', 3),
            'ggtbs': ('港股通(深)', 4),
        }
        self.today = datetime.datetime.today().strftime("%Y-%m-%d")

    def _init_pool(self, cfg: dict):
        """
        eg.
        conf = {
                "host": LOCAL_MYSQL_HOST,
                "port": LOCAL_MYSQL_PORT,
                "user": LOCAL_MYSQL_USER,
                "password": LOCAL_MYSQL_PASSWORD,
                "db": LOCAL_MYSQL_DB,
        }
        :param cfg:
        :return:
        """
        pool = PyMysqlPoolBase(**cfg)
        return pool

    @property
    def cookies(self):
        with open('jqka.js', 'r') as f:
            jscont = f.read()
        cont = execjs.compile(jscont)
        cookie_v = cont.call('v')
        cookies = {
            'v': cookie_v,
        }
        return cookies

    def get(self, url):
        resp = requests.get(url, headers=self.headers, cookies=self.cookies)
        if resp.status_code == 200:
            return resp.text

    def _create_table(self):
        sql_s = '''
        CREATE TABLE `lgt_south_money_data_10jqka` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `Date` datetime NOT NULL COMMENT '日期',
          `Flow` decimal(19,4) DEFAULT NULL COMMENT '当日资金流入(亿元）',
          `Balance` decimal(19,4) DEFAULT NULL COMMENT '当日资金余额（亿元）',
          `Category` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '类别(港股通（沪/深））',
          `CategoryCode` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '类别id',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `unique_key` (`Date`,`CategoryCode`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆股通-南向资金-同花顺';
        '''

        sql_n = '''
        CREATE TABLE `lgt_north_money_data_10jqka` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `Date` datetime NOT NULL COMMENT '日期',
          `Flow` decimal(19,4) DEFAULT NULL COMMENT '当日资金流入(亿元）',
          `Balance` decimal(19,4) DEFAULT NULL COMMENT '当日资金余额（亿元）',
          `Category` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '类别(沪股通+深股通）',
          `CategoryCode` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '类别id',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `unique_key` (`Date`,`CategoryCode`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆股通-北向资金-同花顺'; 
        '''

        spider = self._init_pool(self.spider_cfg)
        spider.insert(sql_s)
        spider.insert(sql_n)
        spider.dispose()

    def _check_if_trading_today(self, category):
        """检查下当前方向是否交易"""
        dc = self._init_pool(self.dc_cfg)
        tradingtype = self.category_map.get(category)[1]
        sql = 'select IfTradingDay from hkland_shszhktradingday where TradingType={} and EndDate = "{}";'.format(
            tradingtype, self.today)
        ret = True if dc.select_one(sql).get('IfTradingDay') == 1 else False
        return ret

    def _start(self):
        self._create_table()
        for category in self.category_map:
            is_trading = self._check_if_trading_today(category)
            if not is_trading:
                logger.info("{} 该方向数据今日关闭".format(self.category_map.get(category)[0]))
                continue
            else:
                url = self.base_url.format(category)
                page = self.get(url)
                ret = re.findall(r"var dataDay = (.*);", page)
                if ret:
                    datas = eval(ret[0])[0]
                    for data in datas:
                        item = dict()
                        item['Date'] = self.today + " " + data[0]
                        item['Flow'] = float(data[1])
                        item['Balance'] = float(data[2])
                        item['Category'] = self.category_map.get(category)[0]
                        item['CategoryCode'] = category
                        # print(item)


if __name__ == "__main__":
    sf = SFLgthisdataspiderSpider()
    # print(sf.cookies)
    sf._start()
