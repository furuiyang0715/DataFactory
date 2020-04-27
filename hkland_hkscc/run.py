'''
CREATE TABLE `hkland_hkscc` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `EndDate` datetime NOT NULL COMMENT '港交所披露易原始数据',
  `InfoPublDate` datetime NOT NULL COMMENT 'EndDate的后一自然日',
  `InnerCode` int(11) NOT NULL COMMENT '聚源内部码',
  `SecuCode` varchar(10) COLLATE utf8_bin NOT NULL COMMENT '港交所披露易原始数据，5位非标准化代码',
  `ExchangeCode` int(5) NOT NULL COMMENT '交易所代码(上海83,深圳90)',
  `SecuAbbr` varchar(50) COLLATE utf8_bin NOT NULL COMMENT '股票简称',
  `SharesHolding` decimal(19,2) NOT NULL COMMENT '于中央结算系统的持股量',
  `HoldRatio` decimal(9,4) NOT NULL COMMENT '占已发行股份的百分比（%）',
  `UpdateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '实际入库时间戳',
  `CMFTime` datetime NOT NULL COMMENT '来源时间',
  `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
  `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unified_key1` (`InnerCode`,`EndDate`) USING BTREE,
  KEY `key1` (`InfoPublDate`) USING BTREE,
  KEY `EndDate` (`EndDate`) USING BTREE,
  KEY `InnerCode` (`InnerCode`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='香港中央结算有限公司-陆股通持股统计';


mysql> select * from holding_shares_sh where Date = '2020-04-23' order by InnerCode limit 10;
+--------+-------------+-----------+--------------+---------------------+---------+-----------+---------------------+---------------------+
| id     | SecuCode    | InnerCode | SecuAbbr     | Date                | Percent | ShareNum  | CREATETIMEJZ        | UPDATETIMEJZ        |
+--------+-------------+-----------+--------------+---------------------+---------+-----------+---------------------+---------------------+
| 333371 | 600000.XSHG |      1120 | 浦发银行     | 2020-04-23 00:00:00 |  1.4900 | 421063402 | 2020-04-24 00:35:40 | 2020-04-24 00:35:40 |
| 333373 | 600004.XSHG |      1125 | 白云机场     | 2020-04-23 00:00:00 |  8.1700 | 169120048 | 2020-04-24 00:35:40 | 2020-04-24 00:35:40 |
| 333375 | 600006.XSHG |      1128 | 东风汽车     | 2020-04-23 00:00:00 |  0.0200 |    545738 | 2020-04-24 00:35:40 | 2020-04-24 00:35:40 |
| 333377 | 600007.XSHG |      1129 | 中国国贸     | 2020-04-23 00:00:00 |  2.6000 |  26208412 | 2020-04-24 00:35:40 | 2020-04-24 00:35:40 |
| 333379 | 600008.XSHG |      1130 | 首创股份     | 2020-04-23 00:00:00 |  1.0900 |  62233337 | 2020-04-24 00:35:40 | 2020-04-24 00:35:40 |
| 333381 | 600009.XSHG |      1131 | 上海机场     | 2020-04-23 00:00:00 | 21.4900 | 235032755 | 2020-04-24 00:35:40 | 2020-04-24 00:35:40 |
| 333383 | 600010.XSHG |      1133 | 包钢股份     | 2020-04-23 00:00:00 |  1.7300 | 549705391 | 2020-04-24 00:35:40 | 2020-04-24 00:35:40 |
| 333385 | 600011.XSHG |      1134 | 华能国际     | 2020-04-23 00:00:00 |  0.8900 |  98777659 | 2020-04-24 00:35:40 | 2020-04-24 00:35:40 |
| 333387 | 600012.XSHG |      1136 | 皖通高速     | 2020-04-23 00:00:00 |  1.2500 |  14684048 | 2020-04-24 00:35:40 | 2020-04-24 00:35:40 |
| 333389 | 600015.XSHG |      1139 | 华夏银行     | 2020-04-23 00:00:00 |  1.7300 | 223035013 | 2020-04-24 00:35:40 | 2020-04-24 00:35:40 |
+--------+-------------+-----------+--------------+---------------------+---------+-----------+---------------------+---------------------+
10 rows in set (0.01 sec)

mysql> select * from hkland_hkscc where ExchangeCode = 83 and EndDate = ' 2020-04-23' order by InnerCode limit 10 ;
+----------+---------------------+---------------------+-----------+----------+--------------+--------------+---------------+-----------+---------------------+---------------------+---------------------+---------------------+
| id       | EndDate             | InfoPublDate        | InnerCode | SecuCode | ExchangeCode | SecuAbbr     | SharesHolding | HoldRatio | UpdateTime          | CMFTime             | CREATETIMEJZ        | UPDATETIMEJZ        |
+----------+---------------------+---------------------+-----------+----------+--------------+--------------+---------------+-----------+---------------------+---------------------+---------------------+---------------------+
| 11045098 | 2020-04-23 00:00:00 | 2020-04-24 00:00:00 |      1120 | 90000    |           83 | 浦发银行     |  421063402.00 |    1.4900 | 2020-04-24 01:20:30 | 2020-04-24 01:01:17 | 2020-04-24 01:20:30 | 2020-04-24 01:20:30 |
| 11045097 | 2020-04-23 00:00:00 | 2020-04-24 00:00:00 |      1125 | 90004    |           83 | 白云机场     |  169120048.00 |    8.1700 | 2020-04-24 01:20:30 | 2020-04-24 01:01:18 | 2020-04-24 01:20:30 | 2020-04-24 01:20:30 |
| 11045096 | 2020-04-23 00:00:00 | 2020-04-24 00:00:00 |      1128 | 90006    |           83 | 东风汽车     |     545738.00 |    0.0200 | 2020-04-24 01:20:30 | 2020-04-24 01:01:18 | 2020-04-24 01:20:30 | 2020-04-24 01:20:30 |
| 11045095 | 2020-04-23 00:00:00 | 2020-04-24 00:00:00 |      1129 | 90007    |           83 | 中国国贸     |   26208412.00 |    2.6000 | 2020-04-24 01:20:30 | 2020-04-24 01:01:19 | 2020-04-24 01:20:30 | 2020-04-24 01:20:30 |
| 11045094 | 2020-04-23 00:00:00 | 2020-04-24 00:00:00 |      1130 | 90008    |           83 | 首创股份     |   62233337.00 |    1.0900 | 2020-04-24 01:20:30 | 2020-04-24 01:01:20 | 2020-04-24 01:20:30 | 2020-04-24 01:20:30 |
| 11045093 | 2020-04-23 00:00:00 | 2020-04-24 00:00:00 |      1131 | 90009    |           83 | 上海机场     |  235032755.00 |   21.4900 | 2020-04-24 01:20:30 | 2020-04-24 01:01:21 | 2020-04-24 01:20:30 | 2020-04-24 01:20:30 |
| 11045092 | 2020-04-23 00:00:00 | 2020-04-24 00:00:00 |      1133 | 90010    |           83 | 包钢股份     |  549705391.00 |    1.7300 | 2020-04-24 01:20:30 | 2020-04-24 01:01:22 | 2020-04-24 01:20:30 | 2020-04-24 01:20:30 |
| 11045091 | 2020-04-23 00:00:00 | 2020-04-24 00:00:00 |      1134 | 90011    |           83 | 华能国际     |   98777659.00 |    0.8900 | 2020-04-24 01:20:30 | 2020-04-24 01:01:23 | 2020-04-24 01:20:30 | 2020-04-24 01:20:30 |
| 11045090 | 2020-04-23 00:00:00 | 2020-04-24 00:00:00 |      1136 | 90012    |           83 | 皖通高速     |   14684048.00 |    1.2500 | 2020-04-24 01:20:30 | 2020-04-24 01:01:23 | 2020-04-24 01:20:30 | 2020-04-24 01:20:30 |
| 11045089 | 2020-04-23 00:00:00 | 2020-04-24 00:00:00 |      1139 | 90015    |           83 | 华夏银行     |  223035013.00 |    1.7300 | 2020-04-24 01:20:30 | 2020-04-24 01:01:24 | 2020-04-24 01:20:30 | 2020-04-24 01:20:30 |
+----------+---------------------+---------------------+-----------+----------+--------------+--------------+---------------+-----------+---------------------+---------------------+---------------------+---------------------+
10 rows in set (0.39 sec)
'''
import base64
import datetime
import hashlib
import hmac
import json
import logging
import re
import sys
import time
import traceback

import opencc
import requests
import urllib.parse
from lxml import html

sys.path.append("./../")
from hkland_hkscc.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER,
                                  SPIDER_MYSQL_PASSWORD, SPIDER_MYSQL_DB, PRODUCT_MYSQL_HOST,
                                  PRODUCT_MYSQL_PORT, PRODUCT_MYSQL_USER, PRODUCT_MYSQL_PASSWORD,
                                  PRODUCT_MYSQL_DB, JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD,
                                  JUY_DB, DC_HOST, DC_PORT, DC_USER, DC_PASSWD, DC_DB,
                                  SECRET, TOKEN)
from hkland_hkscc.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HklandHKSCC(object):
    """滬股通及深股通持股紀錄按日查詢"""
    spider_cfg = {
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

    # 数据中心库
    dc_cfg = {
        "host": DC_HOST,
        "port": DC_PORT,
        "user": DC_USER,
        "password": DC_PASSWD,
        "db": DC_DB,
    }

    def __init__(self, type, offset=1):
        self.type = type
        self.url = 'https://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t={}'.format(type)
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
        self.table_name = 'hkland_hkscc'
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
        sql = '''
        CREATE TABLE IF NOT EXISTS `hkland_hkscc` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `EndDate` datetime NOT NULL COMMENT '港交所披露易原始数据',
          `InfoPublDate` datetime NOT NULL COMMENT 'EndDate的后一自然日',
          `InnerCode` int(11) NOT NULL COMMENT '聚源内部码',
          `SecuCode` varchar(10) COLLATE utf8_bin NOT NULL COMMENT '港交所披露易原始数据，5位非标准化代码',
          `ExchangeCode` int(5) NOT NULL COMMENT '交易所代码(上海83,深圳90)',
          `SecuAbbr` varchar(50) COLLATE utf8_bin NOT NULL COMMENT '股票简称',
          `SharesHolding` decimal(19,2) NOT NULL COMMENT '于中央结算系统的持股量',
          `HoldRatio` decimal(9,4) NOT NULL COMMENT '占已发行股份的百分比（%）',
          `UpdateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '实际入库时间戳',
          `CMFTime` datetime NOT NULL COMMENT '来源时间',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `unified_key1` (`InnerCode`,`EndDate`) USING BTREE,
          KEY `key1` (`InfoPublDate`) USING BTREE,
          KEY `EndDate` (`EndDate`) USING BTREE,
          KEY `InnerCode` (`InnerCode`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='香港中央结算有限公司-陆股通持股统计';
        '''.format(self.table_name)
        # TODO
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

    def suffix_process(self, code):
        """对代码进行加减后缀"""
        if len(code) == 6:
            if code[0] == '6':
                return code+'.XSHG'
            else:
                return code+'.XSHE'
        else:
            raise

    def ding(self, msg):
        def get_url():
            timestamp = str(round(time.time() * 1000))
            secret_enc = SECRET.encode('utf-8')
            string_to_sign = '{}\n{}'.format(timestamp, SECRET)
            string_to_sign_enc = string_to_sign.encode('utf-8')
            hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
            url = 'https://oapi.dingtalk.com/robot/send?access_token={}&timestamp={}&sign={}'.format(
                TOKEN, timestamp, sign)
            return url

        url = get_url()
        header = {
            "Content-Type": "application/json",
            "Charset": "UTF-8"
        }
        message = {
            "msgtype": "text",
            "text": {
                "content": "{}@15626046299".format(msg)
            },
            "at": {
                "atMobiles": [
                    "15626046299",
                ],
                "isAtAll": False
            }
        }
        message_json = json.dumps(message)
        resp = requests.post(url=url, data=message_json, headers=header)
        if resp.status_code == 200:
            pass
        else:
            logger.warning("钉钉消息发送失败")

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
            date = doc.xpath('//*[@id="pnlResult"]/h2/span/text()')[0]
            date = re.findall(r"持股日期: (\d{4}/\d{2}/\d{2})", date)[0]
            trs = doc.xpath('//*[@id="mutualmarket-result"]/tbody/tr')
            jishu = []
            update_fields = ['ExchangeCode', 'EndDate', 'InfoPublDate',
                             "SecuCode", 'InnerCode', 'SecuAbbr',
                             'SharesHolding', 'HoldRatio']
            spider = self._init_pool(self.spider_cfg)
            for tr in trs:
                item = dict()
                item['ExchangeCode'] = self.market
                # 网页上的时间
                item['EndDate'] = date.replace("/", "-")
                # 数据在网站上的发布时间 TODO
                item['InfoPublDate'] = self.today
                # 股份代码
                secu_code = tr.xpath('./td[1]/div[2]/text()')[0].strip()
                item["SecuCode"] = secu_code
                # 聚源内部编码
                _secu_code = self._trans_secucode(secu_code)
                item['InnerCode'] = self.get_inner_code(_secu_code)
                # 股票简称
                secu_name = tr.xpath('./td[2]/div[2]/text()')[0].strip()
                simple_secu_name = self.converter.convert(secu_name)
                if len(simple_secu_name) > 50:
                    simple_secu_name = self.get_secu_name(_secu_code)
                item['SecuAbbr'] = simple_secu_name
                # 於中央結算系統的持股量
                holding = tr.xpath('./td[3]/div[2]/text()')[0]
                if holding:
                    holding = int(holding.replace(',', ''))
                else:
                    holding = 0
                item['SharesHolding'] = holding

                # 占股的百分比
                POAShares = tr.xpath('./td[4]/div[2]/text()')
                if POAShares:
                    POAShares = float(POAShares[0].replace('%', ''))
                else:
                    POAShares = float(0)
                item['HoldRatio'] = POAShares

                item['UpdateTime'] = datetime.datetime.now()
                item['CMFTime'] = datetime.datetime.now()
                self._save(spider, item, self.table_name, update_fields)
            try:
                spider.dispose()
            except:
                pass


if __name__ == "__main__":
    hkscc = HklandHKSCC("sh")
    hkscc._start()
