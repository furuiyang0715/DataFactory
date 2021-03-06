import base64
import datetime
import hashlib
import hmac
import json
import logging
import os
import pprint
import re
import sys
import time
import traceback
import urllib.parse

import pandas as pd
import requests
import opencc
import schedule
from lxml import html

sys.path.append("./../")

from hkland_shares.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER, SPIDER_MYSQL_PASSWORD,
                                   SPIDER_MYSQL_DB, PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_PORT, PRODUCT_MYSQL_USER,
                                   PRODUCT_MYSQL_PASSWORD, PRODUCT_MYSQL_DB, JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD,
                                   JUY_DB, DC_HOST, DC_PORT, DC_USER, DC_PASSWD, DC_DB, LOCAL, SECRET, TOKEN)
from hkland_shares.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


SPIDER = int(os.environ.get("SPIDER", 1))
SYNC = int(os.environ.get("SYNC", 1))


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

        # 敏仪的爬虫表名是 hold_shares_sh hold_shares_sz hold_shares_hk
        # 我这边更新后的表名是 hoding_ .. 区别是跟正式表的字段保持一致
        self.spider_table = 'holding_shares_{}'.format(self.type)
        # 生成正式库中的两个 hkland_shares hkland_hkshares
        if self.type in ("sh", "sz"):
            self.table_name = 'hkland_shares'
        elif self.type == "hk":
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
                logger.info("插入新数据 {}".format(to_insert))

            elif count == 2:
                logger.info("刷新数据{}".format(to_insert))

            else:
                logger.info("已有数据 {} ".format(to_insert))

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
            # print("持股日期" in body)
            # print(body)
            doc = html.fromstring(body)
            # 查询较早数据可能长时间加载不出来
            date = doc.xpath('//*[@id="pnlResult"]/h2/span/text()')[0]
            date = re.findall(r"持股日期: (\d{4}/\d{2}/\d{2})", date)[0]
            print(date)
            trs = doc.xpath('//*[@id="mutualmarket-result"]/tbody/tr')

            jishu = []
            update_fields = ['SecuCode', 'InnerCode', 'SecuAbbr', 'Date', 'Percent', 'ShareNum']
            spider = self._init_pool(self.spider_cfg)
            for tr in trs:
                item = {}
                # 股份代码
                secu_code = tr.xpath('./td[1]/div[2]/text()')[0].strip()
                # item['SecuCode'] = secu_code
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

                if self.type == "hk":
                    item['SecuCode'] = _secu_code
                elif self.type in ("sh", "sz"):
                    item['SecuCode'] = self.suffix_process(_secu_code)
                else:
                    raise

                ret = self._save(spider, item, self.spider_table, update_fields)
                if ret == 1:
                    jishu.append(ret)

            try:
                spider.dispose()
            except:
                pass

            if len(jishu) != 0:
                self.ding("当前的时间是{}, 爬虫数据库 {} 更入了 {} 条新数据".format(datetime.datetime.now(), self.spider_table, len(jishu)))
            else:
                print(len(jishu))

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

    def start(self):
        count = 3
        while True:
            try:
                self._start()
            except Exception as e:
                count -= 1
                if count < 0:
                    traceback.print_exc()
                    self.ding("当前时间{}, 爬虫程序{}出错了, 错误原因是{}".format(datetime.datetime.now(), self.spider_table, e))
                    time.sleep(3)   # 4 次重试后出错就一直 ding
                else:
                    print("爬虫程序失败, 重启.")
            else:
                break

    def sync(self):
        count = 3
        while True:
            try:
                self._sync()
            except Exception as e:
                count -= 1
                if count < 0:
                    traceback.print_exc()
                    self.ding("当前时间{}, 同步程序{}出错了, 错误原因是{}".format(datetime.datetime.now(), self.table_name, e))
                    time.sleep(3)   # 出错一直 ding
                else:
                    print("同步程序失败, 重启.")
            else:
                break

    def _sync(self):
        if LOCAL:
            self._create_product_table()

        spider = self._init_pool(self.spider_cfg)

        start_dt = self.today - datetime.timedelta(days=1)
        # FIXME 在每天的凌晨启动 只能重刷前一天的数据
        end_dt = self.today - datetime.timedelta(days=1)

        dt = start_dt
        _map = {}
        while dt <= end_dt:
            sql = '''select max(Date) as before_max_dt from {} where Date <= '{}'; '''.format(self.spider_table, dt)
            _dt = spider.select_one(sql).get("before_max_dt")
            _map[str(dt)] = _dt
            dt += datetime.timedelta(days=1)

        logger.info(_map)

        if self.type == "sh":
            trading_type = 1
        elif self.type == "sz":
            trading_type = 3
        else:
            trading_type = None
        if trading_type:
            dc = self._init_pool(self.dc_cfg)
            shhk_calendar_map = {}
            dt = start_dt
            while dt <= end_dt:
                # 沪港通 或者是 深港通 当前日期之间最邻近的一个交易日
                sql = '''select max(EndDate) as before_max_dt from  hkland_shszhktradingday where EndDate <= '{}' and TradingType={} and IfTradingDay=1;'''.format(dt, trading_type)
                _dt = dc.select_one(sql).get("before_max_dt")
                shhk_calendar_map[str(dt)] = _dt
                dt += datetime.timedelta(days=1)

            # print(pprint.pformat(shhk_calendar_map))
        # print(pprint.pformat(_map))

        product = self._init_pool(self.product_cfg)
        select_fields = ['SecuCode', 'InnerCode', 'SecuAbbr', 'Percent', 'ShareNum', 'UPDATETIMEJZ']
        # FIXME 加入了 CMFTime 即使用的数据源也要计入在内
        update_fields = ['Date', 'SecuCode', 'InnerCode', 'SecuAbbr', 'Percent', 'ShareNum',
                         'CMFTime',
                         ]
        select_str = ",".join(select_fields).rstrip(",")

        jishu = []
        for dt in _map:
            sql = '''select {} from {} where Date = '{}'; '''.format(select_str, self.spider_table, _map.get(dt))
            datas = spider.select_all(sql)
            for data in datas:
                data.update({"Date": dt})
                data.update({"CMFTime": data.get("UPDATETIMEJZ")})
                data.pop("UPDATETIMEJZ")
                if self.type in ("sh", "sz"):
                    data.update({"HKTradeDay": shhk_calendar_map.get(dt)})
                    update_fields.append("HKTradeDay")
                # print(data)
                ret = self._save(product, data, self.table_name, update_fields)
                if ret == 1:
                    jishu.append(ret)

        try:
            product.dispose()
            spider.dispose()
        except:
            pass

        if len(jishu) != 0:
            self.ding("【datacenter】当前的时间是{}, dc 数据库 {} 更入了 {} 条新数据".format(datetime.datetime.now(), self.table_name, len(jishu)))
        else:
            print(len(jishu))


now = lambda: time.time()


def spider_task():
    # 凌晨更新前一天的数据
    t1 = now()
    for _type in (
            "sh",
            "sz",
            "hk",
    ):
        print("{} SPIDER START.".format(_type))
        h = HoldShares(_type)
        h.start()
        print("Time: {} s".format(now() - t1))


def sync_task():
    # 获取最近 4 天的数据进行天填充以及同步
    t1 = now()
    for _type in (
            "sh",
            "sz",
            "hk",
    ):
        print("{} SYNC START.".format(_type))
        h = HoldShares(_type)
        h.sync()
        print("Time: {} s".format(now() - t1))


def main():
    if SPIDER:
        spider_task()
        schedule.every().hour.do(spider_task)
    if SYNC:
        sync_task()
        schedule.every().day.at("01:30").do(sync_task)
        schedule.every().day.at("04:00").do(sync_task)
        schedule.every().day.at("08:00").do(sync_task)

    while True:
        print("当前调度系统中的任务列表是{}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(180)


if __name__ == "__main__":
    main()


'''
爬虫程序和同步程序部署在同一个进程中
爬虫程序每日凌晨 3 点启动 
同步程序每日凌晨 4 点启动 
同步程序拿最近 4 天的数据进行填充

docker build -f Dockerfile_share -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_shares:v1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_shares:v1 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_shares:v1 


# remote 
## spider 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name flow_shares_spider \
--env LOCAL=0 \
--env SYNC=0 \
--env SPIDER=1 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_shares:v1 
## sync 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name flow_shares_sync0428 \
--env LOCAL=0 \
--env SYNC=1 \
--env SPIDER=0 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_shares:v1 


# local 
## spider 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name flow_shares_spider \
--env LOCAL=1 \
--env SYNC=0 \
--env SPIDER=1 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_shares:v1 
## sync 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name flow_shares_sync \
--env LOCAL=1 \
--env SYNC=1 \
--env SPIDER=0 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_shares:v1 



# sql 检查语句 
select * from hkland_shares where InnerCode = 3 order by Date desc limit 5; 

'''