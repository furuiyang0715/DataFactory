# -*- coding: utf-8 -*-
import base64
import datetime
import hashlib
import hmac
import json
import logging
import os
import pprint
import re
import time
import traceback
import urllib.parse

import requests
import sys
from apscheduler.schedulers.blocking import BlockingScheduler


sys.path.append("./../")
from hkland_historytradestat.configs import (PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_USER, PRODUCT_MYSQL_PASSWORD,
                                             PRODUCT_MYSQL_DB, DC_HOST, DC_PORT, DC_USER, DC_PASSWD,
                                             DC_DB, SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT,
                                             SPIDER_MYSQL_USER, SPIDER_MYSQL_PASSWORD, SPIDER_MYSQL_DB,
                                             PRODUCT_MYSQL_PORT, SECRET, TOKEN)
from hkland_historytradestat.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SSEStatsOnTime(object):
    """
    http://www.sse.com.cn/services/hkexsc/home/
    港股通(沪)
    """
    def __init__(self):
        self.url = 'http://yunhq.sse.com.cn:32041//v1/hkp/status/amount_status'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
        }

    def get_balance_info(self):
        resp = requests.get(self.url)
        if resp.status_code == 200:
            datas = json.loads(resp.text)
            item = dict()
            # 交易所所属类型
            item['Category'] = "SH"

            # 当前的时间
            m_today = str(datas['date'])
            m_today = "-".join([m_today[:4], m_today[4:6], m_today[6:8]])
            m_time = str(datas['status'][0][1])
            # 区分小时时间是 2 位数和 1 位数的 即 9 点以及之前的数据 10 点以及之后的数据
            if len(m_time) >= 9:    # {'date': 20200417, 'status': [[100547000, 100547000], [417, 418], ['3       ', '111     '], 42000000000, 41207590461, '2']}
                m_time = ":".join([m_time[:2], m_time[2:4], m_time[4:6]])
            else: # {'date': 20200417, 'status': [[94338000, 94337000], [417, 418], ['3       ', '111     '], 42000000000, 41543482907, '2']}
                m_time = ":".join([m_time[:1], m_time[1:3], m_time[3:5]])
            _time = " ".join([m_today, m_time])
            item['Time'] = _time

            # 当日额度
            item['DailyLimit'] = datas['status'][3]
            # 当日资金余额
            item['Balance'] = datas['status'][4]
            # print(item)
            return item


class SZSEStatsOnTime(object):
    """
    http://www.szse.cn/szhk/index.html
    港股通(深)
    """
    def __init__(self):
        self.url = 'http://www.szse.cn/api/market/sgt/dailyamount'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
        }

    def get_balance_info(self):
        resp = requests.get(self.url)
        if resp.status_code == 200:
            datas = json.loads(resp.text)
            # print(datas)
            item = dict()
            # 交易所所属类型
            item['Category'] = "SZ"
            # 当前的时间
            item['Time'] = datas.get("gxsj")
            # 当日资金余额
            item['Balance'] = int(datas['edye']) * pow(10, 6)
            # 每日额度
            item['DailyLimit'] = int(datas['mred']) * pow(10, 8)
            # print(item)
            return item


class HistoryCalSpider(object):
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

    product_cfg = {
        "host": PRODUCT_MYSQL_HOST,
        "port": PRODUCT_MYSQL_PORT,
        "user": PRODUCT_MYSQL_USER,
        "password": PRODUCT_MYSQL_PASSWORD,
        "db": PRODUCT_MYSQL_DB,
    }

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36',
        }
        self.today = datetime.datetime.today()
        # 1 沪股通
        # 2 港股通（沪）
        # 3 深股通
        # 4 港股通（深）
        self.hk_sh_his = self.select_last_total(1).get("MoneyInHistoryTotal")
        self.hk_sz_his = self.select_last_total(3).get("MoneyInHistoryTotal")
        self.sh_hk_his = self.select_last_total(2).get("MoneyInHistoryTotal")
        self.sz_hk_his = self.select_last_total(4).get("MoneyInHistoryTotal")
        self.table_name = 'hkland_calhistory'
        self.fields = [
            'Date',
            'MoneyIn',
            'MoneyBalance',
            'MoneyInHistoryTotal',
            'NetBuyAmount',
            'BuyAmount',
            'SellAmount',
            'MarketTypeCode',
            'MarketType',
        ]

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

    @staticmethod
    def re_data(data):
        """人民币的单位是百万 """
        ret = re.findall("RMB(.*) Mil", data)  # RMB52,000 Mil
        if ret:
            data = ret[0]
            data = data.replace(",", '')
            return int(data)

    @staticmethod
    def re_hk_data(data):
        """港元的单位是百万"""
        ret = re.findall(r"HK\$(.*) Mil", data)
        if ret:
            data = ret[0]
            data = data.replace(",", "")
            return int(data)

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

    def _save(self, to_insert, table, update_fields: list):
        spider = self._init_pool(self.spider_cfg)
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            count = spider.insert(insert_sql, values)
        except:
            traceback.print_exc()
            logger.warning("失败")
            count = None
        else:
            if count:
                logger.info("更入新数据 {}".format(to_insert))
        finally:
            spider.dispose()
        return count

    def select_last_total(self, market_type):
        """查找距给出时间最近的一个时间点的累计值"""
        dc = self._init_pool(self.dc_cfg)
        sql = '''select Date, MoneyInHistoryTotal from hkland_historytradestat where Date = (select max(Date) \
        from hkland_historytradestat where MarketTypeCode = {}) and MarketTypeCode = {};'''.format(market_type, market_type)
        # print(">>> ", sql)
        ret = dc.select_one(sql)
        return ret

    def sh_sz(self):
        """港股通 深"""
        url2 = 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_SBSZ_Turnover_chi.js'   # '港股通（深）成交额'

        body2 = requests.get(url2, headers=self.headers).text
        datas2 = json.loads(body2.rstrip(";").lstrip("southbound11 =").lstrip("southbound12 =").lstrip("southbound21 =").lstrip("southbound22 ="))
        logger.info("\n" + pprint.pformat(datas2))
        '''
        [{'category': 'Southbound',
          'section': [{'item': [['买入及卖出', 'HK$5,814 Mil', {}],
                                ['买入', 'HK$3,304 Mil', {}],
                                ['卖出', 'HK$2,510 Mil', {}]],
                       'subtitle': ['成交额', '27/04/2020 (14:51)', {}]}],
          'tablehead': ['港股通（深）', '']}]
        '''
        buy_sell_info = datas2[0].get("section")[0].get("item")
        buy_amount = self.re_hk_data(buy_sell_info[1][1])
        sell_amount = self.re_hk_data(buy_sell_info[2][1])
        netbuyamount = buy_amount - sell_amount
        logger.info("当前分钟的买入金额是 {} 百万港元, 卖出金额是 {} 百万港元".format(buy_amount, sell_amount))
        logger.info("当前分钟的净流入是 {} 百万港元".format(netbuyamount))

        # 当前分钟的历史资金累计流入(百万) = 上一天的历史资金累计流入(百万) + 当前分钟的当日成交净买额(百万元)
        moneyinhistorytotal = self.sz_hk_his + netbuyamount
        logger.info("当前分钟的历史资金累计流入是{}百万".format(moneyinhistorytotal))

        date_min = datas2[0].get("section")[0].get("subtitle")[1]
        date_min = datetime.datetime.strptime(date_min, "%d/%m/%Y (%H:%M)")
        logger.info("解析出的当前的分钟时间是{}".format(date_min))

        sz = SZSEStatsOnTime()
        ret = sz.get_balance_info()
        logger.info("从深交所接口获取的港股通(深)数据是{}".format(ret))
        money_limit = ret.get("DailyLimit")
        money_balance = ret.get("Balance")
        money_in = money_limit - money_balance

        # 港元到百万港元的转换
        money_balance = int(money_balance / 100 / 10000)
        money_in = int(money_in / 100 / 10000)

        item = {
            "Date": date_min,  # 具体到分钟的交易时间
            "MoneyBalance": money_balance,  # 当日余额(百万）
            "MoneyIn": money_in,  # 当日资金流入(百万) = 额度 - 余额
            "BuyAmount": buy_amount,  # 当日买入成交额(百万元)
            "SellAmount": sell_amount,  # 当日卖出成交额(百万元)
            "NetBuyAmount": netbuyamount,  # 当日成交净买额(百万元)  = (当日)买入成交额(百万元) - (当日)卖出成交额(百万元)
            "MoneyInHistoryTotal": moneyinhistorytotal,  # 历史资金累计流入(百万) = 上一天的历史资金累计流入(百万) + 今天的当日成交净买额(百万元)
            "MarketTypeCode": 4,  # 市场类型代码
            "MarketType": '港股通(深)',  # 市场类型
        }

        logger.info("生成一条港股通(深)数据: {}".format(item))
        self._save(item, self.table_name, self.fields)
        return item

    def sh_hk(self):
        """港股通 沪"""
        url2 = 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_SBSH_Turnover_chi.js'   # '港股通（沪）成交额'

        body2 = requests.get(url2, headers=self.headers).text
        datas2 = json.loads(body2.rstrip(";").lstrip("southbound11 =").lstrip("southbound12 =").lstrip("southbound21 =").lstrip("southbound22 ="))
        logger.info("\n" + pprint.pformat(datas2))
        '''
        [{'category': 'Southbound',
          'section': [{'item': [['买入及卖出', 'HK$5,668 Mil', {}],
                                ['买入', 'HK$2,868 Mil', {}],
                                ['卖出', 'HK$2,799 Mil', {}]],
                       'subtitle': ['成交额', '27/04/2020 (14:30)', {}]}],
          'tablehead': ['港股通（沪）', '']}]
        '''
        buy_sell_info = datas2[0].get("section")[0].get("item")
        buy_amount = self.re_hk_data(buy_sell_info[1][1])
        sell_amount = self.re_hk_data(buy_sell_info[2][1])
        netbuyamount = buy_amount - sell_amount
        logger.info("当前分钟的买入金额是 {} 百万港元, 卖出金额是 {} 百万港元".format(buy_amount, sell_amount))
        logger.info("当前分钟的净流入是 {} 百万港元".format(netbuyamount))

        # 当前分钟的历史资金累计流入(百万) = 上一天的历史资金累计流入(百万) + 当前分钟的当日成交净买额(百万元)
        moneyinhistorytotal = self.sh_hk_his + netbuyamount
        logger.info("当前分钟的历史资金累计流入是{}百万".format(moneyinhistorytotal))

        date_min = datas2[0].get("section")[0].get("subtitle")[1]
        date_min = datetime.datetime.strptime(date_min, "%d/%m/%Y (%H:%M)")
        logger.info("解析出的当前的分钟时间是{}".format(date_min))

        sse = SSEStatsOnTime()
        ret = sse.get_balance_info()
        logger.info("上交所的港股通(沪)接口数据是{}".format(ret))
        money_limit = ret.get("DailyLimit")
        money_balance = ret.get("Balance")
        money_in = money_limit - money_balance

        money_balance = int(money_balance / 100 / 10000)
        money_in = int(money_in / 100 / 10000)

        item = {
            "Date": date_min,  # 具体到分钟的交易时间
            "MoneyBalance": money_balance,  # 当日余额(百万）
            "MoneyIn": money_in,  # 当日资金流入(百万) = 额度 - 余额
            "BuyAmount": buy_amount,  # 当日买入成交额(百万元)
            "SellAmount": sell_amount,  # 当日卖出成交额(百万元)
            "NetBuyAmount": netbuyamount,  # 当日成交净买额(百万元)  = (当日)买入成交额(百万元) - (当日)卖出成交额(百万元)
            "MoneyInHistoryTotal": moneyinhistorytotal,  # 历史资金累计流入(百万) = 上一天的历史资金累计流入(百万) + 今天的当日成交净买额(百万元)
            "MarketTypeCode": 2,  # 市场类型代码
            "MarketType": '港股通(沪)',  # 市场类型
        }

        logger.info("生成一条港股通(沪)数据: {}".format(item))
        self._save(item, self.table_name, self.fields)
        return item

    def hk_sz(self):
        """深股通"""
        url = 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSZ_QuotaUsage_chi.js'  # 深股通每日资金余额
        url2 = 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSZ_Turnover_chi.js'   # 深股通成交额

        body = requests.get(url, headers=self.headers).text
        datas = json.loads(body.rstrip(";").lstrip("northbound11 =").lstrip("northbound12 =").lstrip("northbound21 =").lstrip("northbound22 ="))
        logger.info("\n"+pprint.pformat(datas))
        '''
        [{'category': 'Northbound',
          'section': [{'item': [['额度', 'RMB52,000 Mil', {}],
                                ['余额 (于 11:13)', 'RMB50,693 Mil', {}],
                                ['余额占额度百分比', '97%', {}]],
                       'subtitle': ['每日额度', '27/04/2020', {}]}],
          'tablehead': ['香港 > 深圳'],
          'type': 'noHeader'}]
        '''
        # 当前日期
        show_dt = datas[0].get("section")[0].get("subtitle")[1]
        show_dt = datetime.datetime.strptime(show_dt, "%d/%m/%Y").strftime("%Y-%m-%d")
        logger.info("当前的日期是{}".format(show_dt))

        flow_info = datas[0].get("section")[0].get("item")

        # 当前分钟时间
        m_dt = flow_info[1][0]
        m_dt = re.findall("余额 \(于 (.*)\)", m_dt)[0]
        complete_dt = " ".join([show_dt, m_dt])
        complete_dt = datetime.datetime.strptime(complete_dt, "%Y-%m-%d %H:%M")
        logger.info("当前的分钟时间是{}".format(complete_dt))

        # 当前分钟余额 单位: 百万
        money_balance = self.re_data(flow_info[1][1])
        logger.info("当前分钟的余额是{}百万".format(money_balance))

        # 当日资金额度 单位: 百万
        money_limit = self.re_data(flow_info[0][1])
        logger.info("当日资金的额度是{}百万".format(money_limit))

        # 当前分钟资金流入 = 额度 - 余额 单位: 百万
        money_in = money_limit - money_balance
        logger.info("当前分钟的资金流入是{}百万".format(money_in))

        body2 = requests.get(url2, headers=self.headers).text
        datas2 = json.loads(
            body2.rstrip(";").lstrip("northbound11 =").lstrip("northbound12 =").lstrip("northbound21 =").lstrip(
                "northbound22 ="))
        logger.info("\n"+pprint.pformat(datas2))
        '''
        [{'category': 'Northbound',
          'section': [{'item': [['买入及卖出', 'RMB19,462 Mil', {}],
                                ['买入', 'RMB10,347 Mil', {}],
                                ['卖出', 'RMB9,115 Mil', {}]],
                       'subtitle': ['成交额', '27/04/2020 (11:14)', {}]}],
          'tablehead': ['深股通']}]
        '''
        buy_sell_info = datas2[0].get("section")[0].get("item")
        buy_amount = self.re_data(buy_sell_info[1][1])
        sell_amount = self.re_data(buy_sell_info[2][1])
        netbuyamount = buy_amount - sell_amount
        logger.info("当前分钟的买入金额是 {} 百万, 卖出金额是 {} 百万".format(buy_amount, sell_amount))
        logger.info("当前分钟的净流入是 {} 百万".format(netbuyamount))

        # 当前分钟的历史资金累计流入(百万) = 上一天的历史资金累计流入(百万) + 当前分钟的当日成交净买额(百万元)
        moneyinhistorytotal = self.hk_sz_his + netbuyamount
        logger.info("当前分钟的历史资金累计流入是{}百万".format(moneyinhistorytotal))

        item = {
            "Date": complete_dt,   # 具体到分钟的交易时间
            "MoneyBalance": money_balance,  # 当日余额(百万）
            "MoneyIn": money_in,  # 当日资金流入(百万) = 额度 - 余额
            "BuyAmount": buy_amount,  # 当日买入成交额(百万元)
            "SellAmount": sell_amount,  # 当日卖出成交额(百万元)
            "NetBuyAmount": netbuyamount,  # 当日成交净买额(百万元)  = (当日)买入成交额(百万元) - (当日)卖出成交额(百万元)
            "MoneyInHistoryTotal": moneyinhistorytotal,  # 历史资金累计流入(百万) = 上一天的历史资金累计流入(百万) + 今天的当日成交净买额(百万元)
            "MarketTypeCode": 3,  # 市场类型代码
            "MarketType": '深股通',  # 市场类型
        }

        logger.info("生成一条深股通数据: {}".format(item))
        self._save(item, self.table_name, self.fields)
        return item

    def hk_sh(self):
        """沪股通"""
        url = 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSH_QuotaUsage_chi.js'  # 沪股通每日资金余额
        url2 = 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSH_Turnover_chi.js'  # 沪股通成交额

        body = requests.get(url, headers=self.headers).text
        datas = json.loads(body.rstrip(";").lstrip("northbound11 =").lstrip("northbound12 =").lstrip("northbound21 =").lstrip("northbound22 ="))
        logger.info("\n"+pprint.pformat(datas))
        '''
        [{'category': 'Northbound',
          'section': [{'item': [['额度', 'RMB52,000 Mil', {}],
                                ['余额 (于 10:15)', 'RMB50,227 Mil', {}],
                                ['余额占额度百分比', '96%', {}]],
                       'subtitle': ['每日额度', '27/04/2020', {}]}],
          'tablehead': ['香港 > 上海'],
          'type': 'noHeader'}]
        '''
        # 当前日期
        show_dt = datas[0].get("section")[0].get("subtitle")[1]
        show_dt = datetime.datetime.strptime(show_dt, "%d/%m/%Y").strftime("%Y-%m-%d")
        logger.info("当前的日期是{}".format(show_dt))

        flow_info = datas[0].get("section")[0].get("item")

        # 当前分钟时间
        m_dt = flow_info[1][0]
        m_dt = re.findall("余额 \(于 (.*)\)", m_dt)[0]
        complete_dt = " ".join([show_dt, m_dt])
        complete_dt = datetime.datetime.strptime(complete_dt, "%Y-%m-%d %H:%M")
        logger.info("当前的分钟时间是{}".format(complete_dt))

        # 当前分钟余额 单位: 百万
        money_balance = self.re_data(flow_info[1][1])
        logger.info("当前分钟的余额是{}百万".format(money_balance))

        # 当日资金额度 单位: 百万
        money_limit = self.re_data(flow_info[0][1])
        logger.info("当日资金的额度是{}百万".format(money_limit))

        # 当前分钟资金流入 = 额度 - 余额 单位: 百万
        money_in = money_limit - money_balance
        logger.info("当前分钟的资金流入是{}百万".format(money_in))

        body2 = requests.get(url2, headers=self.headers).text
        datas2 = json.loads(body2.rstrip(";").lstrip("northbound11 =").lstrip("northbound12 =").lstrip("northbound21 =").lstrip("northbound22 ="))
        logger.info("\n"+pprint.pformat(datas2))
        '''
        [{'category': 'Northbound',
          'section': [{'item': [['买入及卖出', 'RMB14,239 Mil', {}],
                                ['买入', 'RMB8,022 Mil', {}],
                                ['卖出', 'RMB6,217 Mil', {}]],
                       'subtitle': ['成交额', '27/04/2020 (10:51)', {}]}],
          'tablehead': ['沪股通']}]
        '''
        buy_sell_info = datas2[0].get("section")[0].get("item")
        buy_amount = self.re_data(buy_sell_info[1][1])
        sell_amount = self.re_data(buy_sell_info[2][1])
        netbuyamount = buy_amount - sell_amount
        logger.info("当前分钟的买入金额是 {} 百万, 卖出金额是 {} 百万".format(buy_amount, sell_amount))
        logger.info("当前分钟的净流入是 {} 百万".format(netbuyamount))

        # 当前分钟的历史资金累计流入(百万) = 上一天的历史资金累计流入(百万) + 当前分钟的当日成交净买额(百万元)
        moneyinhistorytotal = self.hk_sh_his + netbuyamount
        logger.info("当前分钟的历史资金累计流入是{}百万".format(moneyinhistorytotal))

        item = {
            "Date": complete_dt,    # 陆股通分钟时间点
            "MoneyBalance": money_balance,   # 当日余额(百万）
            "MoneyIn": money_in,   # 当日资金流入(百万) = 额度 - 余额
            "BuyAmount": buy_amount,   # 当日买入成交额(百万元)
            "SellAmount": sell_amount,   # 当日卖出成交额(百万元)
            "NetBuyAmount": netbuyamount,  # 当日成交净买额(百万元)  = (当日)买入成交额(百万元) - (当日)卖出成交额(百万元)
            "MoneyInHistoryTotal": moneyinhistorytotal,   # 历史资金累计流入(百万) = 上一天的历史资金累计流入(百万) + 今天的当日成交净买额(百万元)
            "MarketTypeCode": 1,   # 市场类型代码
            "MarketType": "沪股通",    # 市场类型
        }

        logger.info("生成一条沪股通数据: {}".format(item))
        self._save(item, self.table_name, self.fields)
        return item

    def _check_if_trading_today(self, category):
        '''
        self.category_map = {
            'hgtb': ('沪股通', 1),
            'ggtb': ('港股通(沪)', 2),
            'sgtb': ('深股通', 3),
            'ggtbs': ('港股通(深)', 4),
        }
        一般来说 1 3 与 2 4 是一致的
        '''
        dc = self._init_pool(self.dc_cfg)
        _map = {
            1: (2, 4),
            2: (1, 3),
        }

        sql = 'select IfTradingDay from hkland_shszhktradingday where TradingType in {} and EndDate = "{}";'.format(
        _map.get(category), self.today.strftime("%Y-%m-%d"))
        ret = dc.select_all(sql)
        ret = [r.get("IfTradingDay") for r in ret]
        if ret == [2, 2]:
            return False
        else:
            return True

    def _check_if_trading_period(self):
        """判断是否是该天的交易时段"""
        _now = datetime.datetime.now()
        if (_now <= datetime.datetime(_now.year, _now.month, _now.day, 8, 0, 0) or
                _now >= datetime.datetime(_now.year, _now.month, _now.day, 16, 30, 0)):
            logger.warning("非当天交易时段")
            return False
        return True

    def _start(self):
        is_trading = self._check_if_trading_period()
        if not is_trading:
            return

        south_bool = self._check_if_trading_today(1)
        if south_bool:
            item_sh_hk = self.sh_hk()
            item_sh_sz = self.sh_sz()
        else:
            logger.warning("今日无南向交易 ")

        north_bool = self._check_if_trading_today(2)
        if north_bool:
            item_hk_sh = self.hk_sh()
            item_hk_sz = self.hk_sz()
        else:
            logger.warning("今日无北向交易 ")

    def start(self):
        try:
            self._create_stock_table()

            # item_hk_sh = self.hk_sh()
            # item_hk_sz = self.hk_sz()
            # item_sh_hk = self.sh_hk()
            # item_sh_sz = self.sh_sz()
            # self.ding("沪股通: {}\n深股通: {}\n港股通(沪): {}\n港股通(深):{}\n".format(item_hk_sh, item_hk_sz, item_sh_hk, item_sh_sz))

            self._start()
        except Exception as e:
            # traceback.print_exc()
            logger.info("cal history error: {}".format(e))
            # self.ding("cal history error: {}".format(e))

    def _create_stock_table(self):
        # 历史资金累计流入 其实是净买额累计流入
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `Date` datetime NOT NULL COMMENT '分钟时间点',
          `MoneyIn` decimal(20,4) NOT NULL COMMENT '当日资金流入(百万）',
          `MoneyBalance` decimal(20,4) NOT NULL COMMENT '当日余额（百万）',
          `MoneyInHistoryTotal` decimal(20,4) NOT NULL COMMENT '历史资金累计流入(其实是净买额累计流入)(百万元）',
          `NetBuyAmount` decimal(20,4) NOT NULL COMMENT '当日成交净买额(百万元）',
          `BuyAmount` decimal(20,4) NOT NULL COMMENT '买入成交额(百万元）',
          `SellAmount` decimal(20,4) NOT NULL COMMENT '卖出成交额(百万元）',
          `MarketTypeCode` int(11) NOT NULL COMMENT '市场类型代码',
          `MarketType` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '市场类型',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un` (`Date`,`MarketTypeCode`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='交易所计算陆股通资金流向汇总(港股通币种为港元，陆股通币种为人民币)';
        '''.format(self.table_name)
        client = self._init_pool(self.spider_cfg)
        client.insert(sql)
        client.dispose()


def task():
    h = HistoryCalSpider()
    h.start()


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    task()
    # TODO 在每天的 9-12 点 13-17 点 每隔 10 s 运行一次 晚点有空再细化时间
    scheduler.add_job(task, 'cron', hour='8-12, 13-17', minute="*", second='0, 10, 20, 30, 40, 50')
    # scheduler.add_job(task, 'interval', seconds=10)
    logger.info('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass

'''
docker build -f Dockerfile_calhistory -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_calhistory:v1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_calhistory:v1
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_calhistory:v1

sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name cal_history \
--env LOCAL=0 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_calhistory:v1
'''
