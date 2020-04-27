# -*- coding: utf-8 -*-

import datetime
import json
import logging
import pprint
import re
import traceback
import requests
import sys


sys.path.append("./../")
from hkland_historytradestat.configs import (PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_USER, PRODUCT_MYSQL_PASSWORD,
                                             PRODUCT_MYSQL_DB, DC_HOST, DC_PORT, DC_USER, DC_PASSWD,
                                             DC_DB, SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT,
                                             SPIDER_MYSQL_USER, SPIDER_MYSQL_PASSWORD, SPIDER_MYSQL_DB,
                                             PRODUCT_MYSQL_PORT)
from hkland_historytradestat.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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
        self.table_name = 'hkland_flow_exchange'
        self.today = datetime.datetime.today()
        self.hk_sh_his = self.select_last_total(1).get("MoneyInHistoryTotal")
        self.hk_sz_his = self.select_last_total(3).get("MoneyInHistoryTotal")
        self.sh_hk_his = self.select_last_total(2).get("MoneyInHistoryTotal")
        self.sz_hk_his = self.select_last_total(4).get("MoneyInHistoryTotal")

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
        sql = '''select Date, MoneyInHistoryTotal from hkland_historytradestat where Date = (select max(Date) from hkland_historytradestat) and MarketTypeCode = {};'''.format(market_type)
        ret = dc.select_one(sql)
        return ret

    def sh_sz(self):
        """港股通 深"""
        url2 = 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_SBSZ_Turnover_chi.js'   # '港股通（深）成交额'

        body2 = requests.get(url2, headers=self.headers).text
        datas2 = json.loads(
            body2.rstrip(";").lstrip("southbound11 =").lstrip("southbound12 =").lstrip("southbound21 =").lstrip(
                "southbound22 ="))
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

        item = {
            "Date": date_min,  # 具体到分钟的交易时间
            # "MoneyBalance": money_balance,  # 当日余额(百万）
            # "MoneyIn": money_in,  # 当日资金流入(百万) = 额度 - 余额
            "BuyAmount": buy_amount,  # 当日买入成交额(百万元)
            "SellAmount": sell_amount,  # 当日卖出成交额(百万元)
            "NetBuyAmount": netbuyamount,  # 当日成交净买额(百万元)  = (当日)买入成交额(百万元) - (当日)卖出成交额(百万元)
            "MoneyInHistoryTotal": moneyinhistorytotal,  # 历史资金累计流入(百万) = 上一天的历史资金累计流入(百万) + 今天的当日成交净买额(百万元)
            "MarketTypeCode": 4,  # 市场类型代码
            "MarketType": '港股通(深)',  # 市场类型
        }

        logger.info("生成一条港股通(深)数据: {}".format(item))

    def sh_hk(self):
        """港股通 沪"""
        # url = 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_SBSH_QuotaUsage_chi.js'   # 港股通(沪) 每日资金余额
        url2 = 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_SBSH_Turnover_chi.js'   # '港股通（沪）成交额'

        # body = requests.get(url, headers=self.headers).text
        # datas = json.loads(body.rstrip(";").lstrip("southbound11 =").lstrip("southbound12 =").lstrip("southbound21 =").lstrip("southbound22 ="))
        # logger.info("\n" + pprint.pformat(datas))
        # '''
        # [{'category': 'Southbound',
        #   'section': [{'item': [],
        #                'subtitle': ['请参阅<A '
        #                             'href=http://www.sse.com.cn/services/hkexsc/home/ '
        #                             'target=_blank alt=SSE>上海证券交易所网站</a>。',
        #                             {'colspan': 2}]}],
        #   'tablehead': ['港股通（沪）', ''],
        #   'type': 'noHeader'}]
        # '''

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

        item = {
            "Date": date_min,  # 具体到分钟的交易时间
            # "MoneyBalance": money_balance,  # 当日余额(百万）
            # "MoneyIn": money_in,  # 当日资金流入(百万) = 额度 - 余额
            "BuyAmount": buy_amount,  # 当日买入成交额(百万元)
            "SellAmount": sell_amount,  # 当日卖出成交额(百万元)
            "NetBuyAmount": netbuyamount,  # 当日成交净买额(百万元)  = (当日)买入成交额(百万元) - (当日)卖出成交额(百万元)
            "MoneyInHistoryTotal": moneyinhistorytotal,  # 历史资金累计流入(百万) = 上一天的历史资金累计流入(百万) + 今天的当日成交净买额(百万元)
            "MarketTypeCode": 2,  # 市场类型代码
            "MarketType": '港股通(沪)',  # 市场类型
        }

        logger.info("生成一条港股通(沪)数据: {}".format(item))

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
            # "Date": show_dt,  # 陆股通交易时间
            # "Time": complete_dt,   # 具体的分钟时间
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
            # "Date": show_dt,     # 陆股通交易日时间
            # "Time": complete_dt,   # 分钟时间线
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

    # def _create_stock_table(self):
    #     # 历史资金累计流入 其实是净买额累计流入
    #     fields = ['Date',
    #               'MoneyIn',
    #               'MoneyBalance',
    #               'MoneyInHistoryTotal',
    #               'NetBuyAmount',
    #               'BuyAmount',
    #               'SellAmount',
    #               'MarketTypeCode',
    #               'MarketType',
    #               ]
    #     sql = '''
    #     CREATE TABLE IF NOT EXISTS `hkland_calhistory` (
    #       `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
    #       `Date` datetime NOT NULL COMMENT '时间',
    #       `MoneyIn` decimal(20,4) NOT NULL COMMENT '当日资金流入(百万）',
    #       `MoneyBalance` decimal(20,4) NOT NULL COMMENT '当日余额（百万）',
    #       `MoneyInHistoryTotal` decimal(20,4) NOT NULL COMMENT '历史资金累计流入(其实是净买额累计流入)(百万元）',
    #       `NetBuyAmount` decimal(20,4) NOT NULL COMMENT '当日成交净买额(百万元）',
    #       `BuyAmount` decimal(20,4) NOT NULL COMMENT '买入成交额(百万元）',
    #       `SellAmount` decimal(20,4) NOT NULL COMMENT '卖出成交额(百万元）',
    #       `MarketTypeCode` int(11) NOT NULL COMMENT '市场类型代码',
    #       `MarketType` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '市场类型',
    #       `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
    #       `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #       PRIMARY KEY (`id`),
    #       UNIQUE KEY `un` (`Date`,`MarketTypeCode`)
    #     ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='交易所计算陆股通资金流向汇总(港股通币种为港元，陆股通币种为人民币)';
    #     '''
    #     client = self._init_pool(self.product_cfg)
    #     client.insert(sql)
    #     client.dispose()


if __name__ == "__main__":
    h = HistoryCalSpider()
    # h.hk_sh()

    # h.hk_sz()

    # h.sh_hk()

    h.sh_sz()
