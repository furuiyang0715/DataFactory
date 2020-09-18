# -*- coding: utf-8 -*-
import datetime
import json
import logging
import re
import sys
import threading
import time
import traceback
import requests
import sys

sys.path.append("./../")
from hkland_flow import tools
from hkland_flow.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER,
                                 SPIDER_MYSQL_PASSWORD, SPIDER_MYSQL_DB, DC_HOST, DC_PORT, DC_USER,
                                 DC_PASSWD, DC_DB)
from hkland_flow.sql_pool import PyMysqlPoolBase
from hkland_flow.stock_hu_ontime import SSEStatsOnTime
from hkland_flow.stock_shen_ontime import SZSEStatsOnTime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FlowExchangeSpider(object):
    """陆股通实时数据 交易所爬虫"""
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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36',
        }
        self.table_name = 'hkland_flow_exchange'
        self.today = datetime.datetime.today()
        self.dc_client = None
        self.spider_client = None
        self.web_url = 'https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/Mutual-Market/Stock-Connect?sc_lang=zh-CN'

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

    def dc_init(self):
        if not self.dc_client:
            self.dc_client = self._init_pool(self.dc_cfg)

    def spider_init(self):
        if not self.spider_client:
            self.spider_client = self._init_pool(self.spider_cfg)

    def __del__(self):
        print("* "*10)
        if self.dc_client:
            self.dc_client.dispose()
        if self.spider_client:
            self.spider_client.dispose()

    @staticmethod
    def re_data(data):
        ret = re.findall("RMB(.*) Mil", data)   # RMB52,000 Mil
        if ret:
            data = ret[0]
            data = data.replace(",", '')
            return int(data) * 100

    def _check_if_trading_period(self, direction):
        """判断是否是该天的交易时段
        北向数据的时间是 9:30-11:30; 13:00-15:00
        南向数据的时间是 9:00-12:00; 13:00-16:10
        """
        _now = datetime.datetime.now()
        if direction == "north":
            morning_start = datetime.datetime(_now.year, _now.month, _now.day, 9, 30, 0)
            morning_end = datetime.datetime(_now.year, _now.month, _now.day, 11, 30, 0)
            afternoon_start = datetime.datetime(_now.year, _now.month, _now.day, 13, 0, 0)
            # afternoon_end = datetime.datetime(_now.year, _now.month, _now.day, 15, 0, 0)
            afternoon_end = datetime.datetime(_now.year, _now.month, _now.day, 15, 10, 0)

        elif direction == "sourth":
            morning_start = datetime.datetime(_now.year, _now.month, _now.day, 9, 0, 0)
            morning_end = datetime.datetime(_now.year, _now.month, _now.day, 12, 0, 0)
            afternoon_start = datetime.datetime(_now.year, _now.month, _now.day, 13, 0, 0)
            # afternoon_end = datetime.datetime(_now.year, _now.month, _now.day, 16, 10, 0)
            afternoon_end = datetime.datetime(_now.year, _now.month, _now.day, 16, 20, 0)
        else:
            raise ValueError("direction is in (north, sourth)")

        if (_now >= morning_start and _now <= morning_end) or (_now >= afternoon_start and _now <= afternoon_end):
            return True
        else:
            return False

    def start(self):
        retry = 1
        while True:
            try:
                self._start()
            except Exception as e:
                retry += 1
                if retry > 30:
                    tools.ding_msg("交易所实时数据爬取多次尝试后最终失败了 失败的原因是{}".format(e))
                    raise

                logger.info("因{}触发第{}次重试 ".format(e, retry))
                time.sleep(retry)
            else:
                break

    def contract_sql(self, datas, table: str, update_fields: list):
        if not isinstance(datas, list):
            datas = [datas, ]

        to_insert = datas[0]
        ks = []
        vs = []
        for k in to_insert:
            ks.append(k)
            vs.append(to_insert.get(k))
        fields_str = "(" + ",".join(ks) + ")"
        values_str = "(" + "%s," * (len(vs) - 1) + "%s" + ")"
        base_sql = '''INSERT INTO `{}` '''.format(table) + fields_str + ''' values ''' + values_str

        params = []
        for data in datas:
            vs = []
            for k in ks:
                vs.append(data.get(k))
            params.append(vs)

        if update_fields:
            # https://stackoverflow.com/questions/12825232/python-execute-many-with-on-duplicate-key-update/12825529#12825529
            # sql = 'insert into A (id, last_date, count) values(%s, %s, %s) on duplicate key update last_date=values(last_date),count=count+values(count)'
            on_update_sql = ''' ON DUPLICATE KEY UPDATE '''
            for update_field in update_fields:
                on_update_sql += '{}=values({}),'.format(update_field, update_field)
            on_update_sql = on_update_sql.rstrip(",")
            sql = base_sql + on_update_sql + """;"""
        else:
            sql = base_sql + ";"
        return sql, params

    def _save(self, sql_pool, to_insert, table, update_fields):
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            value = values[0]
            count = sql_pool.insert(insert_sql, value)
        except:
            traceback.print_exc()
            logger.warning("失败")
        else:
            if count == 1:
                logger.info("插入新数据 {}".format(to_insert))
            elif count == 2:
                logger.info("刷新数据 {}".format(to_insert))
            else:
                logger.info("已有数据 {} ".format(to_insert))
            sql_pool.end()
            return count

    def _create_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `DateTime` datetime NOT NULL COMMENT '交易时间',
          `ShHkFlow` decimal(19,4) NOT NULL COMMENT '沪股通/港股通(沪)当日资金流向(万）',
          `ShHkBalance` decimal(19,4) NOT NULL COMMENT '沪股通/港股通(沪)当日资金余额（万）',
          `SzHkFlow` decimal(19,4) NOT NULL COMMENT '深股通/港股通(深)当日资金流向(万）',
          `SzHkBalance` decimal(19,4) NOT NULL COMMENT '深股通/港股通(深)当日资金余额（万）',
          `Netinflow` decimal(19,4) NOT NULL COMMENT '南北向资金,当日净流入',
          `Category` tinyint(4) NOT NULL COMMENT '类别:1 南向, 2 北向',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `unique_key2` (`DateTime`,`Category`),
          KEY `DateTime` (`DateTime`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆港通-实时资金流向-交易所源';
        """.format(self.table_name)
        self.spider_client.insert(sql)
        self.spider_client.end()

    def _north(self):
        # 北向资金
        north_urls = [
            'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSH_Turnover_chi.js',  # 沪股通成交额
            'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSH_QuotaUsage_chi.js',  # 沪股通每日资金余额
            'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSZ_Turnover_chi.js',  # 深股通成交额
            'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSZ_QuotaUsage_chi.js',  # 深股通每日资金余额
        ]
        # # 南向资金
        # south_urls = [
        #     'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_SBSH_Turnover_chi.js',  # '港股通（沪）成交额'
        #     'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_SBSZ_Turnover_chi.js',  # '港股通（深）成交额'
        #
        # ]

        logger.info("开始处理北向资金")
        '''
        northbound11 =
            [
                {
                    "category": "Northbound",
                    "tablehead": ["沪股通"],
                    "section":
                    [
                        {
                            "subtitle": ["成交额", "07/04/2020 (09:57)", {}],
                            "item":
                            [
                                ["买入及卖出", "RMB9,225 Mil", {}],
                                ["买入", "RMB5,654 Mil", {}],
                                ["卖出", "RMB3,571 Mil", {}]
                            ]
                        }
                    ]
                }
            ];
        '''

        # logger.info("开始处理沪股通每日额度信息")
        '''
        northbound12 =
            [
                {
                    "category": "Northbound",
                    "type": "noHeader",
                    "tablehead": ["香港 > 上海"],
                    "section":
                    [
                        {
                            "subtitle": ["每日额度", "07/04/2020", {}],
                            "item": [
                              ["额度", "RMB52,000 Mil", {}],
                              ["余额 (于 09:30)", "RMB51,557 Mil", {}],
                              ["余额占额度百分比", "99%", {}]
                              
                              
                            ]
                        }
                    ]
                }
            ];
        '''
        body = requests.get(north_urls[1], headers=self.headers).text
        datas = json.loads(
            body.rstrip(";").lstrip("northbound11 =").lstrip("northbound12 =").lstrip("northbound21 =").lstrip(
                "northbound22 ="))
        show_dt = datas[0].get("section")[0].get("subtitle")[1]
        show_dt = datetime.datetime.strptime(show_dt, "%d/%m/%Y").strftime("%Y-%m-%d")
        flow_info = datas[0].get("section")[0].get("item")
        sh_item = dict()
        # 类别:1 南向, 2 北向
        sh_item['Category'] = 2
        # 分钟时间
        m_dt = flow_info[1][0]
        m_dt = re.findall("余额 \(于 (.*)\)", m_dt)[0]
        complete_dt = " ".join([show_dt, m_dt])
        complete_dt = str(datetime.datetime.strptime(complete_dt, "%Y-%m-%d %H:%M"))
        # 分钟交易时间
        sh_item['DateTime'] = complete_dt
        # 沪股通/港股通(沪)当日资金流向(万）此处北向资金表示沪股通
        sh_item['ShHkFlow'] = self.re_data(flow_info[0][1]) - self.re_data(flow_info[1][1])
        # 沪股通/港股通(沪)当日资金余额（万） 此处北向资金表示沪股通
        sh_item['ShHkBalance'] = self.re_data(flow_info[1][1])
        print(sh_item)

        # logger.info("开始处理深股通每日额度信息")
        body = requests.get(north_urls[3], headers=self.headers).text
        '''
        northbound22 =
            [
                {
                    "category": "Northbound",
                    "type": "noHeader",
                    "tablehead": ["香港 > 深圳"],
                    "section":
                    [
                        {
                            "subtitle": ["每日额度", "07/04/2020", {}],
                            "item": [
                              ["额度", "RMB52,000 Mil", {}],
                              ["余额 (于 09:58)", "RMB49,627 Mil", {}],
                              ["余额占额度百分比", "95%", {}]
                              
                              
                            ]
                        }
                    ]
                }
            ];
        '''
        datas = json.loads(
            body.rstrip(";").lstrip("northbound11 =").lstrip("northbound12 =").lstrip("northbound21 =").lstrip(
                "northbound22 ="))
        show_dt = datas[0].get("section")[0].get("subtitle")[1]
        show_dt = datetime.datetime.strptime(show_dt, "%d/%m/%Y").strftime("%Y-%m-%d")
        flow_info = datas[0].get("section")[0].get("item")
        m_dt = flow_info[1][0]
        m_dt = re.findall("余额 \(于 (.*)\)", m_dt)[0]
        complete_dt = " ".join([show_dt, m_dt])
        complete_dt = str(datetime.datetime.strptime(complete_dt, "%Y-%m-%d %H:%M"))
        sz_item = dict()
        # 类别:1 南向, 2 北向
        sz_item['Category'] = 2
        # 分钟交易时间
        sz_item['DateTime'] = complete_dt
        # 深股通/港股通(深)当日资金流向(万） 北向时为深股通
        sz_item['SzHkFlow'] = self.re_data(flow_info[0][1]) - self.re_data(flow_info[1][1])
        # 深股通/港股通(深)当日资金余额（万）北向时为深股通
        sz_item['SzHkBalance'] = self.re_data(flow_info[1][1])
        print(sz_item)

        if sh_item['DateTime'] == sz_item['DateTime']:
            sh_item.update(sz_item)
            # 南北向资金,当日净流入
            sh_item['Netinflow'] = sh_item['ShHkFlow'] + sh_item['SzHkFlow']
            print(sh_item)
            update_fields = ['Category', 'DateTime', 'ShHkFlow', 'ShHkBalance', 'SzHkFlow', 'SzHkBalance', 'Netinflow']
            self._save(self.spider_client, sh_item, self.table_name, update_fields)

    def _south(self):
        logger.info("开始处理南向数据")
        # logger.info("开始处理港股通(深)每日额度信息")
        sz = SZSEStatsOnTime()
        info = sz.get_balance_info()
        sz_item = dict()
        sz_item['Category'] = 1
        sz_item['DateTime'] = info.get("Time") + ":00"
        sz_item['SzHkFlow'] = (info.get("DailyLimit") - info.get("Balance")) / 10000
        sz_item['SzHkBalance'] = info.get("Balance") / 10000
        print(sz_item)

        # logger.info("开始处理港股通(沪)每日额度信息")
        sse = SSEStatsOnTime()
        info = sse.get_balance_info()
        sh_item = dict()
        sh_item['Category'] = 1    # 南向数据
        sh_dt = info.get("Time")
        sh_dt = datetime.datetime.strptime(sh_dt, "%Y-%m-%d %H:%M:%S")
        # sh_dt = str(datetime.datetime(sh_dt.year, sh_dt.month, sh_dt.day, sh_dt.hour, sh_dt.minute - 1, 0))
        sh_dt = str(datetime.datetime(sh_dt.year, sh_dt.month, sh_dt.day, sh_dt.hour, sh_dt.minute, 0) - datetime.timedelta(minutes=1))

        sh_item['DateTime'] = sh_dt
        sh_item['ShHkFlow'] = (info.get("DailyLimit") - info.get("Balance")) / 10000
        sh_item['ShHkBalance'] = info.get("Balance") / 10000
        print(sh_item)

        if sz_item.get("DateTime") == sh_item.get("DateTime"):
            sh_item.update(sz_item)
            sh_item['Netinflow'] = sh_item['ShHkFlow'] + sh_item['SzHkFlow']
            print(sh_item)
            update_fields = ['Category', 'DateTime', 'ShHkFlow', 'ShHkBalance', 'SzHkFlow', 'SzHkBalance', 'Netinflow']
            self._save(self.spider_client, sh_item, self.table_name, update_fields)

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
        _map = {
            1: (2, 4),
            2: (1, 3),
        }

        sql = 'select IfTradingDay from hkland_shszhktradingday where TradingType in {} and EndDate = "{}";'.format(
        _map.get(category), self.today.strftime("%Y-%m-%d"))
        ret = self.dc_client.select_all(sql)
        ret = [r.get("IfTradingDay") for r in ret]
        if ret == [2, 2]:
            return False
        else:
            return True

    def _start(self):
        # TODO 使用时再连接 避免空连
        self.dc_init()
        self.spider_init()

        self._create_table()

        sourth_is_trading_period = self._check_if_trading_period("sourth")
        south_bool = self._check_if_trading_today(1)
        if south_bool and sourth_is_trading_period:
            t1 = threading.Thread(target=self._south,)
            t1.start()
        else:
            logger.warning("[交易所]今日无南向交易或不在交易时间段内")

        north_is_trading_period = self._check_if_trading_period("north")
        north_bool = self._check_if_trading_today(2)
        if north_bool and north_is_trading_period:
            t2 = threading.Thread(target=self._north,)
            t2.start()
        else:
            logger.warning("[交易所]今日无北向交易或不在交易时间段内")


if __name__ == "__main__":
    # h = FlowExchangeSpider()
    # h._start()

    while True:
        h = FlowExchangeSpider()
        h.start()
        time.sleep(3)
        print()
        print()


'''deploy step 
docker build -f Dockerfile_exchange -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_exchange:v1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_exchange:v1 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_exchange:v1 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name flow_exchange --env LOCAL=0 registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_exchange:v1
docker logs -ft --tail 1000  flow_exchange  


# local 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name flow_exchange registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_exchange:v1 
'''
