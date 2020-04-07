# -*- coding: utf-8 -*-
import datetime
import json
import logging
import re
import traceback
import requests

from hkland_flow.stock_hu_ontime import SSEStatsOnTime
from hkland_flow.stock_shen_ontime import SZSEStatsOnTime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HkexlugutongshishispiderSpider(object):
    # name = 'HKEXLuGuTongShiShiSpider'
    # allowed_domains = ['hkex.com']

    def __init__(self):
        # super().__init__(**kwargs)
        # self.rel = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=1)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36',
        }
        # self.item = {}

    @staticmethod
    def re_data(data):
        if data and 'Mil' in data:
            data = data.split(' ')[0]
        if 'RMB' in data:
            data = data.replace('RMB', '')
        if 'HK$' in data:
            data = data.replace('HK$', '')
        if ',' in data:
            data = data.replace(',', '')
        return data

    def start(self):
        try:
            self._start()
        except:
            traceback.print_exc()

    '''
    if category == '沪股通每日额度余额':
    # print(new_json)
    # 沪股通每日额度
    # print(new_json)
    SHSCDQ = new_json[0]['section'][0]['item'][0][1]
    SHSCDQ = self.re_data(SHSCDQ)
    item['SHSCDQ'] = SHSCDQ
    # 沪股通每日额度余额
    T1 = new_json[0]['section'][0]['subtitle'][1]
    T2 = new_json[0]['section'][0]['item'][1][0].split(' ')[2][:-1]
    Time = T1.split('/')[-1] + '-' + T1.split('/')[1] + '-' + T1.split('/')[0] + ' ' + T2
    # print(Time)
    item['SHSCDQ_T'] = Time
    SHSCDB = new_json[0]['section'][0]['item'][1][1]
    SHSCDB = self.re_data(SHSCDB)
    item['SHSCDB'] = SHSCDB
    # 沪股通每日余额占额度百分比
    SHSCD_BPQ = new_json[0]['section'][0]['item'][2][1]
    SHSCD_BPQ = self.re_data(SHSCD_BPQ)
    item['SHSCD_BPQ'] = SHSCD_BPQ
    '''

    def _start(self):
        # 北向资金
        north_urls = [
            'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSH_Turnover_chi.js',  # 沪股通成交额
            'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSH_QuotaUsage_chi.js',  # 沪股通每日资金余额
            'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSZ_Turnover_chi.js',  # 深股通成交额
            'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSZ_QuotaUsage_chi.js',  # 深股通每日资金余额
        ]
        # 南向资金
        south_urls = [
            'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_SBSH_Turnover_chi.js',  # '港股通（沪）成交额'
            'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_SBSZ_Turnover_chi.js',  # '港股通（深）成交额'

        ]

        logger.info("开始处理北向资金")
        # logger.info("开始获取沪股通成交额信息 ")
        # body = requests.get(north_urls[0], headers=self.headers).text
        # print(body)
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

        logger.info("开始处理沪股通每日额度信息")
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
        # print(show_dt)   # 2020-04-07
        flow_info = datas[0].get("section")[0].get("item")
        # print(flow_info)
        # [['额度', 'RMB52,000 Mil', {}], ['余额 (于 09:31)', 'RMB51,505 Mil', {}], ['余额占额度百分比', '99%', {}]]
        item = dict()
        # 分钟时间
        m_dt = flow_info[1][0]
        m_dt = re.findall("余额 \(于 (.*)\)", m_dt)[0]
        complete_dt = " ".join([show_dt, m_dt])
        complete_dt = str(datetime.datetime.strptime(complete_dt, "%Y-%m-%d %H:%M"))
        item['SHSCDQ_T'] = complete_dt
        # 每日额度
        item['SHSCDQ'] = flow_info[0][1]
        # 每日额度余额
        item['SHSCDB'] = flow_info[1][1]
        # 每日余额占额度百分比
        item['SHSCD_BPQ'] = flow_info[2][1]
        print(item)

        logger.info("开始处理深股通每日额度信息")
        body = requests.get(north_urls[3], headers=self.headers).text
        # print(body)
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
        # print(show_dt)
        flow_info = datas[0].get("section")[0].get("item")
        # print(flow_info)
        item = dict()
        # 分钟时间
        m_dt = flow_info[1][0]
        m_dt = re.findall("余额 \(于 (.*)\)", m_dt)[0]
        complete_dt = " ".join([show_dt, m_dt])
        complete_dt = str(datetime.datetime.strptime(complete_dt, "%Y-%m-%d %H:%M"))
        item['SZSCDQ_T'] = complete_dt
        # 每日额度
        item['SZSCDQ'] = flow_info[0][1]
        # 每日额度余额
        item['SZSCDB'] = flow_info[1][1]
        # 每日余额占额度百分比
        item['SZSCD_BPQ'] = flow_info[2][1]
        print(item)

        # logger.info("开始处理港股通(沪)每日额度信息")
        # sse = SSEStatsOnTime()
        # print(sse.get_balance_info())
        #
        # logger.info("开始处理港股通(深)每日额度信息")
        # sz = SZSEStatsOnTime()
        # print(sz.get_balance_info())


if __name__ == "__main__":
    h = HkexlugutongshishispiderSpider()
    h.start()
