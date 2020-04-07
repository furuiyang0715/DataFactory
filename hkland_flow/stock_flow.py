# -*- coding: utf-8 -*-
import datetime
import json
import logging
import re
import sys
import traceback
import requests

from hkland_flow.stock_hu_ontime import SSEStatsOnTime
from hkland_flow.stock_shen_ontime import SZSEStatsOnTime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HkexlugutongshishispiderSpider(object):
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36',
        }

    @staticmethod
    def re_data(data):
        ret = re.findall("RMB(.*) Mil", data)   # RMB52,000 Mil
        if ret:
            data = ret[0]
            data = data.replace(",", '')
            return int(data) * 100

    def start(self):
        try:
            self._start()
        except:
            traceback.print_exc()

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
        # print(sh_item)

        logger.info("开始处理深股通每日额度信息")
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
        # print(sz_item)

        if sh_item['DateTime'] == sz_item['DateTime']:
            sh_item.update(sz_item)
            # 南北向资金,当日净流入
            sh_item['Netinflow'] = sh_item['ShHkFlow'] + sh_item['SzHkFlow']
            print(sh_item)
            self._save(sh_item)


        # logger.info("开始处理港股通(沪)每日额度信息")
        # sse = SSEStatsOnTime()
        # print(sse.get_balance_info())
        #
        # logger.info("开始处理港股通(深)每日额度信息")
        # sz = SZSEStatsOnTime()
        # print(sz.get_balance_info())


if __name__ == "__main__":
    h = HkexlugutongshishispiderSpider()
    h._start()
