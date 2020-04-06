# -*- coding: utf-8 -*-
import datetime
import json
# from .langconv import *
# import redis
# import scrapy
# from ..settings import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
import pprint
import re
import sys
import traceback

import requests


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
        url_dic = {
            '沪股通成交额': 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSH_Turnover_chi.js',
            # '港股通（沪）成交额': 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_SBSH_Turnover_chi.js',
            # '沪股通每日额度余额': 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSH_QuotaUsage_chi.js',
            # '深股通成交额': 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSZ_Turnover_chi.js',
            # '港股通（深）成交额': 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_SBSZ_Turnover_chi.js',
            # '深股通每日额度余额': 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSZ_QuotaUsage_chi.js',
        }
        for category in url_dic:
            url = url_dic[category]
            body = requests.get(url, headers=self.headers).text
            # print(body)
            datas = json.loads(body.rstrip(";").lstrip("northbound12 =").lstrip("northbound22 =").lstrip("southbound11 =").lstrip("southbound21 ="))
            print(pprint.pformat(datas))
            # print(type(datas))
            flow_info = datas[0].get("section")[0].get("subtitle")
            print(flow_info)  # ['成交额', '06/04/2020 (06:00)', {}]
            show_dt = flow_info[1]
            print(show_dt)   # 06/04/2020 (06:00)




    @staticmethod
    def Traditional2Simplified(sentence):
        '''
        将sentence中的繁体字转为简体字
        :param sentence: 待转换的句子
        :return: 将句子中繁体字转换为简体字之后的句子
        '''
        sentence = Converter('zh-hans').convert(sentence)
        return sentence

    def parse(self, response):
        category = response.meta['category']
        html = response.body.decode('utf8')
        if '十大成交股' in category:
            # print(html)
            start_index = html.find('=')
            new_str = html[start_index + 2:]
            new_json_list = json.loads(new_str)
            # print(new_json_list)
            for new_json in new_json_list:
                CategoryCode = new_json['market']
                Date = new_json['date']
                ten_content_tr_lis = new_json['content'][1]['table']['tr']
                for tr in ten_content_tr_lis:
                    # print(tr)
                    item = {}
                    if CategoryCode == 'SSE Northbound':
                        Category = '沪股通' + category
                    elif CategoryCode == 'SSE Southbound':
                        Category = '港股通(沪)' + category
                    elif CategoryCode == 'SZSE Northbound':
                        Category = '深股通' + category
                    else:
                        Category = '港股通(深)' + category
                    item['CategoryCode'] = Category
                    item['Date'] = Date
                    # print(tr['td'][0][1])
                    if '深股通' in Category or '沪股通' in Category:
                        item['SecuCode'] = tr['td'][0][1].zfill(6)
                    else:
                        item['SecuCode'] = tr['td'][0][1]
                    SecuName = tr['td'][0][2].strip()
                    item['SecuName'] = HkexlugutongshishispiderSpider.Traditional2Simplified(SecuName)
                    # 买入金额
                    TMRJE = tr['td'][0][3]
                    if ',' in TMRJE:
                        TMRJE = TMRJE.replace(',', '')
                    item['TMRJE'] = TMRJE
                    # 卖出金额
                    TSRJE = tr['td'][0][4]
                    if ',' in TSRJE:
                        TSRJE = TSRJE.replace(',', '')
                    item['TSRJE'] = TSRJE
                    # 买入及卖出金额
                    TCJJE = tr['td'][0][5]
                    if ',' in TCJJE:
                        TCJJE = TCJJE.replace(',', '')
                    item['TCJJE'] = TCJJE
                    item['category'] = category
                    # print(item)
                    yield item
        else:
            start_index = html.find('=')
            end_index = html.find(';')
            # print(start_index, end_index)
            new_str = html[start_index + 1: end_index]
            # print(new_str)
            new_json = json.loads(new_str)
            # print(new_json)
            # if category == '沪股通成交额':
            #     # print(new_json)
            #     # 沪股通成交额_买入及卖出
            #     # print(new_json[0]['section'][0]['item'][0][1])
            #     SHSCTurnover_BS = new_json[0]['section'][0]['item'][0][1]
            #     SHSCTurnover_BS = self.re_data(SHSCTurnover_BS)
            #     # print(SHSCTurnover_BS)
            #     self.item['SHSCTurnover_BS'] = SHSCTurnover_BS
            #     # 沪股通成交额_买入
            #     SHSCTurnover_B = new_json[0]['section'][0]['item'][1][1]
            #     SHSCTurnover_B = self.re_data(SHSCTurnover_B)
            #     # print(SHSCTurnover_B)
            #     self.item['SHSCTurnover_B'] = SHSCTurnover_B
            #     # 沪股通成交额_卖出
            #     SHSCTurnover_S = new_json[0]['section'][0]['item'][2][1]
            #     SHSCTurnover_S = self.re_data(SHSCTurnover_S)
            #     # print(SHSCTurnover_S)
            #     self.item['SHSCTurnover_S'] = SHSCTurnover_S
            # elif category == '港股通（沪）成交额':
            #     # print(new_json)
            #     # 港股通（沪）_买入及卖出
            #     HKSCSHTurnover_BS = new_json[0]['section'][0]['item'][0][1]
            #     HKSCSHTurnover_BS = self.re_data(HKSCSHTurnover_BS)
            #     self.item['HKSCSHTurnover_BS'] = HKSCSHTurnover_BS
            #     # 港股通（沪）_买入
            #     HKSCSHTurnover_B = new_json[0]['section'][0]['item'][1][1]
            #     HKSCSHTurnover_B = self.re_data(HKSCSHTurnover_B)
            #     self.item['HKSCSHTurnover_B'] = HKSCSHTurnover_B
            #     # 港股通（沪）_买入及卖出
            #     HKSCSHTurnover_S = new_json[0]['section'][0]['item'][2][1]
            #     HKSCSHTurnover_S = self.re_data(HKSCSHTurnover_S)
            #     self.item['HKSCSHTurnover_S'] = HKSCSHTurnover_S
            item = {'category': category}
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
            # elif category == '深股通成交额':
            #     # print(new_json)
            #     # 深股通成交额_买入及卖出
            #     SZSCTurnover_BS = new_json[0]['section'][0]['item'][0][1]
            #     SZSCTurnover_BS = self.re_data(SZSCTurnover_BS)
            #     self.item['SZSCTurnover_BS'] = SZSCTurnover_BS
            #     # 深股通成交额_买入
            #     SZSCTurnover_B = new_json[0]['section'][0]['item'][1][1]
            #     SZSCTurnover_B = self.re_data(SZSCTurnover_B)
            #     self.item['SZSCTurnover_B'] = SZSCTurnover_B
            #     # 深股通成交额_卖出
            #     SZSCTurnover_S = new_json[0]['section'][0]['item'][2][1]
            #     SZSCTurnover_S = self.re_data(SZSCTurnover_S)
            #     self.item['SZSCTurnover_S'] = SZSCTurnover_S
            # elif category == '港股通（深）成交额':
            #     # print(new_json)
            #     # 港股通（沪）_买入及卖出
            #     HKSCSZTurnover_BS = new_json[0]['section'][0]['item'][0][1]
            #     HKSCSZTurnover_BS = self.re_data(HKSCSZTurnover_BS)
            #     self.item['HKSCSZTurnover_BS'] = HKSCSZTurnover_BS
            #     # 港股通（沪）_买入
            #     HKSCSZTurnover_B = new_json[0]['section'][0]['item'][1][1]
            #     HKSCSZTurnover_B = self.re_data(HKSCSZTurnover_B)
            #     self.item['HKSCSZTurnover_B'] = HKSCSZTurnover_B
            #     # 港股通（沪）_买入及卖出
            #     HKSCSZTurnover_S = new_json[0]['section'][0]['item'][2][1]
            #     HKSCSZTurnover_S = self.re_data(HKSCSZTurnover_S)
            #     self.item['HKSCSZTurnover_S'] = HKSCSZTurnover_S
            else:
                # 深股通每日额度余额
                # print(new_json)
                T1 = new_json[0]['section'][0]['subtitle'][1]
                T2 = new_json[0]['section'][0]['item'][1][0].split(' ')[2][:-1]
                Time = T1.split('/')[-1] + '-' + T1.split('/')[1] + '-' + T1.split('/')[0] + ' ' + T2
                # print(Time)
                item['SZSCDQ_T'] = Time
                SZSCDQ = new_json[0]['section'][0]['item'][0][1]
                SZSCDQ = self.re_data(SZSCDQ)
                item['SZSCDQ'] = SZSCDQ
                # 深股通每日额度余额
                SZSCDB = new_json[0]['section'][0]['item'][1][1]
                SZSCDB = self.re_data(SZSCDB)
                item['SZSCDB'] = SZSCDB
                # 深股通每日余额占额度百分比
                SZSCD_BPQ = new_json[0]['section'][0]['item'][2][1]
                SZSCD_BPQ = self.re_data(SZSCD_BPQ)
                item['SZSCD_BPQ'] = SZSCD_BPQ

            yield item


if __name__ == "__main__":
    h = HkexlugutongshishispiderSpider()
    h.start()
