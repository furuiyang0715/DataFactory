# -*- coding: utf-8 -*-

import datetime
import hashlib
import json
import logging
import re
import sys
import urllib.parse

import requests

sys.path.append("./../")

from hkland_historytradestat.base_spider import BaseSpider
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EMLgthisdspiderSpider(BaseSpider):
    def __init__(self):
        self.headers = {
            'Referer': 'http://data.eastmoney.com/hsgt/index.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'
        }

    def _start(self):
        """
        url : http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get?
        type=HSGTHIS&token=70f12f2f4f091e459a279469fe49eca5&filter=(MarketType=3)
        &js=var%20LQRHfxWE={%22data%22:(x),%22pages%22:(tp)}&ps=20&p=1
        :return:
        """
        # type,token是不变的
        # filter, js,是变的
        # ps,是一次返回几条数据（20）
        # p，是页码
        # sr=-1&st=DetailDate 时间降序排列
        """        filter                js
        sh沪股通：(MarketType=1)        var LjQxjkJV={"data":(x),"pages":(tp)}
        sz深股通：(MarketType=3)        var waGmTUWT={"data":(x),"pages":(tp)}
        hkh港股通（沪）：(MarketType=2)   var ffVlSXoE={"data":(x),"pages":(tp)}
        hkz港股通（深）：(MarketType=4)   var PLmMnPai={"data":(x),"pages":(tp)}
        """
        js_dic = {'1': 'LjQxjkJV', '3': 'waGmTUWT', '2': 'ffVlSXoE', '4': 'PLmMnPai'}
        category_lis = ['1', '2', '3', '4']
        # 一页有ps条数据,有p页
        ps = 1000
        p = 1
        for category in category_lis:
            url1 = """http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get?type=HSGTHIS&token=70f12f2f4f091e459a279469fe49eca5&filter=(MarketType={})"""
            url2 = """&js=var%"""
            url3 ="""{%22data%22:(x),%22pages%22:(tp)}"""
            url4 = """&sr=-1&st=DetailDate&ps={}&p={}"""
            url = url1.format(category) + url2 + js_dic[category] + url3 + url4.format(ps, p)
            # url_p = url1.format(category) + url2 + js_dic[category] + url3
            # print(url)

            resp = requests.get(url)
            if resp.status_code == 200:
                body = resp.text
                body = urllib.parse.unquote(body)
                num = body.find("{")
                body = body[num:]
                datas = json.loads(body)
                print(datas)

            # yield scrapy.Request(url=url, headers=headers, callback=self.parse, dont_filter=True,
            #                      meta={'url_p': url_p, 'p': p, 'ps': ps})

        # url = 'http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get?type=HSGTHIS&token=70f12f2f4f091e459a279469fe49eca5&filter=(MarketType=4)&js=var%20PLmMnPai={%22data%22:(x),%22pages%22:(tp)}&ps=20&p=1'
        # yield scrapy.Request(url=url,headers=headers, callback=self.parse, dont_filter=True)

    def parse(self, response):
        html = response.body.decode()
        p = response.meta['p']
        url_p = response.meta['url_p']
        ps = response.meta['ps']
        # print(html)
        num = html.find('{')
        # print(type(num))
        newdata = html[num:]
        # print(newdata)
        dataJson = json.loads(newdata)
        # print(dataJson)
        data = dataJson['data']
        pages = dataJson['pages']
        item = {}
        # print(len(data))
        if len(data) != 0:
            for da in data:
                # 日期
                item['Date'] = da['DetailDate']
                # 当日资金流入(百万）
                item['TCapitalInflow'] = da['DRZJLR']
                # 当日余额（百万）
                item['TBalance'] = da['DRYE']
                # 历史资金累计流入(百万元）
                item['AInflowHisFunds'] = da['LSZJLR']
                # 当日成交净买额(百万元）
                item['NetBuyMoney'] = da['DRCJJME']
                # 买入成交额（百万元）
                item['BuyMoney'] = da['MRCJE']
                # 卖出成交额（百万元）
                item['SellMoney'] = da['MCCJE']
                # 领涨股
                item['LCG'] = da['LCG']
                # 领涨股涨跌幅
                item['LCGChangeRange'] = da['LCGZDF']
                # 上证指数
                item['SSEChange'] = da['SSEChange']
                # 涨跌幅
                item['SSEChangePrecent'] = da['SSEChangePrecent']
                # 类别
                if da['MarketType'] == 1.0:
                    item['Category'] = '沪股通'
                    item['CategoryCode'] = 1
                elif da['MarketType'] == 2.0:
                    item['Category'] = '港股通（沪）'
                    item['CategoryCode'] = 2
                elif da['MarketType'] == 3.0:
                    item['Category'] = '深股通'
                    item['CategoryCode'] = 3
                elif da['MarketType'] == 4.0:
                    item['Category'] = '港股通（深）'
                    item['CategoryCode'] = 4
                result = self.rel.sismember('e_m_LGTHisD', '{}'.format(item['Date']+str(item['CategoryCode'])))
                if not result:
                    yield item
                else:
                    print('数据已存在')
                    logger.info('数据已存在')
                # print(item)
            if p < pages:
                p += 1
                url1 = '&sr=-1&st=DetailDate&ps={}&p={}'
                url = url_p + url1.format(ps, p)
                headers = {
                    'Referer': 'http://data.eastmoney.com/hsgt/index.html',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'
                }
                yield scrapy.Request(url=url, headers=headers, callback=self.parse, dont_filter=True,
                                     meta={'url_p': url_p, 'p': p, 'ps': ps})

        else:
            print('数据不存在')
            logger.info('数据不存在')

    '''
    CREATE TABLE `lgt_historical_data` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `Date` datetime(6) NOT NULL COMMENT '日期',
  `TCapitalInflow` decimal(19,4) NOT NULL COMMENT '当日资金流入(百万）',
  `TBalance` decimal(19,4) NOT NULL COMMENT '当日余额（百万）',
  `AInflowHisFunds` decimal(19,4) DEFAULT NULL COMMENT '历史资金累计流入(百万元）',
  `NetBuyMoney` decimal(19,4) DEFAULT NULL COMMENT '当日成交净买额(百万元）',
  `BuyMoney` decimal(19,4) DEFAULT NULL COMMENT '买入成交额（百万元）',
  `SellMoney` decimal(19,4) DEFAULT NULL COMMENT '卖出成交额（百万元）',
  `LCG` varchar(19) COLLATE utf8_bin DEFAULT NULL COMMENT '领涨股',
  `LCGChangeRange` decimal(19,6) DEFAULT NULL COMMENT '领涨股涨跌幅',
  `SSEChange` decimal(19,4) DEFAULT NULL COMMENT '上证指数',
  `SSEChangePrecent` decimal(19,17) DEFAULT NULL COMMENT '涨跌幅',
  `Category` varchar(10) COLLATE utf8_bin DEFAULT NULL COMMENT '类别',
  `CategoryCode` decimal(10,0) DEFAULT NULL COMMENT '类别编码',
  `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
  `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆股通历史数据-东财';
    '''


if __name__ == "__main__":
    his = EMLgthisdspiderSpider()
    his._start()


    pass