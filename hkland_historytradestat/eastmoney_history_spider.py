# -*- coding: utf-8 -*-
import datetime
import json
import logging
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
        self.page_num = 1000
        self.table_name = "hkland_historytradestat"

    def _start(self):
        """
        type,token是不变的
        filter, js,是变的
        ps,是一次返回几条数据（20）
        p，是页码
        sr=-1&st=DetailDate 时间降序排列

        sh沪股通：(MarketType=1)        var LjQxjkJV={"data":(x),"pages":(tp)}
        sz深股通：(MarketType=3)        var waGmTUWT={"data":(x),"pages":(tp)}
        hkh港股通（沪）：(MarketType=2)   var ffVlSXoE={"data":(x),"pages":(tp)}
        hkz港股通（深）：(MarketType=4)   var PLmMnPai={"data":(x),"pages":(tp)}

        :return:
        """

        js_dic = {'1': 'LjQxjkJV',
                  '3': 'waGmTUWT',
                  '2': 'ffVlSXoE',
                  '4': 'PLmMnPai'}
        category_lis = ['1', '2', '3', '4']
        for category in category_lis:
            url1 = """http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get?type=HSGTHIS&token=70f12f2f4f091e459a279469fe49eca5&filter=(MarketType={})"""
            url2 = """&js=var%"""
            url3 ="""{%22data%22:(x),%22pages%22:(tp)}"""
            url4 = """&sr=-1&st=DetailDate&ps={}&p={}"""
            for page in range(10):
                url = url1.format(category) + url2 + js_dic[category] + url3 + url4.format(self.page_num, page)
                datas = self._get_datas(url)
                if not datas:
                    break

    def _get_datas(self, url):
        items = []
        resp = requests.get(url)
        if resp.status_code == 200:
            body = resp.text
            body = urllib.parse.unquote(body)
            num = body.find("{")
            body = body[num:]
            datas = json.loads(body).get("data")
            '''
            fields = {
                'Date': "Date",    # 日期
                'TCapitalInflow': "MoneyIn",  # 当日资金流入(百万）
                'TBalance': "MoneyBalance",   # 当日余额（百万）
                'AInflowHisFunds': "MoneyInHistoryTotal",   # 历史资金累计流入(百万元）
                'NetBuyMoney': "NetBuyAmount",   # 当日成交净买额(百万元）
                'BuyMoney': "BuyAmount",   # 买入成交额（百万元）
                'SellMoney': "SellAmount",   # 卖出成交额（百万元）
                # 'LCG': "",   # 领涨股
                # 'LCGChangeRange': "",   # 领涨股涨跌幅
                # 'SSEChange': "",   # 上证指数
                # 'SSEChangePrecent': "",  # 涨跌幅
                'Category': "MarketType",  # 类别
                'CategoryCode': "MarketTypeCode",  # 类别编码
            }
            '''
            for da in datas:
                item = dict()
                # 日期
                item['Date'] = da['DetailDate']
                # 当日资金流入(百万）
                item['MoneyIn'] = da['DRZJLR']
                # 当日余额（百万）
                item['MoneyBalance'] = da['DRYE']
                # 历史资金累计流入(百万元）
                item['MoneyInHistoryTotal'] = da['LSZJLR']
                # 当日成交净买额(百万元）
                item['NetBuyAmount'] = da['DRCJJME']
                # 买入成交额（百万元）
                item['BuyAmount'] = da['MRCJE']
                # 卖出成交额（百万元）
                item['SellAmount'] = da['MCCJE']
                # # 领涨股
                # item['LCG'] = da['LCG']
                # # 领涨股涨跌幅
                # item['LCGChangeRange'] = da['LCGZDF']
                # # 上证指数
                # item['SSEChange'] = da['SSEChange']
                # # 涨跌幅
                # item['SSEChangePrecent'] = da['SSEChangePrecent']
                # 类别
                if da['MarketType'] == 1.0:
                    item['MarketType'] = '沪股通'
                    item['MarketTypeCode'] = 1
                elif da['MarketType'] == 2.0:
                    item['MarketType'] = '港股通(沪市)'
                    item['MarketTypeCode'] = 2
                elif da['MarketType'] == 3.0:
                    item['MarketType'] = '深股通'
                    item['MarketTypeCode'] = 3
                elif da['MarketType'] == 4.0:
                    item['MarketType'] = '港股通(深市)'
                    item['MarketTypeCode'] = 4

                # FIXME 临时兼容之前的程序
                item["CMFID"] = 1
                item['CMFTime'] = datetime.datetime.now()
                items.append(item)
                # print(item)
                update_fields = ['Date', "MoneyIn", "MoneyBalance", "MoneyInHistoryTotal", "NetBuyAmount", "BuyAmount",
                                 "SellAmount", "MarketType", "MarketTypeCode"]
                self._save(item, self.table_name, update_fields)
        return items

    def _create_table(self):
        fields = {'Date': "Date",    # 日期
                  'TCapitalInflow': "MoneyIn",  # 当日资金流入(百万）
                  'TBalance': "MoneyBalance",   # 当日余额（百万）
                  'AInflowHisFunds': "MoneyInHistoryTotal",   # 历史资金累计流入(百万元）
                  'NetBuyMoney': "NetBuyAmount",   # 当日成交净买额(百万元）
                  'BuyMoney': "BuyAmount",   # 买入成交额（百万元）
                  'SellMoney': "SellAmount",   # 卖出成交额（百万元）
                  # 'LCG': "",   # 领涨股
                  # 'LCGChangeRange': "",   # 领涨股涨跌幅
                  # 'SSEChange': "",   # 上证指数
                  # 'SSEChangePrecent': "",  # 涨跌幅
                  'Category': "MarketType",  # 类别
                  'CategoryCode': "MarketTypeCode",  # 类别编码
                  }

        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `Date` datetime NOT NULL COMMENT '日期',
          `MoneyIn` decimal(20,4) NOT NULL COMMENT '当日资金流入(百万）',
          `MoneyBalance` decimal(20,4) NOT NULL COMMENT '当日余额（百万）',
          `MoneyInHistoryTotal` decimal(20,4) NOT NULL COMMENT '历史资金累计流入(百万元）',
          `NetBuyAmount` decimal(20,4) NOT NULL COMMENT '当日成交净买额(百万元）',
          `BuyAmount` decimal(20,4) NOT NULL COMMENT '买入成交额(百万元）',
          `SellAmount` decimal(20,4) NOT NULL COMMENT '卖出成交额(百万元）',
          `MarketTypeCode` int(11) NOT NULL COMMENT '市场类型代码',
          `MarketType` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '市场类型',
          `CMFID` bigint(20) NOT NULL COMMENT '来源ID',
          `CMFTime` datetime NOT NULL COMMENT '来源日期',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un` (`Date`,`MarketTypeCode`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆股通资金流向汇总(港股通币种为港元，陆股通币种为人民币)'; 
        '''.format(self.table_name)
        product = self._init_pool(self.product_cfg)
        product.insert(sql)
        product.dispose()


if __name__ == "__main__":
    his = EMLgthisdspiderSpider()
    his._create_table()
    his._start()
