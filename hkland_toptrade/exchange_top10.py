# import json
# import pprint
# import sys
#
# import requests
#
# # url = 'https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Statistics/Historical-Daily?sc_lang=zh-HK#select4=1&select5=0&select3=0&select1=16&select2=5'
# url = 'https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_20200617c.js?_=1592465188627'
# # url = 'https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_20200501c.js?_=1592465188627'
#
# resp = requests.get(url)
#
# if resp.status_code == 200:
#     body = resp.text
#     # print(body)
#     # print(type(body))
#     datas_str = body.replace("tabData = ", "")
#     try:
#         datas = eval(datas_str)
#     except:
#         sys.exit(0)
#     # datas = json.loads(datas_str)
#     # print(datas)
#     # print(type(datas))
#     # print(pprint.pformat(datas))
#     for direction_data in datas:
#         cur_dt = direction_data.get("date")
#         market = direction_data.get("market")
#         is_trading_day = direction_data.get("tradingDay")
#         content = direction_data.get("content")[1].get("table")
#         print(cur_dt)
#         print(market)
#         print(is_trading_day)
#         print(pprint.pformat(content))
#
#         print()
#         print()
#
# else:
#     print(resp)

'''
`Date` date NOT NULL COMMENT '时间',
`SecuCode` varchar(10) COLLATE utf8_bin NOT NULL COMMENT '证券代码',
`InnerCode` int(11) NOT NULL COMMENT '内部编码',
`SecuAbbr` varchar(20) COLLATE utf8_bin NOT NULL COMMENT '股票简称',
`Close` decimal(19,3) NOT NULL COMMENT '收盘价',
`ChangePercent` decimal(19,5) NOT NULL COMMENT '涨跌幅',
`TJME` decimal(19,3) NOT NULL COMMENT '净买额（元/港元）',
`TMRJE` decimal(19,3) NOT NULL COMMENT '买入金额（元/港元）',
`TCJJE` decimal(19,3) NOT NULL COMMENT '成交金额（元/港元）',
`CategoryCode` varchar(10) COLLATE utf8_bin DEFAULT NULL COMMENT '类别代码:GGh: 港股通(沪), GGs: 港股通(深), HG: 沪股通, SG: 深股通',
`CMFID` bigint(20) NOT NULL COMMENT '来源ID',
`CMFTime` datetime NOT NULL COMMENT '来源日期',
'''


import pprint
import time
import traceback

import requests

from hkland_toptrade.base_spider import BaseSpider


class ExchangeTop10(BaseSpider):

    def __init__(self):
        super(ExchangeTop10, self).__init__()
        self.web_url = 'https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Statistics/Historical-Daily?sc_lang=zh-HK#select4=1&select5=0&select3=0&select1=16&select2=5'
        self.dt_str = "20200617"
        self.url = 'https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_{}c.js?_={}'.format(self.dt_str, int(time.time()*1000))

    def process(self, items):
        for item in items:
            item.pop("Rank")
            item.pop("Stock Name")
            item.update({"TMRJE": item.get("Buy Turnover")})
            item.update({})
        pass

    @staticmethod
    def re_money_data(data: str):
        data = float(data.replace(",", ""))
        return data

    def start(self):
        print(self.url)

        resp = requests.get(self.url)

        if resp.status_code == 200:
            body = resp.text
            datas_str = body.replace("tabData = ", "")
            try:
                datas = eval(datas_str)
            except:
                traceback.print_exc()
                return

            fields = [
                'Rank',   # 十大成交排名
                'Stock Code',   # 证券代码
                'Stock Name',   # 证券简称
                'Buy Turnover',  # 买入金额 (RMB)
                'Sell Turnover',  # 卖出金额(RMB)
                'Total Turnover',  # 买入以及卖出金额 (RMB)
            ]
            # 与钱相关的字段 单独列出是为了将字符串转换为数值
            money_fields = ['Buy Turnover', 'Sell Turnover', 'Total Turnover']
            # 类别代码:GGh: 港股通(沪), GGs: 港股通(深), HG: 沪股通, SG: 深股通
            category_map = {
                "SSE Northbound": "HG",
                "SSE Southbound": "GGh",
                "SZSE Northbound": "SG",
                "SZSE Southbound": "GGs",
            }
            for direction_data in datas:
                items = []
                # print(pprint.pformat(direction_data))
                cur_dt = direction_data.get("date")
                market = direction_data.get("market")
                is_trading_day = direction_data.get("tradingDay")
                content = direction_data.get("content")[1].get("table").get("tr")
                # print(cur_dt)
                # print(market)
                category = category_map.get(market)
                # print(is_trading_day)
                # print(pprint.pformat(content))

                for row in content:
                    td = row.get("td")[0]
                    item = dict(zip(fields, td))

                    item.update({"Date": cur_dt, "CategoryCode": category})

                    for field in money_fields:
                        item[field] = self.re_money_data(item[field])

                    # 净买额 = 买入金额 - 卖出金额
                    item['TJME'] = item.get("Buy Turnover") - item.get('Sell Turnover')
                    # 买入金额
                    item.update({"TMRJE": item.get("Buy Turnover")})
                    # 成交金额 = 买入金额 + 卖出金额 (即 买入以及卖出金额 (RMB)）
                    item.update({"TCJJE": item.get("Total Turnover")})

                    item.pop("Rank")
                    item.pop("Stock Name")
                    item.pop("Sell Turnover")
                    item.pop("Buy Turnover")
                    item.pop("Total Turnover")

                    print(item)
                    items.append(item)
        else:
            print(resp)
            # 当天无数据时为 404


if __name__ == "__main__":
    etop10 = ExchangeTop10()
    etop10.start()
