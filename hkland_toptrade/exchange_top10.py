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

import datetime
import os
import pprint
import sys
import time
import traceback

import requests
import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)


from hkland_toptrade.base_spider import BaseSpider, logger


class ExchangeTop10(BaseSpider):

    def __init__(self):
        super(ExchangeTop10, self).__init__()
        self.info = '交易所十大成交股:\n'
        self.web_url = 'https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Statistics/Historical-Daily?sc_lang=zh-HK#select4=1&select5=0&select3=0&select1=16&select2=5'
        _today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
        self.dt_str = _today.strftime("%Y%m%d")
        # TODO test
        # self.dt_str = '20200618'
        self.url = 'https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_{}c.js?_={}'.format(self.dt_str, int(time.time()*1000))
        #  id | Date | SecuCode | InnerCode | SecuAbbr | Close | ChangePercent | TJME | TMRJE | TCJJE | CategoryCode | CMFID | CMFTime | CREATETIMEJZ | UPDATETIMEJZ
        self.fields = ['Date', 'SecuCode', 'InnerCode', 'SecuAbbr',
                       # 'Close', 'ChangePercent',
                       'TJME', 'TMRJE', 'TCJJE', 'CategoryCode', ]
        #  alter table hkland_toptrade modify `Close` decimal(19,3) default  NULL COMMENT '收盘价'
        #  alter table hkland_toptrade modify `ChangePercent` decimal(19,5) default  NULL COMMENT '涨跌幅'


    @staticmethod
    def re_money_data(data: str):
        data = float(data.replace(",", ""))
        return data

    def start(self):
        logger.info(self.url)

        resp = requests.get(self.url)

        if resp.status_code == 200:
            body = resp.text
            datas_str = body.replace("tabData = ", "")
            try:
                datas = eval(datas_str)
            except:
                traceback.print_exc()
                return
            print(pprint.pformat(datas))
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
                print()
                print()
                items = []
                cur_dt = direction_data.get("date")
                market = direction_data.get("market")
                if market not in category_map:
                    continue
                is_trading_day = direction_data.get("tradingDay")
                content = direction_data.get("content")[1].get("table").get("tr")
                category = category_map.get(market)

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

                    # 移除不需要的字段
                    item.pop("Rank")
                    item.pop("Stock Name")
                    item.pop("Sell Turnover")
                    item.pop("Buy Turnover")
                    item.pop("Total Turnover")

                    # TODO  增加收盘价以及涨跌幅字段 暂时不做这两个字段 等待东财更新覆盖

                    if category == "SG":  # 要将深股通的证券编码补充为 6 位的
                        secu_code = "0" * (6 - len(item["Stock Code"])) + item["Stock Code"]
                        item['SecuCode'] = secu_code
                        item["InnerCode"], item['SecuAbbr'] = self.get_juyuan_codeinfo(secu_code)
                    elif category == "HG":     # 沪股通
                        secu_code = item["Stock Code"]
                        item['SecuCode'] = secu_code
                        item["InnerCode"], item['SecuAbbr'] = self.get_juyuan_codeinfo(secu_code)
                    elif category in ('GGh', 'GGs'):   # 港股
                        secu_code = item["Stock Code"]
                        item['SecuCode'] = secu_code
                        item["InnerCode"], item['SecuAbbr'] = self.get_juyuan_hkcodeinfo(secu_code)
                    item.pop("Stock Code")
                    item['CMFID'] = 1
                    item['CMFTime'] = datetime.datetime.now()
                    print(item)
                    items.append(item)

                self._product_init()
                count = self._batch_save(self.product_client, items, self.table_name, self.fields)
                self.info += "{}批量插入{}条\n".format(category, count)

            self.ding(self.info)
            self.refresh_update_time()

        else:
            print(resp)
            logger.warning("{} 当天非交易日或尚无十大成交数据".format(self.dt_str))
            # 当天无数据时为 404


def task():
    ExchangeTop10().start()

    _now = datetime.datetime.now()
    _year, _month, _day = _now.year, _now.month, _now.day
    _start = datetime.datetime(_year, _month, _day, 16, 0, 0)
    _end = datetime.datetime(_year, _month, _day, 19, 0, 0)

    if _now < _start or _now > _end:
        logger.warning("当前时间 {}, 不在正常的更新时间下午 4 点到 7 点之间".format(_now))
        return

    ExchangeTop10().start()


if __name__ == "__main__":
    task()
    # schedule.every(1).minutes.do(task)
    #
    # while True:
    #     schedule.run_pending()
    #     time.sleep(10)


'''
docker build -f Dockerfile_exchangetop -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade_exchange:v1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade_exchange:v1
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade_exchange:v1


# remote 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name toptrade_exchange \
--env LOCAL=0 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade_exchange:v1

# local
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name toptrade_exchange \
--env LOCAL=1 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade_exchange:v1 

'''