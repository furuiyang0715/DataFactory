# -*- coding: utf-8 -*-
import datetime
import json
import os
import sys
import time
import traceback
import urllib.parse
import requests
import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)
from hkland_historytradestat.configs import LOCAL
from hkland_historytradestat.base_spider import BaseSpider, logger


class EastMoneyHistory(BaseSpider):
    def __init__(self):
        super(EastMoneyHistory, self).__init__()
        self.headers = {
            'Referer': 'http://data.eastmoney.com/hsgt/index.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'
        }
        self.page_num = 10

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
        self._product_init()

        if LOCAL:
            self._create_table()

        js_dic = {'1': 'LjQxjkJV',
                  '3': 'waGmTUWT',
                  '2': 'ffVlSXoE',
                  '4': 'PLmMnPai'}
        category_lis = [
            '1',
            '2',
            '3',
            '4',
        ]
        jishu = []
        for category in category_lis:
            print()
            print(category)
            url1 = """http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get?type=HSGTHIS&token=70f12f2f4f091e459a279469fe49eca5&filter=(MarketType={})"""
            url2 = """&js=var%"""
            url3 ="""{%22data%22:(x),%22pages%22:(tp)}"""
            url4 = """&sr=-1&st=DetailDate&ps={}&p={}"""
            page = 0
            url = url1.format(category) + url2 + js_dic[category] + url3 + url4.format(self.page_num, page)
            datas = self._get_datas(url)
            if datas:
                # max_dt = datetime.datetime.strptime(datas[0].get("Date"), "%Y-%m-%dT%H:%M:%S")
                # min_dt = datetime.datetime.strptime(datas[-1].get("Date"), "%Y-%m-%dT%H:%M:%S")
                data = datas[0]
                ret = self._save(self.product_client, data, self.table_name, self.fields)
                if ret:
                    jishu.append(ret)

        self.refresh_update_time()
        if len(jishu) != 0:
            self.ding("【datacenter】当前的时间是{}, 数据库 {} 更入了 {} 条新数据".format(
                datetime.datetime.now(), self.table_name, len(jishu)))
        else:
            print("len of jishu:{}".format(len(jishu)))

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

                # 写死 临时兼容之前的程序
                item["CMFID"] = 1
                item['CMFTime'] = datetime.datetime.now()
                items.append(item)
        return items

    def start(self):
        count = 3
        while True:
            try:
                self._start()
            except Exception as e:
                count -= 1
                if count < 0:
                    self.ding("【datacenter】当前时间{}, 资金流向历史程序 {} 出错了, 错误原因是 {}".format(
                        datetime.datetime.now(), self.table_name, e))
                    time.sleep(30)
                else:
                    traceback.print_exc()
                    print("资金流向历史 程序失败, 重启.")
            else:
                break


def task():
    t_day = datetime.datetime.today()
    start_time = datetime.datetime(t_day.year, t_day.month, t_day.day, 17, 10, 0)
    end_time = datetime.datetime(t_day.year, t_day.month, t_day.day, 18, 0, 0)
    if not (t_day >= start_time and t_day <= end_time):
        logger.warning("不在 17:10 到 18:00 的更新时段内")
        return
    EastMoneyHistory().start()


def main():
    EastMoneyHistory().start()      # 确保临时重启时能运行一遍
    task()
    schedule.every(1).minutes.do(task)

    while True:
        print("当前调度系统中的任务列表是{}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(10)


if __name__ == "__main__":
    main()


'''
docker build -f Dockerfile_history -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_history:v1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_history:v1 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_history:v1 


# remote  
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name hkland_history \
--env LOCAL=0 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_history:v1 


# local 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name hkland_history \
--env LOCAL=1 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_history:v1 
'''
