# -*- coding: utf-8 -*-
import datetime
import json
import os
import re
import sys
import time
import traceback

import requests
import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from hkland_toptrade.base_spider import BaseSpider, logger
from hkland_toptrade.configs import LOCAL


class EastMoneyTop10(BaseSpider):
    """十大成交股 东财数据源 """
    def __init__(self, day: str):
        super(EastMoneyTop10, self).__init__()
        self.headers = {
            'Referer': 'http://data.eastmoney.com/hsgt/top10.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        }
        self.day = day    # datetime.datetime.strftime("%Y-%m-%d")
        self.url = 'http://data.eastmoney.com/hsgt/top10/{}.html'.format(day)

    def _get_inner_code_map(self, market_type):
        """https://dd.gildata.com/#/tableShow/27/column///
           https://dd.gildata.com/#/tableShow/718/column///
        """
        if market_type in ("sh", "sz"):
            sql = 'SELECT SecuCode,InnerCode from SecuMain WHERE SecuCategory in (1, 2) and SecuMarket in (83, 90) and ListedSector in (1, 2, 6, 7);'
        else:
            sql = '''SELECT SecuCode,InnerCode from hk_secumain WHERE SecuCategory in (51, 3, 53, 78) and SecuMarket in (72) and ListedSector in (1, 2, 6, 7);'''
        ret = self.juyuan_client.select_all(sql)
        info = {}
        for r in ret:
            key = r.get("SecuCode")
            value = r.get('InnerCode')
            info[key] = value
        return info

    def _start(self):
        # 检查当前是否是交易日
        is_trading_day = self._check_if_trading_today(2, self.day)
        print("{} 是否交易日 : {} ".format(self.day, is_trading_day))

        if not is_trading_day:
            print("{} 非交易日 ".format(self.day))
            return

        self._juyuan_init()
        self._product_init()

        if LOCAL:
            self._create_table()

        resp = requests.get(self.url, headers=self.headers)
        if resp.status_code == 200:
            body = resp.text
            # 沪股通十大成交股
            data1 = re.findall('var DATA1 = (.*);', body)[0]
            # 深股通十大成交股
            data2 = re.findall('var DATA2 = (.*);', body)[0]
            # 港股通(沪)十大成交股
            data3 = re.findall('var DATA3 = (.*);', body)[0]
            # 港股通(深)十大成交股
            data4 = re.findall('var DATA4 = (.*);', body)[0]

            sh_innercode_map = self._get_inner_code_map("sh")
            sz_innercode_map = self._get_inner_code_map("sz")
            hk_innercode_map = self._get_inner_code_map("hk")

            jishu = []
            for data in [data1, data2, data3, data4]:
                data = json.loads(data)
                top_datas = data.get("data")
                print(top_datas)
                for top_data in top_datas:
                    item = dict()
                    item['Date'] = self.day   # 时间
                    secu_code = top_data.get("Code")
                    item['SecuCode'] = secu_code  # 证券代码
                    item['SecuAbbr'] = top_data.get("Name")   # 证券简称
                    item['Close'] = top_data.get('Close')  # 收盘价
                    item['ChangePercent'] = top_data.get('ChangePercent')  # 涨跌幅
                    item['CMFID'] = 1  # 兼容之前的程序 写死
                    item['CMFTime'] = datetime.datetime.now()   # 兼容和之前的程序 用当前的时间代替
                    # '类别代码:GGh: 港股通(沪), GGs: 港股通(深), HG: 沪股通, SG: 深股通',
                    if top_data['MarketType'] == 1.0:
                        item['CategoryCode'] = 'HG'
                        # item['Category'] = '沪股通'
                        # 净买额
                        item['TJME'] = top_data['HGTJME']
                        # 买入金额
                        item['TMRJE'] = top_data['HGTMRJE']
                        # # 卖出金额
                        # item['TMCJE'] = top_data['HGTMCJE']
                        # 成交金额
                        item['TCJJE'] = top_data['HGTCJJE']
                        item['InnerCode'] = sh_innercode_map.get(secu_code)

                    elif top_data['MarketType'] == 2.0:
                        item['CategoryCode'] = 'GGh'
                        # item['Category'] = '港股通(沪)'
                        # 港股通(沪)净买额(港元）
                        item['TJME'] = top_data['GGTHJME']
                        # 港股通(沪)买入金额(港元）
                        item['TMRJE'] = top_data['GGTHMRJE']
                        # # 港股通(沪)卖出金额(港元）
                        # item['TMCJE'] = top_data['GGTHMCJE']
                        # 港股通(沪)成交金额(港元）
                        item['TCJJE'] = top_data['GGTHCJJE']
                        item['InnerCode'] = hk_innercode_map.get(secu_code)

                    elif top_data['MarketType'] == 3.0:
                        item['CategoryCode'] = 'SG'
                        # item['Category'] = '深股通'
                        # 净买额
                        item['TJME'] = top_data['SGTJME']
                        # 买入金额
                        item['TMRJE'] = top_data['SGTMRJE']
                        # # 卖出金额
                        # item['TMCJE'] = top_data['SGTMCJE']
                        # 成交金额
                        item['TCJJE'] = top_data['SGTCJJE']
                        item['InnerCode'] = sz_innercode_map.get(secu_code)

                    elif top_data['MarketType'] == 4.0:
                        item['CategoryCode'] = 'GGs'
                        # item['Category'] = '港股通(深)'
                        # 港股通(沪)净买额(港元）
                        item['TJME'] = top_data['GGTSJME']
                        # 港股通(沪)买入金额(港元）
                        item['TMRJE'] = top_data['GGTSMRJE']
                        # # 港股通(沪)卖出金额(港元）
                        # item['TMCJE'] = top_data['GGTSMCJE']
                        # 港股通(沪)成交金额(港元）
                        item['TCJJE'] = top_data['GGTSCJJE']
                        item['InnerCode'] = hk_innercode_map.get(secu_code)

                    else:
                        raise
                    ret = self._save(self.product_client, item, self.table_name, self.fields)
                    if ret == 1:
                        jishu.append(ret)
            if len(jishu) != 0:
                self.ding("【datacenter】当前的时间是{}, 数据库 {} 更入了 {} 条新数据".format(
                    datetime.datetime.now(), self.table_name, len(jishu)))

        self.refresh_update_time()

    def start(self):
        count = 0
        while True:
            try:
                self._start()
            except Exception as e:
                count += 1
                if count > 5:
                    self.ding("【datacenter】当前时间{}, 十大成交股程序 {} 第 {} 次尝试出错了, "
                              "错误原因是 {}".format(datetime.datetime.now(), self.table_name, count, e))
                    traceback.print_exc()
                    time.sleep(30)
                else:
                    print("十大成交股爬取程序失败, 重启.")
            else:
                break


def schedule_task():
    # 设置时间段的目的是 在非更新时间内 不去无谓的请求
    t_day = datetime.datetime.today()
    start_time = datetime.datetime(t_day.year, t_day.month, t_day.day, 17, 10, 0)
    end_time = datetime.datetime(t_day.year, t_day.month, t_day.day, 20, 10, 0)
    if not (t_day >= start_time and t_day <= end_time):
        logger.warning("不在 17:10 到 20:10 的更新时段内")
        return

    day_str = t_day.strftime("%Y-%m-%d")
    print("今天:", day_str)  # 今天的时间字符串 如果当前还未出 "十大成交股"数据 返回空列表
    EastMoneyTop10(day_str).start()


def main():
    # TODO first, 在容器启动时,无论时间, 进行一次重启.
    EastMoneyTop10(datetime.datetime.today().strftime("%Y-%m-%d")).start()

    schedule_task()
    schedule.every(2).minutes.do(schedule_task)
    while True:
        schedule.run_pending()
        time.sleep(10)


if __name__ == "__main__":

    main()


'''
docker build -f Dockerfile_top -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade:v1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade:v1
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade:v1


# remote
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name toptrade \
--env LOCAL=0 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade:v1

# local
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name toptrade \
--env LOCAL=1 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade:v1
'''