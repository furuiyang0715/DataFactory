# -*- coding: utf-8 -*-
import base64
import datetime
import hashlib
import hmac
import json
import logging
import os
import sys
import time
import traceback
import urllib.parse

import requests
import schedule

sys.path.append("./../")

from hkland_historytradestat.configs import LOCAL, SECRET, TOKEN
from hkland_historytradestat.base_spider import BaseSpider
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


FIRST = int(os.environ.get("FIRST", 1))


class EMLgthisdspiderSpider(BaseSpider):
    def __init__(self):
        self.headers = {
            'Referer': 'http://data.eastmoney.com/hsgt/index.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'
        }
        self.page_num = 100
        self.table_name = "hkland_historytradestat"
        self.tool_table_name = 'base_table_updatetime'

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
            print(category)
            url1 = """http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get?type=HSGTHIS&token=70f12f2f4f091e459a279469fe49eca5&filter=(MarketType={})"""
            url2 = """&js=var%"""
            url3 ="""{%22data%22:(x),%22pages%22:(tp)}"""
            url4 = """&sr=-1&st=DetailDate&ps={}&p={}"""

            for page in range(100):
                print("page is {}".format(page))
                url = url1.format(category) + url2 + js_dic[category] + url3 + url4.format(self.page_num, page)
                # TODO  请求失败重试的问题
                datas = self._get_datas(url)
                if datas:
                    max_dt = datetime.datetime.strptime(datas[0].get("Date"), "%Y-%m-%dT%H:%M:%S")
                    min_dt = datetime.datetime.strptime(datas[-1].get("Date"), "%Y-%m-%dT%H:%M:%S")
                    print(min_dt)
                    print(max_dt)
                    ret = self.save_many(datas)
                    jishu.extend(ret)
                    if not FIRST:
                        # FIXME 暂定: 历史数据往前回溯 10 天
                        if max_dt < datetime.datetime.today() - datetime.timedelta(days=10):
                            print("SAVE OVER")
                            break
                else:
                    break
        self.refresh_update_time()

        if len(jishu) != 0:
            self.ding("【datacenter】当前的时间是{}, 数据库 {} 更入了 {} 条新数据".format(
                datetime.datetime.now(), self.table_name, len(jishu)))
        else:
            print("len of jishu:{}".format(len(jishu)))

    def ding(self, msg):
        def get_url():
            timestamp = str(round(time.time() * 1000))
            secret_enc = SECRET.encode('utf-8')
            string_to_sign = '{}\n{}'.format(timestamp, SECRET)
            string_to_sign_enc = string_to_sign.encode('utf-8')
            hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
            url = 'https://oapi.dingtalk.com/robot/send?access_token={}&timestamp={}&sign={}'.format(
                TOKEN, timestamp, sign)
            return url

        url = get_url()
        header = {
            "Content-Type": "application/json",
            "Charset": "UTF-8"
        }
        message = {
            "msgtype": "text",
            "text": {
                "content": "{}@15626046299".format(msg)
            },
            "at": {
                "atMobiles": [
                    "15626046299",
                ],
                "isAtAll": False
            }
        }
        message_json = json.dumps(message)
        resp = requests.post(url=url, data=message_json, headers=header)
        if resp.status_code == 200:
            pass
        else:
            logger.warning("钉钉消息发送失败")

    def refresh_update_time(self):
        product = self._init_pool(self.product_cfg)
        sql = '''select max(UPDATETIMEJZ) as max_dt from {}; '''.format(self.table_name)
        max_dt = product.select_one(sql).get("max_dt")
        logger.info("最新的更新时间是{}".format(max_dt))

        refresh_sql = '''replace into {} (id,TableName, LastUpdateTime,IsValid) values (4, "hkland_historytradestat", '{}', 1); 
        '''.format(self.tool_table_name, max_dt)
        count = product.update(refresh_sql)
        logger.info(count)   # 1 首次插入 2 替换插入
        product.dispose()

    def save(self, client, to_insert, table, update_fields: list):
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            count = client.insert(insert_sql, values)
        except:
            traceback.print_exc()
            logger.warning("失败")
            count = None
        else:
            if count:
                logger.info("更入新数据 {}".format(to_insert))
        finally:
            client.end()
        return count

    def save_many(self, datas):
        #  Date                | MoneyIn    | MoneyBalance | MoneyInHistoryTotal | NetBuyAmount | BuyAmount  | SellAmount | MarketTypeCode | MarketType
        update_fields = ['Date', "MoneyIn", "MoneyBalance", "MoneyInHistoryTotal", "NetBuyAmount", "BuyAmount",
                         "SellAmount", "MarketType", "MarketTypeCode"]
        jishu = []
        product = self._init_pool(self.product_cfg)
        for data in datas:
            ret = self.save(product, data, self.table_name, update_fields)
            if ret == 1:
                jishu.append(ret)
        product.dispose()
        return jishu

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
                    # break # TODO 出错就一直 ding
                    time.sleep(3)
                else:
                    traceback.print_exc()
                    print("资金流向历史 程序失败, 重启.")
            else:
                break


def task():
    t_day = datetime.datetime.today()

    start_time = datetime.datetime(t_day.year, t_day.month, t_day.day, 16, 10, 0)
    end_time = datetime.datetime(t_day.year, t_day.month, t_day.day, 20, 0, 0)

    if not (t_day >= start_time and t_day <= end_time):
        logger.warning("不在 16:10 到 20:00 的更新时段内")
        return

    his = EMLgthisdspiderSpider()
    his.start()


def main():
    task()
    schedule.every(10).minutes.do(task)

    while True:
        print("当前调度系统中的任务列表是{}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(180)


if __name__ == "__main__":

    main()


'''
docker build -f Dockerfile_history -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_history:v1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_history:v1 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_history:v1 


# remote  
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name hkland_history \
--env LOCAL=0 \
--env FIRST=0 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_history:v1 


# local 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name hkland_history \
--env LOCAL=1 \
--env FIRST=0 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_history:v1 
'''
