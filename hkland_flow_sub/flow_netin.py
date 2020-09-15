import datetime
import json
import os
import re
import sys
import time
import traceback

from decimal import Decimal
import requests as req

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from hkland_flow_sub.configs import LOCAL
from hkland_flow_sub.flow_base import FlowBase, logger


class EastMoneyFlowNetIn(FlowBase):
    """陆股通资金净流入(东财数据源) """
    def __init__(self):
        super(EastMoneyFlowNetIn, self).__init__()
        # 资金净流入: http://push2.eastmoney.com/api/qt/kamt.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55,f56&ut=b2884a393a59ad64002292a3e90d46a5&cb=jQuery183041256596489447617_1600069691969&_=1600069692968
        # 资金净买额: http://push2.eastmoney.com/api/qt/kamtbs.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f54,f52,f58,f53,f62,f56,f57,f60,f61&ut=b2884a393a59ad64002292a3e90d46a5&cb=jQuery183041256596489447617_1600069691970&_=1600069692969
        # self.url = '''
        # http://push2.eastmoney.com/api/qt/kamt.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55,f56&ut=b2884a393a59ad64002292a3e90d46a5&cb=jQuery18306854619522421488_1566280636697&_=1566284477196'''

        self.url = '''http://push2.eastmoney.com/api/qt/kamtbs.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f54,f52,f58,f53,f62,f56,f57,f60,f61\
&ut=b2884a393a59ad64002292a3e90d46a5&cb=jQuery183041256596489447617_{}&_={}
        '''.format(int(time.time() * 1000), int(time.time() * 1000))
        self.table_name = 'hkland_flow_netin'
        self.today = datetime.datetime.today().strftime("%Y-%m-%d")
        self.update_fields = [
            'DateTime',
            'Category',
            'ShHkNetBuyAmount',  # '沪股通/港股通(沪)净买额（万）',
            'ShHkBuyAmount',  # '沪股通/港股通(沪) 买入额（万）',
            'ShHkSellAmount',  # '沪股通/港股通(沪) 卖出额（万）',
            'SzHkNetBuyAmount',  # '深股通/港股通(深)净买额（万）',
            'SzHkBuyAmount',  # '深股通/港股通(深) 买入额（万）',
            'SzHkSellAmount',  # '深股通/港股通(深) 卖出额（万）',
            'TotalNetBuyAmount',  # '北向/南向净买额（万）',
            'TotalBuyAmount',  # '北向/南向买入额（万）',
            'TotalSellAmount',  # '北向/南向卖出额（万）',
        ]

    def get_response_data(self):
        page = req.get(self.url).text
        data = re.findall(r"jQuery\d{21}_\d{13}\((.*)\)", page)[0]
        py_data = json.loads(data)
        datas = py_data.get("data")
        return datas

    def select_n2s_datas(self):
        """获取已有的南向数据"""
        start_dt = datetime.datetime.combine(datetime.datetime.now(), datetime.time.min)
        end_dt = datetime.datetime.combine(datetime.datetime.now(), datetime.time.max)
        sql = '''select * from {} where Category = 1 and DateTime >= '{}' and DateTime <= '{}';'''.format(
            self.table_name, start_dt, end_dt)
        south_datas = self.spider_client.select_all(sql)
        for data in south_datas:
            data.pop("CREATETIMEJZ")
            data.pop("UPDATETIMEJZ")
        return south_datas

    def select_s2n_datas(self):
        """获取已有的北向数据"""
        start_dt = datetime.datetime.combine(datetime.datetime.now(), datetime.time.min)
        end_dt = datetime.datetime.combine(datetime.datetime.now(), datetime.time.max)
        sql = '''select * from {} where Category = 2 and DateTime >= '{}' and DateTime <= '{}';'''.format(
            self.table_name, start_dt, end_dt)
        north_datas = self.spider_client.select_all(sql)
        for data in north_datas:
            data.pop("CREATETIMEJZ")
            data.pop("UPDATETIMEJZ")
        return north_datas

    def process_n2s(self, py_data):
        """处理陆港通南向数据"""
        n2s = py_data.get("n2s")
        n2s_date = py_data.get("n2sDate")
        logger.info("获取到的南向数据的时间是 {}".format(n2s_date))
        if n2s_date != self.today:
            logger.warning("今天无南向数据 ")
            return

        items = []
        '''370: "16:10,
        330128.90,    # 港股通(沪)净买额
        699372.74,    # 港股通(沪)买入额
        194224.89,    # 港股通(深)净买额
        369243.84,    # 港股通(沪)卖出额
        524353.79,    # 南向资金净买额
        551395.58,    # 港股通(深)买入额
        357170.69,    # 港股通(深)卖出额 
        1250768.32,   # 南向资金买入额 
        726414.53,    # 南向资金卖出额
        "
        
        '''
        for data_str in n2s:
            data = data_str.split(",")
            item = dict()
            dt_moment = n2s_date + " " + data[0]
            item['DateTime'] = datetime.datetime.strptime(dt_moment, "%Y-%m-%d %H:%M")  # 时间点 补全当天的完整时间

            # （1） `ShHkNetBuyAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪)净买额（万）',
            item['ShHkNetBuyAmount'] = Decimal(data[1]) if data[1] != "-" else 0
            # （2） `ShHkBuyAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪) 买入额（万）',
            item['ShHkBuyAmount'] = Decimal(data[2]) if data[2] != "-" else 0
            # （3） `SzHkNetBuyAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深)净买额（万）',
            item['SzHkNetBuyAmount'] = Decimal(data[3]) if data[3] != '-' else 0
            # （4） `ShHkSellAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪) 卖出额（万）',
            item['ShHkSellAmount'] = Decimal(data[4]) if data[4] != '-' else 0
            # （5） `TotalNetBuyAmount` DECIMAL(19,4) COMMENT '北向/南向净买额（万）',
            item['TotalNetBuyAmount'] = Decimal(data[5]) if data[5] != '-' else 0
            # （6） `SzHkBuyAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深) 买入额（万）',
            item['SzHkBuyAmount'] = Decimal(data[6]) if data[6] != '-' else 0
            # （7） `SzHkSellAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深) 卖出额（万）',
            item['SzHkSellAmount'] = Decimal(data[7]) if data[7] != '-' else 0
            # （8） `TotalBuyAmount` DECIMAL(19,4) COMMENT '北向/南向买入额（万）',
            item['TotalBuyAmount'] = Decimal(data[8]) if data[8] != '-' else 0
            # （9）`TotalSellAmount` DECIMAL(19,4) COMMENT '北向/南向卖出额（万）',
            item['TotalSellAmount'] = Decimal(data[9]) if data[9] != '-' else 0
            item['Category'] = 1
            items.append(item)

        to_delete = []
        to_insert = []
        already_sourth_datas = self.select_n2s_datas()
        for r in already_sourth_datas:
            d_id = r.pop("id")
            if not r in items:
                to_delete.append(d_id)

        for r in items:
            if not r in already_sourth_datas:
                to_insert.append(r)

        print(len(to_insert))
        for item in to_insert:
            self._save(self.spider_client, item,  self.table_name, self.update_fields)

    def process_s2n(self, py_data):
        """处理陆港通北向数据"""
        s2n = py_data.get("s2n")
        s2n_date = py_data.get("s2nDate")
        logger.info("获取到的北向数据的时间是 {}".format(s2n_date))
        if s2n_date != self.today:
            logger.warning("今天无北向数据")
            return

        items = []
        '''eg. 240: "15:00,
        -268362.94,   # (1) 沪股通净买额 
        1567811.34,   # (2) 沪股通买入额
        -45969.53,    # (3) 深股通净买额 
        1836174.28,   # (4) 沪股通卖出额
        -314332.47,   # (5) 北向资金净买额
        2651997.41,   # (6) 深股通买入额
        2697966.94,   # (7) 深股通卖出额
        4219808.75,   # (8) 北向资金买入额
        4534141.22"   # (9) 北向资金卖出额
                
        
        '''
        for data_str in s2n:
            data = data_str.split(",")
            item = dict()
            dt_moment = s2n_date + " " + data[0]
            item['Category'] = 2
            # 分钟时间点
            item['DateTime'] = datetime.datetime.strptime(dt_moment + ":00", "%Y-%m-%d %H:%M:%S")
            # （1） `ShHkNetBuyAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪)净买额（万）',
            item['ShHkNetBuyAmount'] = Decimal(data[1]) if data[1] != "-" else 0
            # （2） `ShHkBuyAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪) 买入额（万）',
            item['ShHkBuyAmount'] = Decimal(data[2]) if data[2] != "-" else 0
            # （3） `SzHkNetBuyAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深)净买额（万）',
            item['SzHkNetBuyAmount'] = Decimal(data[3]) if data[3] != '-' else 0
            # （4） `ShHkSellAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪) 卖出额（万）',
            item['ShHkSellAmount'] = Decimal(data[4]) if data[4] != '-' else 0
            # （5） `TotalNetBuyAmount` DECIMAL(19,4) COMMENT '北向/南向净买额（万）',
            item['TotalNetBuyAmount'] = Decimal(data[5]) if data[5] != '-' else 0
            # （6） `SzHkBuyAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深) 买入额（万）',
            item['SzHkBuyAmount'] = Decimal(data[6]) if data[6] != '-' else 0
            # （7） `SzHkSellAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深) 卖出额（万）',
            item['SzHkSellAmount'] = Decimal(data[7]) if data[7] != '-' else 0
            # （8） `TotalBuyAmount` DECIMAL(19,4) COMMENT '北向/南向买入额（万）',
            item['TotalBuyAmount'] = Decimal(data[8]) if data[8] != '-' else 0
            # （9）`TotalSellAmount` DECIMAL(19,4) COMMENT '北向/南向卖出额（万）',
            item['TotalSellAmount'] = Decimal(data[9]) if data[9] != '-' else 0
            items.append(item)

        to_delete = []
        to_insert = []

        already_north_datas = self.select_s2n_datas()
        for r in already_north_datas:
            d_id = r.pop("id")
            if not r in items:
                to_delete.append(d_id)

        for r in items:
            if not r in already_north_datas:
                to_insert.append(r)

        print(len(to_insert))
        for item in to_insert:
            self._save(self.spider_client, item, self.table_name, self.update_fields)

    def _create_table(self):
        sql = '''
         CREATE TABLE IF NOT EXISTS `{}` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `DateTime` datetime NOT NULL COMMENT '交易时间',
          `ShHkNetBuyAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪)净买额（万）',
          `ShHkBuyAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪) 买入额（万）',
          `ShHkSellAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪) 卖出额（万）',
          `SzHkNetBuyAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深)净买额（万）',
          `SzHkBuyAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深) 买入额（万）',
          `SzHkSellAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深) 卖出额（万）',
          `TotalNetBuyAmount` DECIMAL(19,4) COMMENT '北向/南向净买额（万）',
          `TotalBuyAmount` DECIMAL(19,4) COMMENT '北向/南向买入额（万）',
          `TotalSellAmount` DECIMAL(19,4) COMMENT '北向/南向卖出额（万）',
          `Category` tinyint(4) NOT NULL COMMENT '类别:1 南向, 2 北向',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `unique_key2` (`DateTime`,`Category`),
          KEY `DateTime` (`DateTime`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆港通-实时资金净买额-东财数据源';
        '''.format(self.table_name)
        self.spider_client.insert(sql)
        self.spider_client.end()

    def _start(self):
        is_trading = self._check_if_trading_period()
        if not is_trading:
            return

        self.spider_init()
        if LOCAL:
            self._create_table()

        py_data = self.get_response_data()

        logger.info("开始处理陆港通北向数据")
        self.process_s2n(py_data)

        logger.info("开始处理陆港通南向数据")
        self.process_n2s(py_data)

    def start(self):
        try:
            self._start()
        except:
            traceback.print_exc()


if __name__ == "__main__":
    EastMoneyFlowNetIn().start()

    while True:
        EastMoneyFlowNetIn().start()
        time.sleep(3)
