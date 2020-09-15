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

from hkland_flow_sub.flow_base import FlowBase, logger
from hkland_flow_sub.configs import LOCAL


class EastMoneyFlowNetBuy(FlowBase):
    """陆股通资金净买额(东财数据源) """
    def __init__(self):
        super(EastMoneyFlowNetBuy, self).__init__()
        self.url = '''http://push2.eastmoney.com/api/qt/kamt.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55,f56\
&ut=b2884a393a59ad64002292a3e90d46a5&cb=jQuery18306854619522421488_{}&_={}'''.format(int(time.time()*1000), int(time.time()*1000))
        self.table_name = 'hkland_flow_netbuy'
        self.today = datetime.datetime.today().strftime("%Y-%m-%d")
        self.update_fields = [
            'DateTime',
            'Category',
            'ShHkFlow',
            'ShHkBalance',
            'SzHkFlow',
            'SzHkBalance',
            'Netinflow',
        ]

    def get_response_data(self):
        page = req.get(self.url).text
        data = re.findall(r"jQuery\d{20}_\d{13}\((.*)\)", page)[0]
        py_data = json.loads(data).get('data')
        return py_data

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
        _cur_year = datetime.datetime.now().year   # FIXME
        _cur_moment_str = str(_cur_year) + "-" + n2s_date
        logger.info("获取到的南向数据的时间是 {}".format(_cur_moment_str))
        if _cur_moment_str != self.today:
            logger.warning("今天无南向数据 ")
            return

        items = []
        for data_str in n2s:
            data = data_str.split(",")
            item = dict()
            dt_moment = _cur_moment_str + " " + data[0]
            item['DateTime'] = datetime.datetime.strptime(dt_moment, "%Y-%m-%d %H:%M")  # 时间点 补全当天的完整时间
            item['ShHkFlow'] = Decimal(data[1]) if data[1] != '-' else 0  # 港股通（沪）南向资金流
            item['ShHkBalance'] = Decimal(data[2]) if data[2] != "-" else 0  # 港股通(沪) 当日资金余额
            item['SzHkFlow'] = Decimal(data[3]) if data[3] != "-" else 0  # 港股通(深) 南向资金流
            item['SzHkBalance'] = Decimal(data[4]) if data[4] != "-" else 0  # 港股通(深) 当日资金余额
            item['Netinflow'] = Decimal(data[5]) if data[5] != "-" else 0  # 南向资金
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
        _cur_year = datetime.datetime.now().year   # FIXME
        _cur_moment_str = str(_cur_year) + "-" + s2n_date
        logger.info("获取到的北向数据的时间是 {}".format(_cur_moment_str))
        if _cur_moment_str != self.today:
            logger.warning("今天无北向数据")
            return

        items = []
        for data_str in s2n:
            data = data_str.split(",")
            item = dict()
            dt_moment = _cur_moment_str + " " + data[0]
            # 分钟时间点
            item['DateTime'] = datetime.datetime.strptime(dt_moment + ":00", "%Y-%m-%d %H:%M:%S")
            # 沪股通/港股通(沪)当日资金流向(万）北向是沪股通 南向时是港股通(沪）
            item['ShHkFlow'] = Decimal(data[1]) if data[1] != "-" else 0
            # 沪股通/港股通(沪)当日资金余额（万）
            item['ShHkBalance'] = Decimal(data[2]) if data[2] != "-" else 0
            # 深股通/港股通(深)当日资金流向(万）
            item['SzHkFlow'] = Decimal(data[3]) if data[3] != '-' else 0
            # 深股通/港股通(深)当日资金余额（万）
            item['SzHkBalance'] = Decimal(data[4]) if data[4] != '-' else 0
            # 南北向资金,当日净流入
            item['Netinflow'] = Decimal(data[5]) if data[5] != '-' else 0
            # 类别
            item['Category'] = 2    # 1 南  2 北
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
          `ShHkFlow` decimal(19,4) NOT NULL COMMENT '沪股通/港股通(沪)当日资金流向(万）',
          `ShHkBalance` decimal(19,4) NOT NULL COMMENT '沪股通/港股通(沪)当日资金余额（万）',
          `SzHkFlow` decimal(19,4) NOT NULL COMMENT '深股通/港股通(深)当日资金流向(万）',
          `SzHkBalance` decimal(19,4) NOT NULL COMMENT '深股通/港股通(深)当日资金余额（万）',
          `Netinflow` decimal(19,4) NOT NULL COMMENT '南北向资金,当日净流入',
          `Category` tinyint(4) NOT NULL COMMENT '类别:1 南向, 2 北向',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `unique_key2` (`DateTime`,`Category`),
          KEY `DateTime` (`DateTime`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆股通资金净流入-东财数据源';
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
    EastMoneyFlowNetBuy().start()

    while True:
        EastMoneyFlowNetBuy().start()
        time.sleep(3)
