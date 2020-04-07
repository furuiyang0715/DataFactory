import copy
import datetime

import pandas as pd

from hkland_flow.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER,
                                 SPIDER_MYSQL_PASSWORD, SPIDER_MYSQL_DB)
from hkland_flow.sql_pool import PyMysqlPoolBase


class FlowMerge(object):
    spider_cfg = {
        "host": SPIDER_MYSQL_HOST,
        "port": SPIDER_MYSQL_PORT,
        "user": SPIDER_MYSQL_USER,
        "password": SPIDER_MYSQL_PASSWORD,
        "db": SPIDER_MYSQL_DB,
    }

    def __init__(self):
        self.today = datetime.datetime.today()
        self.year = self.today.year
        self.month = self.today.month
        self.day = self.today.day
        self.offset = 1

        self.exchange_table_name = 'hkland_flow_exchange'
        self.eastmoney_table_name = 'hkland_flow_eastmoney'
        self.jqka_table_name = 'hkland_flow_jqka10'

    def _init_pool(self, cfg: dict):
        pool = PyMysqlPoolBase(**cfg)
        return pool

    def fetch(self, table_name, start, end, category):
        spider = self._init_pool(self.spider_cfg)
        sql = '''select * from {} where  Category = {} and DateTime >= '{}' and DateTime <= '{}';'''.format(
            table_name, category, start, end)
        ret = spider.select_all(sql)
        spider.dispose()
        return ret

    def process_sql_datas(self, datas):
        _map = {}
        for data in datas:
            data.pop("id")
            data.pop("CREATETIMEJZ")
            data.pop("UPDATETIMEJZ")
            dt = data.get("DateTime")
            _map[str(dt)] = data

        return _map

    def south(self):
        # 9-12; 13-16:10     (12-9)*60 + (16-13) *60 + 10 + 2 = 372
        start_time = datetime.datetime(self.year, self.month, self.day, 9, 0, 0)

        this_moment = datetime.datetime.now()
        end_time = datetime.datetime(this_moment.year, this_moment.month, this_moment.day,
                                     this_moment.hour, this_moment.minute, 0) + datetime.timedelta(minutes=self.offset)

        exchange_south = self.fetch(self.exchange_table_name, start_time, end_time, 1)
        exchange_south_win = self.process_sql_datas(exchange_south)

        jqka_south = self.fetch(self.jqka_table_name, start_time, end_time, 1)
        jqka_south_win = self.process_sql_datas(jqka_south)

        eastmoney_south = self.fetch(self.eastmoney_table_name, start_time, end_time, 1)
        eastmoney_south_win = self.process_sql_datas(eastmoney_south)

        # 按照优先级别进行更新
        south_win = copy.deepcopy(exchange_south_win)
        south_win.update(jqka_south_win)
        south_win.update(eastmoney_south_win)

        # 前向填充
        south_df = pd.DataFrame(list(south_win.values()))

        print(south_df)

        # 按照交易时间进行截取


if __name__ == "__main__":
    flow = FlowMerge()
    flow.south()
