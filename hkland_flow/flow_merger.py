import copy
import datetime
import logging

import pandas as pd

from hkland_flow.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER,
                                 SPIDER_MYSQL_PASSWORD, SPIDER_MYSQL_DB)
from hkland_flow.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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

    def gen_all_minutes(self, start: datetime.datetime, end: datetime.datetime):
        """
        生成 start 和 end 之间全部分钟时间点列表 包含前后时间点
        """
        idx = pd.date_range(start=start, end=end, freq="min")
        # res = [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in idx]
        dt_list = [dt.to_pydatetime() for dt in idx]
        return dt_list

    def south(self):
        """9-12; 13-16:10     (12-9)*60 + (16-13) *60 + 10 + 2 = 372"""
        morning_start = datetime.datetime(self.year, self.month, self.day, 9, 0, 0)
        morning_end = datetime.datetime(self.year, self.month, self.day, 12, 0, 0)
        afternoon_start = datetime.datetime(self.year, self.month, self.day, 13, 0, 0)
        afternoon_end = datetime.datetime(self.year, self.month, self.day, 16, 10, 0)
        this_moment = datetime.datetime.now()
        this_moment_min = datetime.datetime(this_moment.year, this_moment.month, this_moment.day,
                                            this_moment.hour, this_moment.minute, 0) + datetime.timedelta(minutes=self.offset)
        if this_moment_min < morning_start:
            logger.info("南向未开盘")
            return
        elif this_moment_min <= morning_end:
            dt_list = self.gen_all_minutes(morning_start, this_moment_min)
        elif this_moment_min < afternoon_start:
            dt_list = self.gen_all_minutes(morning_start, morning_end)
        elif this_moment_min <= afternoon_end:
            dt_list = self.gen_all_minutes(morning_start, morning_end).extend(self.gen_all_minutes(afternoon_start, this_moment_min))
        elif this_moment_min > afternoon_end:
            dt_list = self.gen_all_minutes(morning_start, morning_end).extend(self.gen_all_minutes(afternoon_start, afternoon_end))
        else:
            raise
        # print(dt_list)
        # complete_win = {str(dt): dict for dt in dt_list}
        # print(complete_win)

        exchange_south = self.fetch(self.exchange_table_name, morning_start, this_moment_min, 1)
        exchange_south_win = self.process_sql_datas(exchange_south)

        jqka_south = self.fetch(self.jqka_table_name, morning_start, this_moment_min, 1)
        jqka_south_win = self.process_sql_datas(jqka_south)

        eastmoney_south = self.fetch(self.eastmoney_table_name, morning_start, this_moment_min, 1)
        eastmoney_south_win = self.process_sql_datas(eastmoney_south)

        # 按照优先级别进行更新
        south_win = copy.deepcopy(exchange_south_win)
        south_win.update(jqka_south_win)
        south_win.update(eastmoney_south_win)

        south_df = pd.DataFrame(list(south_win.values()))
        south_df = south_df.set_index("DateTime",
                                      # drop=False
                                      )
        south_df.sort_values(by="DateTime", ascending=True, inplace=True)
        need_south_df = south_df.reindex(index=dt_list)
        need_south_df.replace({0: None}, inplace=True)
        need_south_df.fillna(method="ffill", inplace=True)

        # 将索引列恢复到某一列中


if __name__ == "__main__":
    flow = FlowMerge()
    flow.south()
