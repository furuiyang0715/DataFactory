import copy
import datetime
import logging
import sys
import time
import traceback

import pandas as pd

from hkland_flow.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER,
                                 SPIDER_MYSQL_PASSWORD, SPIDER_MYSQL_DB, PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_PORT,
                                 PRODUCT_MYSQL_USER, PRODUCT_MYSQL_PASSWORD, PRODUCT_MYSQL_DB, DC_HOST, DC_PORT,
                                 DC_USER, DC_PASSWD, DC_DB)
from hkland_flow.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FlowMerge(object):
    dc_cfg = {
        "host": DC_HOST,
        "port": DC_PORT,
        "user": DC_USER,
        "password": DC_PASSWD,
        "db": DC_DB,
    }

    spider_cfg = {
        "host": SPIDER_MYSQL_HOST,
        "port": SPIDER_MYSQL_PORT,
        "user": SPIDER_MYSQL_USER,
        "password": SPIDER_MYSQL_PASSWORD,
        "db": SPIDER_MYSQL_DB,
    }

    product_cfg = {
        "host": PRODUCT_MYSQL_HOST,
        "port": PRODUCT_MYSQL_PORT,
        "user": PRODUCT_MYSQL_USER,
        "password": PRODUCT_MYSQL_PASSWORD,
        "db": PRODUCT_MYSQL_DB,
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

        self.product_table_name = 'hkland_flow'
        self.tool_table_name = 'base_table_updatetime'

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

    def select_already_datas(self, category, start_dt, end_dt):
        """获取已有的南北向数据"""
        product = self._init_pool(self.product_cfg)
        sql = '''select * from {} where Category = {} and DateTime >= '{}' and DateTime <= '{}';'''.format(
            self.product_table_name, category, start_dt, end_dt)
        _datas = product.select_all(sql)
        product.dispose()
        for data in _datas:
            data.pop("CREATETIMEJZ")
            data.pop("UPDATETIMEJZ")
            data.pop("id")
            data.pop("HashID")
            data.pop("CMFID")
            data.pop("CMFTime")
        return _datas

    def process_sql_datas(self, datas):
        _map = {}
        for data in datas:
            data.pop("id")
            data.pop("CREATETIMEJZ")
            data.pop("UPDATETIMEJZ")
            dt = data.get("DateTime")
            _map[str(dt)] = data

        return _map

    def _create_table(self):
        """创建正式库的表"""
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
          `HashID` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '哈希ID',
          `CMFID` bigint(20) unsigned DEFAULT NULL COMMENT '源表来源ID',
          `CMFTime` datetime DEFAULT NULL COMMENT 'Come From Time',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `unique_key2` (`DateTime`,`Category`),
          UNIQUE KEY `unique_key` (`CMFID`, `Category`),
          KEY `DateTime` (`DateTime`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆港通-实时资金流向';
        '''.format(self.product_table_name)

        # 创建工具表
        tool_sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `TableName` varchar(100) NOT NULL COMMENT '表名',
          `LastUpdateTime` datetime NOT NULL COMMENT '最后更新时间',
          `IsValid` tinyint(4) DEFAULT '1' COMMENT '是否有效',
          PRIMARY KEY (`id`),
          UNIQUE KEY `u1` (`TableName`) USING BTREE
        ) ENGINE=InnoDB AUTO_INCREMENT=18063 DEFAULT CHARSET=utf8 COMMENT='每个表的最后更新时间';
        '''.format(self.tool_table_name)

        product = self._init_pool(self.product_cfg)
        product.insert(sql)
        product.insert(tool_sql)  # 一般只执行一次
        product.dispose()

    def gen_all_minutes(self, start: datetime.datetime, end: datetime.datetime):
        """
        生成 start 和 end 之间全部分钟时间点列表 包含前后时间点
        """
        # print(start)
        # print(end)
        idx = pd.date_range(start=start, end=end, freq="min")
        # res = [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in idx]
        dt_list = [dt.to_pydatetime() for dt in idx]
        return dt_list

    def contract_sql(self, to_insert: dict, table: str, update_fields: list):
        ks = []
        vs = []
        for k in to_insert:
            ks.append(k)
            vs.append(to_insert.get(k))
        fields_str = "(" + ",".join(ks) + ")"
        values_str = "(" + "%s," * (len(vs) - 1) + "%s" + ")"
        base_sql = '''INSERT INTO `{}` '''.format(table) + fields_str + ''' values ''' + values_str
        on_update_sql = ''' ON DUPLICATE KEY UPDATE '''
        update_vs = []
        for update_field in update_fields:
            on_update_sql += '{}=%s,'.format(update_field)
            update_vs.append(to_insert.get(update_field))
        on_update_sql = on_update_sql.rstrip(",")
        sql = base_sql + on_update_sql + """;"""
        vs.extend(update_vs)
        return sql, tuple(vs)

    def save(self, to_insert, table, update_fields: list):
        product = self._init_pool(self.product_cfg)
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            count = product.insert(insert_sql, values)
        except:
            traceback.print_exc()
            logger.warning("失败")
            count = None
        else:
            if count:
                logger.info("更入新数据 {}".format(to_insert))
        finally:
            product.dispose()
        return count

    def north(self):
        """9:30-11:30; 13:00-15:00  (11:30-9:30)*60+1 + (15-13)*60+1 = 242"""
        morning_start = datetime.datetime(self.year, self.month, self.day, 9, 30, 0)
        morning_end = datetime.datetime(self.year, self.month, self.day, 11, 30, 0)
        afternoon_start = datetime.datetime(self.year, self.month, self.day, 13, 0, 0)
        afternoon_end = datetime.datetime(self.year, self.month, self.day, 15, 0, 0)
        this_moment = datetime.datetime.now()
        this_moment_min = datetime.datetime(this_moment.year, this_moment.month, this_moment.day,
                                            this_moment.hour, this_moment.minute, 0) + datetime.timedelta(minutes=self.offset)

        if this_moment_min < morning_start:
            logger.info("北向未开盘")
            return
        elif this_moment_min <= morning_end:
            dt_list = self.gen_all_minutes(morning_start, this_moment_min)
        elif this_moment_min < afternoon_start:
            dt_list = self.gen_all_minutes(morning_start, morning_end)
        elif this_moment_min <= afternoon_end:
            dt_list = self.gen_all_minutes(morning_start, morning_end)
            dt_list.extend(self.gen_all_minutes(afternoon_start, this_moment_min))
        elif this_moment_min > afternoon_end:
            dt_list = self.gen_all_minutes(morning_start, morning_end)
            dt_list.extend(self.gen_all_minutes(afternoon_start, afternoon_end))
        else:
            raise
        # for i in dt_list:
        #     print(i)

        exchange_north = self.fetch(self.exchange_table_name, morning_start, this_moment_min, 2)
        exchange_north_win = self.process_sql_datas(exchange_north)
        # for k in exchange_north_win:
        #     print(k, ">>> ", exchange_north_win[k])

        jqka_north = self.fetch(self.jqka_table_name, morning_start, this_moment_min, 2)
        jqka_north_win = self.process_sql_datas(jqka_north)
        # for k in jqka_north_win:
        #     print(k, ">>> ", jqka_north_win[k])

        eastmoney_north = self.fetch(self.eastmoney_table_name, morning_start, this_moment_min, 2)
        eastmoney_north_win = self.process_sql_datas(eastmoney_north)
        # for k in eastmoney_north_win:
        #     print(k, ">>> ", eastmoney_north_win[k])

        # 按照优先级别进行更新
        north_win = copy.deepcopy(exchange_north_win)
        north_win.update(jqka_north_win)
        north_win.update(eastmoney_north_win)

        north_df = pd.DataFrame(list(north_win.values()))
        north_df = north_df.set_index("DateTime",
                                      # drop=False
                                      )
        need_north_df = north_df.reindex(index=dt_list)
        need_north_df.replace({0: None}, inplace=True)
        need_north_df.fillna(method="ffill", inplace=True)
        need_north_df.reset_index("DateTime", inplace=True)
        need_north_df.sort_values(by="DateTime", ascending=True, inplace=True)
        datas = need_north_df.to_dict(orient='records')

        # # 首次
        # for data in datas:
        #     data.update({"DateTime": data.get("DateTime").to_pydatetime()})
        #     update_fields = ['DateTime', 'ShHkFlow', 'ShHkBalance', 'SzHkFlow', 'SzHkBalance', 'Netinflow', 'Category']
        #     self.save(data, self.product_table_name, update_fields)

        # 增量
        for data in datas:
            data.update({"DateTime": data.get("DateTime").to_pydatetime()})

        already_datas = self.select_already_datas(2, morning_start, this_moment_min)
        to_insert = []
        for data in datas:
            if not data in already_datas:
                to_insert.append(data)

        print(to_insert)
        print(len(to_insert))
        update_fields = ['DateTime', 'ShHkFlow', 'ShHkBalance', 'SzHkFlow', 'SzHkBalance', 'Netinflow', 'Category']
        for data in to_insert:
            self.save(data, self.product_table_name, update_fields)

    def south(self):
        """9:00-12:00; 13:00-16:10     (12-9)*60 + (16-13) *60 + 10 + 2 = 372"""
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
            dt_list = self.gen_all_minutes(morning_start, morning_end)
            dt_list.extend(self.gen_all_minutes(afternoon_start, this_moment_min))
        elif this_moment_min > afternoon_end:
            dt_list = self.gen_all_minutes(morning_start, morning_end)
            dt_list.extend(self.gen_all_minutes(afternoon_start, afternoon_end))
        else:
            raise
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
        # south_df.sort_values(by="DateTime", ascending=True, inplace=True)
        need_south_df = south_df.reindex(index=dt_list)
        need_south_df.replace({0: None}, inplace=True)
        need_south_df.fillna(method="ffill", inplace=True)
        need_south_df.reset_index("DateTime", inplace=True)
        need_south_df.sort_values(by="DateTime", ascending=True, inplace=True)
        datas = need_south_df.to_dict(orient='records')

        # # 首次
        # for data in datas:
        #     data.update({"DateTime": data.get("DateTime").to_pydatetime()})
        #     update_fields = ['DateTime', 'ShHkFlow', 'ShHkBalance', 'SzHkFlow', 'SzHkBalance', 'Netinflow', 'Category']
        #     self.save(data, self.product_table_name, update_fields)

        # 增量
        for data in datas:
            data.update({"DateTime": data.get("DateTime").to_pydatetime()})

        already_datas = self.select_already_datas(1, morning_start, this_moment_min)
        to_insert = []
        for data in datas:
            if not data in already_datas:
                to_insert.append(data)

        print(to_insert)
        print(len(to_insert))
        update_fields = ['DateTime', 'ShHkFlow', 'ShHkBalance', 'SzHkFlow', 'SzHkBalance', 'Netinflow', 'Category']
        for data in to_insert:
            self.save(data, self.product_table_name, update_fields)

    def _check_if_trading_today(self, category):
        '''
        self.category_map = {
            'hgtb': ('沪股通', 1),
            'ggtb': ('港股通(沪)', 2),
            'sgtb': ('深股通', 3),
            'ggtbs': ('港股通(深)', 4),
        }
        一般来说 1 3 与 2 4 是一致的
        '''
        dc = self._init_pool(self.dc_cfg)
        _map = {
            1: (2, 4),
            2: (1, 3),
        }

        sql = 'select IfTradingDay from hkland_shszhktradingday where TradingType in {} and EndDate = "{}";'.format(
        _map.get(category), self.today.strftime("%Y-%m-%d"))
        ret = dc.select_all(sql)
        ret = [r.get("IfTradingDay") for r in ret]
        if ret == [2, 2]:
            return False
        else:
            return True

    def _start(self):
        # 尝试建表
        self._create_table()
        # 首先判断今天南北向是否交易
        south_trade_bool = self._check_if_trading_today(1)
        if not south_trade_bool:
            logger.warning("今天{}南向无交易".format(self.today))
        else:
            self.south()

        north_trade_bool = self._check_if_trading_today(2)
        if not north_trade_bool:
            logger.warning("今天{}南向无交易".format(self.today))
        else:
            self.north()

    def start(self):
        try:
            self._start()
        except:
            traceback.print_exc()


if __name__ == "__main__":
    # flow = FlowMerge()
    # flow._start()

    now = lambda: time.time()
    while True:
        t1 = now()
        flow = FlowMerge()
        flow.start()
        logger.info("Time:{}s".format(now() - t1))
        time.sleep(5)
        print()
        print()
