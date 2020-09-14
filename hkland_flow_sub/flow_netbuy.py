import datetime
import json
import logging
import re
import sys
import time
import traceback

from decimal import Decimal
import requests as req

sys.path.append("./../")
from hkland_flow.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER,
                                 SPIDER_MYSQL_PASSWORD, SPIDER_MYSQL_DB)
from hkland_flow.sql_pool import PyMysqlPoolBase


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EMLGTNanBeiXiangZiJin(object):
    """陆股通实时数据(东财数据源) """

    spider_cfg = {
        "host": SPIDER_MYSQL_HOST,
        "port": SPIDER_MYSQL_PORT,
        "user": SPIDER_MYSQL_USER,
        "password": SPIDER_MYSQL_PASSWORD,
        "db": SPIDER_MYSQL_DB,
    }

    def __init__(self):
        self.url = '''
        http://push2.eastmoney.com/api/qt/kamt.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55,f56&ut=b2884a393a59ad64002292a3e90d46a5&cb=jQuery18306854619522421488_1566280636697&_=1566284477196'''
        # 资金净流入: http://push2.eastmoney.com/api/qt/kamt.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55,f56&ut=b2884a393a59ad64002292a3e90d46a5&cb=jQuery183041256596489447617_1600069691969&_=1600069692968
        # 资金净买额: http://push2.eastmoney.com/api/qt/kamtbs.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f54,f52,f58,f53,f62,f56,f57,f60,f61&ut=b2884a393a59ad64002292a3e90d46a5&cb=jQuery183041256596489447617_1600069691970&_=1600069692969
        self.table_name = 'hkland_flow_eastmoney'
        self.today = datetime.datetime.today().strftime("%Y-%m-%d")
        self.spider_client = None

    def contract_sql(self, datas, table: str, update_fields: list):
        if not isinstance(datas, list):
            datas = [datas, ]

        to_insert = datas[0]
        ks = []
        vs = []
        for k in to_insert:
            ks.append(k)
            vs.append(to_insert.get(k))
        fields_str = "(" + ",".join(ks) + ")"
        values_str = "(" + "%s," * (len(vs) - 1) + "%s" + ")"
        base_sql = '''INSERT INTO `{}` '''.format(table) + fields_str + ''' values ''' + values_str

        params = []
        for data in datas:
            vs = []
            for k in ks:
                vs.append(data.get(k))
            params.append(vs)

        if update_fields:
            # https://stackoverflow.com/questions/12825232/python-execute-many-with-on-duplicate-key-update/12825529#12825529
            # sql = 'insert into A (id, last_date, count) values(%s, %s, %s) on duplicate key update last_date=values(last_date),count=count+values(count)'
            on_update_sql = ''' ON DUPLICATE KEY UPDATE '''
            for update_field in update_fields:
                on_update_sql += '{}=values({}),'.format(update_field, update_field)
            on_update_sql = on_update_sql.rstrip(",")
            sql = base_sql + on_update_sql + """;"""
        else:
            sql = base_sql + ";"
        return sql, params

    def _save(self, sql_pool, to_insert, table, update_fields):
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            value = values[0]
            count = sql_pool.insert(insert_sql, value)
        except:
            traceback.print_exc()
            logger.warning("失败")
        else:
            if count == 1:
                logger.info("插入新数据 {}".format(to_insert))
            elif count == 2:
                logger.info("刷新数据 {}".format(to_insert))
            else:
                logger.info("已有数据 {} ".format(to_insert))
            sql_pool.end()
            return count

    def _init_pool(self, cfg: dict):
        """
        eg.
        conf = {
                "host": LOCAL_MYSQL_HOST,
                "port": LOCAL_MYSQL_PORT,
                "user": LOCAL_MYSQL_USER,
                "password": LOCAL_MYSQL_PASSWORD,
                "db": LOCAL_MYSQL_DB,
        }
        :param cfg:
        :return:
        """
        pool = PyMysqlPoolBase(**cfg)
        return pool

    def spider_init(self):
        if not self.spider_client:
            self.spider_client = self._init_pool(self.spider_cfg)

    def __del__(self):
        if self.spider_client:
            self.spider_client.dispose()

    def get_response_data(self):
        page = req.get(self.url).text
        data = re.findall(r"jQuery\d{20}_\d{13}\((.*)\)", page)[0]
        py_data = json.loads(data).get('data')
        return py_data

    def _check_if_trading_period(self):
        """判断是否是该天的交易时段"""
        _now = datetime.datetime.now()
        if (_now <= datetime.datetime(_now.year, _now.month, _now.day, 9, 0, 0) or
                _now >= datetime.datetime(_now.year, _now.month, _now.day, 16, 30, 0)):
            logger.warning("非当天交易时段")
            return False
        return True

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

        update_fields = ['DateTime', 'ShHkFlow', 'ShHkBalance', 'SzHkFlow', 'SzHkBalance', 'Netinflow', 'Category']
        # print(items)
        # print(already_sourth_datas)
        print(len(to_insert))

        for item in to_insert:
            self._save(self.spider_client, item,  self.table_name, update_fields)

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

        update_fields = ['DateTime', 'ShHkFlow', 'ShHkBalance', 'SzHkFlow', 'SzHkBalance', 'Netinflow', 'Category']
        print(len(to_insert))

        for item in to_insert:
            self._save(self.spider_client, item, self.table_name, update_fields)

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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆港通-实时资金流向-东财数据源';
        '''.format(self.table_name)
        self.spider_client.insert(sql)
        self.spider_client.end()

    def _start(self):
        # 粗略判断是否是前天的交易时段
        is_trading = self._check_if_trading_period()
        if not is_trading:
            return

        # 建立数据库连接
        self.spider_init()

        # 建表
        self._create_table()

        # 请求并插入数据
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
    # eml = EMLGTNanBeiXiangZiJin()
    # eml._start()

    while True:
        eml = EMLGTNanBeiXiangZiJin()
        eml.start()
        time.sleep(3)
