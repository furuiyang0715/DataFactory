import datetime
import hashlib
import json
import logging
import re
import sys
import time
import traceback

from decimal import Decimal
import requests as req
sys.path.append("./../")

from hkland_flow.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER, SPIDER_MYSQL_PASSWORD,
                                 SPIDER_MYSQL_DB, PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_PORT, PRODUCT_MYSQL_USER,
                                 PRODUCT_MYSQL_PASSWORD, PRODUCT_MYSQL_DB)
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

    product_cfg = {
        "host": PRODUCT_MYSQL_HOST,
        "port": PRODUCT_MYSQL_PORT,
        "user": PRODUCT_MYSQL_USER,
        "password": PRODUCT_MYSQL_PASSWORD,
        "db": PRODUCT_MYSQL_DB,
    }

    def __init__(self):
        self.url = '''
        http://push2.eastmoney.com/api/qt/kamt.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55,f56&ut=b2884a393a59ad64002292a3e90d46a5&cb=jQuery18306854619522421488_1566280636697&_=1566284477196'''
        self.table_name = 'hkland_flow_eastmoney'
        self.tool_table_name = 'base_table_updatetime'
        self.today = datetime.datetime.today().strftime("%Y-%m-%d")

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

    def get_response_data(self):
        page = req.get(self.url).text
        data = re.findall(r"jQuery\d{20}_\d{13}\((.*)\)", page)[0]
        py_data = json.loads(data).get('data')
        return py_data

    def _check_if_trading_period(self):
        """判断是否是该天的交易时段"""
        _now = datetime.datetime.now()
        if (_now <= datetime.datetime(_now.year, _now.month, _now.day, 8, 0, 0) or
                _now >= datetime.datetime(_now.year, _now.month, _now.day, 16, 30, 0)):
            logger.warning("非当天交易时段")
            return False
        return True

    def select_n2s_datas(self):
        """获取已有的南向数据"""
        spider = self._init_pool(self.spider_cfg)
        start_dt = datetime.datetime.combine(datetime.datetime.now(), datetime.time.min)
        end_dt = datetime.datetime.combine(datetime.datetime.now(), datetime.time.max)
        sql = '''select * from {} where Category = 1 and DateTime >= '{}' and DateTime <= '{}';'''.format(
            self.table_name, start_dt, end_dt)
        south_datas = spider.select_all(sql)
        spider.dispose()
        for data in south_datas:
            data.pop("CREATETIMEJZ")
            data.pop("UPDATETIMEJZ")
        return south_datas

    def select_s2n_datas(self):
        """获取已有的北向数据"""
        spider = self._init_pool(self.spider_cfg)
        start_dt = datetime.datetime.combine(datetime.datetime.now(), datetime.time.min)
        end_dt = datetime.datetime.combine(datetime.datetime.now(), datetime.time.max)
        sql = '''select * from {} where Category = 2 and DateTime >= '{}' and DateTime <= '{}';'''.format(
            self.table_name, start_dt, end_dt)
        north_datas = spider.select_all(sql)
        spider.dispose()
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
            self._save(item,  self.table_name, update_fields)

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
            self._save(item, self.table_name, update_fields)

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

    def _save(self, to_insert, table, update_fields: list):
        spider = self._init_pool(self.spider_cfg)
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            count = spider.insert(insert_sql, values)
        except:
            traceback.print_exc()
            logger.warning("失败")
            count = None
        else:
            if count:
                logger.info("更入新数据 {}".format(to_insert))
        finally:
            spider.dispose()
        return count

    def _create_table(self):
        spider = self._init_pool(self.spider_cfg)
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
        spider.insert(sql)
        spider.end()

    def _start(self):
        is_trading = self._check_if_trading_period()
        if not is_trading:
            return

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
    now = lambda: time.time()

    # t1 = now()
    # eml = EMLGTNanBeiXiangZiJin()
    # eml._start()
    # print("Time-spider: {}".format(now() - t1))

    while True:
        t1 = now()
        eml = EMLGTNanBeiXiangZiJin()
        eml.start()
        print("Time-spider: {}".format(now() - t1))

        time.sleep(3)


'''deploy step 
docker build -f Dockerfile_eastmoney -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_eastmoney:v1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_eastmoney:v1 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_eastmoney:v1 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name flow_eastmoney --env LOCAL=0 registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_eastmoney:v1 
docker logs -ft --tail 1000 flow_eastmoney  


# local 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name flow_eastmoney registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_eastmoney:v1  
'''