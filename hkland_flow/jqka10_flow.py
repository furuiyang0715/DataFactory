import datetime
import logging
import re
import time
import traceback
from decimal import Decimal

import execjs
import requests
import sys
sys.path.append("./../")

from hkland_flow.configs import DC_HOST, DC_PORT, DC_USER, DC_DB, DC_PASSWD, SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, \
    SPIDER_MYSQL_USER, SPIDER_MYSQL_PASSWORD, SPIDER_MYSQL_DB
from hkland_flow.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SFLgthisdataspiderSpider(object):
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

    def __init__(self):
        self.headers = {
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            # 'hexin-v': '',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
            'Accept': 'text/html, */*; q=0.01',
            'Referer': 'http://data.10jqka.com.cn/hgt/ggtb/',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
        }
        self.base_url = 'http://data.10jqka.com.cn/hgt/{}/'
        # 1-沪股通 2-港股通（沪）3-深股通，4-港股通（深）5-港股通（沪深）
        self.category_map = {
            'hgtb': ('沪股通', 1),
            'ggtb': ('港股通(沪)', 2),
            'sgtb': ('深股通', 3),
            'ggtbs': ('港股通(深)', 4),
        }
        self.north_map = {
            'hgtb': ('沪股通', 1),
            'sgtb': ('深股通', 3),
        }
        self.south_map = {
            'ggtb': ('港股通(沪)', 2),
            'ggtbs': ('港股通(深)', 4),
        }
        self.today = datetime.datetime.today().strftime("%Y-%m-%d")
        self.table_name = 'hkland_flow_jqka10'
        self.dc_client = None
        self.spider_client = None

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

    def dc_init(self):
        if not self.dc_client:
            self.dc_client = self._init_pool(self.dc_cfg)

    def spider_init(self):
        if not self.spider_client:
            self.spider_client = self._init_pool(self.spider_cfg)

    def __del__(self):
        if self.dc_client:
            self.dc_client.dispose()
        if self.spider_client:
            self.spider_client.dispose()

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

    # def _batch_save(self, sql_pool, to_inserts, table, update_fields):
    #     try:
    #         sql, values = self.contract_sql(to_inserts, table, update_fields)
    #         count = sql_pool.insert_many(sql, values)
    #     except:
    #         traceback.print_exc()
    #         logger.warning("失败")
    #     else:
    #         logger.info("批量插入的数量是{}".format(count))
    #         sql_pool.end()
    #         return count

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

    @property
    def cookies(self):
        with open('jqka.js', 'r') as f:
            jscont = f.read()
        cont = execjs.compile(jscont)
        cookie_v = cont.call('v')
        cookies = {
            'v': cookie_v,
        }
        return cookies

    def get(self, url):
        resp = requests.get(url, headers=self.headers, cookies=self.cookies)
        if resp.status_code == 200:
            return resp.text

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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆港通-实时资金流向-同花顺数据源';
        '''.format(self.table_name)
        self.spider_client.insert(sql)
        self.spider_client.end()

    def _check_if_trading_period(self, direction):
        """判断是否是该天的交易时段
        北向数据的时间是 9:30-11:30; 13:00-15:00
        南向数据的时间是 9:00-12:00; 13:00-16:10
        """
        _now = datetime.datetime.now()
        if direction == "north":
            morning_start = datetime.datetime(_now.year, _now.month, _now.day, 9, 30, 0)
            morning_end = datetime.datetime(_now.year, _now.month, _now.day, 11, 30, 0)
            afternoon_start = datetime.datetime(_now.year, _now.month, _now.day, 13, 0, 0)
            # 兼容同花顺的数据延迟问题
            # afternoon_end = datetime.datetime(_now.year, _now.month, _now.day, 15, 0, 0)
            afternoon_end = datetime.datetime(_now.year, _now.month, _now.day, 15, 10, 0)

        elif direction == "sourth":
            # morning_start = datetime.datetime(_now.year, _now.month, _now.day, 9, 0, 0)
            morning_start = datetime.datetime(_now.year, _now.month, _now.day, 9, 10, 0)
            morning_end = datetime.datetime(_now.year, _now.month, _now.day, 12, 0, 0)
            afternoon_start = datetime.datetime(_now.year, _now.month, _now.day, 13, 0, 0)
            # afternoon_end = datetime.datetime(_now.year, _now.month, _now.day, 16, 10, 0)
            afternoon_end = datetime.datetime(_now.year, _now.month, _now.day, 16, 20, 0)
        else:
            raise ValueError("direction is in (north, sourth)")

        if (_now >= morning_start and _now <= morning_end) or (_now >= afternoon_start and _now <= afternoon_end):
            return True
        else:
            return False

    def _check_if_trading_today(self, category):
        """检查下当前方向是否交易"""
        tradingtype = self.category_map.get(category)[1]
        sql = 'select IfTradingDay from hkland_shszhktradingday where TradingType={} and EndDate = "{}";'.format(
            tradingtype, self.today)
        ret = True if self.dc_client.select_one(sql).get('IfTradingDay') == 1 else False
        return ret

    def select_south_datas(self):
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

    def select_north_datas(self):
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

    def _start(self):
        self.dc_init()
        self.spider_init()

        self._create_table()

        north_is_trading_period = self._check_if_trading_period("north")
        if north_is_trading_period:
            logger.info("同花顺 开始更新北向数据（类型 2）,北向数据的时间是 9:30-11:30; 13:00-15:00 ")
            self._north()
        else:
            logger.info("同花顺 非北向数据更新时间")

        sourth_is_trading_period = self._check_if_trading_period("sourth")
        if sourth_is_trading_period:
            logger.info("开始更新南向数据（类型 1）, 南向数据的时间是 9:00-12:00; 13:00-16:10 ")
            self._south()
        else:
            logger.info("同花顺 非南向数据更新时间")

    def start(self):
        # TODO 同花顺的存在一个问题：就是延迟。 例如在 9：00 去请求南向数据的时候，接口拿到的可能还是昨天的数据
        # TODO 这个问题可暂时不处理的原因： merge 程序的保证; 数据重复插入时进行更新。
        # TODO 而且有十几根一起刷新出来的情况
        try:
            self._start()
        except:
            traceback.print_exc()

    def re_data(self, d):
        ret = float(d) * 10000
        ret = int(ret * 10000) // 10000
        ret = Decimal(ret)
        return ret

    def _south(self):
        '''
        self.south_map = {
            'ggtb': ('港股通(沪)', 2),
            'ggtbs': ('港股通(深)', 4),
        }
        '''
        sh_items = []
        sz_items = []
        # 南向 沪
        category = 'ggtb'
        is_trading = self._check_if_trading_today(category)
        if not is_trading:
            logger.info("{} {} 该方向数据今日关闭".format(self.today, self.south_map.get(category)[0]))
        else:
            logger.info("{} 是正常的交易日".format(self.today))
            url = self.base_url.format(category)
            page = self.get(url)
            ret = re.findall(r"var dataDay = (.*);", page)
            if ret:
                datas = eval(ret[0])[0]
                for data in datas:
                    item = dict()
                    item['DateTime'] = datetime.datetime.strptime(self.today + " " + data[0] + ":00", "%Y-%m-%d %H:%M:%S")
                    item['ShHkFlow'] = self.re_data(data[1])
                    item['ShHkBalance'] = self.re_data(data[2])
                    item['Category'] = 1
                    # print(item)
                    sh_items.append(item)

        # 南向 深
        category = 'ggtbs'
        is_trading = self._check_if_trading_today(category)
        if not is_trading:
            logger.info("{} 该方向数据今日关闭".format(self.south_map.get(category)[0]))
        else:
            url = self.base_url.format(category)
            page = self.get(url)
            ret = re.findall(r"var dataDay = (.*);", page)
            if ret:
                datas = eval(ret[0])[0]
                for data in datas:
                    item = dict()
                    item['DateTime'] = datetime.datetime.strptime(self.today + " " + data[0] + ":00", "%Y-%m-%d %H:%M:%S")
                    item['SzHkFlow'] = self.re_data(data[1])
                    item['SzHkBalance'] = self.re_data(data[2])
                    item['Category'] = 1
                    # print(item)
                    sz_items.append(item)

        if sh_items and sz_items:
            sh_map = {}
            sz_map = {}
            for item in sh_items:
                sh_map[str(item["DateTime"])] = item
            for item in sz_items:
                sz_map[str(item["DateTime"])] = item

            _map = {}
            for k in sh_map:
                if k in sz_map:
                    sh_map[k].update(sz_map[k])
                    new = sh_map[k]
                    new['Netinflow'] = new['ShHkFlow'] + new['SzHkFlow']
                    _map[k] = new

            items = list(_map.values())
            to_delete = []
            to_insert = []

            already_sourth_datas = self.select_south_datas()
            for r in already_sourth_datas:
                d_id = r.pop("id")
                if not r in items:
                    to_delete.append(d_id)

            for r in items:
                if not r in already_sourth_datas:
                    to_insert.append(r)

            update_fields = ['DateTime', 'ShHkFlow', 'ShHkBalance', 'SzHkFlow', 'SzHkBalance', 'Netinflow', 'Category']
            print(len(to_insert))
            for item in to_insert:
                self._save(self.spider_client, item, self.table_name, update_fields)

    def _north(self):
        '''
        self.north_map = {
            'hgtb': ('沪股通', 1),
            'sgtb': ('深股通', 3),
        }
        '''
        sh_items = []
        sz_items = []

        # 北向 沪
        category = 'hgtb'
        is_trading = self._check_if_trading_today(category)
        if not is_trading:
            logger.info("{} {} 该方向数据今日关闭".format(self.today, self.north_map.get(category)[0]))
        else:
            logger.info("{} 是正常的交易日".format(self.today))
            url = self.base_url.format(category)
            page = self.get(url)
            ret = re.findall(r"var dataDay = (.*);", page)
            if ret:
                datas = eval(ret[0])[0]
                for data in datas:
                    item = dict()
                    item['DateTime'] = datetime.datetime.strptime(self.today + " " + data[0] + ":00", "%Y-%m-%d %H:%M:%S")
                    item['ShHkFlow'] = self.re_data(data[1])
                    item['ShHkBalance'] = self.re_data(data[2])
                    item['Category'] = 2
                    # print(item)
                    sh_items.append(item)

        # 北向 深
        category = 'sgtb'
        is_trading = self._check_if_trading_today(category)
        if not is_trading:
            logger.info("{} 该方向数据今日关闭".format(self.north_map.get(category)[0]))
        else:
            url = self.base_url.format(category)
            page = self.get(url)
            ret = re.findall(r"var dataDay = (.*);", page)
            # sz_items = []
            if ret:
                datas = eval(ret[0])[0]
                for data in datas:
                    item = dict()
                    item['DateTime'] = datetime.datetime.strptime(self.today + " " + data[0] + ":00", "%Y-%m-%d %H:%M:%S")
                    item['SzHkFlow'] = self.re_data(data[1])
                    item['SzHkBalance'] = self.re_data(data[2])
                    item['Category'] = 2
                    # print(item)
                    sz_items.append(item)

        if sh_items and sz_items:
            sh_map = {}
            sz_map = {}
            for item in sh_items:
                sh_map[str(item["DateTime"])] = item
            for item in sz_items:
                sz_map[str(item["DateTime"])] = item
            # print(sh_map)
            # print(sz_map)

            _map = {}
            for k in sh_map:
                if k in sz_map:
                    sh_map[k].update(sz_map[k])
                    new = sh_map[k]
                    new['Netinflow'] = new['ShHkFlow'] + new['SzHkFlow']
                    _map[k] = new
            items = list(_map.values())
            to_delete = []
            to_insert = []

            already_north_datas = self.select_north_datas()
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


if __name__ == "__main__":
    # sf = SFLgthisdataspiderSpider()
    # sf._start()

    while True:
        sf = SFLgthisdataspiderSpider()
        sf.start()
        time.sleep(3)
        print()
        print()


'''
docker build -f Dockerfile_jqka10 -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_jqka10:v1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_jqka10:v1 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_jqka10:v1 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name flow_jqka10 --env LOCAL=0 registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_jqka10:v1 
docker logs -ft --tail 1000 flow_jqka10

# local 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name flow_jqka10 registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_flow_jqka10:v1 
'''