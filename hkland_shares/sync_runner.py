import base64
import datetime
import hashlib
import hmac
import json
import logging
import os
import pprint
import sys
import time
import traceback
import urllib.parse
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

sys.path.append("./../")
from hkland_shares.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER, SPIDER_MYSQL_PASSWORD,
                                   SPIDER_MYSQL_DB, PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_PORT, PRODUCT_MYSQL_USER,
                                   PRODUCT_MYSQL_PASSWORD, PRODUCT_MYSQL_DB, JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD,
                                   JUY_DB, DC_HOST, DC_PORT, DC_USER, DC_PASSWD, DC_DB, LOCAL, SECRET, TOKEN)
from hkland_shares.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HoldSharesSync(object):
    spider_cfg = {   # 爬虫库
        "host": SPIDER_MYSQL_HOST,
        "port": SPIDER_MYSQL_PORT,
        "user": SPIDER_MYSQL_USER,
        "password": SPIDER_MYSQL_PASSWORD,
        "db": SPIDER_MYSQL_DB,
    }

    product_cfg = {    # 正式库
        "host": PRODUCT_MYSQL_HOST,
        "port": PRODUCT_MYSQL_PORT,
        "user": PRODUCT_MYSQL_USER,
        "password": PRODUCT_MYSQL_PASSWORD,
        "db": PRODUCT_MYSQL_DB,
    }

    juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,
    }

    # 数据中心库
    dc_cfg = {
        "host": DC_HOST,
        "port": DC_PORT,
        "user": DC_USER,
        "password": DC_PASSWD,
        "db": DC_DB,
    }

    def __init__(self, type):
        self.type = type

        self.today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)

        # 爬虫数据库名
        self.spider_table = 'holding_shares_{}'.format(self.type)

        # 生成正式库中的两个 hkland_shares hkland_hkshares
        if self.type in ("sh", "sz"):
            self.table_name = 'hkland_shares'
        elif self.type == "hk":
            self.table_name = 'hkland_hkshares'
        else:
            raise

    def _init_pool(self, cfg):
        pool = PyMysqlPoolBase(**cfg)
        return pool

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

    def _save(self, sql_pool, to_insert, table, update_fields):
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            count = sql_pool.insert(insert_sql, values)
        except:
            traceback.print_exc()
            logger.warning("失败")
        else:
            if count == 1:
                logger.info("插入新数据 {}".format(to_insert))
            elif count == 2:
                logger.info("刷新数据 {}".format(to_insert))
            else:
                logger.info("数据已存在{} ".format(to_insert))
            sql_pool.end()
            return count

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

    def _create_product_table(self):
        sql1 = '''
        CREATE TABLE IF NOT EXISTS `hkland_shares` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `SecuCode` varchar(16) COLLATE utf8_bin NOT NULL COMMENT '股票交易代码',
          `InnerCode` int(11) NOT NULL COMMENT '内部编码',
          `SecuAbbr` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '股票简称',
          `Date` datetime NOT NULL COMMENT '自然日',
          `HKTradeDay` datetime NOT NULL COMMENT '港交所交易日',
          `Percent` decimal(20,4) DEFAULT NULL COMMENT '占A股总股本的比例（%）',
          `ShareNum` decimal(20,0) DEFAULT NULL COMMENT '股票数量(股)',
          `HashID` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT 'HashID',
          `CMFTime` datetime NOT NULL COMMENT '日期',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un` (`Date`,`HKTradeDay`,`SecuCode`),
          UNIQUE KEY `un2` (`InnerCode`,`Date`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='沪/深股通持股记录'; 
        '''

        sql2 = '''
        CREATE TABLE IF NOT EXISTS `hkland_hkshares` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `SecuCode` varchar(16) COLLATE utf8_bin NOT NULL COMMENT '股票交易代码',
          `InnerCode` int(11) NOT NULL COMMENT '内部编码',
          `SecuAbbr` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '股票简称',
          `Date` datetime NOT NULL COMMENT '日期',
          `Percent` decimal(20,4) DEFAULT NULL COMMENT '占已发行港股的比例（%）',
          `ShareNum` decimal(20,0) DEFAULT NULL COMMENT '股票数量（股）',
          `HashID` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT 'HashID',
          `CMFTime` datetime NOT NULL COMMENT '日期',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un` (`Date`,`SecuCode`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='港股通持股记录-港股'; 
        '''
        product = self._init_pool(self.product_cfg)
        product.insert(sql1)
        product.insert(sql2)
        product.dispose()

    def sync(self):
        is_error = False
        for i in range(3):
            try:
                self._sync()
            except Exception as e:
                logger.info("{} {} 次 sync 失败, 原因{}".format(self.type, i, e))
                time.sleep(10)
                if i == 2:
                    traceback.print_exc()
                    is_error = True
            else:
                break

        if is_error:
            raise Exception("{} 同步失败请检查".format(self.type))

    def _sync(self):
        if LOCAL:
            self._create_product_table()
        spider = self._init_pool(self.spider_cfg)

        start_dt = self.today - datetime.timedelta(days=10)
        # 注意: 这里是固定的 当前时间的前一天
        end_dt = self.today - datetime.timedelta(days=1)

        dt = start_dt
        _map = {}
        while dt <= end_dt:
            sql = '''select max(Date) as before_max_dt from {} where Date <= '{}'; '''.format(self.spider_table, dt)
            _dt = spider.select_one(sql).get("before_max_dt")
            _map[str(dt)] = _dt
            dt += datetime.timedelta(days=1)

        logger.info("与爬虫数据源日期的对应情况是\n" + pprint.pformat(_map))

        # 如果是北向数据 需要加上一个字段
        if self.type == "sh":
            trading_type = 1
        elif self.type == "sz":
            trading_type = 3
        else:
            trading_type = None
        if trading_type:
            dc = self._init_pool(self.dc_cfg)
            shhk_calendar_map = {}
            dt = start_dt
            while dt <= end_dt:
                # 沪港通 或者是 深港通 当前日期之间最邻近的一个交易日
                sql = '''select max(EndDate) as before_max_dt from  hkland_shszhktradingday where EndDate <= '{}' and TradingType={} and IfTradingDay=1;'''.format(dt, trading_type)
                _dt = dc.select_one(sql).get("before_max_dt")
                shhk_calendar_map[str(dt)] = _dt
                dt += datetime.timedelta(days=1)

            logger.info("与当前最近的陆股通交易日的对应情况是\n"+pprint.pformat(shhk_calendar_map))

        product = self._init_pool(self.product_cfg)
        select_fields = ['SecuCode', 'InnerCode', 'SecuAbbr', 'Percent', 'ShareNum', 'UPDATETIMEJZ']
        # FIXME 加入了 CMFTime 即使用的数据源时间也要计入在内
        update_fields = ['Date', 'SecuCode', 'InnerCode', 'SecuAbbr', 'Percent', 'ShareNum',
                         'CMFTime',
                         ]
        select_str = ",".join(select_fields).rstrip(",")

        for dt in _map:
            sql = '''select {} from {} where Date = '{}'; '''.format(select_str, self.spider_table, _map.get(dt))
            datas = spider.select_all(sql)
            for data in datas:
                data.update({"Date": dt})
                data.update({"CMFTime": data.get("UPDATETIMEJZ")})
                data.pop("UPDATETIMEJZ")
                if self.type in ("sh", "sz"):
                    data.update({"HKTradeDay": shhk_calendar_map.get(dt)})
                    update_fields.append("HKTradeDay")
                ret = self._save(product, data, self.table_name, update_fields)

        try:
            product.dispose()
            spider.dispose()
        except:
            pass


now = lambda: time.time()


def sync_task():
    retry = 1
    while True:
        try:
            t1 = now()
            for _type in ("sh", "sz", "hk"):
                logger.info("{} SYNC START.".format(_type))
                h = HoldSharesSync(_type)
                h.sync()
                logger.info("Time: {} s".format(now() - t1))
        except Exception as e:
            logger.info("第 {} 次执行任务失败 原因是 {}".format(retry, e))
            # traceback.print_exc()
            retry += 1
            if retry > 3:
                raise
            time.sleep(10)
        else:
            break


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    # 确保重启时可以执行一次
    sync_task()
    scheduler.add_job(sync_task, 'cron', hour='1-6', minute='30, 50')
    logger.info('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception as e:
        logger.info(f"本次任务执行出错{e}")
        sys.exit(0)


# if __name__ == "__main__":
#     sync_task()
