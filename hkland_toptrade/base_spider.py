import base64
import hashlib
import hmac
import json
import logging
import time
import traceback
import urllib.parse

import requests

from hkland_toptrade.configs import (PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_USER, PRODUCT_MYSQL_PORT, PRODUCT_MYSQL_PASSWORD,
                                     PRODUCT_MYSQL_DB, JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB, SECRET, TOKEN)
from hkland_toptrade.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BaseSpider(object):
    product_cfg = {
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

    def __init__(self):
        self.tool_table_name = 'base_table_updatetime'
        self.table_name = 'hkland_toptrade'
        self.fields = ['Date', 'SecuCode', 'InnerCode', 'SecuAbbr', 'Close', 'ChangePercent', 'TJME', 'TMRJE', 'TCJJE',
                       'CategoryCode']
        self.juyuan_client = None
        self.product_client = None

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

    def _juyuan_init(self):
        if not self.juyuan_client:
            self.juyuan_client = self._init_pool(self.juyuan_cfg)

    def _product_init(self):
        if not self.product_client:
            self.product_client = self._init_pool(self.product_cfg)

    def __del__(self):
        if self.juyuan_client:
            self.juyuan_client.dispose()
        if self.product_client:
            self.product_client.dispose()

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

    def _batch_save(self, sql_pool, to_inserts, table, update_fields):
        try:
            sql, values = self.contract_sql(to_inserts, table, update_fields)
            count = sql_pool.insert_many(sql, values)
        except:
            traceback.print_exc()
            logger.warning("失败")
        else:
            logger.info("批量插入的数量是{}".format(count))
            sql_pool.end()
            return count

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
        sql = '''select max(UPDATETIMEJZ) as max_dt from {}; '''.format(self.table_name)
        max_dt = self.product_client.select_one(sql).get("max_dt")
        logger.info("最新的更新时间是{}".format(max_dt))

        refresh_sql = '''replace into {} (id,TableName, LastUpdateTime,IsValid) values (10, "hkland_toptrade", '{}', 1); 
        '''.format(self.tool_table_name, max_dt)
        count = self.product_client.update(refresh_sql)
        logger.info("1 首次插入 2 替换插入: {}".format(count))
        self.product_client.end()

    def _create_table(self):
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `Date` date NOT NULL COMMENT '时间',
          `SecuCode` varchar(10) COLLATE utf8_bin NOT NULL COMMENT '证券代码',
          `InnerCode` int(11) NOT NULL COMMENT '内部编码',
          `SecuAbbr` varchar(20) COLLATE utf8_bin NOT NULL COMMENT '股票简称',
          `Close` decimal(19,3) NOT NULL COMMENT '收盘价',
          `ChangePercent` decimal(19,5) NOT NULL COMMENT '涨跌幅',
          `TJME` decimal(19,3) NOT NULL COMMENT '净买额（元/港元）',
          `TMRJE` decimal(19,3) NOT NULL COMMENT '买入金额（元/港元）',
          `TCJJE` decimal(19,3) NOT NULL COMMENT '成交金额（元/港元）',
          `CategoryCode` varchar(10) COLLATE utf8_bin DEFAULT NULL COMMENT '类别代码:GGh: 港股通(沪), GGs: 港股通(深), HG: 沪股通, SG: 深股通',
          `CMFID` bigint(20) NOT NULL COMMENT '来源ID',
          `CMFTime` datetime NOT NULL COMMENT '来源日期',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un` (`SecuCode`,`Date`,`CategoryCode`) USING BTREE,
          UNIQUE KEY `un2` (`InnerCode`,`Date`,`CategoryCode`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆港通十大成交股';
        '''.format(self.table_name)
        self.product_client.insert(sql)
        self.product_client.end()
