import base64
import hashlib
import hmac
import json
import logging
import time
import traceback
import urllib.parse
import requests

from hkland_component_imp.configs import TARGET_HOST, LOCAL, TARGET_PORT, TARGET_USER, TARGET_PASSWD, TARGET_DB, \
    JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB, DATACENTER_HOST, DATACENTER_PORT, DATACENTER_USER, \
    DATACENTER_PASSWD, DATACENTER_DB, SPIDER_HOST, SPIDER_PORT, SPIDER_USER, SPIDER_PASSWD, SPIDER_DB, TEST_HOST, \
    TEST_PORT, TEST_USER, TEST_PASSWD, TEST_DB, SECRET, TOKEN
from hkland_component_imp.sql_pool import PyMysqlPoolBase

if LOCAL:
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BaseSpider(object):
    product_cfg = {
        "host": TARGET_HOST,
        "port": TARGET_PORT,
        "user": TARGET_USER,
        "password": TARGET_PASSWD,
        "db": TARGET_DB,
    }

    juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,
    }

    dc_cfg = {
        "host": DATACENTER_HOST,
        "port": DATACENTER_PORT,
        "user": DATACENTER_USER,
        "password": DATACENTER_PASSWD,
        "db": DATACENTER_DB,
    }

    spider_cfg = {
        "host": SPIDER_HOST,
        "port": SPIDER_PORT,
        "user": SPIDER_USER,
        "password": SPIDER_PASSWD,
        "db": SPIDER_DB,
    }

    test_cfg = {
        "host": TEST_HOST,
        "port": TEST_PORT,
        "user": TEST_USER,
        "password": TEST_PASSWD,
        "db": TEST_DB,
    }

    def __init__(self):
        self.juyuan_client = None
        self.product_client = None
        self.dc_client = None
        self.spider_client = None
        self.test_client = None

    def _init_pool(self, cfg: dict):
        pool = PyMysqlPoolBase(**cfg)
        return pool

    def _juyuan_init(self):
        if not self.juyuan_client:
            self.juyuan_client = self._init_pool(self.juyuan_cfg)

    def _product_init(self):
        if not self.product_client:
            self.product_client = self._init_pool(self.product_cfg)

    def _dc_init(self):
        if not self.dc_client:
            self.dc_client = self._init_pool(self.dc_cfg)

    def _spider_init(self):
        if not self.spider_client:
            self.spider_client = self._init_pool(self.spider_cfg)

    def _test_init(self):
        if not self.test_client:
            self.test_client = self._init_pool(self.test_cfg)

    def __del__(self):
        if self.juyuan_client:
            self.juyuan_client.dispose()
        if self.product_client:
            self.product_client.dispose()
        if self.spider_client:
            self.spider_client.dispose()
        if self.dc_client:
            self.dc_client.dispose()
        if self.test_client:
            self.test_client.dispose()

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

    def get_juyuan_codeinfo(self, secu_code):
        self._juyuan_init()
        sql = 'SELECT SecuCode,InnerCode, SecuAbbr from SecuMain WHERE SecuCategory in (1, 2, 8) \
and SecuMarket in (83, 90) \
and ListedSector in (1, 2, 6, 7) and SecuCode = "{}";'.format(secu_code)
        ret = self.juyuan_client.select_one(sql)
        if not ret:
            return None, None
        return ret.get('InnerCode'), ret.get("SecuAbbr")

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

    def sync_dc2test(self, table_name):
        """测试使用 将 dc 数据库导出到测试库"""
        self._dc_init()
        self._test_init()
        sql = '''select * from {}; '''.format(table_name)
        datas = self.dc_client.select_all(sql)
        self._batch_save(self.test_client, datas, table_name, [])

    def sync_spider2test(self):
        pass


# if __name__ == '__main__':
#     for table in (
#             "hkland_sgcomponent",
#             "hkland_hgcomponent",
#
#
#             "hkland_hgelistocks",
#             "hkland_sgelistocks",
#                   ):
#         BaseSpider().sync_dc2test(table)
