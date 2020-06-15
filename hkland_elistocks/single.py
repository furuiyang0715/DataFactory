import base64
import datetime
import hashlib
import hmac
import json
import logging
import pprint
import time
import traceback
import urllib.parse


import requests

from hkland_elistocks.configs import (LOCAL, TARGET_HOST, TARGET_PORT, TARGET_USER, TARGET_DB, TARGET_PASSWD,
                                      JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB, SECRET, TOKEN, DATACENTER_HOST,
                                      DATACENTER_PASSWD, DATACENTER_USER, DATACENTER_DB, DATACENTER_PORT, SPIDER_HOST,
                                      SPIDER_PORT, SPIDER_USER, SPIDER_PASSWD, SPIDER_DB)
from hkland_elistocks.sql_pool import PyMysqlPoolBase
from hkland_elistocks.sh_human_gene import SHHumanTools
from hkland_elistocks.zh_human_gene import ZHHumanTools


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

    def __init__(self):
        self.juyuan_client = None
        self.product_client = None
        self.dc_client = None
        self.spider_client = None

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

    def __del__(self):
        if self.juyuan_client:
            self.juyuan_client.dispose()
        if self.product_client:
            self.product_client.dispose()
        if self.spider_client:
            self.spider_client.dispose()
        if self.dc_client:
            self.dc_client.dispose()

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

    def get_juyuan_codeinfo(self, secu_code):
        self._juyuan_init()
        sql = 'SELECT SecuCode,InnerCode, SecuAbbr from SecuMain WHERE SecuCategory in (1, 2, 8) \
and SecuMarket in (83, 90) \
and ListedSector in (1, 2, 6, 7) and SecuCode = "{}";'.format(secu_code)
        ret = self.juyuan_client.select_one(sql)
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


class DailyUpdate(BaseSpider):

    def run_0615(self):
        # TradingType | TargetCategory | InnerCode | SecuCode | SecuAbbr| InDate | OutDate | Flag | CCASSCode | ParValue
        # | CREATETIMEJZ | UPDATETIMEJZ | CMFID | CMFTime
        sh_table_name = 'hkland_hgelistocks'
        sh_fields = ["TradingType", "TargetCategory", "InnerCode", "SecuCode", "SecuAbbr", "InDate", "OutDate", "Flag"]

        sh_add_1 = [('600070', datetime.date(2020, 6, 15)), ('600984', datetime.date(2020, 6, 15)), ('601512', datetime.date(2020, 6, 15)), ('601816', datetime.date(2020, 6, 15)), ('603053', datetime.date(2020, 6, 15)), ('603068', datetime.date(2020, 6, 15)), ('603218', datetime.date(2020, 6, 15)), ('603489', datetime.date(2020, 6, 15)), ('603520', datetime.date(2020, 6, 15)), ('603610', datetime.date(2020, 6, 15)), ('603690', datetime.date(2020, 6, 15)), ('603786', datetime.date(2020, 6, 15)), ('603920', datetime.date(2020, 6, 15)), ('603927', datetime.date(2020, 6, 15)), ('603960', datetime.date(2020, 6, 15))]
        sh_add_134 = [('600131', datetime.date(2020, 6, 15)), ('600223', datetime.date(2020, 6, 15)), ('600529', datetime.date(2020, 6, 15)), ('600764', datetime.date(2020, 6, 15)), ('601519', datetime.date(2020, 6, 15)), ('603012', datetime.date(2020, 6, 15)), ('603018', datetime.date(2020, 6, 15)), ('603601', datetime.date(2020, 6, 15)), ('603678', datetime.date(2020, 6, 15))]
        sh_recover_1 = [('600988', datetime.date(2020, 6, 15))]
        sh_recover_134 = [('600079', datetime.date(2020, 6, 15)), ('600143', datetime.date(2020, 6, 15)), ('600621', datetime.date(2020, 6, 15)), ('600737', datetime.date(2020, 6, 15)), ('600776', datetime.date(2020, 6, 15)), ('600802', datetime.date(2020, 6, 15)), ('603000', datetime.date(2020, 6, 15))]
        sh_remove_1 = [('600693', datetime.date(2020, 6, 15)), ('603007', datetime.date(2020, 6, 15)), ('603080', datetime.date(2020, 6, 15)), ('603165', datetime.date(2020, 6, 15)), ('603332', datetime.date(2020, 6, 15)), ('603339', datetime.date(2020, 6, 15)), ('603351', datetime.date(2020, 6, 15)), ('603603', datetime.date(2020, 6, 15)), ('603773', datetime.date(2020, 6, 15)), ('603877', datetime.date(2020, 6, 15)), ('603897', datetime.date(2020, 6, 15)), ('603898', datetime.date(2020, 6, 15))]
        sh_remove_134 = [('600123', datetime.date(2020, 6, 15)), ('600230', datetime.date(2020, 6, 15)), ('600231', datetime.date(2020, 6, 15)), ('600239', datetime.date(2020, 6, 15)), ('600297', datetime.date(2020, 6, 15)), ('600398', datetime.date(2020, 6, 15)), ('600418', datetime.date(2020, 6, 15)), ('600499', datetime.date(2020, 6, 15)), ('600528', datetime.date(2020, 6, 15)), ('600535', datetime.date(2020, 6, 15)), ('600623', datetime.date(2020, 6, 15)), ('600661', datetime.date(2020, 6, 15)), ('600664', datetime.date(2020, 6, 15)), ('600771', datetime.date(2020, 6, 15)), ('600826', datetime.date(2020, 6, 15)), ('600986', datetime.date(2020, 6, 15)), ('601002', datetime.date(2020, 6, 15)), ('601222', datetime.date(2020, 6, 15)), ('601997', datetime.date(2020, 6, 15)), ('603959', datetime.date(2020, 6, 15))]

        self._dc_init()
        self._product_init()
        select_fields = ' CCASSCode, Flag, InDate, InnerCode, OutDate, ParValue,SecuAbbr, SecuCode, TargetCategory, TradingType '
        base_sql = """select""" + select_fields + """from hkland_hgelistocks where SecuCode = '{}' order by InDate;"""

        # items1 = []
        # for code, _dt in sh_add_1:
        #     sql = base_sql.format(code)
        #     logger.debug(sql)
        #     ret = self.dc_client.select_all(sql)
        #     # 首次新增 1 的在之前的查询中应该为空
        #     assert not ret
        #     item = dict()
        #     item['TradingType'] = 1     # 沪股通 1
        #     item['TargetCategory'] = 1
        #     item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
        #     item['InDate'] = _dt
        #     # item['OutDate'] = None
        #     item['Flag'] = 1
        #     # item['CMFID'] = None
        #     # item['CMFTime'] = None
        #     # item['CCASSCode'] = None
        #     # item['ParValue'] = None
        #     items1.append(item)
        # # ret1 = self._batch_save(self.product_client, items1, sh_table_name, sh_fields)
        # # print(ret1)    # 15
        #
        # print("* " * 20)
        # items2 = []
        # for code, _dt in sh_add_134:
        #     sql = base_sql.format(code)
        #     logger.debug(sql)
        #     ret = self.dc_client.select_all(sql)
        #     # 首次新增 134 的在之前的查询中应该为空
        #     assert not ret
        #     _item1, _item2, _item3 = dict(), dict(), dict()
        #     for item in (_item1, _item2, _item3):
        #         item['TradingType'] = 1  # 沪股通 1
        #         item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
        #         item['InDate'] = _dt
        #         item['Flag'] = 1
        #     _item1['TargetCategory'] = 1
        #     _item2['TargetCategory'] = 3
        #     _item3['TargetCategory'] = 4
        #     # print(_item1)
        #     # print(_item2)
        #     # print(_item3)
        #     items2.extend([_item1, _item2, _item3])
        # # ret2 = self._batch_save(self.product_client, items2, sh_table_name, sh_fields)
        # # print(ret2)    # 27

        # print("* " * 20)
        # items3 = []
        # for code, _dt in sh_recover_1:
        #     sql = base_sql.format(code)
        #     ret = self.dc_client.select_all(sql)
        #     print(pprint.pformat(ret))
        #
        #     item = dict()
        #     item['TradingType'] = 1     # 沪股通 1
        #     item['TargetCategory'] = 1
        #     item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
        #     item['InDate'] = _dt
        #     item['Flag'] = 1
        #     items3.append(item)
        # print(items3)
        # ret3 = self._batch_save(self.product_client, items3, sh_table_name, sh_fields)
        # print(ret3)   # 1


# for r in ret:
#     if r.get("OutDate") is None and r.get("TargetCategory") == 1:
#         item = r
# if item:
#     item.update({"OutDate": _dt, "Flag": 2})


    def refresh_time(self):
        sh = SHHumanTools()
        zh = ZHHumanTools()
        sh.refresh_update_time()
        zh.refresh_update_time()

    def start(self):
        self.run_0615()

        self.refresh_time()


if __name__ == "__main__":
    dp = DailyUpdate()
    dp.start()
