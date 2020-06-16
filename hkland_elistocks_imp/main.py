import base64
import datetime
import hashlib
import hmac
import json
import logging
import pprint
import sys
import time
import traceback
import urllib.parse


import requests

from hkland_elistocks_imp.configs import LOCAL, TARGET_HOST, TARGET_PORT, TARGET_USER, TARGET_PASSWD, TARGET_DB, \
    JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB, DATACENTER_HOST, DATACENTER_PORT, DATACENTER_USER, \
    DATACENTER_PASSWD, DATACENTER_DB, SPIDER_HOST, SPIDER_PORT, SPIDER_USER, SPIDER_PASSWD, SPIDER_DB, TEST_HOST, \
    TEST_PORT, TEST_USER, TEST_PASSWD, TEST_DB, SECRET, TOKEN
from hkland_elistocks_imp.sql_pool import PyMysqlPoolBase

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


class DailyUpdate(BaseSpider):
    def __init__(self):
        super(DailyUpdate, self).__init__()
        self.is_local = True
        # sh
        self.sh_only_sell_list_table = 'hkex_lgt_special_sse_securities'
        self.sh_buy_and_sell_list_table = 'hkex_lgt_sse_securities'
        self.sh_buy_margin_trading_list_table = 'hkex_lgt_special_sse_securities_for_margin_trading'
        self.sh_short_sell_list_table = 'hkex_lgt_special_sse_securities_for_short_selling'

    def sh_buy_and_sell_list(self):
        self._test_init()
        self._spider_init()

        sql = '''select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 1 and Flag = 1; '''
        if self.is_local:
            ret = self.test_client.select_all(sql)
        else:
            ret = self.product_client.select_all(sql)
        datas = set([r.get("SecuCode") for r in ret])

        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
            self.sh_buy_and_sell_list_table, self.sh_buy_and_sell_list_table)
        if self.is_local:
            ret = self.test_client.select_all(sql)
        else:
            ret = self.spider_client.select_all(sql)
        lst = set([r.get("SSESCode") for r in ret])
        print(datas - lst)
        print(lst - datas)

    def sh_only_sell_list(self):
        self._test_init()
        self._spider_init()

        sql = '''select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 2 and Flag = 1; '''
        if self.is_local:
            ret = self.test_client.select_all(sql)
        else:
            ret = self.product_client.select_all(sql)
        datas = set([r.get("SecuCode") for r in ret])

        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
            self.sh_only_sell_list_table, self.sh_only_sell_list_table)
        if self.is_local:
            ret = self.test_client.select_all(sql)
        else:
            ret = self.spider_client.select_all(sql)

        lst = set([r.get("SSESCode") for r in ret])
        print(datas - lst)
        print(lst - datas)

    def sh_buy_margin_trading_list(self):
        self._test_init()
        self._spider_init()

        sql = '''select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 3 and Flag = 1; '''
        if self.is_local:
            ret = self.test_client.select_all(sql)
        else:
            ret = self.product_client.select_all(sql)
        datas = set([r.get("SecuCode") for r in ret])

        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
            self.sh_buy_margin_trading_list_table, self.sh_buy_margin_trading_list_table)
        if self.is_local:
            ret = self.test_client.select_all(sql)
        else:
            ret = self.spider_client.select_all(sql)

        lst = set([r.get("SSESCode") for r in ret])
        print(datas - lst)
        print(lst - datas)

    def sh_short_sell_list(self):
        self._test_init()
        self._spider_init()

        sql = '''select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 4 and Flag = 1; '''
        if self.is_local:
            ret = self.test_client.select_all(sql)
        else:
            ret = self.product_client.select_all(sql)
        datas = set([r.get("SecuCode") for r in ret])

        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
            self.sh_short_sell_list_table, self.sh_short_sell_list_table)
        if self.is_local:
            ret = self.test_client.select_all(sql)
        else:
            ret = self.spider_client.select_all(sql)

        lst = set([r.get("SSESCode") for r in ret])
        print(datas - lst)
        print(lst - datas)

    def run_0615_sh(self):
        sh_table_name = 'hkland_hgelistocks'
        sh_fields = ["TradingType", "TargetCategory", "InnerCode", "SecuCode", "SecuAbbr", "InDate", "OutDate", "Flag"]
        sh_add_1 = [('600070', datetime.date(2020, 6, 15)), ('600984', datetime.date(2020, 6, 15)), ('601512', datetime.date(2020, 6, 15)), ('601816', datetime.date(2020, 6, 15)), ('603053', datetime.date(2020, 6, 15)), ('603068', datetime.date(2020, 6, 15)), ('603218', datetime.date(2020, 6, 15)), ('603489', datetime.date(2020, 6, 15)), ('603520', datetime.date(2020, 6, 15)), ('603610', datetime.date(2020, 6, 15)), ('603690', datetime.date(2020, 6, 15)), ('603786', datetime.date(2020, 6, 15)), ('603920', datetime.date(2020, 6, 15)), ('603927', datetime.date(2020, 6, 15)), ('603960', datetime.date(2020, 6, 15))]
        sh_add_134 = [('600131', datetime.date(2020, 6, 15)), ('600223', datetime.date(2020, 6, 15)), ('600529', datetime.date(2020, 6, 15)), ('600764', datetime.date(2020, 6, 15)), ('601519', datetime.date(2020, 6, 15)), ('603012', datetime.date(2020, 6, 15)), ('603018', datetime.date(2020, 6, 15)), ('603601', datetime.date(2020, 6, 15)), ('603678', datetime.date(2020, 6, 15))]
        sh_recover_1 = [('600988', datetime.date(2020, 6, 15))]
        sh_recover_134 = [('600079', datetime.date(2020, 6, 15)), ('600143', datetime.date(2020, 6, 15)), ('600621', datetime.date(2020, 6, 15)), ('600737', datetime.date(2020, 6, 15)), ('600776', datetime.date(2020, 6, 15)), ('600802', datetime.date(2020, 6, 15)), ('603000', datetime.date(2020, 6, 15))]
        sh_remove_1 = [('600693', datetime.date(2020, 6, 15)), ('603007', datetime.date(2020, 6, 15)), ('603080', datetime.date(2020, 6, 15)), ('603165', datetime.date(2020, 6, 15)), ('603332', datetime.date(2020, 6, 15)), ('603339', datetime.date(2020, 6, 15)), ('603351', datetime.date(2020, 6, 15)), ('603603', datetime.date(2020, 6, 15)), ('603773', datetime.date(2020, 6, 15)), ('603877', datetime.date(2020, 6, 15)), ('603897', datetime.date(2020, 6, 15)), ('603898', datetime.date(2020, 6, 15))]
        sh_remove_134 = [('600123', datetime.date(2020, 6, 15)), ('600230', datetime.date(2020, 6, 15)), ('600231', datetime.date(2020, 6, 15)), ('600239', datetime.date(2020, 6, 15)), ('600297', datetime.date(2020, 6, 15)), ('600398', datetime.date(2020, 6, 15)), ('600418', datetime.date(2020, 6, 15)), ('600499', datetime.date(2020, 6, 15)), ('600528', datetime.date(2020, 6, 15)), ('600535', datetime.date(2020, 6, 15)), ('600623', datetime.date(2020, 6, 15)), ('600661', datetime.date(2020, 6, 15)), ('600664', datetime.date(2020, 6, 15)), ('600771', datetime.date(2020, 6, 15)), ('600826', datetime.date(2020, 6, 15)), ('600986', datetime.date(2020, 6, 15)), ('601002', datetime.date(2020, 6, 15)), ('601222', datetime.date(2020, 6, 15)), ('601997', datetime.date(2020, 6, 15)), ('603959', datetime.date(2020, 6, 15))]
        sh_removal_2 = [('600074', datetime.date(2020, 6, 3))]

        if self.is_local:
            self._test_init()
        else:
            self._dc_init()
            self._product_init()

        select_fields = ' CCASSCode, Flag, InDate, InnerCode, OutDate, ParValue,SecuAbbr, SecuCode, TargetCategory, TradingType '
        base_sql = """select""" + select_fields + """from hkland_hgelistocks where SecuCode = '{}' order by InDate;"""

        items = []
        for code, _dt in sh_removal_2:
            sql = base_sql.format(code)
            logger.debug(sql)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            to_removal = None
            for r in ret:
                if r.get("OutDate") is None and r.get("TargetCategory") == 2 and r.get("Flag") == 1:
                    to_removal = r
            print(to_removal)
            # 从只可买入名单中移除
            to_removal.update({"OutDate": _dt, "Flag": 2})
            items.append(to_removal)

        if self.is_local:
            ret = self._batch_save(self.test_client, items, sh_table_name, sh_fields)
        else:
            ret = self._batch_save(self.product_client, items, sh_table_name, sh_fields)
        print(ret)

        print("* " * 20)
        items1 = []
        for code, _dt in sh_add_1:
            sql = base_sql.format(code)
            logger.debug(sql)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            # 首次新增 1 的在之前的查询中应该为空
            # assert not ret
            item = dict()
            item['TradingType'] = 1     # 沪股通 1
            item['TargetCategory'] = 1
            item['SecuCode'] = code
            item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
            item['InDate'] = _dt
            # item['OutDate'] = None
            item['Flag'] = 1
            # item['CMFID'] = None
            # item['CMFTime'] = None
            # item['CCASSCode'] = None
            # item['ParValue'] = None
            items1.append(item)

        if self.is_local:
            ret1 = self._batch_save(self.test_client, items1, sh_table_name, sh_fields)
        else:
            ret1 = self._batch_save(self.product_client, items1, sh_table_name, sh_fields)
        print(ret1)

        print("* " * 20)
        items2 = []
        for code, _dt in sh_add_134:
            sql = base_sql.format(code)
            logger.debug(sql)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            # 首次新增 134 的在之前的查询中应该为空
            # assert not ret
            _item1, _item2, _item3 = dict(), dict(), dict()
            for item in (_item1, _item2, _item3):
                item['TradingType'] = 1  # 沪股通 1
                item['SecuCode'] = code
                item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
                item['InDate'] = _dt
                item['Flag'] = 1
            _item1['TargetCategory'] = 1
            _item2['TargetCategory'] = 3
            _item3['TargetCategory'] = 4
            logger.debug(_item1)
            logger.debug(_item2)
            logger.debug(_item3)
            items2.extend([_item1, _item2, _item3])
        if self.is_local:
            ret2 = self._batch_save(self.test_client, items2, sh_table_name, sh_fields)
        else:
            ret2 = self._batch_save(self.product_client, items2, sh_table_name, sh_fields)
        print(ret2)     # 27

        print("* " * 20)
        items3 = []
        for code, _dt in sh_recover_1:
            sql = base_sql.format(code)
            logger.debug(sql)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            # print(pprint.pformat(ret))
            # 结束 2
            to_over = None
            for r in ret:
                if r.get("OutDate") is None and r.get("TargetCategory") == 2:
                    to_over = r
            if to_over:
                to_over.update({"OutDate": _dt, "Flag": 2})
                items3.append(to_over)
            # 增加 1
            item = dict()
            item['TradingType'] = 1     # 沪股通 1
            item['TargetCategory'] = 1
            item['SecuCode'] = code
            item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
            item['InDate'] = _dt
            item['Flag'] = 1
            items3.append(item)
        # print(pprint.pformat(items3))
        if self.is_local:
            ret3 = self._batch_save(self.test_client, items3, sh_table_name, sh_fields)
        else:
            ret3 = self._batch_save(self.product_client, items3, sh_table_name, sh_fields)
        print(ret3)    # 3

        print("* " * 20)
        items4 = []
        for code, _dt in sh_recover_134:
            sql = base_sql.format(code)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            logger.debug(pprint.pformat(ret))
            # 结束 2
            to_over = None
            for r in ret:
                if r.get("OutDate") is None and r.get("TargetCategory") == 2:
                    to_over = r
            if to_over:
                logger.debug("to over: {}".format(to_over))
                to_over.update({"OutDate": _dt, "Flag": 2})
                items4.append(to_over)
            # 增加 134
            _item1, _item2, _item3 = dict(), dict(), dict()
            for item in (_item1, _item2, _item3):
                item['TradingType'] = 1  # 沪股通 1
                item['SecuCode'] = code
                item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
                item['InDate'] = _dt
                item['Flag'] = 1
            _item1['TargetCategory'] = 1
            _item2['TargetCategory'] = 3
            _item3['TargetCategory'] = 4
            logger.debug("增加 1:{}".format(_item1))
            logger.debug("增加 3:{}".format(_item2))
            logger.debug("增加 4:{}".format(_item3))
            items4.extend([_item1, _item2, _item3])
        if self.is_local:
            ret4 = self._batch_save(self.test_client, items4, sh_table_name, sh_fields)
        else:
            ret4 = self._batch_save(self.product_client, items4, sh_table_name, sh_fields)
        print(ret4)    # 35

        print("* " * 20)
        items5 = []
        for code, _dt in sh_remove_1:
            # 移除 1
            sql = base_sql.format(code)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            logger.debug(pprint.pformat(ret))
            to_over = None
            for r in ret:
                if r.get("OutDate") is None and r.get("TargetCategory") == 1:
                    to_over = r
            if to_over:
                logger.debug("to over: {}".format(to_over))
                to_over.update({"OutDate": _dt, "Flag": 2})
                items5.append(to_over)
            # 增加 2
            item = dict()
            item['TradingType'] = 1  # 沪股通 1
            item['SecuCode'] = code
            item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
            item['InDate'] = _dt
            item['Flag'] = 1
            item['TargetCategory'] = 2
            logger.debug("增加 2: {}".format(item))
            items5.append(item)
        if self.is_local:
            ret5 = self._batch_save(self.test_client, items5, sh_table_name, sh_fields)
        else:
            ret5 = self._batch_save(self.product_client, items5, sh_table_name, sh_fields)
        print(ret5)    # 36

        print("* " * 20)
        items6 = []
        print(len(sh_recover_134))
        for code, _dt in sh_remove_134:
            print()
            print()
            sql = base_sql.format(code)
            # 增加 2
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            logger.debug(pprint.pformat(ret))
            item = dict()
            item['TradingType'] = 1
            item['SecuCode'] = code
            item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
            item['InDate'] = _dt
            item['Flag'] = 1
            item['TargetCategory'] = 2
            logger.debug("增加 2: {}".format(item))
            items6.append(item)
            # 结束 134
            _to_over1 = None
            _to_over2 = None
            _to_over3 = None
            for r in ret:
                if r.get("OutDate") is None and r.get("TargetCategory") == 1:
                    _to_over1 = r
                elif r.get("OutDate") is None and r.get("TargetCategory") == 3:
                    _to_over2 = r
                elif r.get("OutDate") is None and r.get("TargetCategory") == 4:
                    _to_over3 = r
            for tov in (_to_over1, _to_over2, _to_over3):
                if tov:
                    tov.update({"OutDate": _dt, "Flag": 2})
                    items6.append(tov)
        if self.is_local:
            ret6 = self._batch_save(self.test_client, items6, sh_table_name, sh_fields)
        else:
            ret6 = self._batch_save(self.product_client, items6, sh_table_name, sh_fields)
        print(ret6)     # 140

    def run_0615_sz(self):
        sz_table_name = 'hkland_sgelistocks'
        sz_fields = ["TradingType", "TargetCategory", "InnerCode", "SecuCode", "SecuAbbr", "InDate", "OutDate", "Flag"]

        sz_add_1 = [('000032', datetime.date(2020, 6, 15)), ('000785', datetime.date(2020, 6, 15)), ('002015', datetime.date(2020, 6, 15)), ('002351', datetime.date(2020, 6, 15)), ('002459', datetime.date(2020, 6, 15)), ('002541', datetime.date(2020, 6, 15)), ('002552', datetime.date(2020, 6, 15)), ('002793', datetime.date(2020, 6, 15)), ('002803', datetime.date(2020, 6, 15)), ('002837', datetime.date(2020, 6, 15)), ('002955', datetime.date(2020, 6, 15)), ('002959', datetime.date(2020, 6, 15)), ('002961', datetime.date(2020, 6, 15)), ('002962', datetime.date(2020, 6, 15)), ('002966', datetime.date(2020, 6, 15)), ('300080', datetime.date(2020, 6, 15)), ('300455', datetime.date(2020, 6, 15)), ('300468', datetime.date(2020, 6, 15)), ('300552', datetime.date(2020, 6, 15)), ('300677', datetime.date(2020, 6, 15)), ('300775', datetime.date(2020, 6, 15)), ('300776', datetime.date(2020, 6, 15)), ('300777', datetime.date(2020, 6, 15)), ('300782', datetime.date(2020, 6, 15)), ('300783', datetime.date(2020, 6, 15)), ('300785', datetime.date(2020, 6, 15)), ('300788', datetime.date(2020, 6, 15)), ('300793', datetime.date(2020, 6, 15)), ('300799', datetime.date(2020, 6, 15)), ('002201', datetime.date(2020, 6, 15)), ('002641', datetime.date(2020, 6, 15)), ('002706', datetime.date(2020, 6, 15)), ('002756', datetime.date(2020, 6, 15)), ('002838', datetime.date(2020, 6, 15)), ('002869', datetime.date(2020, 6, 15)), ('002880', datetime.date(2020, 6, 15)), ('300114', datetime.date(2020, 6, 15)), ('300132', datetime.date(2020, 6, 15)), ('300209', datetime.date(2020, 6, 15)), ('300319', datetime.date(2020, 6, 15)), ('300388', datetime.date(2020, 6, 15)), ('300395', datetime.date(2020, 6, 15)), ('300448', datetime.date(2020, 6, 15)), ('300525', datetime.date(2020, 6, 15)), ('300526', datetime.date(2020, 6, 15)), ('300573', datetime.date(2020, 6, 15)), ('300579', datetime.date(2020, 6, 15)), ('300590', datetime.date(2020, 6, 15)), ('300603', datetime.date(2020, 6, 15)), ('300604', datetime.date(2020, 6, 15)), ('300653', datetime.date(2020, 6, 15)), ('300657', datetime.date(2020, 6, 15)), ('300659', datetime.date(2020, 6, 15)), ('300662', datetime.date(2020, 6, 15)), ('300709', datetime.date(2020, 6, 15)), ('300771', datetime.date(2020, 6, 15))]
        sz_add_134 = [('000912', datetime.date(2020, 6, 15)), ('002214', datetime.date(2020, 6, 15)), ('300663', datetime.date(2020, 6, 15)), ('002243', datetime.date(2020, 6, 15)), ('002947', datetime.date(2020, 6, 15)), ('300328', datetime.date(2020, 6, 15))]
        sz_recover_1 = [('000058', datetime.date(2020, 6, 15)), ('002239', datetime.date(2020, 6, 15)), ('002312', datetime.date(2020, 6, 15)), ('002605', datetime.date(2020, 6, 15)), ('300083', datetime.date(2020, 6, 15)), ('300376', datetime.date(2020, 6, 15)), ('002791', datetime.date(2020, 6, 15)), ('000601', datetime.date(2020, 6, 15)), ('000796', datetime.date(2020, 6, 15)), ('000903', datetime.date(2020, 6, 15)), ('002083', datetime.date(2020, 6, 15)), ('002126', datetime.date(2020, 6, 15)), ('002135', datetime.date(2020, 6, 15)), ('002324', datetime.date(2020, 6, 15)), ('002479', datetime.date(2020, 6, 15)), ('002484', datetime.date(2020, 6, 15)), ('002528', datetime.date(2020, 6, 15)), ('002609', datetime.date(2020, 6, 15)), ('002616', datetime.date(2020, 6, 15)), ('002850', datetime.date(2020, 6, 15)), ('002918', datetime.date(2020, 6, 15)), ('300031', datetime.date(2020, 6, 15)), ('300045', datetime.date(2020, 6, 15)), ('300229', datetime.date(2020, 6, 15)), ('300303', datetime.date(2020, 6, 15)), ('300386', datetime.date(2020, 6, 15)), ('300438', datetime.date(2020, 6, 15)), ('300477', datetime.date(2020, 6, 15)), ('300568', datetime.date(2020, 6, 15)), ('300571', datetime.date(2020, 6, 15)), ('300607', datetime.date(2020, 6, 15)), ('300613', datetime.date(2020, 6, 15)), ('300623', datetime.date(2020, 6, 15)), ('300624', datetime.date(2020, 6, 15)), ('300664', datetime.date(2020, 6, 15)), ('300672', datetime.date(2020, 6, 15)), ('300684', datetime.date(2020, 6, 15)), ('300737', datetime.date(2020, 6, 15))]
        sz_recover_134 = [('000030', datetime.date(2020, 6, 15)), ('000519', datetime.date(2020, 6, 15)), ('000700', datetime.date(2020, 6, 15)), ('000719', datetime.date(2020, 6, 15)), ('000917', datetime.date(2020, 6, 15)), ('002169', datetime.date(2020, 6, 15)), ('002250', datetime.date(2020, 6, 15)), ('002287', datetime.date(2020, 6, 15)), ('000652', datetime.date(2020, 6, 15)), ('000823', datetime.date(2020, 6, 15)), ('000829', datetime.date(2020, 6, 15)), ('002022', datetime.date(2020, 6, 15)), ('002079', datetime.date(2020, 6, 15)), ('002106', datetime.date(2020, 6, 15)), ('002117', datetime.date(2020, 6, 15)), ('002161', datetime.date(2020, 6, 15)), ('002182', datetime.date(2020, 6, 15)), ('002276', datetime.date(2020, 6, 15)), ('002313', datetime.date(2020, 6, 15)), ('002428', datetime.date(2020, 6, 15)), ('002518', datetime.date(2020, 6, 15)), ('300020', datetime.date(2020, 6, 15)), ('300177', datetime.date(2020, 6, 15)), ('300202', datetime.date(2020, 6, 15)), ('300256', datetime.date(2020, 6, 15)), ('300287', datetime.date(2020, 6, 15)), ('300397', datetime.date(2020, 6, 15))]
        sz_remove_1 = [('000429', datetime.date(2020, 6, 15)), ('000863', datetime.date(2020, 6, 15)), ('002314', datetime.date(2020, 6, 15)), ('000657', datetime.date(2020, 6, 15)), ('000666', datetime.date(2020, 6, 15)), ('000815', datetime.date(2020, 6, 15)), ('000882', datetime.date(2020, 6, 15)), ('002057', datetime.date(2020, 6, 15)), ('002309', datetime.date(2020, 6, 15)), ('002550', datetime.date(2020, 6, 15)), ('300185', datetime.date(2020, 6, 15)), ('300252', datetime.date(2020, 6, 15))]
        sz_remove_134 = [('000088', datetime.date(2020, 6, 15)), ('000552', datetime.date(2020, 6, 15)), ('002280', datetime.date(2020, 6, 15)), ('002293', datetime.date(2020, 6, 15)), ('002370', datetime.date(2020, 6, 15)), ('002608', datetime.date(2020, 6, 15)), ('000040', datetime.date(2020, 6, 15)), ('000525', datetime.date(2020, 6, 15)), ('000980', datetime.date(2020, 6, 15)), ('002366', datetime.date(2020, 6, 15)), ('300367', datetime.date(2020, 6, 15)), ('000036', datetime.date(2020, 6, 15)), ('000592', datetime.date(2020, 6, 15)), ('000861', datetime.date(2020, 6, 15)), ('000926', datetime.date(2020, 6, 15)), ('000928', datetime.date(2020, 6, 15)), ('002215', datetime.date(2020, 6, 15)), ('002274', datetime.date(2020, 6, 15)), ('002378', datetime.date(2020, 6, 15)), ('002639', datetime.date(2020, 6, 15)), ('300266', datetime.date(2020, 6, 15)), ('300355', datetime.date(2020, 6, 15)), ('002681', datetime.date(2020, 5, 6)), ('002176', datetime.date(2020, 5, 6))]
        sz_removal_2 = []

        if self.is_local:
            self._test_init()
        else:
            self._dc_init()
            self._product_init()

        select_fields = ' CCASSCode, Flag, InDate, InnerCode, OutDate, ParValue,SecuAbbr, SecuCode, TargetCategory, TradingType '
        base_sql = """select""" + select_fields + """from hkland_sgelistocks where SecuCode = '{}' order by InDate;"""

        print("* " * 20)
        items = []
        for code, _dt in sz_removal_2:
            sql = base_sql.format(code)
            logger.debug(sql)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            to_removal = None
            for r in ret:
                if r.get("OutDate") is None and r.get("TargetCategory") == 2 and r.get("Flag") == 1:
                    to_removal = r
            print(to_removal)
            # 从只可买入名单中移除
            to_removal.update({"OutDate": _dt, "Flag": 2})
            items.append(to_removal)

        if self.is_local:
            ret = self._batch_save(self.test_client, items, sz_table_name, sz_fields)
        else:
            ret = self._batch_save(self.product_client, items, sz_table_name, sz_fields)
        print(ret)

        print("* " * 20)
        items1 = []
        for code, _dt in sz_add_1:
            sql = base_sql.format(code)
            logger.debug(sql)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            # 首次新增 1 的在之前的查询中应该为空
            # assert not ret
            item = dict()
            item['TradingType'] = 1  # 沪股通 1
            item['TargetCategory'] = 1
            item['SecuCode'] = code
            item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
            item['InDate'] = _dt
            item['Flag'] = 1
            items1.append(item)

        print(len(items1))
        if self.is_local:
            ret1 = self._batch_save(self.test_client, items1, sz_table_name, sz_fields)
        else:
            ret1 = self._batch_save(self.product_client, items1, sz_table_name, sz_fields)
        print(ret1)   # 56

        print("* " * 20)
        items2 = []
        for code, _dt in sz_add_134:
            sql = base_sql.format(code)
            logger.debug(sql)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            # 首次新增 134 的在之前的查询中应该为空
            # assert not ret
            _item1, _item2, _item3 = dict(), dict(), dict()
            for item in (_item1, _item2, _item3):
                item['TradingType'] = 1  # 沪股通 1
                item['SecuCode'] = code
                item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
                item['InDate'] = _dt
                item['Flag'] = 1
            _item1['TargetCategory'] = 1
            _item2['TargetCategory'] = 3
            _item3['TargetCategory'] = 4
            logger.debug(_item1)
            logger.debug(_item2)
            logger.debug(_item3)
            items2.extend([_item1, _item2, _item3])
        # print(len(items2))
        if self.is_local:
            ret2 = self._batch_save(self.test_client, items2, sz_table_name, sz_fields)
        else:
            ret2 = self._batch_save(self.product_client, items2, sz_table_name, sz_fields)
        print(ret2)    # 18

        print("* " * 20)
        items3 = []
        for code, _dt in sz_recover_1:
            sql = base_sql.format(code)
            logger.debug(sql)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            # print(pprint.pformat(ret))
            # 结束 2
            to_over = None
            for r in ret:
                if r.get("OutDate") is None and r.get("TargetCategory") == 2:
                    to_over = r
            if to_over:
                to_over.update({"OutDate": _dt, "Flag": 2})
                items3.append(to_over)
            # 增加 1
            item = dict()
            item['TradingType'] = 1  # 沪股通 1
            item['TargetCategory'] = 1
            item['SecuCode'] = code
            item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
            item['InDate'] = _dt
            item['Flag'] = 1
            items3.append(item)

        print(len(items3))   # 76
        if self.is_local:
            ret3 = self._batch_save(self.test_client, items3, sz_table_name, sz_fields)
        else:
            ret3 = self._batch_save(self.product_client, items3, sz_table_name, sz_fields)
        print(ret3)    # 114

        print("* " * 20)
        items4 = []
        for code, _dt in sz_recover_134:
            sql = base_sql.format(code)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            # logger.debug(pprint.pformat(ret))
            # 结束 2
            to_over = None
            for r in ret:
                if r.get("OutDate") is None and r.get("TargetCategory") == 2:
                    to_over = r
            if to_over:
                logger.debug("to over: {}".format(to_over))
                to_over.update({"OutDate": _dt, "Flag": 2})
                items4.append(to_over)
            # 增加 134
            _item1, _item2, _item3 = dict(), dict(), dict()
            for item in (_item1, _item2, _item3):
                item['TradingType'] = 1  # 沪股通 1
                item['SecuCode'] = code
                item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
                item['InDate'] = _dt
                item['Flag'] = 1
            _item1['TargetCategory'] = 1
            _item2['TargetCategory'] = 3
            _item3['TargetCategory'] = 4
            logger.debug("增加 1:{}".format(_item1))
            logger.debug("增加 3:{}".format(_item2))
            logger.debug("增加 4:{}".format(_item3))
            items4.extend([_item1, _item2, _item3])

        print(len(items4))   # 108
        if self.is_local:
            ret4 = self._batch_save(self.test_client, items4, sz_table_name, sz_fields)
        else:
            ret4 = self._batch_save(self.product_client, items4, sz_table_name, sz_fields)
        print(ret4)    # 135

        print("* " * 20)
        items5 = []
        for code, _dt in sz_remove_1:
            # 移除 1
            sql = base_sql.format(code)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            # logger.debug(pprint.pformat(ret))
            to_over = None
            for r in ret:
                if r.get("OutDate") is None and r.get("TargetCategory") == 1:
                    to_over = r
            if to_over:
                logger.debug("to over: {}".format(to_over))
                to_over.update({"OutDate": _dt, "Flag": 2})
                items5.append(to_over)
            # 增加 2
            item = dict()
            item['TradingType'] = 1  # 沪股通 1
            item['SecuCode'] = code
            item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
            item['InDate'] = _dt
            item['Flag'] = 1
            item['TargetCategory'] = 2
            logger.debug("增加 2: {}".format(item))
            items5.append(item)

        # print(len(items5))    # 24
        if self.is_local:
            ret5 = self._batch_save(self.test_client, items5, sz_table_name, sz_fields)
        else:
            ret5 = self._batch_save(self.product_client, items5, sz_table_name, sz_fields)
        print(ret5)    # 36

        print("* " * 20)
        items6 = []
        print(len(sz_recover_134))
        for code, _dt in sz_remove_134:
            print()
            print()
            sql = base_sql.format(code)
            # 增加 2
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.dc_client.select_all(sql)
            logger.debug(pprint.pformat(ret))
            item = dict()
            item['TradingType'] = 1
            item['SecuCode'] = code
            item['InnerCode'], item['SecuAbbr'] = self.get_juyuan_codeinfo(code)
            item['InDate'] = _dt
            item['Flag'] = 1
            item['TargetCategory'] = 2
            logger.debug("增加 2: {}".format(item))
            items6.append(item)
            # 结束 134
            _to_over1 = None
            _to_over2 = None
            _to_over3 = None
            for r in ret:
                if r.get("OutDate") is None and r.get("TargetCategory") == 1:
                    _to_over1 = r
                elif r.get("OutDate") is None and r.get("TargetCategory") == 3:
                    _to_over2 = r
                elif r.get("OutDate") is None and r.get("TargetCategory") == 4:
                    _to_over3 = r
            for tov in (_to_over1, _to_over2, _to_over3):
                if tov:
                    tov.update({"OutDate": _dt, "Flag": 2})
                    items6.append(tov)

        print(len(items6))    # 90
        if self.is_local:
            ret6 = self._batch_save(self.test_client, items6, sz_table_name, sz_fields)
        else:
            ret6 = self._batch_save(self.product_client, items6, sz_table_name, sz_fields)
        print(ret6)     # 140

    def refresh_update_time(self):
        """更新工具表的刷新时间"""
        def sh_refresh(client):
            sql = '''select max(UPDATETIMEJZ) as max_dt from hkland_hgelistocks; '''
            max_dt = client.select_one(sql).get("max_dt")
            refresh_sql = '''replace into base_table_updatetime (id,TableName, LastUpdateTime,IsValid) values (3, 'hkland_hgelistocks', '{}', 1); '''.format(max_dt)
            client.update(refresh_sql)

        def sz_refresh(client):
            sql = '''select max(UPDATETIMEJZ) as max_dt from hkland_sgelistocks; '''
            max_dt = client.select_one(sql).get("max_dt")
            refresh_sql = '''replace into base_table_updatetime (id,TableName, LastUpdateTime,IsValid) values (8, 'hkland_sgelistocks', '{}', 1); '''.format(max_dt)
            client.update(refresh_sql)

        if self.is_local:
            cli = self.test_client
        else:
            cli = self.product_client
        sh_refresh(cli)
        sz_refresh(cli)

    def sync(self):
        # (1) 服务器同步 spider --> test

        # (2) 模拟同步 dc --> test
        self.sync_dc2test("hkland_hgelistocks")
        self.sync_dc2test("hkland_sgelistocks")

    def start(self):
        # self.run_0615_sh()
        # self.sh_short_sell_list()
        # self.sh_buy_margin_trading_list()
        # self.sh_only_sell_list()
        # self.sh_buy_and_sell_list()

        self.run_0615_sz()

        # self.refresh_update_time()

        pass


if __name__ == "__main__":
    dp = DailyUpdate()

    dp.start()
