import base64
import hashlib
import hmac
import json
import logging
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
        '''
        sh_add_1:  ['600070', '600984', '601512', '601816', '603053', '603068', '603218', '603489', '603520',
                    '603610', '603690', '603786', '603920', '603927', '603960']
        sh_add_134:  ['600131', '600223', '600529', '600764', '601519', '603012', '603018', '603601', '603678']
        sh_recover_1:  ['600988']
        sh_recover_134:  ['600079', '600143', '600621', '600737', '600776', '600802', '603000']
        sh_remove_1:  ['600693', '603007', '603080', '603165', '603332', '603339', '603351', '603603', '603773',
                        '603877', '603897', '603898']
        sh_remove_134 ['600123', '600230', '600231', '600239', '600297', '600398', '600418', '600499', '600528',
                        '600535', '600623', '600661', '600664', '600771', '600826', '600986', '601002', '601222',
                        '601997', '603959']

        sz_add_1:  ['000032', '000785', '002015', '002351', '002459', '002541', '002552', '002793', '002803',
                    '002837', '002955', '002959', '002961', '002962', '002966', '300080', '300455', '300468',
                    '300552', '300677', '300775', '300776', '300777', '300782', '300783', '300785', '300788',
                    '300793', '300799', '002201', '002641', '002706', '002756', '002838', '002869', '002880',
                    '300114', '300132', '300209', '300319', '300388', '300395', '300448', '300525', '300526',
                    '300573', '300579', '300590', '300603', '300604', '300653', '300657', '300659', '300662',
                    '300709', '300771']
        sz_add_134:  ['000912', '002214', '300663', '002243', '002947', '300328']
        sz_recover_1:  ['000058', '002239', '002312', '002605', '300083', '300376', '002791', '000601', '000796',
                        '000903', '002083', '002126', '002135', '002324', '002479', '002484', '002528', '002609',
                        '002616', '002850', '002918', '300031', '300045', '300229', '300303', '300386', '300438',
                        '300477', '300568', '300571', '300607', '300613', '300623', '300624', '300664', '300672',
                        '300684', '300737']
        sz_recover_134:  ['000030', '000519', '000700', '000719', '000917', '002169', '002250', '002287', '000652',
                        '000823', '000829', '002022', '002079', '002106', '002117', '002161', '002182', '002276',
                        '002313', '002428', '002518', '300020', '300177', '300202', '300256', '300287', '300397']
        sz_remove_1:  ['000429', '000863', '002314', '000657', '000666', '000815', '000882', '002057', '002309',
                        '002550', '300185', '300252']
        sz_remove_134 ['000088', '000552', '002280', '002293', '002370', '002608', '000040', '000525', '000980',
                        '002366', '300367', '000036', '000592', '000861', '000926', '000928', '002215', '002274',
                        '002378', '002639', '300266', '300355']
        '''
        self._dc_init()
        base_sql = """select * from hkland_hgelistocks where SecuCode = '{}' order by InDate;"""
        for code in ['600070', '600984', '601512', '601816', '603053', '603068', '603218', '603489', '603520',
                    '603610', '603690', '603786', '603920', '603927', '603960']:
            sql = base_sql.format(code)
            logger.debug(sql)
            ret = self.dc_client.select_all(sql)
            print(ret)
            # 首次新增的在之前的查询中应该为空
            assert not ret


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
