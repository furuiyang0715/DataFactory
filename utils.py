import base64
import datetime
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
import requests

from hkland_configs import SECRET, TOKEN, USER_PHONE, DC_HOST, DC_PORT, DC_USER, DC_PASSWD, DC_DB, PRODUCT_MYSQL_HOST, \
    PRODUCT_MYSQL_DB, PRODUCT_MYSQL_USER, PRODUCT_MYSQL_PASSWORD, PRODUCT_MYSQL_PORT
from sql_base import Connection

logger = logging.getLogger()


def ding_msg(msg: str):
    """发送钉钉预警消息"""
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
            "content": f"{msg}@{USER_PHONE}"
        },
        "at": {
            "atMobiles": [
                USER_PHONE,
            ],
            "isAtAll": False
        }
    }
    message_json = json.dumps(message)
    resp = requests.post(url=url, data=message_json, headers=header)
    if resp.status_code == 200:
        logger.info("钉钉发送消息成功: {}".format(msg))
    else:
        logger.warning("钉钉消息发送失败")


def check_iftradingday(category: str, day: datetime.datetime):
    '''
    TradingType:
        ('沪股通', 1),
        ('港股通(沪)', 2),
        ('深股通', 3),
        ('港股通(深)', 4)
    }
    一般来说 1 3 与 2 4 方向是否是一致的
    '''
    day = day.date()
    dc_conn = Connection(
        host=DC_HOST,
        port=DC_PORT,
        user=DC_USER,
        password=DC_PASSWD,
        database=DC_DB,
    )

    if category == 's':
        trading_types = (2, 4)
    elif category == 'n':
        trading_types = (1, 3)
    else:
        raise ValueError

    sql = 'select IfTradingDay from hkland_shszhktradingday where TradingType in {} and EndDate = "{}";'.format(
        trading_types, day)
    ret = dc_conn.query(sql)
    ret = [r.get("IfTradingDay") for r in ret]
    if ret == [2, 2]:
        return False
    else:
        return True


def refresh_update_time():
    product_conn = Connection(
        host=PRODUCT_MYSQL_HOST,
        database=PRODUCT_MYSQL_DB,
        user=PRODUCT_MYSQL_USER,
        password=PRODUCT_MYSQL_PASSWORD,
        port=PRODUCT_MYSQL_PORT,
    )

    dc_conn = Connection(
        host=DC_HOST,
        port=DC_PORT,
        user=DC_USER,
        password=DC_PASSWD,
        database=DC_DB,
    )

    last_update_times = {}

    for table_name, order_num in {
        'hkland_flow': 1,
        'hkland_hgcomponent': 2,
        'hkland_hgelistocks': 3,
        'hkland_historytradestat': 4,
        'hkland_hkscc': 5,
        'hkland_hkshares': 6,
        'hkland_sgcomponent': 7,
        'hkland_sgelistocks': 8,
        'hkland_shares': 9,
        'hkland_toptrade': 10,
        'hkland_shszhktradingday': 11,
    }.items():
        sql = '''select max(UPDATETIMEJZ) as max_dt from {}; '''.format(table_name)
        max_dt = dc_conn.get(sql).get("max_dt")
        if max_dt is None:
            continue
        logger.info(f"{table_name} 最新的更新时间是{max_dt}")
        last_update_times[table_name] = max_dt
        product_conn.table_update('base_table_updatetime', {'LastUpdateTime': max_dt}, 'id', order_num)

    return last_update_times
