import datetime
import sys
import traceback


sys.path.append("./../")
from hkland_elistocks.configs import TARGET_HOST, TARGET_PORT, TARGET_USER, TARGET_PASSWD, TARGET_DB
from hkland_elistocks.my_log import logger
from hkland_elistocks.sql_pool import PyMysqlPoolBase


def contract_sql(to_insert: dict, table: str, update_fields: list):
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


def save(to_insert, table: str, update_fields: list):

    target_cfg = {
        "host": TARGET_HOST,
        "port": TARGET_PORT,
        "user": TARGET_USER,
        "password": TARGET_PASSWD,
        "db": TARGET_DB,
    }

    def _init_pool(cfg: dict):
        pool = PyMysqlPoolBase(**cfg)
        return pool

    product = _init_pool(target_cfg)

    try:
        insert_sql, values = contract_sql(to_insert, table, update_fields)
        count = product.insert(insert_sql, values)
    except:
        traceback.print_exc()
        logger.warning("失败")
        count = None
    else:
        if count:
            logger.info("更入新数据 {}".format(to_insert))
    finally:
        product.dispose()
    return count


def human_insert(table: str, data: dict):
    """
    将不一致的数据单独插入
    :param table:
    :param data:
    :return:
    """
    # ID   | TradingType | TargetCategory | InnerCode | SecuCode | SecuAbbr | InDate | OutDate | Flag | CCASSCode | ParValue | CREATETIMEJZ        | UPDATETIMEJZ        | CMFID | CMFTime |
    fields = ["TradingType", "TargetCategory", "InnerCode", "SecuCode", "SecuAbbr", "InDate", "OutDate", "Flag", "CCASSCode", "ParValue"]
    save(data, table, fields)


records_190422 = [{'Ch_ange': 'Addition to List of Eligible SSE Securities for Margin Trading '
             'and List of Eligible SSE Securities for Short Selling',
  'EffectiveDate': datetime.date(2020, 4, 15),
  'Remarks': 'Change is resulted from the change in SSE stock list for margin '
             'trading and shortselling. For details, please refer to '
             'http://www.sse.com.cn/lawandrules/sserules/main/trading/universal/c/c_20200410_5034247.shtml '
             '(Chinese Version Only).',
  'SSESCode': '601162',
  'StockName': 'TIANFENG SECURITIES'},
 {'Ch_ange': 'Addition to List of Eligible SSE Securities for Margin Trading '
             'and List of Eligible SSE Securities for Short Selling',
  'EffectiveDate': datetime.date(2020, 4, 15),
  'Remarks': 'Change is resulted from the change in SSE stock list for margin '
             'trading and shortselling. For details, please refer to '
             'http://www.sse.com.cn/lawandrules/sserules/main/trading/universal/c/c_20200410_5034247.shtml '
             '(Chinese Version Only).',
  'SSESCode': '601236',
  'StockName': 'HONGTA SECURITIES'},
 {'Ch_ange': 'Addition to List of Eligible SSE Securities for Margin Trading '
             'and List of Eligible SSE Securities for Short Selling',
  'EffectiveDate': datetime.date(2020, 4, 15),
  'Remarks': 'Change is resulted from the change in SSE stock list for margin '
             'trading and shortselling. For details, please refer to '
             'http://www.sse.com.cn/lawandrules/sserules/main/trading/universal/c/c_20200410_5034247.shtml '
             '(Chinese Version Only).',
  'SSESCode': '603501',
  'StockName': 'WILL SEMICONDUCTOR'},
 {'Ch_ange': 'Transfer to List of Special SSE Securities/Special China Connect '
             'Securities (stocks eligible for sell only)',
  'EffectiveDate': datetime.date(2020, 3, 31),
  'Remarks': 'Change is resulted from the announcement issued by the listed '
             'company on the Shanghai Stock Exchange website on 30 March 2020. '
             'The stock is included in the Risk Alert Board. For details, '
             'please refer to '
             'http://static.sse.com.cn//disclosure/listedinfo/announcement/c/2020-03-28/600860_20200328_19.pdf '
             '(Chinese Version Only).',
  'SSESCode': '600860',
  'StockName': 'BEIJING JINGCHENG MACHINERY ELEC (*ST)'}]

# 601162
record3 = {"TradingType": 1, "TargetCategory": 3, "SecuCode": "601162", 'InDate': datetime.datetime(2020, 4, 15), "OutDate": None, 'Flag': 1, "InnerCode": 65468, "SecuAbbr": "天风证券", "CCASSCode": None, "ParValue": None}
record4 = {"TradingType": 1, "TargetCategory": 4, "SecuCode": "601162", 'InDate': datetime.datetime(2020, 4, 15), "OutDate": None, 'Flag': 1, "InnerCode": 65468, "SecuAbbr": "天风证券", 'CCASSCode': None, 'ParValue': None}
human_insert("hkland_hgelistocks", record3)
human_insert("hkland_hgelistocks", record4)

# 601236
record3 = {"TradingType": 1, "TargetCategory": 3, "SecuCode": "601236", 'InDate': datetime.datetime(2020, 4, 15), "OutDate": None, 'Flag': 1, "InnerCode": 135812, "SecuAbbr": "红塔证券", "CCASSCode": None, "ParValue": None}
record4 = {"TradingType": 1, "TargetCategory": 4, "SecuCode": "601236", 'InDate': datetime.datetime(2020, 4, 15), "OutDate": None, 'Flag': 1, "InnerCode": 135812, "SecuAbbr": "红塔证券", 'CCASSCode': None, 'ParValue': None}
human_insert("hkland_hgelistocks", record3)
human_insert("hkland_hgelistocks", record4)

# 603501
record3 = {"TradingType": 1, "TargetCategory": 3, "SecuCode": "603501", 'InDate': datetime.datetime(2020, 4, 15), "OutDate": None, 'Flag': 1, "InnerCode": 50936, "SecuAbbr": "韦尔股份", "CCASSCode": None, "ParValue": None}
record4 = {"TradingType": 1, "TargetCategory": 4, "SecuCode": "603501", 'InDate': datetime.datetime(2020, 4, 15), "OutDate": None, 'Flag': 1, "InnerCode": 50936, "SecuAbbr": "韦尔股份", 'CCASSCode': None, 'ParValue': None}
human_insert("hkland_hgelistocks", record3)
human_insert("hkland_hgelistocks", record4)

# 600860
# 已处理

records_sh = records_190422
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

records2_190422 = [
    {'Ch_ange': 'Addition to List of Eligible SZSE Securities for Margin Trading '
             'and List of Eligible SZSE Securities for Short Selling',
  'EffectiveDate': datetime.date(2020, 4, 15),
  'Remarks': 'Change is resulted from the change in SZSE stock list for margin '
             'trading and shortselling. For details, please refer to '
             'http://www.szse.cn/disclosure/margin/business/t20200410_575881.html '
             '(Chinese Version Only)',
  'SSESCode': '002463',
  'StockName': 'WUS PRINTED CIRCUIT KUNSHAN'},
 {'Ch_ange': 'Buy orders resumed',
  'EffectiveDate': datetime.date(2020, 3, 18),
  'Remarks': 'Change is resulted from the notification from SZSE that '
             'Aggregate Foreign Shareholdings of the stock have gone below '
             '26%. ',
  'SSESCode': '000333',
  'StockName': 'MIDEA GROUP'}]

# 002463
record3 = {"TradingType": 3, "TargetCategory": 3, "SecuCode": "002463", 'InDate': datetime.datetime(2020, 4, 15), "OutDate": None, 'Flag': 1, "InnerCode": 10860, "SecuAbbr": "沪电股份", "CCASSCode": None, "ParValue": None}
record4 = {"TradingType": 3, "TargetCategory": 4, "SecuCode": "002463", 'InDate': datetime.datetime(2020, 4, 15), "OutDate": None, 'Flag': 1, "InnerCode": 10860, "SecuAbbr": "沪电股份", 'CCASSCode': None, 'ParValue': None}
human_insert("hkland_sgelistocks", record3)
human_insert("hkland_sgelistocks", record4)
# 000333
# 已处理


records_sz = records2_190422
