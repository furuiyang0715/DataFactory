'''20200507
hkland_hgelistocks 与第一相比 应该删除的记录是: []
hkland_hgelistocks 与第一次相比, 应该增加的记录是:
# 将 600618 加入只可卖出列表, 增加 2 结束 1 3 4
[{'EffectiveDate': datetime.date(2020, 5, 6), 'SSESCode': '600816', 'StockName': 'ANXIN TRUST (*ST)',
 'Ch_ange': 'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
 'Remarks': 'Change is resulted from the announcement issued by the listed company on the Shanghai Stock Exchange website on 30 April 2020.
 The stock is included in the Risk Alert Board. For details, please refer to http://static.sse.com.cn//disclosure/listedinfo/announcement/c/2020-04-30/600816_20200430_10.pdf (Chinese Version Only).
 This stock will also be removed from the List of Eligible SSE Securities for Margin Trading and the List of Eligible SSE Securities for Short Selling.'}]

hkland_sgelistocks 与第一相比 应该删除的记录是: []

hkland_sgelistocks 与第一次相比, 应该增加的记录是:
# 将 002681 加入只可卖出列表, 增加 2, 结束 1 3 4
[{'EffectiveDate': datetime.date(2020, 5, 6), 'SSESCode': '002681',
'StockName': 'SHENZHEN FENDA TECHNOLOGY (*ST)',
'Ch_ange': 'Transfer to List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)',
'Remarks': 'Change is resulted from the announcement issued by the listed company on the Shenzhen Stock Exchange website on 30 April 2020.
The stock is included in the Risk Alert Board. For details, please refer to http://www.szse.cn/disclosure/listed/bulletinDetail/index.html?78188180-67ae-4ec6-ad73-ea83d6b435f9 (Chinese Version Only).
This stock will also be removed from the List of Eligible SZSE Securities for Margin Trading and the List of Eligible SZSE Securities for Short Selling.'},

# 将 002176 加入只可卖出列表, 增加 2，结束 1 3 4
{'EffectiveDate': datetime.date(2020, 5, 6), 'SSESCode': '002176', 'StockName': 'JIANGXI SPECIAL ELECTRIC MOTOR (*ST)',
'Ch_ange': 'Transfer to List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)',
'Remarks': 'Change is resulted from the announcement issued by the listed company on the Shenzhen Stock Exchange website on 29 April 2020.
The stock is included in the Risk Alert Board. For details, please refer to http://www.szse.cn/disclosure/listed/bulletinDetail/index.html?5862b568-e8cd-47fd-b7d7-0c310acac936 (Chinese Version Only).
This stock will also be removed from the List of Eligible SZSE Securities for Margin Trading and the List of Eligible SZSE Securities for Short Selling.'}]
沪股合资格校对的结果是 False, 深股合资格校对的结果是 False
'''

import sys
import traceback

sys.path.append("./../")
from hkland_elistocks.sh_human_gene import SHHumanTools
from hkland_elistocks.zh_human_gene import ZHHumanTools
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
        if count == 1:
            logger.info("插入新数据 {}".format(to_insert))
        elif count == 2:
            logger.info("刷新数据 {}".format(to_insert))
        else:
            logger.info("数据已存在 {}".format(to_insert))
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
    # ID   | TradingType | TargetCategory | InnerCode | SecuCode | SecuAbbr     | InDate              | OutDate | Flag | CCASSCode | ParValue | CREATETIMEJZ        | UPDATETIMEJZ        | CMFID | CMFTime
    fields = ["TradingType", "TargetCategory", "InnerCode", "SecuCode", "SecuAbbr", "InDate", "OutDate", "Flag", "CCASSCode", "ParValue"]
    save(data, table, fields)


def run_0507():
    '''
    mysql> select * from hkland_hgelistocks where SecuCode = '600816' order by InDate;
    +------+-------------+----------------+-----------+----------+--------------+---------------------+---------+------+-----------+----------+---------------------+---------------------+-------+---------+
    | ID   | TradingType | TargetCategory | InnerCode | SecuCode | SecuAbbr     | InDate              | OutDate | Flag | CCASSCode | ParValue | CREATETIMEJZ        | UPDATETIMEJZ        | CMFID | CMFTime |
    +------+-------------+----------------+-----------+----------+--------------+---------------------+---------+------+-----------+----------+---------------------+---------------------+-------+---------+
    | 7041 |           1 |              1 |      2051 | 600816   | 安信信托     | 2014-11-17 00:00:00 | NULL    |    1 | NULL      | NULL     | 2020-03-22 23:51:27 | 2020-03-22 23:51:27 |  NULL | NULL    |
    | 7042 |           1 |              3 |      2051 | 600816   | 安信信托     | 2015-03-02 00:00:00 | NULL    |    1 | NULL      | NULL     | 2020-03-22 23:51:27 | 2020-03-22 23:51:27 |  NULL | NULL    |
    | 7043 |           1 |              4 |      2051 | 600816   | 安信信托     | 2015-03-02 00:00:00 | NULL    |    1 | NULL      | NULL     | 2020-03-22 23:51:27 | 2020-03-22 23:51:27 |  NULL | NULL    |
    +------+-------------+----------------+-----------+----------+--------------+---------------------+---------+------+-----------+----------+---------------------+---------------------+-------+---------+
    3 rows in set (0.02 sec)
    '''
    item1 = {"TradingType": 1, "TargetCategory": 1, "InnerCode": 2051, "SecuCode": "600816", "SecuAbbr": "安信信托", "InDate": "2014-11-17 00:00:00", "OutDate": "2020-05-06 00:00:00", "Flag": 2}
    item2 = {"TradingType": 1, "TargetCategory": 3, "InnerCode": 2051, "SecuCode": "600816", "SecuAbbr": "安信信托", "InDate": "2015-03-02 00:00:00", "OutDate": "2020-05-06 00:00:00", "Flag": 2}
    item3 = {"TradingType": 1, "TargetCategory": 4, "InnerCode": 2051, "SecuCode": "600816", "SecuAbbr": "安信信托", "InDate": "2015-03-02 00:00:00", "OutDate": "2020-05-06 00:00:00", "Flag": 2}
    item4 = {"TradingType": 1, "TargetCategory": 2, "InnerCode": 2051, "SecuCode": "600816", "SecuAbbr": "安信信托", "InDate": "2020-05-06 00:00:00", "Flag": 1}
    human_insert("hkland_hgelistocks", item1)
    human_insert("hkland_hgelistocks", item2)
    human_insert("hkland_hgelistocks", item3)
    human_insert("hkland_hgelistocks", item4)

    '''
    mysql> select * from hkland_sgelistocks where SecuCode = '002681' order by InDate;
    +-------+-------------+----------------+-----------+----------+--------------+---------------------+---------+------+-----------+----------+---------------------+---------------------+-------+---------+
    | ID    | TradingType | TargetCategory | InnerCode | SecuCode | SecuAbbr     | InDate              | OutDate | Flag | CCASSCode | ParValue | CREATETIMEJZ        | UPDATETIMEJZ        | CMFID | CMFTime |
    +-------+-------------+----------------+-----------+----------+--------------+---------------------+---------+------+-----------+----------+---------------------+---------------------+-------+---------+
    | 10668 |           3 |              1 |     16668 | 002681   | 奋达科技     | 2016-12-05 00:00:00 | NULL    |    1 | NULL      | NULL     | 2020-03-23 00:22:59 | 2020-03-23 00:22:59 |  NULL | NULL    |
    | 10669 |           3 |              3 |     16668 | 002681   | 奋达科技     | 2016-12-05 00:00:00 | NULL    |    1 | NULL      | NULL     | 2020-03-23 00:22:59 | 2020-03-23 00:22:59 |  NULL | NULL    |
    | 10670 |           3 |              4 |     16668 | 002681   | 奋达科技     | 2016-12-05 00:00:00 | NULL    |    1 | NULL      | NULL     | 2020-03-23 00:22:59 | 2020-03-23 00:22:59 |  NULL | NULL    |
    +-------+-------------+----------------+-----------+----------+--------------+---------------------+---------+------+-----------+----------+---------------------+---------------------+-------+---------+
    3 rows in set (0.03 sec)
    '''
    item1 = {"TradingType": 3, "TargetCategory": 1, "InnerCode": 16668, "SecuCode": "002681", "SecuAbbr": "奋达科技", "InDate": "2016-12-05 00:00:00", "OutDate": "2020-05-06 00:00:00", "Flag": 2}
    item2 = {"TradingType": 3, "TargetCategory": 3, "InnerCode": 16668, "SecuCode": "002681", "SecuAbbr": "奋达科技", "InDate": "2016-12-05 00:00:00", "OutDate": "2020-05-06 00:00:00", "Flag": 2}
    item3 = {"TradingType": 3, "TargetCategory": 4, "InnerCode": 16668, "SecuCode": "002681", "SecuAbbr": "奋达科技", "InDate": "2016-12-05 00:00:00", "OutDate": "2020-05-06 00:00:00", "Flag": 2}
    item4 = {"TradingType": 3, "TargetCategory": 2, "InnerCode": 16668, "SecuCode": "002681", "SecuAbbr": "奋达科技", "InDate": "2020-05-06 00:00:00", "Flag": 1}
    human_insert("hkland_sgelistocks", item1)
    human_insert("hkland_sgelistocks", item2)
    human_insert("hkland_sgelistocks", item3)
    human_insert("hkland_sgelistocks", item4)

    '''
    mysql> select * from hkland_sgelistocks where SecuCode = '002176' order by InDate;
    +-------+-------------+----------------+-----------+----------+--------------+---------------------+---------+------+-----------+----------+---------------------+---------------------+-------+---------+
    | ID    | TradingType | TargetCategory | InnerCode | SecuCode | SecuAbbr     | InDate              | OutDate | Flag | CCASSCode | ParValue | CREATETIMEJZ        | UPDATETIMEJZ        | CMFID | CMFTime |
    +-------+-------------+----------------+-----------+----------+--------------+---------------------+---------+------+-----------+----------+---------------------+---------------------+-------+---------+
    | 10039 |           3 |              1 |      6139 | 002176   | 江特电机     | 2016-12-05 00:00:00 | NULL    |    1 | NULL      | NULL     | 2020-03-23 00:17:02 | 2020-03-23 00:17:02 |  NULL | NULL    |
    | 10040 |           3 |              3 |      6139 | 002176   | 江特电机     | 2016-12-05 00:00:00 | NULL    |    1 | NULL      | NULL     | 2020-03-23 00:17:02 | 2020-03-23 00:17:02 |  NULL | NULL    |
    | 10041 |           3 |              4 |      6139 | 002176   | 江特电机     | 2016-12-05 00:00:00 | NULL    |    1 | NULL      | NULL     | 2020-03-23 00:17:02 | 2020-03-23 00:17:02 |  NULL | NULL    |
    +-------+-------------+----------------+-----------+----------+--------------+---------------------+---------+------+-----------+----------+---------------------+---------------------+-------+---------+
    3 rows in set (0.02 sec)
    '''
    item1 = {"TradingType": 3, "TargetCategory": 1, "InnerCode": 6139, "SecuCode": "002176", "SecuAbbr": "江特电机", "InDate": "2016-12-05 00:00:00", "OutDate": "2020-05-06 00:00:00", "Flag": 2}
    item2 = {"TradingType": 3, "TargetCategory": 3, "InnerCode": 6139, "SecuCode": "002176", "SecuAbbr": "江特电机", "InDate": "2016-12-05 00:00:00", "OutDate": "2020-05-06 00:00:00", "Flag": 2}
    item3 = {"TradingType": 3, "TargetCategory": 4, "InnerCode": 6139, "SecuCode": "002176", "SecuAbbr": "江特电机", "InDate": "2016-12-05 00:00:00", "OutDate": "2020-05-06 00:00:00", "Flag": 2}
    item4 = {"TradingType": 3, "TargetCategory": 2, "InnerCode": 6139, "SecuCode": "002176", "SecuAbbr": "江特电机", "InDate": "2020-05-06 00:00:00", "Flag": 1}
    human_insert("hkland_sgelistocks", item1)
    human_insert("hkland_sgelistocks", item2)
    human_insert("hkland_sgelistocks", item3)
    human_insert("hkland_sgelistocks", item4)


def run_0615():
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
    base_sql = """select * from hkland_hgelistocks where SecuCode = '{}' order by InDate;"""
    for code in ['600070', '600984', '601512', '601816', '603053', '603068', '603218', '603489', '603520',
                '603610', '603690', '603786', '603920', '603927', '603960']:
        sql = base_sql.format(code)

        pass




    pass


def refresh_time():
    sh = SHHumanTools()
    zh = ZHHumanTools()
    sh.refresh_update_time()
    zh.refresh_update_time()


if __name__ == "__main__":

    run_0507()


    refresh_time()
