import datetime
import traceback

import sys
sys.path.append("./../")

from hkland_component.sh_version_2 import SHSCComponent, ZHSCComponent
from hkland_component.configs import TARGET_HOST, TARGET_PORT, TARGET_USER, TARGET_PASSWD, TARGET_DB
from hkland_component.my_log import logger
from hkland_component.sql_pool import PyMysqlPoolBase

'''
查询出全部有过更名情况的： 
mysql> select SSESCode, EffectiveDate, Ch_ange  from hkex_lgt_change_of_szse_securities_lists where Ch_ange like "%Stock Name are changed to%";
+----------+---------------+--------------------------------------------------------------------------------------------------------------+
| SSESCode | EffectiveDate | Ch_ange                                                                                                      |
+----------+---------------+--------------------------------------------------------------------------------------------------------------+
| 000043   | 2019-12-16    | SZSE Stock Code and Stock Name are changed to 001914 and CHINA MERCHANTS PPTY OPERATION&SERVICE respectively |
| 000022   | 2018-12-27    | SZSE Stock Code and Stock Name are changed to 1872 and CHINA MERCHANTS PORT GROUP respectively               |
| 000043   | 2019-12-16    | SZSE Stock Code and Stock Name are changed to 001914 and CHINA MERCHANTS PPTY OPERATION&SERVICE respectively |
| 000022   | 2018-12-27    | SZSE Stock Code and Stock Name are changed to 1872 and CHINA MERCHANTS PORT GROUP respectively               |
| 000043   | 2019-12-16    | SZSE Stock Code and Stock Name are changed to 001914 and CHINA MERCHANTS PPTY OPERATION&SERVICE respectively |
| 000022   | 2018-12-27    | SZSE Stock Code and Stock Name are changed to 1872 and CHINA MERCHANTS PORT GROUP respectively               |
+----------+---------------+--------------------------------------------------------------------------------------------------------------+
6 rows in set (0.02 sec)

mysql> select SSESCode, EffectiveDate, Ch_ange  from hkex_lgt_change_of_sse_securities_lists where Ch_ange like "%Stock Name are changed to%";
+----------+---------------+----------------------------------------------------------------------------------------------+
| SSESCode | EffectiveDate | Ch_ange                                                                                      |
+----------+---------------+----------------------------------------------------------------------------------------------+
| 601313   | 2018-02-26    | SSE Stock Code and Stock Name are changed to 601360 and 360 SECURITY TECHNOLOGY respectively |
| 601313   | 2018-02-26    | SSE Stock Code and Stock Name are changed to 601360 and 360 SECURITY TECHNOLOGY respectively |
| 601313   | 2018-02-26    | SSE Stock Code and Stock Name are changed to 601360 and 360 SECURITY TECHNOLOGY respectively |
+----------+---------------+----------------------------------------------------------------------------------------------+
3 rows in set (0.06 sec) 


mysql>select SSESCode, EffectiveDate, Ch_ange from hkex_lgt_change_of_szse_securities_lists where SSESCode = '000043' order by  EffectiveDate;
+----------+---------------+------------------------------------------------------------------------------------------------------------------+
| SSESCode | EffectiveDate | Ch_ange                                                                                                          |
+----------+---------------+------------------------------------------------------------------------------------------------------------------+
| 000043   | 2016-12-05    | Addition                                                                                                         |
| 000043   | 2016-12-05    | Addition                                                                                                         |
| 000043   | 2017-01-03    | Transfer to List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)     |
| 000043   | 2017-01-03    | Transfer to List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)     |
| 000043   | 2017-07-03    | Addition (from List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)) |
| 000043   | 2017-07-03    | Addition (from List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)) |
| 000043   | 2019-01-02    | Transfer to List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)     |
| 000043   | 2019-01-02    | Transfer to List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)     |

| 000043   | 2019-12-16    | Addition (from List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)) |
| 000043   | 2019-12-16    | SZSE Stock Code and Stock Name are changed to 001914 and CHINA MERCHANTS PPTY OPERATION&SERVICE respectively     |
| 000043   | 2019-12-16    | Addition (from List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)) |
| 000043   | 2019-12-16    | SZSE Stock Code and Stock Name are changed to 001914 and CHINA MERCHANTS PPTY OPERATION&SERVICE respectively     |
+----------+---------------+------------------------------------------------------------------------------------------------------------------+
12 rows in set (0.03 sec)


mysql> select * from lc_zhsccomponent where SecuCode = '000043';
+--------------+----------+-----------+----------+---------------------+---------------------+------+---------------------+--------------+
| ID           | CompType | InnerCode | SecuCode | InDate              | OutDate             | Flag | UpdateTime          | JSID         |
+--------------+----------+-----------+----------+---------------------+---------------------+------+---------------------+--------------+
| 534243730428 |        3 |        87 | 000043   | 2016-12-05 00:00:00 | 2017-01-03 00:00:00 |    2 | 2016-12-26 08:16:13 | 536055373049 |
| 552479654737 |        3 |        87 | 000043   | 2017-07-03 00:00:00 | 2019-01-02 00:00:00 |    2 | 2019-01-07 08:39:10 | 600165550162 |
+--------------+----------+-----------+----------+---------------------+---------------------+------+---------------------+--------------+
2 rows in set (0.02 sec) 

# 改名之后的记录: 
mysql> select * from lc_zhsccomponent where SecuCode = '001914';
+--------------+----------+-----------+----------+---------------------+---------+------+---------------------+--------------+
| ID           | CompType | InnerCode | SecuCode | InDate              | OutDate | Flag | UpdateTime          | JSID         |
+--------------+----------+-----------+----------+---------------------+---------+------+---------------------+--------------+
| 629367901158 |        3 |        87 | 001914   | 2019-12-16 00:00:00 | NULL    |    1 | 2019-12-16 07:10:05 | 629795446554 |
+--------------+----------+-----------+----------+---------------------+---------+------+---------------------+--------------+
'''

''' 关于 000022 的记录 
mysql> select SSESCode, EffectiveDate, Ch_ange from hkex_lgt_change_of_szse_securities_lists where SSESCode = '000022' order by  EffectiveDate;
+----------+---------------+------------------------------------------------------------------------------------------------+
| SSESCode | EffectiveDate | Ch_ange                                                                                        |
+----------+---------------+------------------------------------------------------------------------------------------------+
| 000022   | 2018-01-02    | Addition                                                                                       |
| 000022   | 2018-01-02    | Addition                                                                                       |
| 000022   | 2018-12-27    | SZSE Stock Code and Stock Name are changed to 1872 and CHINA MERCHANTS PORT GROUP respectively |
| 000022   | 2018-12-27    | SZSE Stock Code and Stock Name are changed to 1872 and CHINA MERCHANTS PORT GROUP respectively |
+----------+---------------+------------------------------------------------------------------------------------------------+
4 rows in set (0.01 sec)

mysql> select * from lc_zhsccomponent where SecuCode = '001872';
+--------------+----------+-----------+----------+---------------------+---------+------+---------------------+--------------+
| ID           | CompType | InnerCode | SecuCode | InDate              | OutDate | Flag | UpdateTime          | JSID         |
+--------------+----------+-----------+----------+---------------------+---------+------+---------------------+--------------+
| 567765260735 |        3 |        58 | 001872   | 2018-01-02 00:00:00 | NULL    |    1 | 2018-12-26 07:25:20 | 599124319577 |
+--------------+----------+-----------+----------+---------------------+---------+------+---------------------+--------------+
1 row in set (0.01 sec) 
'''


''' 4.20 运行结果同上 
[I 2020-04-20 10:36:48|sh_version_2|process_zh_changes|164] 新增一条恢复记录{'CompType': 3, 'SecuCode': '000043', 'InDate': datetime.datetime(2019, 12, 16, 0, 0)}
请检查: 000022 0
[I 2020-04-20 10:38:05|sh_version_2|process_zh_changes|158] 新增一条记录: {'CompType': 3, 'SecuCode': '000022', 'InDate': datetime.datetime(2018, 1, 2, 0, 0), 'InnerCode': None, 'Flag': 1}


[I 2020-04-20 10:49:20|sh_version_2|process_hk_changes|244] 需要新增一条调入记录 {'CompType': 4, 'SecuCode': '00697', 'InDate': datetime.datetime(2020, 4, 15, 0, 0), 'InnerCode': 1000543, 'Flag': 1} 
mysql> select * from hkland_sgcomponent where SecuCode = '00697';
+------+----------+-----------+----------+---------------------+---------------------+------+---------------------+---------------------+-------+---------+
| ID   | CompType | InnerCode | SecuCode | InDate              | OutDate             | Flag | CREATETIMEJZ        | UPDATETIMEJZ        | CMFID | CMFTime |
+------+----------+-----------+----------+---------------------+---------------------+------+---------------------+---------------------+-------+---------+
| 9920 |        4 |   1000543 | 00697    | 2020-03-09 00:00:00 | 2020-03-30 00:00:00 |    2 | 2020-03-31 06:10:26 | 2020-03-31 06:10:26 |  NULL | NULL    |
+------+----------+-----------+----------+---------------------+---------------------+------+---------------------+---------------------+-------+---------+
1 row in set (0.02 sec)

需要插入一条数据: {'CompType': 4, 'SecuCode': '00697', 'InDate': datetime.datetime(2020, 4, 15, 0, 0), 'InnerCode': 1000543, 'Flag': 1}  


'''


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
    fields = ["CompType", "InnerCode", "SecuCode", "InDate", "OutDate", "Flag"]
    save(data, table, fields)


def run_20200420():
    human_insert("hkland_sgcomponent", {'CompType': 4, 'SecuCode': '00697', 'InDate': datetime.datetime(2020, 4, 15, 0, 0), 'InnerCode': 1000543, 'Flag': 1})
    human_insert("hkland_hgcomponent", {'CompType': 1, 'SecuCode': '00187', 'OutDate': datetime.datetime(2020, 4, 20, 0, 0), 'InnerCode': 1000176, 'Flag': 2, "InDate": datetime.datetime(2018, 4, 23, 0, 0),})


def run_20200506():
    item = {'CompType': 2, "InnerCode": "2051", 'SecuCode': '600816', 'OutDate': datetime.datetime(2020, 5, 6, 0, 0), "InDate": datetime.datetime(2014, 11, 17), "Flag": 2}
    human_insert("hkland_hgcomponent", item)

    item2 = {'CompType': 3, 'InnerCode': "16668", 'SecuCode': '002681', 'OutDate': datetime.datetime(2020, 5, 6, 0, 0), "InDate": datetime.datetime(2016, 12, 5), "Flag": 2}
    human_insert('hkland_sgcomponent', item2)

    item3 = {'CompType': 3, 'InnerCode': '6139', 'SecuCode': '002176', 'OutDate': datetime.datetime(2020, 5, 6, 0, 0), 'InDate': datetime.datetime(2016, 12, 5), "Flag": 2}
    human_insert('hkland_sgcomponent', item3)


def run_20200511():
    '''
    钉钉:
    深股成分股变更: 需要新增一条恢复记录{'CompType': 3, 'SecuCode': '000043', 'InDate': datetime.datetime(2019, 12, 16, 0, 0)}
    深股成分股变更: 需要新增一条记录{'CompType': 3, 'SecuCode': '000022', 'InDate': datetime.datetime(2018, 1, 2, 0, 0), 'InnerCode': None, 'Flag': 1}
    深股的核对结果: True
    港股(深)成分变更: 需要新增一条调出记录{'CompType': 4, 'SecuCode': '02989', 'OutDate': datetime.datetime(2020, 5, 8, 0, 0)}
    港股(深)的核对结果: False

    mysql> select * from hkland_sgcomponent where SecuCode = '02989';
    +------+----------+-----------+----------+---------------------+---------+------+---------------------+---------------------+-------+---------+
    | ID   | CompType | InnerCode | SecuCode | InDate              | OutDate | Flag | CREATETIMEJZ        | UPDATETIMEJZ        | CMFID | CMFTime |
    +------+----------+-----------+----------+---------------------+---------+------+---------------------+---------------------+-------+---------+
    | 9921 |        4 |   1253782 | 02989    | 2020-03-30 00:00:00 | NULL    |    1 | 2020-03-31 06:10:26 | 2020-03-31 06:10:26 |  NULL | NULL    |
    +------+----------+-----------+----------+---------------------+---------+------+---------------------+---------------------+-------+---------+
    1 row in set (0.02 sec)

    '''
    item = {'CompType': 4, 'InnerCode': '1253782', 'SecuCode': '02989', 'OutDate': datetime.datetime(2020, 5, 8, 0, 0), 'InDate': datetime.datetime(2020, 3, 30), "Flag": 2}
    human_insert('hkland_sgcomponent', item)


def run_20200611():
    # 沪股 需要新增记录的
    sh_first_in = [
        {'CompType': 2, 'SecuCode': '600070', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 1192, 'Flag': 1} ,
        {'CompType': 2, 'SecuCode': '600984', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 2918, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '601512', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 34877, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '601816', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 258899, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603053', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 180960, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603068', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 147247, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603218', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 40969, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603489', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 181703, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603520', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 34854, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603610', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 182479, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603690', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 35258, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603786', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 159848, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603920', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 48149, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603927', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 159829, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603960', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 47129, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '600131', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 1258, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '600223', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 1345, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '600529', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 1693, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '600764', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 1993, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '601519', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 12293, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603012', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 35000, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603018', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 35480, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603601', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 36410, 'Flag': 1},
        {'CompType': 2, 'SecuCode': '603678', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 35970, 'Flag': 1},
    ]
    # 沪股 需要恢复记录的
    sh_recover_in = [
        {'CompType': 2, 'SecuCode': '600988', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 2, 'SecuCode': '600079', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 2, 'SecuCode': '600143', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 2, 'SecuCode': '600621', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 2, 'SecuCode': '600737', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 2, 'SecuCode': '600776', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 2, 'SecuCode': '600802', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 2, 'SecuCode': '603000', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
    ]
    # 沪股 移出到只能卖出
    sh_remove_out = [
         {'CompType': 2, 'SecuCode': '600693', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '603007', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '603080', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '603165', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '603332', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '603339', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '603351', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '603603', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '603773', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '603877', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '603897', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '603898', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600123', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600230', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600231', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600239', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600297', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600398', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600418', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600499', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600528', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600535', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600623', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600661', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600664', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600771', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600826', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '600986', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '601002', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '601222', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '601997', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
         {'CompType': 2, 'SecuCode': '603959', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
    ]

    # 港股通（沪） 调出的
    sh_hk_remove_out = [
        {'CompType': 1, 'SecuCode': '00494', 'InnerCode': 1000405, 'InDate': datetime.datetime(2014, 11, 17), 'OutDate': datetime.datetime(2020, 5, 18, 0, 0), 'Flag': 2},
    ]
    for r in sh_hk_remove_out:
        # ["CompType", "InnerCode", "SecuCode", "InDate", "OutDate", "Flag"]
        # select * from hkland_hgcomponent where SecuCode = '00494';
        human_insert("hkland_hgcomponent", r)

    # * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    sz_first_in = [
        {'CompType': 3, 'SecuCode': '000032', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 75, 'Flag': 1}, 
        {'CompType': 3, 'SecuCode': '000785', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 442, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002015', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 2700, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002351', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 9642, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002459', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 10494, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002541', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 12241, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002552', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 12294, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002793', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 35055, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002803', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 35792, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002837', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 40167, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002955', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 180207, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002959', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 177196, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002961', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 48142, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002962', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 174214, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002966', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 104419, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300080', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 10590, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300455', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 34699, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300468', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 35842, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300552', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 34836, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300677', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 76667, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300775', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 135806, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300776', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 135830, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300777', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 100636, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300782', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 168983, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300783', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 124267, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300785', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 124269, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300788', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 130080, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300793', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 180967, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300799', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 106213, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '000912', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 541, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002214', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 6581, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300663', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 61468, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002201', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 6392, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002641', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 14222, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002706', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 12373, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002756', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 9104, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002838', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 17752, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002869', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 51251, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002880', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 61380, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300114', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 11219, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300132', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 11625, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300209', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 12912, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300319', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 16376, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300388', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 17408, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300395', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 35511, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300448', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 34764, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300525', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 36222, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300526', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 35011, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300573', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 36407, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300579', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 34342, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300590', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 51253, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300603', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 61457, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300604', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 65452, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300653', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 64857, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300657', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 38984, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300659', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 47133, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300662', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 61466, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300709', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 85466, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300771', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 130069, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002243', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 6815, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '002947', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 146464, 'Flag': 1},
        {'CompType': 3, 'SecuCode': '300328', 'InDate': datetime.datetime(2020, 6, 15, 0, 0), 'InnerCode': 16596, 'Flag': 1},
    ]
    sz_recover_in = [
        {'CompType': 3, 'SecuCode': '000058', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002239', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002312', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002605', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300083', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300376', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000030', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000519', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000700', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000719', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000917', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002169', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002250', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002287', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002791', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000601', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000796', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000903', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002083', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002126', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002135', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002324', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002479', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002484', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002528', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002609', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002616', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002850', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002918', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300031', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300045', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300229', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300303', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300386', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300438', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300477', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300568', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300571', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300607', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300613', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300623', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300624', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300664', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300672', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300684', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300737', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000652', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000823', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000829', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002022', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002079', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002106', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002117', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002161', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002182', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002276', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002313', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002428', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002518', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300020', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300177', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300202', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300256', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300287', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300397', 'InDate': datetime.datetime(2020, 6, 15, 0, 0)},
    ]
    sz_remove_out = [
        {'CompType': 3, 'SecuCode': '000429', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000863', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002314', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000088', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000552', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002280', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002293', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002370', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002608', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000657', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000666', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000815', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000882', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002057', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002309', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002550', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300185', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300252', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000040', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000525', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000980', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002366', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300367', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000036', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000592', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000861', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000926', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '000928', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002215', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002274', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002378', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '002639', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300266', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
        {'CompType': 3, 'SecuCode': '300355', 'OutDate': datetime.datetime(2020, 6, 15, 0, 0)},
    ]
    sz_hk_remove_out = [
        {'CompType': 4, 'SecuCode': '00494', 'InDate': datetime.datetime(2016, 12, 5), 'OutDate': datetime.datetime(2020, 5, 18, 0, 0), 'Flag': 2, "InnerCode": 1000405},
        {'CompType': 4, 'SecuCode': '02989', 'InDate': datetime.datetime(2020, 3, 30), 'OutDate': datetime.datetime(2020, 5, 8, 0, 0), 'Flag': 2, "InnerCode": 1253782},
    ]
    for r in sz_hk_remove_out:
        # ["CompType", "InnerCode", "SecuCode", "InDate", "OutDate", "Flag"]
        # select * from hkland_sgcomponent where SecuCode = '00494';
        human_insert('hkland_sgcomponent', r)


if __name__ == "__main__":
    run_20200611()
    sh = SHSCComponent()
    zh = ZHSCComponent()
    sh.refresh_update_time()
    zh.refresh_update_time()
