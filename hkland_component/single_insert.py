# 解决需要单独进行处理的情况
# (1) 沪港通: 经核对无误

# （1） 深港通: 经核对无误
'''
[I 2020-03-20 10:00:37|merge_lc_zhsccomponent|process_zh_changes|129] 新增一条恢复记录{'CompType': 3, 'SecuCode': '000043', 'InDate': datetime.datetime(2019, 12, 16, 0, 0)}
请检查: 000022 0
[I 2020-03-20 10:02:04|merge_lc_zhsccomponent|process_zh_changes|123] 新增一条记录: {'CompType': 3, 'SecuCode': '000022', 'InDate': datetime.datetime(2018, 1, 2, 0, 0), 'InnerCode': None, 'Flag': 1}
'''
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
    fields = ["CompType", "InnerCode", "SecuCode", "InDate", "OutDate", "Flag"]
    save(data, table, fields)


human_insert("hkland_sgcomponent", {'CompType': 4, 'SecuCode': '00697', 'InDate': datetime.datetime(2020, 4, 15, 0, 0), 'InnerCode': 1000543, 'Flag': 1})


# 港股(深)成分变更: 需要新增一条调出记录{'CompType': 1, 'SecuCode': '00187', 'OutDate': datetime.datetime(2020, 4, 20, 0, 0), 'InnerCode': 1000176, 'Flag': 2}
# mysql> select * from hkland_hgcomponent where  SecuCode = '00187';
# +------+----------+-----------+----------+---------------------+---------------------+------+---------------------+---------------------+-------+---------+
# | ID   | CompType | InnerCode | SecuCode | InDate              | OutDate             | Flag | CREATETIMEJZ        | UPDATETIMEJZ        | CMFID | CMFTime |
# +------+----------+-----------+----------+---------------------+---------------------+------+---------------------+---------------------+-------+---------+
# | 2953 |        1 |   1000176 | 00187    | 2015-04-13 00:00:00 | 2017-04-24 00:00:00 |    2 | 2020-03-21 17:17:15 | 2020-03-21 17:17:15 |  NULL | NULL    |
# | 2954 |        1 |   1000176 | 00187    | 2018-04-23 00:00:00 | NULL                |    1 | 2020-03-21 17:17:15 | 2020-03-21 17:17:15 |  NULL | NULL    |
# +------+----------+-----------+----------+---------------------+---------------------+------+---------------------+---------------------+-------+---------+
# 2 rows in set (0.01 sec)
human_insert("hkland_hgcomponent", {'CompType': 1, 'SecuCode': '00187', 'OutDate': datetime.datetime(2020, 4, 20, 0, 0), 'InnerCode': 1000176,
              'Flag': 2, "InDate": datetime.datetime(2018, 4, 23, 0, 0),
              })


sh = SHSCComponent()
zh = ZHSCComponent()
sh.refresh_update_time()
zh.refresh_update_time()
