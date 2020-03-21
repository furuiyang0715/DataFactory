# 解决需要单独进行处理的情况
# (1) 沪港通: 经核对无误

# （1） 深港通: 经核对无误
'''
[I 2020-03-20 10:00:37|merge_lc_zhsccomponent|process_zh_changes|129] 新增一条恢复记录{'CompType': 3, 'SecuCode': '000043', 'InDate': datetime.datetime(2019, 12, 16, 0, 0)}
请检查: 000022 0
[I 2020-03-20 10:02:04|merge_lc_zhsccomponent|process_zh_changes|123] 新增一条记录: {'CompType': 3, 'SecuCode': '000022', 'InDate': datetime.datetime(2018, 1, 2, 0, 0), 'InnerCode': None, 'Flag': 1}
'''

'''
select SSESCode, EffectiveDate, Ch_ange from hkex_lgt_change_of_szse_securities_lists where SSESCode = '000043' order by  EffectiveDate;
select * from lc_zhsccomponent where SecuCode = '000043';
'''


'''  这条是 ok 的 
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


