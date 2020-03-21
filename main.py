import datetime
import hashlib
import sys
import time
import traceback
from warnings import filterwarnings

import pymysql
import pandas as pd
import numpy as np


def coning(cfg):
    filterwarnings('ignore', category=pymysql.Warning)
    return pymysql.connect(host=cfg['host'], port=cfg['port'], user=cfg['user'], password=cfg['password'],
                           db=cfg['db'], charset=cfg['charset'], connect_timeout=3000)


def hashstr(text):
    """
    对字符串进行哈希
    :param text:
    :return:
    """
    if isinstance(text, bytes):
        pass
    elif isinstance(text, str):
        text = text.encode()
    else:
        raise Exception("text 类型错误")

    md5 = hashlib.md5(text)
    md5.update(text)
    ret = md5.hexdigest()
    return ret


def hashdataframebycols(data, cols):
    """
    对数据框中的每行数据进行哈希---只针对部分字段
    :param data:
    :param cols:
    :return:
    """
    df = data[cols]
    arr = df.values  # [["每一行数据"] [] [] ...]
    # print(arr)
    # print(arr[1, :])   # 拿出某一行的数据
    shape = arr.shape
    # print(shape)   # (10, 4) 即（行, 列）
    # sys.exit(0)
    l1 = []
    for i in range(shape[0]):
        lnarr = arr[i, :]  # eg. ['金融支持疫情防控和经济社会发展座谈会在京召开' Timestamp('2020-03-04 00:00:00') 2 Timestamp('2020-03-05 05:00:33')]

        lnstr = ''
        for j in lnarr:
            if not pd.isnull(j):  # 存在值
                lnstr += str(j)   # 将几个字段的值合并"+"

        hashln = hashstr(lnstr)
        l1.append(hashln)
    # l1 是每一行 hash 值的一个列表
    # print(l1)
    # sys.exit(0)
    data['HashID'] = l1
    # 根据生成的 hashid 进行去重
    data = data.drop_duplicates(subset=['HashID'], keep='last')
    return data


def _write_df_to_sql(df, table, conn):
    """
    以去重的方式来写入 dataframe 至 sql 数据库
    需要表结构中建立了唯一索引
    """
    conn.ping(reconnect=True)
    if not df.empty:
        df = df.fillna('NULL')
        cl = list(df.columns)  # 字段列表

        # print(cl)  # ['Title', 'PubDate', 'Content', 'CMFTime', 'CMFID', 'Source', 'HashID']
        # sys.exit(0)
        str1 = ""
        for i in cl:
            i = "`%s`" % i  # 等同于 i = "`{}`".format(i)
            # print(">> ", i)
            str1 = str1 + i + ','
        # 去掉最后加上的 ", "
        str1 = str1[:-1]
        str1 = "(%s)" % str1
        # print("<<< ", str1)  # (`Title`,`PubDate`,`Content`,`CMFTime`,`CMFID`,`Source`,`HashID`)
        # sys.exit(0)

        df = df[cl]
        values = df.values
        sqlstr = "INSERT INTO %s %s VALUES " % (table, str1)
        # print(">> ", sqlstr)  # (`Title`,`PubDate`,`Content`,`CMFTime`,`CMFID`,`Source`,`HashID`)
        # sys.exit(0)
        value_list = []
        for row in values:
            # print("row: ", row)
            percent_str = "%s," * len(row)
            # print("percent_str_1: ", percent_str)   # %s,%s,%s,%s,%s,%s,%s,
            percent_str = "(%s)" % percent_str[:-1]
            # print("percent_str_2: ", percent_str)   # (%s,%s,%s,%s,%s,%s,%s)
            sqlstr = sqlstr + percent_str + ','
            # print(sqlstr)  # INSERT INTO new_china_bank (`Title`,`PubDate`,`Content`,`CMFTime`,`CMFID`,`Source`,`HashID`) VALUES (%s,%s,%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s,%s,%s),
            vlist = list(row)
            # print("vlist: ", vlist)
            value_list.extend(vlist)
            # print("value_list: ", value_list)
            # print()
        # sys.exit(0)
        # 当insert已经存在的记录时，执行 Update
        sqlstr = sqlstr[:-1] + " ON DUPLICATE KEY UPDATE "
        str2 = ""
        for col in cl:
            str2 = str2 + "`%s`=VALUES(`%s`)," % (col, col)
        str2 = str2[:-1]
        sqlstr = sqlstr + str2 + ';'
        # print(">>> ", sqlstr)
        """
        INSERT INTO new_china_bank (`Title`,`PubDate`,`Content`,`CMFTime`,`CMFID`,`Source`,`HashID`) 
        VALUES (%s,%s,%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s,%s,%s),
        (%s,%s,%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s,%s,%s),
        (%s,%s,%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s,%s,%s) 
        ON DUPLICATE KEY UPDATE `Title`=VALUES(`Title`),`PubDate`=VALUES(`PubDate`),`Content`=VALUES(`Content`),
        `CMFTime`=VALUES(`CMFTime`),`CMFID`=VALUES(`CMFID`),`Source`=VALUES(`Source`),`HashID`=VALUES(`HashID`);
        """
        # sys.exit(0)

        f = lambda x: None if x == 'NULL' else str(x)
        value_tp = tuple(list(map(f, value_list)))
        # print(value_tp)  # 全部的数据，不分组，在 sqlstr里面进行了对应分组
        # sys.exit(0)

        # 执行插入
        with conn.cursor() as cur:
            cur.execute(sqlstr, value_tp)
            conn.commit()


def write_df_to_sql(df, table_name, conn):
    """
    将 dataframe 数据写入 mysql 数据库
    :param df:
    :param table_name:  表明
    :param conn:
    :return:
    """
    retry = 5
    while True:
        try:
            _write_df_to_sql(df, table_name, conn)
        except Exception as e:
            retry -= 1
            if retry < 0:
                break
            conn.rollback()
            traceback.print_exc()
            time.sleep(60)
        else:
            break
    conn.close()


def save_data(df, table, conn):
    """每次3000条写入数据库"""
    arr = df.values   # [[每一行数据] [] [] [] ... ]
    cols = df.columns
    # print(cols)   # Index(['Title', 'PubDate', 'Content', 'CMFTime', 'CMFID', 'Source', 'HashID'], dtype='object')
    # sys.exit(0)
    shape = arr.shape   # (行, 列)
    l1 = []
    num = 0

    for i in range(shape[0]):
        l1.append(arr[i, :])
        num += 1

        if num % 3000 == 0:
            # 将每一行按"列"进行拼接
            ar = np.concatenate([l1], axis=0)
            # 每 3000 行组成一个新的 df
            adf = pd.DataFrame(ar, columns=cols)
            # 清空 l1
            l1 = []
            write_df_to_sql(adf, table, conn)
            print(">>> ", num)
    # 不足 3000 行数据的部分
    if len(l1) >= 1:
        ar = np.concatenate([l1], axis=0)
        adf = pd.DataFrame(ar, columns=cols)
        l1 = []
        write_df_to_sql(adf, table, conn)
        print("<<< ", num)

    print('update total:', num)


def main():
    updatetime = datetime.date.today() - datetime.timedelta(days=10)

    # 源数据库的配置
    source_sql_cfg = {
        "host": "14.152.49.155",
        "port": 8998,
        "user": "rootb",
        "password": "3x870OV649AMSn*",
        "db": "test_furuiyang",
        'charset': 'utf8',
    }

    # 目标数据库配置
    target_sql_cfg = {
        "host": "14.152.49.155",
        "port": 8998,
        "user": "rootb",
        "password": "3x870OV649AMSn*",
        "db": "test_furuiyang",
        'charset': 'utf8',
    }

    official_dict = {
        "中国银行": """SELECT title AS Title, pub_date AS PubDate, article AS Content, UPDATETIMEJZ AS CMFTime, id AS CMFID FROM chinabank WHERE UPDATETIMEJZ > '%s';""",
        "国家统计局": """SELECT title AS Title, pub_date AS PubDate, article AS Content, UPDATETIMEJZ AS CMFTime, id AS CMFID FROM gov_stats  WHERE UPDATETIMEJZ > '%s';""",
    }

    # 将每种数据来源对应一个标号
    source_id = {
        "中国银行": 2,
        "国家统计局": 3,
    }

    for key in official_dict:
        sql = official_dict[key] % updatetime
        # print(">>  ", sql)
        conn = coning(source_sql_cfg)
        # print(">> ", conn)
        # conn.close()
        df = pd.read_sql(sql, conn)
        conn.close()
        # print(df)

        # 对 df 数据进行处理
        # （1） 首先按照 "更新时间" 进行 "降序" 排列
        # inplace 是改变了原来的 df 排序的意思, 如果 inplace 为 False, 意思是原来的 df 不变, 返回一个新的 df
        df.sort_values(by="CMFTime", ascending=False, inplace=True)
        # (2) 然后根据"标题&发布时间" 进行联合去重
        df.drop_duplicates(subset=['Title', 'PubDate'], keep='first', inplace=True)
        # (3) 为其加入来源标识列
        df['Source'] = source_id[key]
        # (4) 加入 hashid 后进行去重
        source_data = hashdataframebycols(df, ["Title", "PubDate", "Source", "CMFTime"])

        # 将处理之后的数据进行写入
        if not df.empty:
            # 即将写入的数据库的配置
            conn = coning(target_sql_cfg)
            save_data(df, "news_official", conn)
            try:
                conn.close()
            except:
                pass


main()
# 创建的新表
'''
 CREATE TABLE `news_official` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `Source` int(10) NOT NULL COMMENT '来源媒体: 证监会 1',
  `PubDate` datetime NOT NULL COMMENT '新闻发布日期',
  `Title` varchar(1000) COLLATE utf8_bin NOT NULL COMMENT '新闻标题',
  `Content` longtext COLLATE utf8_bin COMMENT '新闻正文',
  `HashID` varchar(50) COLLATE utf8_bin NOT NULL COMMENT '标题,日期, 来源, 更新时间哈希ID',
  `CMFTime` datetime NOT NULL COMMENT 'Come From Time',
  `CMFID` bigint(20) unsigned NOT NULL COMMENT 'Come From ID',
  `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
  `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `OrgSource` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '原始来源',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `unique_key` (`HashID`) USING BTREE,
  UNIQUE KEY `unique_key2` (`PubDate`,`Source`,`Title`) USING BTREE,
  KEY `Source` (`Source`),
  KEY `PubDate` (`PubDate`)
) ENGINE=InnoDB AUTO_INCREMENT=91004 DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='官媒-资讯'; 
'''