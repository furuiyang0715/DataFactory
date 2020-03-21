import datetime
import logging
import sys
import time
from warnings import filterwarnings

import pandas as pd
import numpy as np
import pymysql

logger = logging.getLogger()


class BaseDerived(object):
    def __init__(self, name, **kwargs):
        self.name = name
        self.get_data_type = kwargs.get("get_data_type")   # 表明获取数据源的方式 "1" 从数据库中获取 "2"从文件中获取
        self.sql_cfg = kwargs.get("sql_cfg")      # 从 mysql 数据库读取数据的 mysql 配置
        self.sql_path_str = kwargs.get("sql_path_str")    # 读出 mysql 数据将其存放的文件位置
        self.file_path_str = kwargs.get("file_path_str")   # 数据来源为文件时的文件名
        self.source_data = None  # df 格式的源数据
        self.to_db_cfg = kwargs.get("to_db_cfg")   # 保存最终数据的数据库的配置
        self.to_table = kwargs.get("to_table")    # 要插入的数据库表的表名

        # self.to_mysql = MysqlHandler(TO_DB_DICT)
        # self.from_db = from_db
        # self.data_path = data_path

    def coning(self, cfg):
        filterwarnings('ignore', category=pymysql.Warning)
        return pymysql.connect(host=cfg['host'], port=cfg['port'], user=cfg['user'], password=cfg['password'],
                               db=cfg['db'], charset=cfg['charset'], connect_timeout=3000)

    def _get_from_mysql(self):
        conn = self.coning(self.sql_cfg)
        df = pd.read_sql(self.sql_path_str, conn)
        conn.close()
        return df

    def _get_from_file(self):
        df = pd.read_csv(self.file_path_str)
        return df

    def init_source_data(self):
        df_obj = None
        if self.get_data_type == "1":
            df_obj = self._get_from_mysql()
        elif self.get_data_type == "2":
            df_obj = self._get_from_file()
        self.source_data = df_obj

    # def pre_process(self):
    #     if self.update_name in conf_p.options("source_code"):   # 用到源数据, 加编号
    #         self.source_data["OrgTableCode"] = int(conf_p.get("source_code", self.update_name))

    def process(self):
        pass

    def after_process(self):
        pass

    def save2db(self, df, table, conn):
        """
        以去重的方式来写入dataframe至sql数据库
        需要表结构中建立了唯一索引
        """
        # 如果 dataframe 不为空则执行,为空则不做任何操作
        # 断开重连数据库
        conn.ping(reconnect=True)
        if not df.empty:
            df = df.fillna('NULL')
            cl = list(df.columns)
            str1 = ""
            for i in cl:
                i = "`%s`" % i
                str1 = str1 + i + ','
            str1 = str1[:-1]
            str1 = "(%s)" % str1

            df = df[cl]
            values = df.values
            # 拼接插入字符串
            sqlstr = "INSERT INTO %s %s VALUES " % (table, str1)
            value_list = []
            for row in values:
                percent_str = "%s," * len(row)
                percent_str = "(%s)" % percent_str[:-1]
                sqlstr = sqlstr + percent_str + ','
                vlist = list(row)
                value_list.extend(vlist)
            sqlstr = sqlstr[:-1] + " ON DUPLICATE KEY UPDATE "
            str2 = ""
            for col in cl:
                str2 = str2 + "`%s`=VALUES(`%s`)," % (col, col)
            str2 = str2[:-1]
            sqlstr = sqlstr + str2 + ';'

            f = lambda x: None if x == 'NULL' else str(x)
            value_tp = tuple(list(map(f, value_list)))
            with conn.cursor() as cur:
                cur.execute(sqlstr, value_tp)
                conn.commit()

    def __write_to_sql(self, df, table_name, conn):
        """写入失败后, 60秒后重新尝试"""
        flag = False
        while True:
            if flag is False:
                try:
                    self.save2db(df, table_name, conn)
                    flag = True
                    break
                except Exception as e:
                    conn.rollback()
                    e = str(e)
                    er1 = "Lost connection to MySQL"
                    er2 = "Can't connect to MySQL"
                    if er1 in e:
                        logging.info("%s,try again 10 sec later !" % er1)
                        time.sleep(60)
                    elif er2 in e:
                        logging.info("%s,try again 10 sec later !" % er2)
                        time.sleep(60)
                    else:
                        conn.close()
                        logging.info("unconnection Erro,need check !")
                        logging.info(e)
                        print(e)
                        sys.exit()  # ---出现非链接错误,强制程序退出,需人工检查

    def save_data(self):
        conn = self.coning(self.to_db_cfg)
        self._save_data(self.source_data, self.to_table, conn)
        conn.close()

    def _save_data(self, df, table, conn):
        """每次3000条写入数据库"""
        arr = df.values
        cols = df.columns
        shape = arr.shape
        l1 = []
        num = 0

        for i in range(shape[0]):
            l1.append(arr[i, :])
            num += 1

            if num % 3000 == 0:
                ar = np.concatenate([l1], axis=0)
                adf = pd.DataFrame(ar, columns=cols)
                l1 = []
                self.__write_to_sql(adf, table, conn)
                print('--------------------------', num)
        if len(l1) >= 1:
            ar = np.concatenate([l1], axis=0)
            adf = pd.DataFrame(ar, columns=cols)
            l1 = []
            self.__write_to_sql(adf, table, conn)
            print('--------------------------', num)
        print('update total:', num)

    def _update(self):
        self.init_source_data()
        self.process()
        self.save_data()


class OfficialUpdate(BaseDerived):
    # 官方媒体数据清洗处理 继承数据清洗的基类
    def process(self):
        # 标识id
        source_id = {
            "证监会-行政执法文书": 1,
            "证监会-政策法规": 1,
            "证监会-新闻发布": 1,
            "中国银行": 2,
            "国家统计局": 3,
        }

        # 官媒自身对 df 数据的处理逻辑
        # 排序去重 按照更新时间排序
        self.source_data.sort_values(by="CMFTime", ascending=False,  inplace=True)
        # 根据标题以及发布时间去重
        self.source_data.drop_duplicates(subset=['Title', 'PubDate'], keep='first', inplace=True)
        # 加入来源标识
        self.source_data["Source"] = source_id[self.update_name]
        # 加入hashid后去重
        # self.source_data = hashdataframebycols(self.source_data, ["Title", "PubDate", "Source", "CMFTime"])
        # print(self.source_data)
        # 与数据库对比去重
        # self.source_data = self.compare_drop(condition="HashID", subset="HashID")


def official_new():
    # 将更新时间确定到当前时间两天之前的时间点
    updatetime = datetime.date.today() - datetime.timedelta(days=2)
    # 拉取更新的 sql 语句
    official_dict = {
        # "证监会-行政执法文书": """SELECT tdLawName AS Title, tdLawPubDate AS PubDate, Cases AS Content, UPDATETIMEJZ AS CMFTime, id AS CMFID FROM csrc_administrative_enforcement_documents WHERE UPDATETIMEJZ > '%s';""",
        # "证监会-政策法规": """SELECT tdLawName AS Title, tdLawPubDate AS PubDate, Content, UPDATETIMEJZ AS CMFTime, id AS CMFID FROM csrc_laws_regulations_update WHERE UPDATETIMEJZ > '%s';""",
        # "证监会-新闻发布": """SELECT Title, Time AS PubDate, Content, UPDATETIMEJZ AS CMFTime, id AS CMFID FROM csrc_news_publish WHERE UPDATETIMEJZ > '%s';""",

        "中国银行": """SELECT title AS Title, pub_date AS PubDate, article AS Content, UPDATETIMEJZ AS CMFTime, id AS CMFID FROM chinabank WHERE UPDATETIMEJZ > '%s';""",
        "国家统计局": """SELECT title AS Title, pub_date AS PubDate, article AS Content, UPDATETIMEJZ AS CMFTime, id AS CMFID FROM gov_stats  WHERE UPDATETIMEJZ > '%s';""",
    }

    for key in official_dict:
        sql = official_dict[key] % updatetime
        # print(">>>>> ", sql)
        # official_news = OfficialUpdate(key, "news_official", FROM_DB_MY2, sql)
        # official_news.update()


if __name__ == "__main__":
    # 数据被插入的库

    official_new()

