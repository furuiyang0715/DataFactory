import datetime
import traceback

from ganggutong_list.configs import JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB, SPIDER_HOST, SPIDER_PORT, \
    SPIDER_USER, SPIDER_PASSWD, SPIDER_DB, TARGET_PORT, TARGET_PASSWD, TARGET_HOST, TARGET_USER, TARGET_DB
from ganggutong_list.my_log import logger
from ganggutong_list.sql_pool import PyMysqlPoolBase


class MergeTools(object):
    # 本地数据库
    local_cfg = {
        "host": '127.0.0.1',
        "port": 3306,
        "user": 'root',
        "password": 'ruiyang',
        "db": 'test_db',
    }

    # 聚源数据库
    juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,

    }

    # 目标数据库 意思是规整完成的数据要插入的数据库
    target_cfg = {
        "host": TARGET_HOST,
        "port": TARGET_PORT,
        "user": TARGET_USER,
        "password": TARGET_PASSWD,
        "db": TARGET_DB,
    }

    # 爬虫数据库
    source_cfg = {
        "host": SPIDER_HOST,
        "port": SPIDER_PORT,
        "user": SPIDER_USER,
        "password": SPIDER_PASSWD,
        "db": SPIDER_DB,
    }

    def __init__(self):
        #  不对成分股记录产生影响的状态
        self.stats_todonothing = [
            'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
            'Remove from List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
            'SSE Stock Code and Stock Name are changed to 601360 and 360 SECURITY TECHNOLOGY respectively',
            'SSE Stock Code and Stock Name are changed from 601313 and SJEC respectively',
            'Buy orders suspended',
            'Buy orders resumed',
            'Addition to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
            # 详情见 601200
        ]
        self.stats_addition = ['Addition']
        self.stats_recover = [
            'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))']  # 从移除中恢复的状态

        self.stats_removal = ['Removal']
        self.stats_transfer = [
            'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
            # 'Addition to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
        ]  # Transfer to 加入这个名单; Addition to 从这个名单移除

    def init_sql_pool(self, sql_cfg: dict):
        pool = PyMysqlPoolBase(**sql_cfg)
        return pool

    # def sync_juyuan_table(self, table, target_cli, target_table_name, fields=None):
    #     # 直接同步聚源表的部分
    #     # TODO
    #     juyuan = self.init_sql_pool(self.juyuan_cfg)
    #     sql = 'select * from {}; '.format(table)
    #     datas = juyuan.select_all(sql)
    #     juyuan.dispose()
    #
    #     data = datas[0]
    #     fields = sorted(data.keys())
    #     columns = ", ".join(fields)
    #     placeholders = ', '.join(['%s'] * len(data))
    #     insert_sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (target_table_name, columns, placeholders)
    #     values = []
    #     for data in datas:
    #         value = tuple(data.get(field) for field in fields)
    #         values.append(value)
    #     try:
    #         count = target_cli.insert_many(insert_sql, values)
    #     except:
    #         logger.warning("导入聚源历史数据失败 ")
    #         traceback.print_exc()
    #     else:
    #         logger.info("沪港通成分股历史数据插入数量: {}".format(count))
    #     target_cli.dispose()

    # def target_insert(self, datas: list):
    #     target = self.init_sql_pool(self.target_cfg)
    #     data = datas[0]
    #     fields = sorted(data.keys())
    #     columns = ", ".join(fields)
    #     placeholders = ', '.join(['%s'] * len(data))
    #     insert_sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (self.target_table_name, columns, placeholders)
    #     values = []
    #     for data in datas:
    #         value = tuple(data.get(field) for field in fields)
    #         values.append(value)
    #     target.insert_many(insert_sql, values)
    #     target.dispose()

    def start(self):
        logger.info("Update Time: {}".format(datetime.datetime.now()))
        try:
            self._start()
        except:
            traceback.print_exc()

