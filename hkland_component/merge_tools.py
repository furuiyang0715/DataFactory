import datetime
import traceback

from hkland_component.configs import JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB, SPIDER_HOST, SPIDER_PORT, \
    SPIDER_USER, SPIDER_PASSWD, SPIDER_DB, TARGET_PORT, TARGET_PASSWD, TARGET_HOST, TARGET_USER, TARGET_DB
from hkland_component.my_log import logger
from hkland_component.sql_pool import PyMysqlPoolBase


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

    def init_sql_pool(self, sql_cfg: dict):
        pool = PyMysqlPoolBase(**sql_cfg)
        return pool

    def start(self):
        logger.info("Update Time: {}".format(datetime.datetime.now()))
        try:
            self._start()
        except:
            traceback.print_exc()

