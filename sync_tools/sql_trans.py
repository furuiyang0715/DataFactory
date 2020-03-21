# coding=utf8
import configparser
import os
import pymysql
from DBUtils.PooledDB import PooledDB
from pymysql.cursors import DictCursor

env = os.environ

cf = configparser.ConfigParser()
thisdir = os.path.dirname(__file__)
cf.read(os.path.join(thisdir, '.conf'))

# 聚源
JUY_HOST = env.get("JUY_HOST", cf.get('juyuan', 'JUY_HOST'))
JUY_PORT = int(env.get("JUY_PORT", cf.get('juyuan', 'JUY_PORT')))
JUY_USER = env.get("JUY_USER", cf.get('juyuan', 'JUY_USER'))
JUY_PASSWD = env.get("JUY_PASSWD", cf.get('juyuan', 'JUY_PASSWD'))
JUY_DB = env.get("JUY_DB", cf.get('juyuan', 'JUY_DB'))

# 爬虫数据源
SPIDER_HOST = env.get("SPIDER_HOST", cf.get('spider', 'SPIDER_HOST'))
SPIDER_PORT = int(env.get("SPIDER_PORT", cf.get('spider', 'SPIDER_PORT')))
SPIDER_USER = env.get("SPIDER_USER", cf.get('spider', 'SPIDER_USER'))
SPIDER_PASSWD = env.get("SPIDER_PASSWD", cf.get('spider', 'SPIDER_PASSWD'))
SPIDER_DB = env.get("SPIDER_DB", cf.get('spider', 'SPIDER_DB'))

# test
TARGET_HOST = env.get("TARGET_HOST", cf.get('target', 'TARGET_HOST'))
TARGET_PORT = int(env.get("TARGET_PORT", cf.get('target', 'TARGET_PORT')))
TARGET_USER = env.get("TARGET_USER", cf.get('target', 'TARGET_USER'))
TARGET_PASSWD = env.get("TARGET_PASSWD", cf.get('target', 'TARGET_PASSWD'))
TARGET_DB = env.get("TARGET_DB", cf.get('target', 'TARGET_DB'))


class PyMysqlPoolBase(object):
    """
        建立 mysql 连接池的 python 封装
        与直接建立 mysql 连接的不同之处在于连接在完成操作之后不会立即提交关闭
        可复用以提高效率
    """
    _pool = None

    def __init__(self,
                 host,
                 port,
                 user,
                 password,
                 db=None):
        self.db_host = host
        self.db_port = int(port)
        self.user = user
        self.password = str(password)
        self.db = db
        self.connection = self._getConn()
        self.cursor = self.connection.cursor()

    def _getConn(self):
        """
        @summary: 静态方法，从连接池中取出连接
        @return MySQLdb.connection
        """
        if PyMysqlPoolBase._pool is None:
            _pool = PooledDB(creator=pymysql,
                             mincached=1,
                             maxcached=20,
                             host=self.db_host,
                             port=self.db_port,
                             user=self.user,
                             passwd=self.password,
                             db=self.db,
                             use_unicode=True,
                             charset="utf8",
                             cursorclass=DictCursor)
        print("已经成功从连接池中获取连接")
        return _pool.connection()

    def _exec_sql(self, sql, param=None):
        if param is None:
            count = self.cursor.execute(sql)
        else:
            count = self.cursor.execute(sql, param)
        return count

    def insert(self, sql, params=None):
        """
        @summary: 更新数据表记录
        @param sql: SQL 格式及条件，使用 (%s,%s)
        @param params: 要更新的值: tuple/list
        @return: count 受影响的行数
        """
        return self._exec_sql(sql, params)

    def select_all(self, sql, params=None):
        self.cursor.execute(sql, params)
        results = self.cursor.fetchall()
        return results

    def select_many(self, sql, params=None, size=1):
        self.cursor.execute(sql, params)
        results = self.cursor.fetchmany(size)
        return results

    def select_one(self, sql, params=None):
        self.cursor.execute(sql, params)
        result = self.cursor.fetchone()
        return result

    def insert_many(self, sql, values):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的 SQL 格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        count = self.cursor.executemany(sql, values)
        return count

    def update(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: SQL 格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self._exec_sql(sql, param)

    def delete(self, sql, param=None):
        """
        @summary: 删除数据表记录
        @param sql: SQL 格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """
        return self._exec_sql(sql, param)

    def begin(self):
        """
        @summary: 开启事务
        """
        self.connection.autocommit(0)

    def end(self, option='commit'):
        """
        @summary: 结束事务
        """
        if option == 'commit':
            self.connection.commit()
        else:
            self.connection.rollback()

    def dispose(self, isEnd=1):
        """
        @summary: 释放连接池资源
        """
        if isEnd == 1:
            self.end('commit')
        else:
            self.end('rollback')
        self.cursor.close()
        self.connection.close()


class ImportTools(object):
    # 聚源数据库
    juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,

    }

    # 爬虫数据库
    spider_cfg = {
        "host": SPIDER_HOST,
        "port": SPIDER_PORT,
        "user": SPIDER_USER,
        "password": SPIDER_PASSWD,
        "db": SPIDER_DB,
    }

    # 本地数据库配置
    local_cfg = {
        "host": '127.0.0.1',
        "port": 3306,
        "user": 'root',
        "password": 'ruiyang',
        "db": 'test_db',
    }

    # test
    test_cfg = {
        "host": TARGET_HOST,
        "port": TARGET_PORT,
        "user": TARGET_USER,
        "password": TARGET_PASSWD,
        "db": TARGET_DB,
    }

    need_tables = [
        "hkex_lgt_change_of_sse_securities_lists",  # 更改上交所证券/中华通证券名单
        'hkex_lgt_sse_securities',  # 上交所证券/中华通证券清单（可同时买卖的股票）
        'hkex_lgt_special_sse_securities',  # 特殊上证所证券/特殊中华通证券清单（仅适合出售的股票）
        'hkex_lgt_special_sse_securities_for_margin_trading',  # 保证金交易合格的上证所证券清单
        'hkex_lgt_special_sse_securities_for_short_selling',  # 合格卖空的上证所证券清单
        'hkex_lgt_sse_list_of_eligible_securities',  # 沪港通进行南向交易的合格证券清单（可同时买卖的股票）
        'lgt_sse_underlying_securities_adjustment',  # 新增

        'hkex_lgt_change_of_szse_securities_lists',  # 更改深交所证券/中华通证券名单（2020年1月）
        'hkex_lgt_szse_securities',  # 深交所证券/中华通证券清单（可同时买卖的股票）
        'hkex_lgt_special_szse_securities',  # 特殊深交所证券/特殊中华通证券清单（只限出售股票）
        'hkex_lgt_special_szse_securities_for_margin_trading',  # 合格保证金交易的深交所证券清单
        'hkex_lgt_special_szse_securities_for_short_selling',  # 合格的深交所合格股票清单
        'hkex_lgt_szse_list_of_eligible_securities',  # 深港通南向交易合资格证券一览表（可买卖的股票）
        'lgt_szse_underlying_securities_adjustment',  # 新增
    ]

    def init_sql_pool(self, sql_cfg: dict):
        pool = PyMysqlPoolBase(**sql_cfg)
        return pool

    def show_table(self, show_cfg, show_table):
        pool = self.init_sql_pool(show_cfg)
        ret = pool.select_one("show create table {};".format(show_table))
        ret = ret.get("Create Table") + ';'
        pool.dispose()
        return ret

    def transdatas(self, source_cfg, target_cfg):
        # 不包含建表这一步 需自行建立表结构
        for table in self.need_tables:
            source = self.init_sql_pool(source_cfg)
            sql = 'select * from {}; '.format(table)
            datas = source.select_all(sql)
            source.dispose()

            data = datas[0]
            fields = sorted(data.keys())
            columns = ", ".join(fields)
            placeholders = ', '.join(['%s'] * len(data))
            insert_sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (table, columns, placeholders)
            values = []
            for data in datas:
                value = tuple(data.get(field) for field in fields)
                values.append(value)

            target = self.init_sql_pool(target_cfg)
            # ret = target.delete('delete from {}; '.format(table))
            # print("delete count: ", ret)

            try:
                target.delete('drop table {}; '.format(table))
            except:
                pass
            sql = self.show_table(source_cfg, table)
            target.insert(sql)

            count = target.insert_many(insert_sql, values)
            print("{} insert count: {}".format(table, count))
            target.dispose()

    def start(self):

        self.transdatas(self.spider_cfg, self.test_cfg)

        # self.transdatas(self.test_cfg, self.local_cfg)


if __name__ == "__main__":
    tool = ImportTools()
    tool.start()

    # ret = tool.show_table(tool.local_cfg, tool.need_tables[0])
    # print(ret)
    # local = tool.init_sql_pool(tool.local_cfg)
    # local.delete("drop table {}; ".format(tool.need_tables[0]))
    # ret = local.insert(ret)
    # print(ret)
