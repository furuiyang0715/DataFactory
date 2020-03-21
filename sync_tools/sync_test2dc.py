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

# test
TEST_HOST = env.get("TEST_HOST", cf.get('test', 'TEST_HOST'))
TEST_PORT = env.get("TEST_PORT", cf.get('test', 'TEST_PORT'))
TEST_USER = env.get("TEST_USER", cf.get('test', 'TEST_USER'))
TEST_PASSWD = env.get("TEST_PASSWD", cf.get('test', 'TEST_PASSWD'))
TEST_DB = env.get("TEST_DB", cf.get('test', 'TEST_DB'))

# target
TARGET_HOST = env.get("TARGET_HOST", cf.get('target', 'TARGET_HOST'))
TARGET_PORT = int(env.get("TARGET_PORT", cf.get('target', 'TARGET_PORT')))
TARGET_USER = env.get("TARGET_USER", cf.get('target', 'TARGET_USER'))
TARGET_PASSWD = env.get("TARGET_PASSWD", cf.get('target', 'TARGET_PASSWD'))
TARGET_DB = env.get("TARGET_DB", cf.get('target', 'TARGET_DB'))


target_cfg = {
        "host": TARGET_HOST,
        "port": TARGET_PORT,
        "user": TARGET_USER,
        "password": TARGET_PASSWD,
        "db": TARGET_DB,
}

test_cfg = {
    'host': TEST_HOST,
    'port': TEST_PORT,
    'user': TEST_USER,
    'password': TEST_PASSWD,
    'db': TEST_DB,
}

local_cfg = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'ruiyang',
    'db': 'test_db',
}


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


class SyncTools(object):

    need_tables = [
        'hkland_shszhktradingday',   # 交易日
        'hkland_sgelistocks',   # 深港通合资格股
        'hkland_hgelistocks',   # 沪港通合资格股
        'hkland_sgcomponent',   # 深港成分股
        'hkland_hgcomponent',   # 沪港成分股
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

    def transdatas(self, need_tables, source_cfg, target_cfg, re_create=False, replace=True):
        for table in need_tables:
            source = self.init_sql_pool(source_cfg)
            sql = 'select * from {}; '.format(table)
            datas = source.select_all(sql)
            source.dispose()

            data = datas[0]
            fields = sorted(data.keys())
            columns = ", ".join(fields)
            placeholders = ', '.join(['%s'] * len(data))
            if replace:   # https://segmentfault.com/a/1190000015591496
                insert_sql = "REPLACE INTO %s ( %s ) VALUES ( %s )" % (table, columns, placeholders)
            else:
                insert_sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (table, columns, placeholders)
            values = []
            for data in datas:
                value = tuple(data.get(field) for field in fields)
                values.append(value)

            target = self.init_sql_pool(target_cfg)

            if re_create:
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
        # test --> dc
        # self.transdatas(self.need_tables, test_cfg, target_cfg, re_create=False)

        # test --> local
        self.transdatas(self.need_tables, test_cfg, local_cfg, re_create=True)


if __name__ == "__main__":
    ins = SyncTools()

    ins.start()
