import time
import logging
import traceback
import pymysql
import pymysql.cursors


class Connection(object):
    """A lightweight wrapper around PyMySQL.
    """
    def __init__(self, host,
                 database,
                 user=None,
                 password=None,
                 port=0,
                 max_idle_time=7 * 3600,
                 connect_timeout=10,
                 time_zone="+0:00",
                 charset="utf8mb4",
                 sql_mode="TRADITIONAL"):
        self.host = host  # 指定主机
        self.database = database  # 指定数据库
        # 用来配置连接的最大空闲时间。如果一个连接空闲的超过这个时间，他的session就会被 mysql 销毁
        self.max_idle_time = float(max_idle_time)

        args = dict(use_unicode=True, charset=charset,
                    database=database,
                    # 初始化命令是为数据库设置时区
                    init_command=('SET time_zone = "%s"' % time_zone),
                    cursorclass=pymysql.cursors.DictCursor,
                    connect_timeout=connect_timeout, sql_mode=sql_mode)
        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        # We accept a path to a MySQL socket file or a host(:port) string
        # 初始化的时间设置多种连接方式
        if "/" in host:
            args["unix_socket"] = host
        else:
            self.socket = None
            pair = host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = host
                args["port"] = 3306
        if port:
            args['port'] = port

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to MySQL on %s", self.host, exc_info=True)

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if self._db is None or (time.time() - self._last_use_time > self.max_idle_time):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = pymysql.connect(**self._db_args)
        self._db.autocommit(True)

    def query(self, query, *parameters, **kwparameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            cursor.execute(query, kwparameters or parameters)
            result = cursor.fetchall()
            return result
        finally:
            cursor.close()

    def get(self, query, *parameters, **kwparameters):
        """Returns the (singular) row returned by the given query.
        """
        cursor = self._cursor()
        try:
            cursor.execute(query, kwparameters or parameters)
            return cursor.fetchone()
        finally:
            cursor.close()

    def execute(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            cursor.execute(query, kwparameters or parameters)
            return cursor.lastrowid
        except Exception as e:
            if e.args[0] == 1062:
                print("重复插入")
            else:
                traceback.print_exc()
                raise e
        finally:
            cursor.close()

    def insert_many(self, query, values):
        """
        @summary: 向数据表插入多条记录
        @param query:要插入的 SQL 格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        cursor = self._cursor()
        count = cursor.executemany(query, values)
        return count

    insert = execute

    # =============== high level method for table ===================

    def table_has(self, table_name, field, value):
        if isinstance(value, str):
            value = value.encode('utf8')
        sql = 'SELECT %s FROM %s WHERE %s="%s"' % (
            field,
            table_name,
            field,
            value)
        d = self.get(sql)
        return d

    def tables_insert(self, table_name, items):
        '''items is a list of dict.for one in items, key is mysql table field'''
        item = items[0]
        fields = list(item.keys())
        fieldstr = ','.join(fields)
        valstr = ','.join(['%s'] * len(item))
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table_name, fieldstr, valstr)

        values_list = [list(item.values()) for item in items]
        for values in values_list:
            for i in range(len(values)):
                if isinstance(values[i], str):
                    values[i] = values[i].encode('utf8')
        try:
            count = self.insert_many(sql, values_list)
            return count
        except:
            traceback.print_exc()

    def table_insert(self, table_name, item):
        '''item is a dict : key is mysql table field'''
        fields = list(item.keys())
        values = list(item.values())
        fieldstr = ','.join(fields)
        valstr = ','.join(['%s'] * len(item))
        for i in range(len(values)):
            if isinstance(values[i], str):
                values[i] = values[i].encode('utf8')
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table_name, fieldstr, valstr)
        try:
            last_id = self.execute(sql, *values)
            return last_id
        except Exception as e:
            if e.args[0] == 1062:
                # just skip duplicated item
                pass
            else:
                traceback.print_exc()
                print('sql:', sql)
                print('item:')
                for i in range(len(fields)):
                    vs = str(values[i])
                    if len(vs) > 300:
                        print(fields[i], ' : ', len(vs), type(values[i]))
                    else:
                        print(fields[i], ' : ', vs, type(values[i]))
                raise e

    def table_update(self, table_name, updates,
                     field_where, value_where):
        '''updates is a dict of {field_update:value_update}'''
        upsets = []
        values = []
        for k, v in updates.items():
            s = '%s=%%s' % k
            upsets.append(s)
            values.append(v)
        upsets = ','.join(upsets)
        sql = 'UPDATE %s SET %s WHERE %s="%s"' % (
            table_name,
            upsets,
            field_where, value_where,
        )
        self.execute(sql, *(values))


if __name__ == '__main__':
    db = Connection(
        '127.0.0.1',
        'test_furuiyang',
        'root',
        'ruiyang'
    )
    # 获取一条记录
    # sql = 'select * from sz_margin_history where id=%s'
    # data = db.get(sql, 2)
    # print(data)

    # # 获取多天记录
    # sql = 'select * from sz_margin_history where id>%s limit 10'
    # datas = db.query(sql, 100)
    # print(datas)
    # print(len(datas))

    # # 创建一个只有两个数据的测试数据库
    # create_sql = '''
    # CREATE TABLE IF NOT EXISTS `runoob_tbl`(
    #    `runoob_id` INT UNSIGNED AUTO_INCREMENT,
    #    `runoob_title` VARCHAR(100) NOT NULL,
    #    `runoob_author` VARCHAR(40) NOT NULL,
    #    `submission_date` DATE,
    #    PRIMARY KEY ( `runoob_id` ),
    #    UNIQUE KEY `title` (`runoob_title`)
    # )ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='测试数据库';
    # '''
    # ret = db.execute(create_sql)
    # print(ret)

    # # 插入一条数据
    # sql = 'insert into runoob_tbl(runoob_title, runoob_author, submission_date) values(%s, %s, %s)'
    # last_id = db.execute(sql, 'test01', 'au11', '2020-05-01')
    # print(last_id)

    # # 使用字典插入一条数据
    # item = {
    #     'runoob_title': 'test02',
    #     'runoob_author': 'au2',
    #     'submission_date': "2020-05-02",
    # }
    # last_id = db.table_insert('runoob_tbl', item)
    # print(last_id)

    # # 测试插入多条数据
    # sql = 'insert into runoob_tbl(runoob_title, runoob_author, submission_date) values (%s, %s, %s);'
    # count = db.insert_many(sql, [('test03', 'au3', '2020-05-03'), ('test04', 'au4', '2020-05-04')])
    # print(count)

    # # 使用字典列表插入数据
    # items = [
    #     {'runoob_title': 'test05', 'runoob_author': 'au5', 'submission_date': "2020-05-05"},
    #     {'runoob_title': 'test06', 'runoob_author': 'au6', 'submission_date': "2020-05-06"},
    #     {'runoob_title': 'test07', 'runoob_author': 'au7', 'submission_date': "2020-05-07"},
    # ]
    # count = db.tables_insert('runoob_tbl', items)
    # print(count)
