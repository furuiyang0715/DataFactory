import time
import logging
import traceback
import pymysql
import pymysql.cursors

logger = logging.getLogger(__name__)


class Connection(object):
    def __init__(self,
                 host: str,
                 database: str,
                 user: str = None,
                 password: str = None,
                 port: int = 0,
                 max_idle_time=7 * 3600,
                 connect_timeout=10,
                 charset="utf8mb4",
                 sql_mode="TRADITIONAL",
                 ):
        self.host = host
        self.database = database
        # 用来配置连接的最大空闲时间, 如果一个连接空闲的超过这个时间, 其 session 就会被 mysql 销毁
        self.max_idle_time = float(max_idle_time)

        args = dict(use_unicode=True,
                    charset=charset,
                    database=database,
                    cursorclass=pymysql.cursors.DictCursor,
                    connect_timeout=connect_timeout,
                    sql_mode=sql_mode,
                    )

        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        # We accept a path to a MySQL socket file or a host(:port) string
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
            logger.error(f'Cannot connect to MySQL on {self.host}', exc_info=True)

    def _ensure_connected(self):
        """
        Mysql by default closes client connections that are idle for
        8 hours, but the client library does not report this fact until
        you try to perform a query and it fails.  Protect against this
        case by preemptively closing and reopening the connection
        if it has been idle for too long (7 hours by default).
        :return:
        """
        if self._db is None or (time.time() - self._last_use_time > self.max_idle_time):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def __del__(self):
        self.close()

    def close(self):
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        self.close()
        self._db = pymysql.connect(**self._db_args)
        self._db.autocommit(True)

    def query(self, query, *parameters, **kwparameters):
        with self._cursor() as cursor:
            cursor.execute(query, kwparameters or parameters)
            return cursor.fetchall()

    def get(self, query, *parameters, **kwparameters):
        with self._cursor() as cursor:
            cursor.execute(query, kwparameters or parameters)
            return cursor.fetchone()

    def execute(self, query, *parameters, **kwparameters):
        with self._cursor() as cursor:
            cursor.execute(query, kwparameters or parameters)
            return cursor.lastrowid

    def insert_many(self, sql: str, values: tuple or list):
        """批量插入 """
        cursor = self._cursor()
        try:
            count = cursor.executemany(sql, values)
            return count
        except Exception as e:
            logger.warning(f'批量插入失败', exc_info=True)
            raise e
        finally:
            cursor.close()

    insert = execute

    def table_has(self, table_name, field, value):
        """判断对应字段的值在数据库中是否已经存在"""
        sql = 'SELECT %s FROM %s WHERE %s="%s"' % (field, table_name, field, value)
        is_exist = self.get(sql)
        return is_exist

    def table_insert(self, table_name: str, item: dict, updatefields: list = None):
        """以字典对象的形式插入"""
        fields = list(item.keys())
        values = list(item.values())
        fieldstr = ','.join(fields)
        valstr = ','.join(['%s'] * len(item))
        sql = 'INSERT INTO `%s` (%s) VALUES (%s)' % (table_name, fieldstr, valstr)
        try:
            last_id = self.execute(sql, *values)
            return last_id
        except Exception as e:
            if e.args[0] == 1062:
                if updatefields:
                    on_update_sql = ''' ON DUPLICATE KEY UPDATE '''
                    for update_field in updatefields:
                        on_update_sql += '{}=values({}),'.format(update_field, update_field)
                    on_update_sql = on_update_sql.rstrip(",")
                    sql = sql + on_update_sql + ';'
                    try:
                        last_id = self.execute(sql, *values)
                        return last_id
                    except:
                        traceback.print_exc()
                        raise
                else:
                    logger.info(f"数据已存在{item}")
                    pass
                    # raise e
            else:
                traceback.print_exc()
                raise e

    def table_update(self, table_name, updates, field_where, value_where):
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

    def contract_sql(self, datas, table: str, update_fields: list):
        """拼接 sql 语句"""
        if not isinstance(datas, list):
            datas = [datas, ]

        to_insert = datas[0]
        ks = []
        vs = []
        for k in to_insert:
            ks.append(k)
            vs.append(to_insert.get(k))
        fields_str = "(" + ",".join(ks) + ")"
        values_str = "(" + "%s," * (len(vs) - 1) + "%s" + ")"
        base_sql = '''INSERT INTO `{}` '''.format(table) + fields_str + ''' values ''' + values_str

        params = []
        for data in datas:
            vs = []
            for k in ks:
                vs.append(data.get(k))
            params.append(vs)

        if update_fields:
            on_update_sql = ''' ON DUPLICATE KEY UPDATE '''
            for update_field in update_fields:
                on_update_sql += '{}=values({}),'.format(update_field, update_field)
            on_update_sql = on_update_sql.rstrip(",")
            sql = base_sql + on_update_sql + """;"""
        else:
            sql = base_sql + ";"
        return sql, params

    def batch_insert(self, to_inserts, table, update_fields):
        """批量插入"""
        if len(to_inserts) == 0:
            logger.info("批量插入数据量为 0")
            return 0
        try:
            sql, values = self.contract_sql(to_inserts, table, update_fields)
            count = self.insert_many(sql, values)
        except:
            logger.warning(f"失败: {traceback.format_exc()}")
        else:
            logger.info(f"{table}批量插入的数量是{count}")
            return count
