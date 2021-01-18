import logging

from hkland_configs import JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB, PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_DB, \
    PRODUCT_MYSQL_USER, PRODUCT_MYSQL_PASSWORD, PRODUCT_MYSQL_PORT
from sql_base import Connection

logger = logging.getLogger()


class TopTradeMixin(object):
    table_name = 'hkland_toptrade'

    juyuan_conn = Connection(
        host=JUY_HOST,
        port=JUY_PORT,
        user=JUY_USER,
        password=JUY_PASSWD,
        database=JUY_DB,
    )

    product_conn = Connection(
        host=PRODUCT_MYSQL_HOST,
        database=PRODUCT_MYSQL_DB,
        user=PRODUCT_MYSQL_USER,
        password=PRODUCT_MYSQL_PASSWORD,
        port=PRODUCT_MYSQL_PORT,
    )

    def _get_inner_code_map(self, market_type):
        """https://dd.gildata.com/#/tableShow/27/column///
           https://dd.gildata.com/#/tableShow/718/column///
        """
        if market_type in ("sh", "sz"):
            sql = 'SELECT SecuCode,InnerCode from SecuMain WHERE SecuCategory in (1, 2) and SecuMarket in (83, 90) and ListedSector in (1, 2, 6, 7);'
        else:
            sql = '''SELECT SecuCode,InnerCode from hk_secumain WHERE SecuCategory in (51, 3, 53, 78) and SecuMarket in (72) and ListedSector in (1, 2, 6, 7);'''
        ret = self.juyuan_conn.query(sql)
        info = {}
        for r in ret:
            key = r.get("SecuCode")
            value = r.get('InnerCode')
            info[key] = value
        return info

    def refresh_updatetime(self):
        sql = '''select max(UPDATETIMEJZ) as max_dt from {}; '''.format(self.table_name)
        max_dt = self.product_conn.get(sql).get("max_dt")
        logger.info(f"{self.table_name} 最新的更新时间是{max_dt}")
        self.product_conn.table_update('base_table_updatetime', {'LastUpdateTime': max_dt}, 'TableName', self.table_name)
