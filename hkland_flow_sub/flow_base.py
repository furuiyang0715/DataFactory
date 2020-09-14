import datetime
import logging
import traceback

from hkland_flow_sub.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER,
                                     SPIDER_MYSQL_PASSWORD, SPIDER_MYSQL_DB,
                                     )
from hkland_flow_sub.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FlowBase(object):
    spider_cfg = {
        "host": SPIDER_MYSQL_HOST,
        "port": SPIDER_MYSQL_PORT,
        "user": SPIDER_MYSQL_USER,
        "password": SPIDER_MYSQL_PASSWORD,
        "db": SPIDER_MYSQL_DB,
    }

    def __init__(self):
        self.spider_client = None
        # self.final_table_name = 'hkland_flow_new'

    def contract_sql(self, datas, table: str, update_fields: list):
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
            # https://stackoverflow.com/questions/12825232/python-execute-many-with-on-duplicate-key-update/12825529#12825529
            # sql = 'insert into A (id, last_date, count) values(%s, %s, %s) on duplicate key update last_date=values(last_date),count=count+values(count)'
            on_update_sql = ''' ON DUPLICATE KEY UPDATE '''
            for update_field in update_fields:
                on_update_sql += '{}=values({}),'.format(update_field, update_field)
            on_update_sql = on_update_sql.rstrip(",")
            sql = base_sql + on_update_sql + """;"""
        else:
            sql = base_sql + ";"
        return sql, params

    def _save(self, sql_pool, to_insert, table, update_fields):
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            value = values[0]
            count = sql_pool.insert(insert_sql, value)
        except:
            traceback.print_exc()
            logger.warning("失败")
        else:
            if count == 1:
                logger.info("插入新数据 {}".format(to_insert))
            elif count == 2:
                logger.info("刷新数据 {}".format(to_insert))
            else:
                logger.info("已有数据 {} ".format(to_insert))
            sql_pool.end()
            return count

    def _init_pool(self, cfg: dict):
        """
        eg.
        conf = {
                "host": LOCAL_MYSQL_HOST,
                "port": LOCAL_MYSQL_PORT,
                "user": LOCAL_MYSQL_USER,
                "password": LOCAL_MYSQL_PASSWORD,
                "db": LOCAL_MYSQL_DB,
        }
        :param cfg:
        :return:
        """
        pool = PyMysqlPoolBase(**cfg)
        return pool

    def spider_init(self):
        if not self.spider_client:
            self.spider_client = self._init_pool(self.spider_cfg)

    def __del__(self):
        if self.spider_client:
            self.spider_client.dispose()

    def _check_if_trading_period(self):
        """判断是否是该天的交易时段"""
        _now = datetime.datetime.now()
        if (_now <= datetime.datetime(_now.year, _now.month, _now.day, 9, 0, 0) or
                _now >= datetime.datetime(_now.year, _now.month, _now.day, 16, 30, 0)):
            logger.warning("非当天交易时段")
            return False
        return True

    # def _create_table(self):
    #     sql = '''
    #     CREATE TABLE IF NOT EXISTS `{}` (
    #       `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
    #       `DateTime` datetime NOT NULL COMMENT '交易时间',
    #       `ShHkFlow` decimal(19,4) NOT NULL COMMENT '沪股通/港股通(沪)当日资金流向(万）',
    #       `ShHkBalance` decimal(19,4) NOT NULL COMMENT '沪股通/港股通(沪)当日资金余额（万）',
    #       `SzHkFlow` decimal(19,4) NOT NULL COMMENT '深股通/港股通(深)当日资金流向(万）',
    #       `SzHkBalance` decimal(19,4) NOT NULL COMMENT '深股通/港股通(深)当日资金余额（万）',
    #       `Netinflow` decimal(19,4) NOT NULL COMMENT '南北向资金,当日净流入（万）',
    #       `ShHkNetBuyAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪)净买额（万）',
    #       `ShHkBuyAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪) 买入额（万）',
    #       `ShHkSellAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪) 卖出额（万）',
    #       `SzHkNetBuyAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深)净买额（万）',
    #       `SzHkBuyAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深) 买入额（万）',
    #       `SzHkSellAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深) 卖出额（万）',
    #       `TotalNetBuyAmount` DECIMAL(19,4) COMMENT '北向/南向净买额（万）',
    #       `TotalBuyAmount` DECIMAL(19,4) COMMENT '北向/南向买入额（万）',
    #       `TotalSellAmount` DECIMAL(19,4) COMMENT '北向/南向卖出额（万）',
    #       `Category` tinyint(4) NOT NULL COMMENT '类别:1 南向, 2 北向',
    #       `HashID` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '哈希ID',
    #       `CMFID` bigint(20) unsigned DEFAULT NULL COMMENT '源表来源ID',
    #       `CMFTime` datetime DEFAULT NULL COMMENT 'Come From Time',
    #       `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
    #       `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #       PRIMARY KEY (`id`),
    #       UNIQUE KEY `unique_key2` (`DateTime`,`Category`),
    #       UNIQUE KEY `unique_key` (`CMFID`,`Category`),
    #       KEY `DateTime` (`DateTime`) USING BTREE,
    #       KEY `k` (`UPDATETIMEJZ`) USING BTREE
    #     ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆港通-实时资金流向';
    #     '''.format(self.final_table_name)
    #     self.spider_client.insert(sql)
    #     self.spider_client.end()
