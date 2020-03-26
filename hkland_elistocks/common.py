import datetime
import pprint
import traceback

import pymysql

from hkland_elistocks.configs import TARGET_HOST, TARGET_PORT, TARGET_USER, TARGET_PASSWD, TARGET_DB, SPIDER_HOST, \
    SPIDER_PORT, SPIDER_USER, SPIDER_PASSWD, SPIDER_DB, JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB
from hkland_elistocks.my_log import logger
from hkland_elistocks.sql_pool import PyMysqlPoolBase


class CommonHumamTools(object):
    target_cfg = {
        "host": TARGET_HOST,
        "port": TARGET_PORT,
        "user": TARGET_USER,
        "password": TARGET_PASSWD,
        "db": TARGET_DB,
    }

    local_cfg = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "password": 'ruiyang',
        "db": 'test_db',
    }

    spider_cfg = {
        "host": SPIDER_HOST,
        "port": SPIDER_PORT,
        "user": SPIDER_USER,
        "password": SPIDER_PASSWD,
        "db": SPIDER_DB,
    }

    juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,
    }

    related_tables = [
        "hkex_lgt_change_of_sse_securities_lists",  # 更改上交所证券/中华通证券名单
        'hkex_lgt_sse_securities',  # 上交所证券/中华通证券清单（可同时买卖的股票）
        'hkex_lgt_special_sse_securities',  # 特殊上证所证券/特殊中华通证券清单（仅适合出售的股票）  # 含有面值等信息
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

    @property
    def inner_code_map(self):
        secu_codes = self.get_distinct_spider_secucode()
        inner_code_map = self.get_inner_code_map(secu_codes)
        return inner_code_map

    @property
    def css_code_map(self):
        secu_codes = self.get_distinct_spider_secucode()
        css_code_map = self.get_css_code_map(secu_codes)
        return css_code_map

    def get_distinct_spider_secucode(self):
        spider = self.init_sql_pool(self.spider_cfg)
        sql = 'select distinct(SSESCode) from {}; '.format(self.change_table_name)
        ret = spider.select_all(sql)
        spider.dispose()
        ret = [r.get("SSESCode") for r in ret]
        return ret

    def get_inner_code_map(self, secu_codes):
        juyuan = self.init_sql_pool(self.juyuan_cfg)
        sql = 'select SecuCode, InnerCode, SecuAbbr from secumain where SecuMarket = {} and SecuCode in {};'.format(self.market, tuple(secu_codes))
        ret = juyuan.select_all(sql)
        juyuan.dispose()
        info = {}
        for r in ret:
            key = r.get("SecuCode")
            value = (r.get('InnerCode'), r.get("SecuAbbr"))
            info[key] = value
        return info

    def get_css_code_map(self, secu_codes):
        sql = 'select SSESCode, CCASSCode, FaceValue from {} where SSESCode in {}; '.format(
            self.only_sell_list_table, tuple(secu_codes))
        spider = self.init_sql_pool(self.spider_cfg)
        ret = spider.select_all(sql)
        spider.dispose()
        info = {}
        for r in ret:
            key = r.get("SSESCode")
            value = (r.get('CCASSCode'), r.get("FaceValue"))
            info[key] = value
        return info

    def get_juyuan_inner_code(self, secu_code):
        ret = self.inner_code_map.get(secu_code)
        if ret:
            return ret
        else:
            return None, None

    def get_ccas_code(self, secu_code):
        ret = self.css_code_map.get(secu_code)
        if ret:
            return ret
        else:
            return None, None

    @property
    def only_sell_list(self):
        spider = self.init_sql_pool(self.spider_cfg)
        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
            self.only_sell_list_table, self.only_sell_list_table)
        ret = spider.select_all(sql)
        spider.dispose()
        lst = [r.get("SSESCode") for r in ret]
        return lst

    @property
    def buy_and_sell_list(self):
        # 可买入以及卖出清单
        spider = self.init_sql_pool(self.spider_cfg)
        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
            self.buy_and_sell_list_table, self.buy_and_sell_list_table)
        ret = spider.select_all(sql)
        spider.dispose()
        lst = [r.get("SSESCode") for r in ret]
        return lst

    @property
    def buy_margin_trading_list(self):
        # 可进行保证金交易的清单
        spider = self.init_sql_pool(self.spider_cfg)
        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
            self.buy_margin_trading_list_table, self.buy_margin_trading_list_table)
        ret = spider.select_all(sql)
        spider.dispose()
        lst = [r.get("SSESCode") for r in ret]
        return lst

    @property
    def short_sell_list(self):
        # 可进行担保卖空的清单
        spider = self.init_sql_pool(self.spider_cfg)
        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
            self.short_sell_list_table, self.short_sell_list_table)
        ret = spider.select_all(sql)
        spider.dispose()
        lst = [r.get("SSESCode") for r in ret]
        return lst

    def show_code_spider_records(self, code):
        sql = 'select SSESCode, EffectiveDate, Ch_ange, Remarks from {} where Time = (select max(Time) from {}) and SSESCode = "{}" order by EffectiveDate;'.format(
            self.change_table_name, self.change_table_name, code)
        print(sql)
        spider = self.init_sql_pool(self.spider_cfg)
        ret = spider.select_all(sql)
        spider.dispose()
        return ret

    def select_spider_records_with_a_num(self, num):
        sql = 'select SSESCode from {} where Time = (select max(Time) from {}) group by SSESCode having count(1) = {}; '.format(
            self.change_table_name, self.change_table_name, num)
        spider = self.init_sql_pool(self.spider_cfg)
        ret = spider.select_all(sql)
        spider.dispose()
        ret = [r.get('SSESCode') for r in ret]
        return ret

    def select_latest_records(self):
        sql = '''select * from {} where Time = (select max(Time) from {}); '''.format(self.change_table_name, self.change_table_name)
        spider = self.init_sql_pool(self.spider_cfg)
        ret = spider.select_all(sql)
        spider.dispose()
        return ret

    def insert(self, data):
        in_date = data.get("InDate")
        if isinstance(in_date, datetime.date):
            in_date = datetime.datetime(in_date.year, in_date.month, in_date.day)
            data.update({"InDate": in_date})

        out_date = data.get("OutDate")
        if isinstance(out_date, datetime.date):
            out_date = datetime.datetime(out_date.year, out_date.month, out_date.day)
            data.update({"OutDate": out_date})

        fields = sorted(data.keys())
        columns = ", ".join(fields)
        placeholders = ', '.join(['%s'] * len(data))
        insert_sql = "REPLACE INTO %s ( %s ) VALUES ( %s )" % (self.table_name, columns, placeholders)
        value = tuple(data.get(field) for field in fields)
        target = self.init_sql_pool(self.target_cfg)
        try:
            count = target.insert(insert_sql, value)
            logger.info("replace count:  {}".format(count))
        except pymysql.err.IntegrityError as e:
            traceback.print_exc()
        except Exception as e:
            traceback.print_exc()
        else:
            pass
        finally:
            target.dispose()

    def get_history_records(self, code):
        sql = 'select ID, TradingType,TargetCategory,InnerCode,SecuCode,SecuAbbr,InDate,OutDate,Flag,CCASSCode, ParValue from {} where SecuCode = {} ;'.format(self.table_name, code)
        target = self.init_sql_pool(self.target_cfg)
        ret = target.select_all(sql)
        return ret

    def delete_with_id(self, _id):
        sql = '''delete from {} where id = {}; '''.format(self.table_name, _id)
        target = self.init_sql_pool(self.target_cfg)
        ret = target.delete(sql)
        logger.info("delete with id ret: {}".format(ret))
        target.dispose()

    def update_code_info(self, code, current_records):
        # print(pprint.pformat(current_records))
        for c in current_records:
            in_date = c.get("InDate")
            if isinstance(in_date, datetime.date):
                in_date = datetime.datetime(in_date.year, in_date.month, in_date.day)
                c.update({"InDate": in_date})

            out_date = c.get("OutDate")
            if isinstance(out_date, datetime.date):
                out_date = datetime.datetime(out_date.year, out_date.month, out_date.day)
                c.update({"OutDate": out_date})

        history_records = self.get_history_records(code)
        # print(pprint.pformat(history_records))

        if not history_records:
            for c in current_records:
                self.insert(c)
        elif not current_records:
            for h in history_records:
                _id = h.pop("ID")
                self.delete_with_id(_id)
        else:
            to_insert = []
            to_delete = []

            for h in history_records:
                _id = h.pop("ID")
                if h not in current_records:
                    to_delete.append(_id)

            for r in current_records:
                if r not in history_records:
                    to_insert.append(r)

            logger.info("to_delete: {}".format(to_delete))
            logger.info("to_insert: {}".format(to_insert))

            for _id in to_delete:
                self.delete_with_id(_id)

            for i in to_insert:
                self.insert(i)

    def assert_stats(self, stats, secu_code):
        """
        判断当前状态与清单是否一致
        :param stats:
        :param secu_code:
        :return:
        """
        if stats.get("s1"):
            assert secu_code in self.buy_and_sell_list
        else:
            assert secu_code not in self.buy_and_sell_list

        if stats.get("s2"):
            assert secu_code in self.only_sell_list
        else:
            assert secu_code not in self.only_sell_list

        if stats.get("s3"):
            assert secu_code in self.buy_margin_trading_list
        else:
            assert secu_code not in self.buy_margin_trading_list

        if stats.get("s4"):
            assert secu_code in self.short_sell_list
        else:
            assert secu_code not in self.short_sell_list

    # def delete_codes_records(self, codes):
    #     """删除 codes 对应的记录"""
    #     sql = 'delete from {} where SecuCode in {}; '.format(self.table_name, tuple(codes))
    #     target = self.init_sql_pool(self.target_cfg)
    #     ret = target.delete(sql)
    #     logger.info("delete count: {}".format(ret))
    #     target.dispose()
