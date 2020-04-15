import datetime
import sys
import traceback

sys.path.append("./../")
from hkland_shszhktradingday.configs import (TARGET_HOST, TARGET_PORT, TARGET_USER, TARGET_PASSWD,
                                             TARGET_DB, DATACENTER_HOST, DATACENTER_PORT, DATACENTER_DB,
                                             DATACENTER_PASSWD, DATACENTER_USER)
from hkland_shszhktradingday.sql_pool import PyMysqlPoolBase


class ExtendCalendar(object):
    """根据历史资金流向扩充交易日历
       只运行一次
    """
    dc_cfg = {
        "host": DATACENTER_HOST,
        "port": DATACENTER_PORT,
        "user": DATACENTER_USER,
        "password": DATACENTER_PASSWD,
        "db": DATACENTER_DB,
    }

    product_cfg = {
        "host": TARGET_HOST,
        "port": TARGET_PORT,
        "user": TARGET_USER,
        "password": TARGET_PASSWD,
        "db": TARGET_DB,
    }

    def __init__(self):
        self.his_table_name = "hkland_historytradestat"
        self.calendar_table_name = 'hkland_shszhktradingday'
        self.today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)

        self.info_source_map = {
            1: 72,     # 北向沪股通, 消息源是港交所
            2: 83,     # 南向 港股通(沪）,消息源是上交所
            3: 72,     # 北向深股通, 消息源是港交所
            # 4: 90,
            4: 83,     # 南向 港股通(深), 消息源是深交所,  83 和 90 对于 const_tradingday 表的查询统一使用 83
        }

    def _init_pool(self, cfg):
        pool = PyMysqlPoolBase(**cfg)
        return pool

    def _fetch(self, market_type):
        dc = self._init_pool(self.dc_cfg)
        sql = '''select Date from {} where MarketTypeCode = {} order by Date; '''.format(self.his_table_name, market_type)
        ret = dc.select_all(sql)
        dates = [r.get("Date") for r in ret]
        dc.dispose()
        return dates

    def _check(self, market_type):
        """检查已经存在的交易日历与历史流向中的时间是否一致"""
        his_dates = self._fetch(market_type)
        cal_dates = self._fetch_calendar(market_type)
        # cal_dates 只取昨天以及之前的日期 因为 cal 是提前预算了 2020 全年的, 历史是准确到昨天。
        cal_dates = [cal_date for cal_date in cal_dates if cal_date < self.today]
        lst = []
        for date in cal_dates:
            if not date in his_dates:
                lst.append(date)
        assert not lst

    def _fetch_calendar(self, market_type):
        dc = self._init_pool(self.dc_cfg)
        sql = ''' select EndDate  from {} where TradingType = {} and IfTradingDay = 1 order by EndDate; '''.format(self.calendar_table_name, market_type)
        ret = dc.select_all(sql)
        dates = [r.get("EndDate") for r in ret]
        return dates

    def check_if_end(self, end_date, info_source):
        dc = self._init_pool(self.dc_cfg)
        sql = '''select IfWeekEnd, IfMonthEnd, IfQuarterEnd, IfYearEnd from const_tradingday 
        where Date = '{}' and SecuMarket = {} and UPDATETIMEJZ = (select max(UPDATETIMEJZ) 
        from const_tradingday where Date = '{}' and SecuMarket = {});'''.format(
            end_date, info_source, end_date, info_source,)
        ret = dc.select_one(sql)
        dc.dispose()
        return ret

    def contract_sql(self, to_insert: dict, table: str, update_fields: list):
        ks = []
        vs = []
        for k in to_insert:
            ks.append(k)
            vs.append(to_insert.get(k))
        fields_str = "(" + ",".join(ks) + ")"
        values_str = "(" + "%s," * (len(vs) - 1) + "%s" + ")"
        base_sql = '''INSERT INTO `{}` '''.format(table) + fields_str + ''' values ''' + values_str
        on_update_sql = ''' ON DUPLICATE KEY UPDATE '''
        update_vs = []
        for update_field in update_fields:
            on_update_sql += '{}=%s,'.format(update_field)
            update_vs.append(to_insert.get(update_field))
        on_update_sql = on_update_sql.rstrip(",")
        sql = base_sql + on_update_sql + """;"""
        vs.extend(update_vs)
        return sql, tuple(vs)

    def _save(self, client, to_insert, table, update_fields: list):
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            count = client.insert(insert_sql, values)
        except:
            traceback.print_exc()
            print("失败")
            count = None
        else:
            if count:
                print("更入新数据 {}".format(to_insert))
        finally:
            client.dispose()
        return count

    def _save_many(self, client, dts, table_name, market_type):
        for dt in dts:
            item = dict()
            info_source = self.info_source_map.get(market_type)
            item['InfoSource'] = info_source
            item['EndDate'] = dt
            item['TradingType'] = market_type
            item['IfTradingDay'] = 1
            _info = self.check_if_end(dt, info_source)
            item.update(_info)
            product = self._init_pool(self.product_cfg)
            fields = ['InfoSource', 'EndDate', 'TradingType', 'IfTradingDay',
                      'IfWeekEnd', 'IfMonthEnd', 'IfQuarterEnd', 'IfYearEnd']
            self._save(product, item, table_name, fields)

    def _extend(self, market_type):
        his_dates = self._fetch(market_type)
        cal_dates = self._fetch_calendar(market_type)
        lst = []
        for date in his_dates:
            if not date in cal_dates:
                lst.append(date)

        client = self._init_pool(self.product_cfg)
        self._save_many(client, lst, self.calendar_table_name, market_type)

    def start(self):
        for mtype in range(1, 5):
            """
            1: 沪股通 
            2: 港股通(沪) 
            3: 深股通 
            4: 港股通(深) 
            """
            print(mtype)
            self._check(mtype)
            self._extend(mtype)


if __name__ == "__main__":
    e = ExtendCalendar()
    e.start()

'''
select count(*)  from hkland_shszhktradingday where TradingType = 1 and IfTradingDay = 1; 
select min(EndDate), max(EndDate) from hkland_shszhktradingday where TradingType = 1 and IfTradingDay = 1; 
'''
