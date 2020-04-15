import datetime

from hkland_historytradestat.configs import DC_HOST, DC_PORT, DC_USER, DC_PASSWD, DC_DB
from hkland_shszhktradingday.sql_pool import PyMysqlPoolBase


class ExtendCalendar(object):
    """根据历史资金流向扩充交易日历"""
    dc_cfg = {
        "host": DC_HOST,
        "port": DC_PORT,
        "user": DC_USER,
        "password": DC_PASSWD,
        "db": DC_DB,
    }

    def __init__(self):
        self.his_table_name = "hkland_historytradestat"
        self.calendar_table_name = 'hkland_shszhktradingday'
        self.today = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)

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

    def start(self):
        for mtype in range(1, 5):
            """
            1: 沪股通 
            2: 港股通(沪) 
            3: 深股通 
            4: 港股通(深) 
            """
            # print(mtype)
            self._check(mtype)


if __name__ == "__main__":
    e = ExtendCalendar()
    e.start()
