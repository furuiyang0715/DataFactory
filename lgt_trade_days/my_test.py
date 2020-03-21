import datetime
import pprint

from lgt_trade_days.configs import JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB, TARGET_HOST, TARGET_PORT, \
    TARGET_USER, TARGET_PASSWD, TARGET_DB
from lgt_trade_days.sql_pool import PyMysqlPoolBase


'''
docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_trading_days:v0.0.1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_trading_days:v0.0.1
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_trading_days:v0.0.1
sudo docker run -itd --name trade_days registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_trading_days:v0.0.1
'''


class CheckTool(object):
    juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,
    }

    test_cfg = {
        "host": TARGET_HOST,
        "port": TARGET_PORT,
        "user": TARGET_USER,
        "password": TARGET_PASSWD,
        "db": TARGET_DB,
    }

    tables = [
        'trading_calendar_sz',  # 深交所交易日历
        'trading_calendar_sh',  # 上交所交易日历
        'trading_calendar_hk',  # 港交所交易日历
    ]

    def __init__(self):
        self.test = self.init_sql_pool(self.test_cfg)
        self.juyuan = self.init_sql_pool(self.juyuan_cfg)

    def __del__(self):
        try:
            self.test.dispose()
            self.juyuan.dispose()
        except:
            pass

    def init_sql_pool(self, sql_cfg: dict):
        pool = PyMysqlPoolBase(**sql_cfg)
        return pool

    def juyuan_sh_trading_days(self):
        # sql = """select  *  from qt_shszhsctradingday; """
        # ret = self.juyuan.select_one(sql)
        # print(pprint.pformat(ret))
        '''
        {'EndDate': datetime.datetime(2017, 8, 15, 0, 0),
         'ID': 550865382382,
         'IfMonthEnd': 2,
         'IfQuarterEnd': 2,
         'IfTradingDay': 1,
         'IfWeekEnd': 2,
         'IfYearEnd': 2,
         'InfoSource': 72,
         'JSID': 550951400113,
         'Reason': None,
         'TradingPeriod': 1,
         'TradingType': 1,
         'UpdateTime': datetime.datetime(2017, 6, 16, 18, 3, 20)}
        '''
        sql2 = '''select EndDate from qt_shszhsctradingday where InfoSource = 83 and TradingType=2 
        and IfTradingDay=1 and EndDate >= "{}" and EndDate <= "{}";'''.format(
            datetime.datetime(2019, 10, 12, 0, 0), datetime.datetime(2020, 3, 12, 0, 0))
        ret = self.juyuan.select_all(sql2)
        ret = [r.get("EndDate") for r in ret]
        # print(ret)
        return ret

    def sh_trading_days(self):
        # sql = 'select min(Date) as min_dt, max(Date) as max_dt, count(1) as count from {}; '.format(self.tables[1])
        # ret1 = self.test.select_one(sql)
        # print(ret1)   # {'min_dt': datetime.date(2019, 10, 12), 'max_dt': datetime.date(2020, 3, 12), 'count': 153}

        # ret = self.test.select_one("select * from {} ; ".format(self.tables[1]))
        # print(pprint.pformat(ret))
        '''
        {'BuyStatus': '不接受',
         'SellStatus': '不接受', 
         'Category': 'SH',
         'Date': datetime.date(2019, 10, 12),
         'ExchangeDate': '否',
         'UPDATETIMEJZ': datetime.datetime(2019, 10, 12, 14, 40, 45),
         'CREATETIMEJZ': datetime.datetime(2019, 10, 12, 14, 40, 45), 
         'id': 42251}
        '''
        sql = 'select Date from {} where ExchangeDate != "否"; '.format(self.tables[1])
        ret = self.test.select_all(sql)
        # datetime.datetime.combine(r.get("Date"), datetime.datetime.min.time())
        ret = [datetime.datetime.combine(r.get("Date"), datetime.datetime.min.time()) for r in ret]
        return ret


if __name__ == "__main__":
    ins = CheckTool()
    r1 = ins.sh_trading_days()
    r2 = ins.juyuan_sh_trading_days()
    print(set(r1) == set(r2))
    print(set(r1) - set(r2))
    print(set(r2) - set(r1))