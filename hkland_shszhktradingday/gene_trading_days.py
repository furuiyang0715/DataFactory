import csv
import datetime
import traceback

import pandas as pd


from hkland_shszhktradingday.configs import (DATACENTER_HOST, DATACENTER_PORT, DATACENTER_USER,
                                             DATACENTER_PASSWD, DATACENTER_DB, TARGET_HOST,
                                             TARGET_PORT, TARGET_USER, TARGET_PASSWD, TARGET_DB,
                                             LOCAL, TEST_HOST, TEST_PORT, TEST_USER, TEST_PASSWD,
                                             TEST_DB)
from hkland_shszhktradingday.sql_pool import PyMysqlPoolBase


class CSVLoader(object):
    local_cfg = {
        "host": '127.0.0.1',
        "port": 3306,
        "user": 'root',
        "password": 'ruiyang',
        "db": 'test_db',
    }

    test_cfg = {
        "host": TEST_HOST,
        "port": TEST_PORT,
        "user": TEST_USER,
        "password": TEST_PASSWD,
        "db": TEST_DB,
    }

    target_cfg = {
        "host": TARGET_HOST,
        "port": TARGET_PORT,
        "user": TARGET_USER,
        "password": TARGET_PASSWD,
        "db": TARGET_DB,
    }

    dc_cfg = {
        "host": DATACENTER_HOST,
        "port": DATACENTER_PORT,
        "user": DATACENTER_USER,
        "password": DATACENTER_PASSWD,
        "db": DATACENTER_DB,
    }

    def __init__(self, csv_file_path='', year=2019):
        self.csv_file_path = csv_file_path
        self.year = year
        self.table_name = 'hkland_shszhktradingday'
        self.tool_table_name = 'base_table_updatetime'

    def init_sql_pool(self, sql_cfg: dict):
        pool = PyMysqlPoolBase(**sql_cfg)
        return pool

    def read_origin_rows(self):
        with open(self.csv_file_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            rows = [row for row in reader]
        return rows

    def gene_insert_records(self, rows):
        fields = ['日期', '星期', '香港', '上海及深圳', '北向交易', '南向交易']
        rows = rows[3:]
        records = []
        for row in rows:
            record = dict(zip(fields, row))
            records.append(record)
        return records

    def create_table(self):
        c_sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` bigint(20)  unsigned NOT NULL AUTO_INCREMENT,
          `InfoSource` int(11) DEFAULT NULL COMMENT '信息来源',
          `EndDate` datetime NOT NULL COMMENT '截止日期',
          `TradingType` int(11) NOT NULL COMMENT '交易类型',
          `IfTradingDay` int(11) DEFAULT NULL COMMENT '是否交易日',
          `TradingPeriod` int(11) DEFAULT NULL COMMENT '交易时段',
          `Reason` varchar(50) DEFAULT NULL COMMENT '非周末非交易日原因',
          `IfWeekEnd` int(11) DEFAULT NULL COMMENT '是否周最后交易日',
          `IfMonthEnd` int(11) DEFAULT NULL COMMENT '是否月最后交易日',
          `IfQuarterEnd` int(11) DEFAULT NULL COMMENT '是否季最后交易日',
          `IfYearEnd` int(11) DEFAULT NULL COMMENT '是否年最后交易日',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY `IX_QT_SHSZHSCTradingDay` (`EndDate`,`TradingType`),
          UNIQUE KEY `IX_QT_SHSZHSCTradingDay_ID` (`ID`)
        ) ENGINE=InnoDB DEFAULT CHARSET=gbk COMMENT '陆股通交易日'; 
        '''.format(self.table_name)
        target = self.init_sql_pool(self.target_cfg)
        target.insert(c_sql)
        target.dispose()

    def process_records(self, records):
        wkmap = {
            "一": 1,
            "二": 2,
            "三": 3,
            "四": 4,
            "五": 5,
        }

        for record in records:
            end_date = record.get('日期')
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
            week_date = wkmap.get(record.get("星期"))
            hk_status = record.get("香港")
            shzh_status = record.get("上海及深圳")
            # 对于港交所来说的
            north = record.get("北向交易")
            sourth = record.get("南向交易")
            # 针对香港的北向交易关闭
            if north == '關閉':
                itd1 = 2
                itd3 = 2
                reason1 = "香港:"+hk_status + "," + "上海及深圳:"+shzh_status
                reason3 = "香港:"+hk_status + "," + "上海及深圳:"+shzh_status
                tp1 = None
                tp3 = None
            else:
                itd1 = 1
                itd3 = 1
                reason1 = None
                reason3 = None
                if hk_status == "半日市":
                    tp1 = 4
                    tp3 = 4
            # 针对沪深的南向交易关闭
            if sourth == '關閉':
                itd2 = 2
                itd4 = 2
                reason2 = "香港:" + hk_status + "," + "上海及深圳:" + shzh_status
                reason4 = "香港:" + hk_status + "," + "上海及深圳:" + shzh_status
                tp2 = None
                tp4 = None
            else:
                itd2 = 1
                itd4 = 1
                reason2 = None
                reason4 = None
                if shzh_status == '半日市':
                    tp2 = 4
                    tp3 = 4

            # 生成该天的 4 条数据
            # TradingPeriod 1 全天 2 上午 3 下午 4 仅说明半日市
            base72_83 = {"InfoSource": 72, "EndDate": end_date, "TradingType": 1, "IfTradingDay": itd1, "TradingPeriod": tp1, "Reason": reason1}
            base83_72 = {"InfoSource": 83, "EndDate": end_date, "TradingType": 2, "IfTradingDay": itd2, "TradingPeriod": tp2, "Reason": reason2}
            base72_90 = {"InfoSource": 72, "EndDate": end_date, "TradingType": 3, "IfTradingDay": itd3, "TradingPeriod": tp3, "Reason": reason3}
            base90_72 = {"InfoSource": 90, "EndDate": end_date, "TradingType": 4, "IfTradingDay": itd4, "TradingPeriod": tp4, "Reason": reason4}

            for d in [base72_83, base83_72, base72_90, base90_72]:
                secu_market = d.get("InfoSource")
                if secu_market in (83, 90):
                    ret = self.check_if_end(end_date, 83)
                else:    # 72
                    ret = self.check_if_end(end_date, 72)
                d.update(ret)

            # 插入
            self.insert_many([base72_83, base83_72, base72_90, base90_72])

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
            else:
                print("已有数据 {}".format(to_insert))
        finally:
            client.end()
        return count

    def insert_many(self, datas):
        client = self.init_sql_pool(self.target_cfg)
        fields = ['InfoSource', 'EndDate', 'TradingType', 'IfTradingDay',
                  'IfWeekEnd', 'IfMonthEnd', 'IfQuarterEnd', 'IfYearEnd']
        for data in datas:
            self._save(client, data, self.table_name, fields)

        try:
            client.dispose()
        except:
            pass

    def check_if_end(self, end_date, info_source):
        dc = self.init_sql_pool(self.dc_cfg)
        sql = '''select IfWeekEnd, IfMonthEnd, IfQuarterEnd, IfYearEnd from const_tradingday 
        where Date = '{}' and SecuMarket = {} and UPDATETIMEJZ = (select max(UPDATETIMEJZ) 
        from const_tradingday where Date = '{}' and SecuMarket = {});'''.format(
            end_date, info_source, end_date, info_source,)
        ret = dc.select_one(sql)
        dc.dispose()
        return ret

    def gen_all_quarters(self, start: datetime.datetime, end: datetime.datetime):
        """
        生成 start 和 end 之间全部季度时间点列表
        """
        idx = pd.date_range(start=start, end=end, freq="D")
        dt_list = [dt.to_pydatetime() for dt in idx]
        return dt_list

    def gene_wk_days(self, year):
        # （1） 生成某一年的全部时间列表
        # （2） 减去 csv 文件中时间列的已有时间 得到需要计算的周末时间
        start_date = datetime.datetime(year, 1, 1)
        end_date = datetime.datetime(year, 12, 31)
        rng = self.gen_all_quarters(start_date, end_date)
        rows = self.read_origin_rows()
        records = self.gene_insert_records(rows)
        dts = [datetime.datetime.strptime(record.get("日期"), "%Y-%m-%d") for record in records]
        wk_days = sorted(set(rng) - set(dts))
        return wk_days

    def process_wk_records(self, wk_days):
        common_info = {"IfTradingDay": 2,    # 非交易日
                       "TradingPeriod": None,   # 非交易日的交易时段是 None
                       "Reason": None,  # 双休的非交易日的原因为 None
                       'IfWeekEnd': 2,   # 不是本周的最后一个交易日
                       'IfMonthEnd': 2,  # 不是本月的最后一个交易日
                       'IfQuarterEnd': 2,  # 不是本季度的最后一个交易日
                       'IfYearEnd': 2,  # 是否是本年的最后一个交易日
                       }
        for row in wk_days:
            # 每一天生成 4 条记录
            base72_83 = {"InfoSource": 72, "EndDate": row, "TradingType": 1}
            base83_72 = {"InfoSource": 83, "EndDate": row, "TradingType": 2}
            base72_90 = {"InfoSource": 72, "EndDate": row, "TradingType": 3}
            base90_72 = {"InfoSource": 90, "EndDate": row, "TradingType": 4}
            for d in [base72_83, base83_72, base72_90, base90_72]:
                d.update(common_info)

            self.insert_many([base72_83, base83_72, base72_90, base90_72])

    def refresh_update_time(self):
        product = self.init_sql_pool(self.target_cfg)
        sql = '''select max(UPDATETIMEJZ) as max_dt from {}; '''.format(self.table_name)
        max_dt = product.select_one(sql).get("max_dt")
        print("最新的更新时间是{}".format(max_dt))

        refresh_sql = '''replace into {} (id, TableName, LastUpdateTime,IsValid) values (11, "hkland_shszhktradingday", '{}', 1); 
        '''.format(self.tool_table_name, max_dt)
        count = product.update(refresh_sql)
        print(count)   # 1 首次插入 2 替换插入
        product.dispose()

    def _start(self):
        if LOCAL:
            self.create_table()
        rows = self.read_origin_rows()
        records = self.gene_insert_records(rows)
        self.process_records(records)

        wks = self.gene_wk_days(self.year)
        self.process_wk_records(wks)

        self.refresh_update_time()

    def start(self):
        try:
            self._start()
        except:
            traceback.print_exc()


# (1) TradingPeriod 这个更加详细的数据源是没有的。
# (2) 更加具体关闭原因是没有的。

'''
mysql> select distinct(Reason)  from qt_shszhsctradingday;
+--------------------------------------+
| Reason                               |
+--------------------------------------+
| NULL                                 |
| 八号台风信号(天鸽)                   |
| 国庆节                               |
| 聖誕節                               |
| 元旦                                 |
| 春节                                 |
| 耶穌受難節                           |
| 復活節                               |
| 清明節                               |
| 勞動節                               |
| 佛誕                                 |
| 端午節                               |
| 香港特別行政區成立紀念日             |
| 中秋節                               |
| 國慶日                               |
| 重陽節                               |
+--------------------------------------+
16 rows in set (0.15 sec)
'''
