import csv
import datetime
import logging
import traceback
import re
import requests as req
import pandas as pd
from lxml import html
from urllib.request import urlretrieve

from hkland_configs import (DC_HOST, DC_PORT, DC_USER, DC_PASSWD, DC_DB, PRODUCT_MYSQL_HOST,
                            PRODUCT_MYSQL_DB, PRODUCT_MYSQL_USER, PRODUCT_MYSQL_PASSWORD,
                            PRODUCT_MYSQL_PORT)
from sql_base import Connection

logger = logging.getLogger()


class CSVLoader(object):
    dc_conn = Connection(
            host=DC_HOST,
            port=DC_PORT,
            user=DC_USER,
            password=DC_PASSWD,
            database=DC_DB
    )

    product_conn = Connection(
        host=PRODUCT_MYSQL_HOST,
        database=PRODUCT_MYSQL_DB,
        user=PRODUCT_MYSQL_USER,
        password=PRODUCT_MYSQL_PASSWORD,
        port=PRODUCT_MYSQL_PORT,
    )

    def __init__(self, csv_file_path='', year=2019):
        self.csv_file_path = csv_file_path
        self.year = year
        self.table_name = 'hkland_shszhktradingday'
        self.tool_table_name = 'base_table_updatetime'
        self.fields = ['InfoSource', 'IfTradingDay', 'TradingPeriod', 'Reason', 'IfWeekEnd',
                       'IfMonthEnd', 'IfQuarterEnd', 'IfYearEnd']

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
            self.product_conn.batch_insert([base72_83, base83_72, base72_90, base90_72], self.table_name, self.fields)

    def check_if_end(self, end_date, info_source):
        sql = '''select IfWeekEnd, IfMonthEnd, IfQuarterEnd, IfYearEnd from const_tradingday 
        where Date = '{}' and SecuMarket = {} and UPDATETIMEJZ = (select max(UPDATETIMEJZ) 
        from const_tradingday where Date = '{}' and SecuMarket = {});'''.format(
            end_date, info_source, end_date, info_source,)
        ret = self.dc_conn.get(sql)
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
            self.product_conn.batch_insert([base72_83, base83_72, base72_90, base90_72], self.table_name, self.fields)

    def start(self):
        rows = self.read_origin_rows()
        records = self.gene_insert_records(rows)
        self.process_records(records)

        wks = self.gene_wk_days(self.year)
        self.process_wk_records(wks)

        # self.refresh_update_time()


def download_lastst_csv_file():
    # load_2019_file_path = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar/2019-Calendar_csv_c.csv?la=zh-HK'
    # load_2020_file_path = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar/2020-Calendar_csv_c.csv?la=zh-HK'
    load_2021_file_path = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar/2021-Calendar_csv_c.csv?la=zh-HK'
    try:
        # urlretrieve(load_2019_file_path, '2019 Calendar_csv_c.csv')
        # urlretrieve(load_2020_file_path, '2020 Calendar_csv_c.csv')
        urlretrieve(load_2021_file_path, '2021 Calendar_csv_c.csv')
    except:
        logger.debug(f"Update csv file fail. {traceback.format_exc()}")
    else:
        logger.info("Download success.")


def get_lastest_update_dt():
    # 解析页面更新时间 确定是否需要更新 csv 文件
    page_url = 'https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar?sc_lang=zh-HK'
    body = req.get(page_url).text
    doc = html.fromstring(body)
    '''
    <p class="loadMore__timetag" data-last-updated-display="更新日期 2020年1月28日">更新日期 2020年1月28日</p>
    '''
    update_dt_str = doc.xpath("//p[@class='loadMore__timetag']")[0].text_content()
    dt_str = re.findall(r"更新日期 (\d{4}年\d{1,2}月\d{1,2}日)", update_dt_str)[0]
    update_dt = datetime.datetime.strptime(dt_str, "%Y年%m月%d日")
    logger.info(f"上一次的更新时间是 {update_dt}")
    return update_dt


def update_calendar():
    logger.info("开始下载更新后的文件")
    download_lastst_csv_file()
    logger.info("下载完毕")

    for file_path, year in [
                            # ('2019 Calendar_csv_c.csv', 2019),
                            # ('2020 Calendar_csv_c.csv', 2020),
                            ('2021 Calendar_csv_c.csv', 2021),

    ]:
        logger.info("开始刷新 {} 年的数据".format(year))
        CSVLoader(csv_file_path=file_path, year=year).start()


def tradingday_task():
    _now = datetime.datetime.now()
    logger.info(f"Now: {_now}")
    try:
        lastest_update_dt = get_lastest_update_dt()
    except:
        lastest_update_dt = None
    logger.info("Last Update Time: {}".format(lastest_update_dt))
    if lastest_update_dt and (not lastest_update_dt >= _now - datetime.timedelta(days=2)):
        logger.info("No Update in Latest 2 Days, Return.")
        return

    update_calendar()
