import datetime
import logging
import time
import traceback
import requests
import utils
from hkland_configs import (DC_HOST, DC_PORT, DC_USER, DC_PASSWD, DC_DB, PRODUCT_MYSQL_HOST,
                            PRODUCT_MYSQL_DB, PRODUCT_MYSQL_USER, PRODUCT_MYSQL_PASSWORD,
                            PRODUCT_MYSQL_PORT, JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB)
from sql_base import Connection

logger = logging.getLogger()


class ExchangeTop10(object):
    """十大成交股交易所数据源"""
    def __init__(self, day: datetime.datetime):
        self.info = '交易所十大成交股:\n'
        self.web_url = 'https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Statistics/Historical-Daily?sc_lang=zh-HK#select4=1&select5=0&select3=0&select1=16&select2=5'
        self.dt = day
        self.dt_str = day.strftime("%Y%m%d")
        self.url = 'https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_{}c.js?_={}'.format(self.dt_str, int(time.time()*1000))
        self.fields = ['Date', 'SecuCode', 'InnerCode', 'SecuAbbr',
                       'TJME', 'TMRJE', 'TCJJE', 'CategoryCode', ]
        self.table_name = 'hkland_toptrade'
        self.category_map = {
            "SSE Northbound": ("HG", 1),     # 沪股通
            "SSE Southbound": ("GGh", 2),    # 港股通（沪）
            "SZSE Northbound": ("SG", 3),    # 深股通
            "SZSE Southbound": ("GGs", 4),   # 港股通（深）
        }
        self.dc_conn = Connection(
            host=DC_HOST,
            port=DC_PORT,
            user=DC_USER,
            password=DC_PASSWD,
            database=DC_DB,
        )

        self.product_conn = Connection(
            host=PRODUCT_MYSQL_HOST,
            database=PRODUCT_MYSQL_DB,
            user=PRODUCT_MYSQL_USER,
            password=PRODUCT_MYSQL_PASSWORD,
            port=PRODUCT_MYSQL_PORT,
        )

        self.juyuan_conn = Connection(
            host=JUY_HOST,
            port=JUY_PORT,
            user=JUY_USER,
            password=JUY_PASSWD,
            database=JUY_DB,
        )

    @staticmethod
    def re_money_data(data: str):
        data = float(data.replace(",", ""))
        return data

    def get_juyuan_codeinfo(self, secu_code):
        """A 股的聚源内部编码以及证券简称"""
        sql = 'SELECT SecuCode,InnerCode, SecuAbbr from SecuMain WHERE SecuCategory in (1, 2, 8) \
and SecuMarket in (83, 90) \
and ListedSector in (1, 2, 6, 7) and SecuCode = "{}";'.format(secu_code)
        ret = self.juyuan_conn.get(sql)
        return ret.get('InnerCode'), ret.get("SecuAbbr")

    def get_juyuan_hkcodeinfo(self, secu_code):
        """港股的聚源内部编码以及证券简称"""
        sql = 'select SecuCode,InnerCode, SecuAbbr  from hk_secumain where SecuCode = "{}";'.format(secu_code)
        ret = self.juyuan_conn.get(sql)
        return ret.get('InnerCode'), ret.get("SecuAbbr")

    def start(self):
        logger.info(self.url)
        resp = requests.get(self.url)
        if resp.status_code == 200:
            body = resp.text
            datas_str = body.replace("tabData = ", "")
            try:
                datas = eval(datas_str)
            except:
                traceback.print_exc()
                return

            fields = [
                'Rank',   # 十大成交排名
                'Stock Code',   # 证券代码
                'Stock Name',   # 证券简称
                'Buy Turnover',  # 买入金额 (RMB)
                'Sell Turnover',  # 卖出金额(RMB)
                'Total Turnover',  # 买入以及卖出金额 (RMB)
            ]
            # 与钱相关的字段 单独列出是为了将字符串转换为数值
            money_fields = ['Buy Turnover', 'Sell Turnover', 'Total Turnover']

            for direction_data in datas:
                items = []
                cur_dt = direction_data.get("date")
                market = direction_data.get("market")
                is_trading_day = direction_data.get("tradingDay")
                if is_trading_day == 0:
                    logger.warning("{} 方向无交易".format(market))
                    continue

                content = direction_data.get("content")[1].get("table").get("tr")
                category = self.category_map.get(market)[0]
                for row in content:
                    td = row.get("td")[0]
                    item = dict(zip(fields, td))
                    item.update({"Date": cur_dt, "CategoryCode": category})
                    for field in money_fields:
                        item[field] = self.re_money_data(item[field])

                    # 净买额 = 买入金额 - 卖出金额
                    item['TJME'] = item.get("Buy Turnover") - item.get('Sell Turnover')
                    # 买入金额
                    item.update({"TMRJE": item.get("Buy Turnover")})
                    # 成交金额 = 买入金额 + 卖出金额 (即 买入以及卖出金额 (RMB)）
                    item.update({"TCJJE": item.get("Total Turnover")})

                    # 移除不需要的字段
                    item.pop("Rank")
                    item.pop("Stock Name")
                    item.pop("Sell Turnover")
                    item.pop("Buy Turnover")
                    item.pop("Total Turnover")

                    # TODO  增加收盘价以及涨跌幅字段 暂时不做这两个字段 等待东财更新覆盖
                    if category == "SG":  # 要将深股通的证券编码补充为 6 位的
                        secu_code = "0" * (6 - len(item["Stock Code"])) + item["Stock Code"]
                        item['SecuCode'] = secu_code
                        item["InnerCode"], item['SecuAbbr'] = self.get_juyuan_codeinfo(secu_code)
                    elif category == "HG":     # 沪股通
                        secu_code = item["Stock Code"]
                        item['SecuCode'] = secu_code
                        item["InnerCode"], item['SecuAbbr'] = self.get_juyuan_codeinfo(secu_code)
                    elif category in ('GGh', 'GGs'):   # 港股
                        secu_code = item["Stock Code"]
                        item['SecuCode'] = secu_code
                        item["InnerCode"], item['SecuAbbr'] = self.get_juyuan_hkcodeinfo(secu_code)
                    item.pop("Stock Code")
                    item['CMFID'] = 1
                    item['CMFTime'] = datetime.datetime.now()
                    logger.info(item)
                    items.append(item)
                count = self.product_conn.batch_insert(items, self.table_name, self.fields)
                self.info += "{}批量插入{}条\n".format(category, count)
            utils.ding_msg(self.info)
        else:
            logger.warning(resp)
            logger.warning("{} 当天非交易日或尚无十大成交数据".format(self.dt_str))
            # 当天无数据时为 404
