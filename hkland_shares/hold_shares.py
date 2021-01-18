import datetime
import logging
import os
import opencc

logger = logging.getLogger(__name__)


class HoldShares(object):
    def __init__(self, type, offset=1):
        self.type = type
        self.url = 'https://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t={}'.format(type)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        }
        self.today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        self.offset = offset
        # 当前只能查询之前一天的记录
        self.check_day = (datetime.date.today() - datetime.timedelta(days=self.offset)).strftime("%Y/%m/%d")
        self.converter = opencc.OpenCC('t2s')  # 中文繁体转简体
        _type_map = {
            'sh': '沪股通',
            'sz': '深股通',
            'hk': '港股通',
        }

        _market_map = {
            "sh": 83,
            "sz": 90,
            'hk': 72,

        }
        self.market = _market_map.get(self.type)

        self.type_name = _type_map.get(self.type)
        _percent_comment_map = {
            'sh': '占于上交所上市及交易的A股总数的百分比(%)',
            'sz': '占于深交所上市及交易的A股总数的百分比(%)',
            'hk': '占已发行股份的百分比(%)',
        }
        self.percent_comment = _percent_comment_map.get(self.type)

        # 敏仪的爬虫表名是 hold_shares_sh hold_shares_sz hold_shares_hk
        # 我这边更新后的表名是 hoding_ .. 区别是跟正式表的字段保持一致
        self.spider_table = 'holding_shares_{}'.format(self.type)
        # 生成正式库中的两个 hkland_shares hkland_hkshares
        if self.type in ("sh", "sz"):
            self.table_name = 'hkland_shares'
        elif self.type == "hk":
            self.table_name = 'hkland_hkshares'
        else:
            raise

    def _create_product_table(self):
        sql1 = '''
        CREATE TABLE IF NOT EXISTS `hkland_shares` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `SecuCode` varchar(16) COLLATE utf8_bin NOT NULL COMMENT '股票交易代码',
          `InnerCode` int(11) NOT NULL COMMENT '内部编码',
          `SecuAbbr` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '股票简称',
          `Date` datetime NOT NULL COMMENT '自然日',
          `HKTradeDay` datetime NOT NULL COMMENT '港交所交易日',
          `Percent` decimal(20,4) DEFAULT NULL COMMENT '占A股总股本的比例（%）',
          `ShareNum` decimal(20,0) DEFAULT NULL COMMENT '股票数量(股)',
          `HashID` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT 'HashID',
          `CMFTime` datetime NOT NULL COMMENT '日期',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un` (`Date`,`HKTradeDay`,`SecuCode`),
          UNIQUE KEY `un2` (`InnerCode`,`Date`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='沪/深股通持股记录'; 
        '''

        sql2 = '''
        CREATE TABLE IF NOT EXISTS `hkland_hkshares` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `SecuCode` varchar(16) COLLATE utf8_bin NOT NULL COMMENT '股票交易代码',
          `InnerCode` int(11) NOT NULL COMMENT '内部编码',
          `SecuAbbr` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '股票简称',
          `Date` datetime NOT NULL COMMENT '日期',
          `Percent` decimal(20,4) DEFAULT NULL COMMENT '占已发行港股的比例（%）',
          `ShareNum` decimal(20,0) DEFAULT NULL COMMENT '股票数量（股）',
          `HashID` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT 'HashID',
          `CMFTime` datetime NOT NULL COMMENT '日期',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un` (`Date`,`SecuCode`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='港股通持股记录-港股'; 
        '''

    def sync(self):
        start_dt = self.today - datetime.timedelta(days=1)
        # FIXME 在每天的凌晨启动 只能重刷前一天的数据
        end_dt = self.today - datetime.timedelta(days=1)

        dt = start_dt
        _map = {}
        while dt <= end_dt:
            sql = '''select max(Date) as before_max_dt from {} where Date <= '{}'; '''.format(self.spider_table, dt)
            _dt = spider.select_one(sql).get("before_max_dt")
            _map[str(dt)] = _dt
            dt += datetime.timedelta(days=1)

        logger.info(_map)

        if self.type == "sh":
            trading_type = 1
        elif self.type == "sz":
            trading_type = 3
        else:
            trading_type = None
        if trading_type:
            dc = self._init_pool(self.dc_cfg)
            shhk_calendar_map = {}
            dt = start_dt
            while dt <= end_dt:
                # 沪港通 或者是 深港通 当前日期之间最邻近的一个交易日
                sql = '''select max(EndDate) as before_max_dt from  hkland_shszhktradingday where EndDate <= '{}' and TradingType={} and IfTradingDay=1;'''.format(dt, trading_type)
                _dt = dc.select_one(sql).get("before_max_dt")
                shhk_calendar_map[str(dt)] = _dt
                dt += datetime.timedelta(days=1)

        product = self._init_pool(self.product_cfg)
        select_fields = ['SecuCode', 'InnerCode', 'SecuAbbr', 'Percent', 'ShareNum', 'UPDATETIMEJZ']
        # FIXME 加入了 CMFTime 即使用的数据源也要计入在内
        update_fields = ['Date', 'SecuCode', 'InnerCode', 'SecuAbbr', 'Percent', 'ShareNum',
                         'CMFTime',
                         ]
        select_str = ",".join(select_fields).rstrip(",")

        jishu = []
        for dt in _map:
            sql = '''select {} from {} where Date = '{}'; '''.format(select_str, self.spider_table, _map.get(dt))
            datas = spider.select_all(sql)
            for data in datas:
                data.update({"Date": dt})
                data.update({"CMFTime": data.get("UPDATETIMEJZ")})
                data.pop("UPDATETIMEJZ")
                if self.type in ("sh", "sz"):
                    data.update({"HKTradeDay": shhk_calendar_map.get(dt)})
                    update_fields.append("HKTradeDay")
                # print(data)
                ret = self._save(product, data, self.table_name, update_fields)
                if ret == 1:
                    jishu.append(ret)

        if len(jishu) != 0:
            self.ding("【datacenter】当前的时间是{}, dc 数据库 {} 更入了 {} 条新数据".format(datetime.datetime.now(), self.table_name, len(jishu)))
        else:
            print(len(jishu))


def sync_task():
    # 获取最近 4 天的数据进行天填充以及同步
    for _type in (
            "sh",
            "sz",
            "hk",
    ):
        print("{} SYNC START.".format(_type))
        HoldShares(_type).sync()


if __name__ == "__main__":
    sync_task()
