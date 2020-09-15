import copy
import datetime
import time
import pandas as pd

from hkland_flow_sub.flow_base import FlowBase, logger


class FlowPadding(FlowBase):
    def __init__(self):
        super(FlowPadding, self).__init__()
        self.final_table_name = 'hkland_flow_new'
        self.netin_table = 'hkland_flow_netin'
        self.netin_fields = [
            'DateTime',
            'Category',
            'ShHkNetBuyAmount',  # '沪股通/港股通(沪)净买额（万）',
            'ShHkBuyAmount',  # '沪股通/港股通(沪) 买入额（万）',
            'ShHkSellAmount',  # '沪股通/港股通(沪) 卖出额（万）',
            'SzHkNetBuyAmount',  # '深股通/港股通(深)净买额（万）',
            'SzHkBuyAmount',  # '深股通/港股通(深) 买入额（万）',
            'SzHkSellAmount',  # '深股通/港股通(深) 卖出额（万）',
            'TotalNetBuyAmount',  # '北向/南向净买额（万）',
            'TotalBuyAmount',  # '北向/南向买入额（万）',
            'TotalSellAmount',  # '北向/南向卖出额（万）',
        ]
        self.netbuy_table = 'hkland_flow_netbuy'
        self.netbuy_fields = [
            'DateTime',
            'Category',
            'ShHkFlow',
            'ShHkBalance',
            'SzHkFlow',
            'SzHkBalance',
            'Netinflow',
        ]

        self.merge_fields = [
            'DateTime',
            'Category',
            'ShHkNetBuyAmount',  # '沪股通/港股通(沪)净买额（万）',
            'ShHkBuyAmount',  # '沪股通/港股通(沪) 买入额（万）',
            'ShHkSellAmount',  # '沪股通/港股通(沪) 卖出额（万）',
            'SzHkNetBuyAmount',  # '深股通/港股通(深)净买额（万）',
            'SzHkBuyAmount',  # '深股通/港股通(深) 买入额（万）',
            'SzHkSellAmount',  # '深股通/港股通(深) 卖出额（万）',
            'TotalNetBuyAmount',  # '北向/南向净买额（万）',
            'TotalBuyAmount',  # '北向/南向买入额（万）',
            'TotalSellAmount',  # '北向/南向卖出额（万）',

            'ShHkFlow',
            'ShHkBalance',
            'SzHkFlow',
            'SzHkBalance',
            'Netinflow',
        ]

        self.today = datetime.datetime.today()
        self.year = self.today.year
        self.month = self.today.month
        self.day = self.today.day
        self.offset = 0

    def _create_table(self):
        self.spider_init()
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `DateTime` datetime NOT NULL COMMENT '交易时间',
          `ShHkFlow` decimal(19,4) NOT NULL COMMENT '沪股通/港股通(沪)当日资金流向(万）',
          `ShHkBalance` decimal(19,4) NOT NULL COMMENT '沪股通/港股通(沪)当日资金余额（万）',
          `SzHkFlow` decimal(19,4) NOT NULL COMMENT '深股通/港股通(深)当日资金流向(万）',
          `SzHkBalance` decimal(19,4) NOT NULL COMMENT '深股通/港股通(深)当日资金余额（万）',
          `Netinflow` decimal(19,4) NOT NULL COMMENT '南北向资金,当日净流入（万）',
          `ShHkNetBuyAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪)净买额（万）',
          `ShHkBuyAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪) 买入额（万）',
          `ShHkSellAmount` DECIMAL(19,4) COMMENT '沪股通/港股通(沪) 卖出额（万）',
          `SzHkNetBuyAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深)净买额（万）',
          `SzHkBuyAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深) 买入额（万）',
          `SzHkSellAmount` DECIMAL(19,4) COMMENT '深股通/港股通(深) 卖出额（万）',
          `TotalNetBuyAmount` DECIMAL(19,4) COMMENT '北向/南向净买额（万）',
          `TotalBuyAmount` DECIMAL(19,4) COMMENT '北向/南向买入额（万）',
          `TotalSellAmount` DECIMAL(19,4) COMMENT '北向/南向卖出额（万）',
          `Category` tinyint(4) NOT NULL COMMENT '类别:1 南向, 2 北向',
          `HashID` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '哈希ID',
          `CMFID` bigint(20) unsigned DEFAULT NULL COMMENT '源表来源ID',
          `CMFTime` datetime DEFAULT NULL COMMENT 'Come From Time',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `unique_key2` (`DateTime`,`Category`),
          UNIQUE KEY `unique_key` (`CMFID`,`Category`),
          KEY `DateTime` (`DateTime`) USING BTREE,
          KEY `k` (`UPDATETIMEJZ`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆港通-实时资金流向';
        '''.format(self.final_table_name)
        self.spider_client.insert(sql)
        self.spider_client.end()

    def gen_all_minutes(self, start: datetime.datetime, end: datetime.datetime):
        """
        生成 start 和 end 之间全部分钟时间点列表 包含前后时间点
        """
        idx = pd.date_range(start=start, end=end, freq="min")
        dt_list = [dt.to_pydatetime() for dt in idx]
        return dt_list

    def get_dt_list(self, category):
        if category == 2:    # 北向
            """9:30-11:30; 13:00-15:00  (11:30-9:30)*60+1 + (15-13)*60+1 = 242"""
            morning_start = datetime.datetime(self.year, self.month, self.day, 9, 30, 0)
            morning_end = datetime.datetime(self.year, self.month, self.day, 11, 30, 0)
            afternoon_start = datetime.datetime(self.year, self.month, self.day, 13, 1, 0)
            afternoon_end = datetime.datetime(self.year, self.month, self.day, 15, 0, 0)
            this_moment = datetime.datetime.now()
            this_moment_min = datetime.datetime(this_moment.year, this_moment.month, this_moment.day,
                                                this_moment.hour, this_moment.minute, 0) + datetime.timedelta(
                minutes=self.offset)

            if this_moment_min < morning_start:
                logger.info("北向未开盘")
                return
            elif this_moment_min <= morning_end:
                dt_list = self.gen_all_minutes(morning_start, this_moment_min)
            elif this_moment_min < afternoon_start:
                dt_list = self.gen_all_minutes(morning_start, morning_end)
            elif this_moment_min <= afternoon_end:
                dt_list = self.gen_all_minutes(morning_start, morning_end)
                dt_list.extend(self.gen_all_minutes(afternoon_start, this_moment_min))
            elif this_moment_min > afternoon_end:
                dt_list = self.gen_all_minutes(morning_start, morning_end)
                dt_list.extend(self.gen_all_minutes(afternoon_start, afternoon_end))
            else:
                raise
            return dt_list

        elif category == 1:   # 南向
            """9:00-12:00; 13:00-16:10     (12-9)*60 + (16-13) *60 + 10 + 2 = 372"""
            morning_start = datetime.datetime(self.year, self.month, self.day, 9, 0, 0)
            morning_end = datetime.datetime(self.year, self.month, self.day, 12, 0, 0)
            """
            软件上，早晨第一根 K 线是 09:30--09:31,
            中午最后一根 K 线是 11:29--11:30/13:00,   
            下午第一根 K 线是 13:00--13:01,
            下午最后一根 K 线是 14:59--15:00

            北向同理 
            """
            afternoon_start = datetime.datetime(self.year, self.month, self.day, 13, 1, 0)
            afternoon_end = datetime.datetime(self.year, self.month, self.day, 16, 10, 0)
            this_moment = datetime.datetime.now()
            this_moment_min = datetime.datetime(this_moment.year, this_moment.month, this_moment.day,
                                                this_moment.hour, this_moment.minute, 0) + datetime.timedelta(
                minutes=self.offset)
            if this_moment_min < morning_start:
                logger.info("南向未开盘")
                return
            elif this_moment_min <= morning_end:
                dt_list = self.gen_all_minutes(morning_start, this_moment_min)
            elif this_moment_min < afternoon_start:
                dt_list = self.gen_all_minutes(morning_start, morning_end)
            elif this_moment_min <= afternoon_end:
                dt_list = self.gen_all_minutes(morning_start, morning_end)
                dt_list.extend(self.gen_all_minutes(afternoon_start, this_moment_min))
            elif this_moment_min > afternoon_end:
                dt_list = self.gen_all_minutes(morning_start, morning_end)
                dt_list.extend(self.gen_all_minutes(afternoon_start, afternoon_end))
            else:
                raise
            return dt_list

    def select_already_datas(self, category, dt_list):
        """
        获取数据库中的已入库数据
        category = 1 南向
        category = 2 北向
        """
        self.product_init()
        sql = '''select * from {} where Category = {} and DateTime >= '{}' and DateTime <= '{}';'''.format(
            self.final_table_name, category, dt_list[0], dt_list[-1])
        print(sql)
        _datas = self.product_client.select_all(sql)
        for data in _datas:
            data.pop("CREATETIMEJZ")
            data.pop("UPDATETIMEJZ")
            data.pop("id")
            data.pop("HashID")
            data.pop("CMFID")
            data.pop("CMFTime")
        return _datas

    def get_flow_netbuy_datas(self, category):
        """
        category:1 南向数据
                 2 北向数据
        """
        self.spider_init()
        select_fields = ",".join(self.netbuy_fields)
        sql = '''select {} from {} where Category = {}; '''.format(select_fields, self.netbuy_table, category)
        ret = self.spider_client.select_all(sql)
        # 转换为以时间为 key 的字典
        netbuy_datas = {}
        for r in ret:
            netbuy_datas.update({r.get("DateTime"): r})
        # print(netbuy_datas)
        return netbuy_datas

    def get_flow_netin_datas(self, category):
        self.spider_init()
        select_fields = ','.join(self.netin_fields)
        sql = '''select {} from {} where Category = {};'''.format(select_fields, self.netin_table, category)
        ret = self.spider_client.select_all(sql)
        netin_datas = {}
        for r in ret:
            netin_datas.update({r.get("DateTime"): r})
        return netin_datas

    def start(self):
        # 建表
        self._create_table()

        for category in (1, 2):
            # 合成北向数据 合成数据数据以分钟线为 key
            part_datas1 = self.get_flow_netbuy_datas(category)
            part_datas2 = self.get_flow_netin_datas(category)
            merge_datas = {}
            for _min, v1 in part_datas1.items():
                if _min in part_datas2:
                    _val = copy.deepcopy(v1)
                    _val.update(part_datas2.get(_min))
                    merge_datas[_min] = _val

            north_df = pd.DataFrame(list(merge_datas.values()))
            # 以分钟时间为索引
            north_df = north_df.set_index("DateTime")
            dt_list = self.get_dt_list(category)
            need_north_df = north_df.reindex(index=dt_list)
            need_north_df.replace({0: None}, inplace=True)
            need_north_df.fillna(method="ffill", inplace=True)
            need_north_df.reset_index("DateTime", inplace=True)
            need_north_df.sort_values(by="DateTime", ascending=True, inplace=True)
            datas = need_north_df.to_dict(orient='records')
            # 转换分钟线的时间类型
            for data in datas:
                data.update({"DateTime": data.get("DateTime").to_pydatetime()})

            # 插入前程序判断是否已经存在
            already_datas = self.select_already_datas(category, dt_list)
            to_insert_lst = []
            for data in datas:
                if not data in already_datas:
                    to_insert_lst.append(data)

            print(category, len(to_insert_lst))
            self.product_init()
            for data in to_insert_lst:
                self._save(self.product_client, data, self.final_table_name, self.merge_fields)


if __name__ == '__main__':
    FlowPadding().start()

    while True:
        FlowPadding().start()
        time.sleep(3)

