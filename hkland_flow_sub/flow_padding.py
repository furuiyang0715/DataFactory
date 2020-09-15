import pprint

from hkland_flow_sub.flow_base import FlowBase


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

    def get_flow_netbuy_datas(self, category):
        """
        category:1 南向数据
                 2 北向数据
        """
        self.spider_init()
        sql = '''select * from {} where Category = {}; '''.format(self.netbuy_table, category)
        ret = self.spider_client.select_all(sql)
        # 转换为以时间为key的字典
        netbuy_datas = {}
        for r in ret:
            netbuy_datas.update({r.get("DateTime"): r})
        return netbuy_datas

    def get_flow_netin_datas(self, category):
        self.spider_init()
        sql = '''select * from {} where Category = {};'''.format(self.netin_table, category)
        ret = self.spider_client.select_all(sql)
        netin_datas = {}
        for r in ret:
            netin_datas.update({r.get("DateTime"): r})
        return netin_datas

    def start(self):
        # 建表
        self._create_table()

        # 合成北向数据 合成数据数据以分钟线为 key
        part_datas1 = self.get_flow_netbuy_datas(category=2)
        part_datas2 = self.get_flow_netin_datas(category=2)


if __name__ == '__main__':
    FlowPadding().start()
