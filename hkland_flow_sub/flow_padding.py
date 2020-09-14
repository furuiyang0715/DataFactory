from hkland_flow_sub.flow_base import FlowBase


class FlowPadding(FlowBase):
    def __init__(self):
        super(FlowPadding, self).__init__()
        self.final_table_name = 'hkland_flow_new'

    def _create_table(self):
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
