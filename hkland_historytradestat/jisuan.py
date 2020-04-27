# 查询要生成的历史数据中有哪些字段
# id | Date | MoneyIn | MoneyBalance | MoneyInHistoryTotal | NetBuyAmount | BuyAmount  | SellAmount | MarketTypeCode |
# MarketType | CMFID | CMFTime | CREATETIMEJZ | UPDATETIMEJZ
'''
CREATE TABLE `hkland_historytradestat` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `Date` datetime NOT NULL COMMENT '日期',
  `MoneyIn` decimal(20,4) NOT NULL COMMENT '当日资金流入(百万）',
  `MoneyBalance` decimal(20,4) NOT NULL COMMENT '当日余额（百万）',
  `MoneyInHistoryTotal` decimal(20,4) NOT NULL COMMENT '历史资金累计流入(百万元）',
  `NetBuyAmount` decimal(20,4) NOT NULL COMMENT '当日成交净买额(百万元）',
  `BuyAmount` decimal(20,4) NOT NULL COMMENT '买入成交额(百万元）',
  `SellAmount` decimal(20,4) NOT NULL COMMENT '卖出成交额(百万元）',
  `MarketTypeCode` int(11) NOT NULL COMMENT '市场类型代码',
  `MarketType` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '市场类型',
  `CMFID` bigint(20) NOT NULL COMMENT '来源ID',
  `CMFTime` datetime NOT NULL COMMENT '来源日期',
  `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
  `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un` (`Date`,`MarketTypeCode`)
) ENGINE=InnoDB AUTO_INCREMENT=536384 DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆股通资金流向汇总(港股通币种为港元，陆股通币种为人民币)'

'''

'''
Date 陆股通交易时间

MoneyBalance 当日余额(百万） 

MoneyIn 当日资金流入(百万) = 额度 - 余额

BuyAmount 当日买入成交额(百万元)

SellAmount 当日卖出成交额(百万元)

NetBuyAmount 当日成交净买额(百万元)  = (当日)买入成交额(百万元) - (当日)卖出成交额(百万元) 

MoneyInHistoryTotal 历史资金累计流入(百万) = 上一天的历史资金累计流入(百万) + 今天的当日成交净买额(百万元)

MarketTypeCode 市场类型代码 

MarketType 市场类型 

'''