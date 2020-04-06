CREATE TABLE `lgt_historical_data` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `Date` datetime(6) NOT NULL COMMENT '日期',
  `TCapitalInflow` decimal(19,4) NOT NULL COMMENT '当日资金流入(百万）',
  `TBalance` decimal(19,4) NOT NULL COMMENT '当日余额（百万）',
  `AInflowHisFunds` decimal(19,4) DEFAULT NULL COMMENT '历史资金累计流入(百万元）',
  `NetBuyMoney` decimal(19,4) DEFAULT NULL COMMENT '当日成交净买额(百万元）',
  `BuyMoney` decimal(19,4) DEFAULT NULL COMMENT '买入成交额（百万元）',
  `SellMoney` decimal(19,4) DEFAULT NULL COMMENT '卖出成交额（百万元）',
  `LCG` varchar(19) COLLATE utf8_bin DEFAULT NULL COMMENT '领涨股',
  `LCGChangeRange` decimal(19,6) DEFAULT NULL COMMENT '领涨股涨跌幅',
  `SSEChange` decimal(19,4) DEFAULT NULL COMMENT '上证指数',
  `SSEChangePrecent` decimal(19,17) DEFAULT NULL COMMENT '涨跌幅',
  `Category` varchar(10) COLLATE utf8_bin DEFAULT NULL COMMENT '类别',
  `CategoryCode` decimal(10,0) DEFAULT NULL COMMENT '类别编码',
  `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
  `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆股通历史数据-东财';