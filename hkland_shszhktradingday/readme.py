##  陆股通交易日
### 对应的聚源表为: qt_shszhsctradingday
### 字段说明:   https://dd.gildata.com/#/tableShow/1783/column///
### 其他说明
'''
1.反映沪港通和深港通交易日的基本属性和补充属性。
2.历史数据：2014年11月起
3.数据来源：聚源按照上交所、深交所、港交所披露整理
表数据更新频率： 不定时更新
'''

# (1) ID: 数据库 id
# (2) InfoSource: 交易日信息来源: 信息来源(InfoSource)与(CT_SystemConst)表中的DM字段关联，令LB=201 and DM in (72,83,90)，
# 得到信息来源的具体描述：72-香港联交所，83-上海证券交易所，90-深圳证券交易所。

# (3) EndDate: 截止日期, datetime 类型
# (4) TradingType 交易类型 交易类型(TradingType)与(CT_SystemConst)表中的DM字段关联，令LB=1844，
# 得到交易类型的具体描述：1-沪股通，2-港股通（沪），3-深股通，4-港股通（深），5-港股通（沪深）。
# (5) IfTradingDay 是否交易日（IfTradingDay），该字段固定以下常量： 1-是 2-否。
# (6) TradingPeriod	交易时段	该字段固定以下常量： 1-全天，2-上午，3-下午。
# (7) Reason 非周末非交易日原因

# (8) IfWeekEnd	是否周最后交易日	1-是 2-否
# (9) IfMonthEnd	是否月最后交易日	1-是 2-否
# (10) IfQuarterEnd	是否季最后交易日	1-是 2-否
# (11) IfYearEnd	是否年最后交易日	1-是 2-否
# (12) UpdateTime	更新时间	datetime
# (13) JSID


# sql:
# select max(EndDate), min(EndDate), count(1)  from qt_shszhsctradingday;

# select EndDate from qt_shszhsctradingday group by EndDate having count(1) != 4;

# select  count(1) from qt_shszhsctradingday where EndDate >= '2017-08-15 00:00:00' and EndDate < '2018-01-01 00:00:00';
# select  count(1) from qt_shszhsctradingday where EndDate >= '2018-01-01 00:00:00' and EndDate < '2019-01-01 00:00:00';

# 2017-08-15 00:00:00 2050-12-31 00:00:00 之间有 8 个闰年:
# 2020 2024 2028 2032 2036 2040 2044 2048

# 爬虫数据源:
'''
CREATE TABLE `trading_calendar_sz` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `Date` date NOT NULL COMMENT '日期',
  `ExchangeDate` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '是否为港股通交易日',
  `ServiceStatus` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '服务状态',
  `BuyStatus` varchar(255) COLLATE utf8_bin DEFAULT NULL COMMENT '买入申报状态',
  `SellStatus` varchar(255) COLLATE utf8_bin DEFAULT NULL COMMENT '卖出申报状态',
  `Category` varchar(200) COLLATE utf8_bin DEFAULT NULL COMMENT '类别（交易所）',
  `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
  `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `unique_key` (`Date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=43727 DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='深交所交易日历'; 


CREATE TABLE `trading_calendar_sh` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `Date` date NOT NULL COMMENT '日期',
  `ExchangeDate` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '是否为港股通交易日',
  `BuyStatus` varchar(255) COLLATE utf8_bin DEFAULT NULL COMMENT '买入申报状态',
  `SellStatus` varchar(255) COLLATE utf8_bin DEFAULT NULL COMMENT '卖出申报状态',
  `Category` varchar(200) COLLATE utf8_bin DEFAULT NULL COMMENT '类别（交易所）',
  `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
  `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `unique_key` (`Date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=43751 DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='上交所交易日历';

CREATE TABLE `trading_calendar_hk` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `Date` date NOT NULL COMMENT '日期',
  `Northbound` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '北向状态',
  `Southbound` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '南向状态',
  `Category` varchar(200) COLLATE utf8_bin DEFAULT NULL COMMENT '类别（交易所）',
  `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
  `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `unique_key` (`Date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=248891 DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='港交所交易日历'; 

'''
