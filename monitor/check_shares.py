'''
CREATE TABLE `hkland_shares` (
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

'''
# 获取平安银行在最近 10 天的数据 
select * from hkland_shares where InnerCode = 3 order by Date desc limit 10;

# 将 4月10日 更入的那波长度没有做校准的数据删除
select * from hkland_hkshares  where SecuCode not like '_____' and Date = '2020-04-10 00:00:00';
delete  from hkland_hkshares  where SecuCode not like '_____' and Date = '2020-04-10 00:00:00';
'''