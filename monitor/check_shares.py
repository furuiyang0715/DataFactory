''' hkland_shares 字段说明
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



CREATE TABLE `hkland_hkshares` (
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

# 手动的检查时间是在每天的 4 点之后 9 点之前
'''
# 获取平安银行在最近 10 天的数据 用来检查 hkland_shares
select * from hkland_shares where InnerCode = 3 order by Date desc limit 10;
# 检查一下爬虫数据库最近有数据的时间
select distinct(Date)  from holding_shares_sh order by Date desc  limit 5; 
select distinct(Date)  from holding_shares_sz order by Date desc  limit 5; 


# 获取北京燃气蓝天在最近 20 天的数据 用来检查 hkland_hkshares 
select * from hkland_hkshares where InnerCode = '1056190' order by Date desc limit 20 ;
# 检查下爬虫数据库最近有数据的时间 
select distinct(Date)  from holding_shares_hk  order by Date desc  limit 5; 

# 观察下网站是否更新 
https://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t=sh 
https://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t=sz 
https://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t=hk

# 观察下 docker 进程是否正常 
爬虫任务为每小时执行一次 : 
docker logs -ft --tail 1000 flow_shares_spider 

同步程序是在每天的凌晨 4 点 以及 8 点开盘前各执行一次 : 
docker logs -ft --tail 1000 flow_shares_sync

 
# 将 hkland_hkshares 4 月10 日 更入的那波长度没有做校准的数据删除. 
select * from hkland_hkshares  where SecuCode not like '_____' and Date = '2020-04-10 00:00:00';
delete  from hkland_hkshares  where SecuCode not like '_____' and Date = '2020-04-10 00:00:00';
'''