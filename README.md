## 爬虫数据加工 


### 陆股通 

#### 陆港通持股记录 
文件夹: hkland_shares 

生成 3 张表: 
hkland_shares 陆股通持股记录 
hkland_hkshares 港股通持股记录 

hkland_hkscc 陆股通持股记录 V2

运行时间： 
每个交易日的数据，在第二天凌晨 1:20 左右发布 


#### 陆股通成分股  
文件夹同表名: hkland_component

#### 陆股通合资格股
文件夹同表名: hkland_elistocks 

#### 陆股通交易日表 
文件夹同表名: hkland_shszhktradingday 

数据源: https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar?sc_lang=zh-HK 

同时根据历史的每日资金流向汇总进行了交易日的扩充。 

运行时间: 按照要求是每天的 8: 00 以及 12: 00 各更新一遍。 

根据: https://baike.baidu.com/item/%E6%B2%AA%E6%B8%AF%E9%80%9A/13613585 
沪港通的开通时间是 2014年11月17日 
对应： 

    select min(EndDate), max(EndDate) from hkland_shszhktradingday where TradingType = 1 and IfTradingDay = 1; 
    select min(EndDate), max(EndDate) from hkland_shszhktradingday where TradingType = 2 and IfTradingDay = 1; 

根据: https://baike.baidu.com/item/%E6%B7%B1%E6%B8%AF%E9%80%9A
深港通的开通时间是 2016年12月5日 
对应: 

    select min(EndDate), max(EndDate) from hkland_shszhktradingday where TradingType = 3 and IfTradingDay = 1;
    select min(EndDate), max(EndDate) from hkland_shszhktradingday where TradingType = 4 and IfTradingDay = 1;

### 陆股通实时资金流向 
文件夹同表名: hkland_flow 
网站数据源:
东财数据中心：http://data.eastmoney.com/hsgt/index.html
港交所官网 https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/Mutual-Market/Stock-Connect?sc_lang=zh-CN 
同花顺数据中心  http://data.10jqka.com.cn/hgt/hgtb/ 

运行时间: 开盘期间每分钟一次 

关于分钟线的说明: 
对于北向数据: 
软件上，早晨第一根 K 线是 09:30--09:31, 中午最后一根 K 线是 11:29--11:30/13:00, 下午第一根 K 线是 13:00--13:01, 下午最后一根 K 线是 14:59--15:00 
所以北向的分钟线的根数一共是 (11:30 - 9:30) * 60 + (15:00 - 13:00) * 60 = 240 根  
包含开盘前的一根 9:30(表征 9:29-9:00）的 K 线,即 241 根。 

对于南向数据同理: 
一共是 (12 - 9)*60 + (16:10-13:00)*60 + 1 = 180 + 190 + 1 = 371 根。 


### 陆港通每日资金流向汇总 
文件夹同表名: hkland_historytradestat 
网站数据源: 
港交所官网 https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/Mutual-Market/Stock-Connect?sc_lang=zh-CN （数据推算） 
同花顺数据中心 http://data.10jqka.com.cn/hgt/hgtb/ 
东财数据中心http://data.eastmoney.com/hsgt/index.html 

运行时间: 
TODO 每个交易日盘后可以根据最新实时流向数据计算一次 
18:00 再从东财获取一次 


### 陆港通十大成交股
文件夹同表名: hkland_toptrade
网站数据源: 
港交所 https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Statistics/Historical-Daily?sc_lang=zh-HK#select4=1&select5=0&select3=0&select2=2&select1=19 
东财数据中心http://data.eastmoney.com/hsgt/index.html 
同花顺数据中心：http://data.10jqka.com.cn/hgt/hgtb/ 

运行时间: 
每个交易日收盘后发布, 具体的发布时间待观察。 


### 说明
#### 陆股通 
属于北向资金。 
港资买卖 A 股的股票（包括：沪股通以及深股通） 
####  港股通 
属于南向资金 
陆资买卖港股的股票（包括：港股通(沪),港股通(深))
#### 陆港通 
陆股通以及港股通的合并叫法。
####  A 股的交易时间 
集合竞价的时间区间： 9:15-9:25 (9:15 ~ 9:20 可以申报和撤销; 9:20 ~9:25 可以申报，不可以撤销)
连续竞价区间：上午 9:30-11:30, 下午 13:00-15:00（我们平时所说的早晨开盘通常是指 9:30 ）
####  港股交易时间 
上午： 9:00-12:00
下午：13:00-16:10 

### 建表语句 
1.十大成交股  
```shell script
 CREATE TABLE IF NOT EXISTS `hkland_toptrade` (
    `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
    `Date` date NOT NULL COMMENT '时间',
    `SecuCode` varchar(10) COLLATE utf8_bin NOT NULL COMMENT '证券代码',
    `InnerCode` int(11) NOT NULL COMMENT '内部编码',
    `SecuAbbr` varchar(20) COLLATE utf8_bin NOT NULL COMMENT '股票简称',
    `Close` decimal(19,3) NOT NULL COMMENT '收盘价',
    `ChangePercent` decimal(19,5) NOT NULL COMMENT '涨跌幅',
    `TJME` decimal(19,3) NOT NULL COMMENT '净买额（元/港元）',
    `TMRJE` decimal(19,3) NOT NULL COMMENT '买入金额（元/港元）',
    `TCJJE` decimal(19,3) NOT NULL COMMENT '成交金额（元/港元）',
    `CategoryCode` varchar(10) COLLATE utf8_bin DEFAULT NULL COMMENT '类别代码:GGh: 港股通(沪), GGs: 港股通(深), HG: 沪股通, SG: 深股通',
    `CMFID` bigint(20) NOT NULL COMMENT '来源ID',
    `CMFTime` datetime NOT NULL COMMENT '来源日期',
    `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
    `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `un` (`SecuCode`,`Date`,`CategoryCode`) USING BTREE,
    UNIQUE KEY `un2` (`InnerCode`,`Date`,`CategoryCode`) USING BTREE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆港通十大成交股';

```
2.交易日 
```shell script
 CREATE TABLE IF NOT EXISTS `hkland_shszhktradingday` (
      `id` bigint(20)  unsigned NOT NULL AUTO_INCREMENT,
      `InfoSource` int(11) DEFAULT NULL COMMENT '信息来源',
      `EndDate` datetime NOT NULL COMMENT '截止日期',
      `TradingType` int(11) NOT NULL COMMENT '交易类型',
      `IfTradingDay` int(11) DEFAULT NULL COMMENT '是否交易日',
      `TradingPeriod` int(11) DEFAULT NULL COMMENT '交易时段',
      `Reason` varchar(50) DEFAULT NULL COMMENT '非周末非交易日原因',
      `IfWeekEnd` int(11) DEFAULT NULL COMMENT '是否周最后交易日',
      `IfMonthEnd` int(11) DEFAULT NULL COMMENT '是否月最后交易日',
      `IfQuarterEnd` int(11) DEFAULT NULL COMMENT '是否季最后交易日',
      `IfYearEnd` int(11) DEFAULT NULL COMMENT '是否年最后交易日',
      `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
      `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY `IX_QT_SHSZHSCTradingDay` (`EndDate`,`TradingType`),
      UNIQUE KEY `IX_QT_SHSZHSCTradingDay_ID` (`ID`)
    ) ENGINE=InnoDB DEFAULT CHARSET=gbk COMMENT '陆股通交易日'; 
```
