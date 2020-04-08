## 陆股通实时数据 
### 正式数据库 
    hkland_flow 

### 正式表字段释义 
    DateTime: 交易时间 datetime 类型，对应于爬虫库中的日期 
    ShHkFlow: 如果 Category=1, 为 sh>>hk 当日资金流向; Category=2, 为 hk>>sh 当日资金流向 
    ShHkBalance: 如果 Category=1, 为 sh>>hk 当日资金余额; Category=2, 为 hk>>sh 当日资金余额
    SzHkFlow: 如果 Category=1, 为 sz>>hk 当日资金流向; Category=2, 为 hk>>sz 当日资金流向  
    SzHkBalance: 如果 Category=1, 为 sz>>hk 当日资金余额; Category=2, 为 hk>>sz 当日资金余额 
    Netinflow: 如果 Category=1, 为 sh>>hk + sz>>hk 总额; Category=2, 为 hk>>sh + hk>>sz 总额
    Category: 类别:1 南向, 2 北向


### 正式表建表语句 
    CREATE TABLE IF NOT EXISTS `{}` (
      `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
      `DateTime` datetime NOT NULL COMMENT '交易时间',
      `ShHkFlow` decimal(19,4) NOT NULL COMMENT '沪股通/港股通(沪)当日资金流向(万）',
      `ShHkBalance` decimal(19,4) NOT NULL COMMENT '沪股通/港股通(沪)当日资金余额（万）',
      `SzHkFlow` decimal(19,4) NOT NULL COMMENT '深股通/港股通(深)当日资金流向(万）',
      `SzHkBalance` decimal(19,4) NOT NULL COMMENT '深股通/港股通(深)当日资金余额（万）',
      `Netinflow` decimal(19,4) NOT NULL COMMENT '南北向资金,当日净流入',
      `Category` tinyint(4) NOT NULL COMMENT '类别:1 南向, 2 北向',
      `HashID` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '哈希ID',
      `CMFID` bigint(20) unsigned DEFAULT NULL COMMENT '源表来源ID',
      `CMFTime` datetime DEFAULT NULL COMMENT 'Come From Time',
      `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
      `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (`id`),
      UNIQUE KEY `unique_key2` (`DateTime`,`Category`),
      UNIQUE KEY `unique_key` (`CMFID`,`Category`),
      KEY `DateTime` (`DateTime`) USING BTREE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆港通-实时资金流向';
    
### 数据源
#### 东财 

#### 同花顺

#### 交易所 


### 数据源有限级
东财 > 同花顺 > 东财
