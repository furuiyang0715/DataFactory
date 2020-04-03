sql = '''
CREATE TABLE IF NOT EXIST `base_table_updatetime` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `TableName` varchar(100) NOT NULL COMMENT '表名',
  `LastUpdateTime` datetime NOT NULL COMMENT '最后更新时间',
  `IsValid` tinyint(4) DEFAULT '1' COMMENT '是否有效',
  PRIMARY KEY (`id`),
  UNIQUE KEY `u1` (`TableName`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=18063 DEFAULT CHARSET=utf8 COMMENT='每个表的最后更新时间'; 
'''


'''
mysql> select * from base_table_updatetime;
+----+-------------------------+---------------------+---------+
| id | TableName               | LastUpdateTime      | IsValid |
+----+-------------------------+---------------------+---------+
|  1 | hkland_flow             | 2020-04-03 14:23:19 |       1 |
|  2 | hkland_hgcomponent      | 2020-04-01 05:01:33 |       1 |
|  3 | hkland_hgelistocks      | 2020-03-25 16:21:46 |       1 |
|  4 | hkland_historytradestat | 2020-04-02 17:51:52 |       1 |
|  5 | hkland_hkscc            | 2020-04-03 06:30:50 |       1 |
|  6 | hkland_hkshares         | 2020-04-03 01:20:38 |       1 |
|  7 | hkland_sgcomponent      | 2020-04-01 06:17:46 |       1 |
|  8 | hkland_sgelistocks      | 2020-03-25 16:21:49 |       1 |
|  9 | hkland_shares           | 2020-04-03 01:20:37 |       1 |
| 10 | hkland_toptrade         | 2020-04-02 17:51:52 |       1 |
| 11 | hkland_shszhktradingday | 2020-03-21 15:13:45 |       1 |
+----+-------------------------+---------------------+---------+
'''


base_datas = [{
    "id": 1,
    "TableName": "hkland_flow",
    "LastUpdateTime": "2020-04-03 14:23:19",
    "IsValid": 1,
},
]