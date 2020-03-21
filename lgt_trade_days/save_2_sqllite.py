# import sqlite3
#
#
# class SQLiteTool(object):
#     def __init__(self, table_name=''):
#         self.table_name = 'lgt_trading_days'
#
#     def init_sql_pool(self):
#         conn = sqlite3.connect(self.table_name + ".db")
#         print("open sqlite database.")
#         return conn
#
#     def create_table(self):
#         pool = self.init_sql_pool()
#         # 日期,星期,香港,上海及深圳,北向交易,南向交易
#         sql = '''
#         CREATE TABLE IF NOT EXISTS `{}` (
#           `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
#           `Category` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '类别',
#           `E_xplain` longtext COLLATE utf8_bin COMMENT '涨停解释',
#           `Overview` longtext COLLATE utf8_bin COMMENT '盘面概览',
#           `HardenTime` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '涨停时间',
#           `SecuName` varchar(50) COLLATE utf8_bin NOT NULL COMMENT '股票简称',
#           `Reason` longtext COLLATE utf8_bin COMMENT '涨停原因类别',
#           `CHD` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '连续涨停天数',
#           `N_umber` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '涨停开板次数',
#           `UrlID` varchar(200) COLLATE utf8_bin DEFAULT NULL COMMENT '网页id',
#           `SFT` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '封流比',
#           `PublishTime` datetime DEFAULT NULL COMMENT '发布时间',
#           `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
#           `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#           PRIMARY KEY (`id`),
#           UNIQUE KEY `unique_key` (`SecuName`,`HardenTime`)
#         ) ENGINE=InnoDB AUTO_INCREMENT=70459 DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='云财经-涨停分析';
#         '''
