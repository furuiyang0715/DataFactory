import datetime
import traceback

from ganggutong_list.configs import LOCAL
from ganggutong_list.merge_tools import MergeTools
from ganggutong_list.my_log import logger


class ZHMergeTools(MergeTools):
    def __init__(self):
        super(ZHMergeTools, self).__init__()
        self.juyuan_table_name = 'lc_zhsccomponent'    # 深港通成分股
        self.target_table_name = 'hkland_sgcomponent'

        # 深港通中的深股通成分和更改
        self.zh_list_table_name = 'hkex_lgt_szse_securities'
        self.zh_change_table_name = 'hkex_lgt_change_of_szse_securities_lists'

        # 深港通中的港股通中的成分以及更改
        self.hk_list_table_name = 'hkex_lgt_szse_list_of_eligible_securities'
        self.hk_change_table_name = 'lgt_szse_underlying_securities_adjustment'

        #  不对成分股记录产生影响的状态
        self.stats_todonothing = [
            # 'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
            'Addition to List of Eligible SZSE Securities for Margin Trading and List of Eligible SZSE Securities for Short Selling',

            # 'Remove from List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
            'Remove from List of Eligible SZSE Securities for Margin Trading and List of Eligible SZSE Securities for Short Selling',

            # 'SSE Stock Code and Stock Name are changed to 601360 and 360 SECURITY TECHNOLOGY respectively',
            # 'SSE Stock Code and Stock Name are changed from 601313 and SJEC respectively',
            'SZSE Stock Code and Stock Name are changed from 22 and SHENZHEN CHIWAN WHARF HOLDINGS respectively',
            'SZSE Stock Code and Stock Name are changed to 1872 and CHINA MERCHANTS PORT GROUP respectively',
            'SZSE Stock Code and Stock Name are changed to 001914 and CHINA MERCHANTS PPTY OPERATION&SERVICE respectively',
            'SZSE Stock Code and Stock Name are changed from 000043 and AVIC SUNDA HOLDING respectively',

            'Buy orders suspended',
            'Buy orders resumed',

            # 'Addition to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
        ]
        self.stats_addition = ['Addition']
        self.stats_recover = [
            # 'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))',
            'Addition (from List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only))',

        ]  # 从移除中恢复的状态

        self.stats_removal = ['Removal']
        self.stats_transfer = [
            # 'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
            'Transfer to List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)'
        ]

    def get_zh_changes(self):
        # 获取深股通的更改
        spider_source = self.init_sql_pool(self.source_cfg)
        sql = 'select * from {} where Time = (select max(Time) from {});'.format(self.zh_change_table_name, self.zh_change_table_name)
        zh_changes = spider_source.select_all(sql)
        spider_source.dispose()
        return zh_changes

    def check_target_exist(self, record):
        # 检测某条记录在目标数据库中是否已经存在
        target = self.init_sql_pool(self.target_cfg)
        if record.get("InDate"):
            is_exist = target.select_one(
                "select * from {} where CompType = {} and SecuCode = '{}' and InDate = '{}'; ".format(
                    self.target_table_name, record.get("CompType"), record.get("SecuCode"), record.get("InDate")
                ))
        else:
            is_exist = target.select_one(
                "select * from {} where CompType = {} and SecuCode = '{}' and OutDate = '{}';".format(
                    self.target_table_name, record.get("CompType"), record.get("SecuCode"), record.get("OutDate")
                ))
        target.dispose()
        if is_exist:
            return True
        else:
            return False

    def is_in_list(self, code):
        # 判断当前在目标数据库中是否成分股
        target = self.init_sql_pool(self.target_cfg)
        ret = target.select_all(
            "select flag from {} where SecuCode = '{}' and InDate = (select max(InDate) from {} where SecuCode = '{}'); ".format(
                self.target_table_name, code, self.target_table_name, code))[0]
        if ret.get("flag") == 1:
            return True
        else:
            return False

    def process_zh_changes(self, zh_changes):
        def get_zh_inner_code(secu_code):
            juyuan = self.init_sql_pool(self.juyuan_cfg)
            sql = 'select * from secumain where SecuMarket = 90 and SecuCode = "{}";'.format(secu_code)
            ret = juyuan.select_all(sql)
            juyuan.dispose()
            if len(ret) != 1:
                print("请检查: {} {}".format(secu_code, len(ret)))  # 请检查: 000043 0; 000022 0
            else:
                inner_code = ret[0].get("InnerCode")
                return inner_code

        for change in zh_changes:
            stats = change.get("Ch_ange")
            # 状态无影响的
            if stats in self.stats_todonothing:
                continue

            secu_code = change.get("SSESCode")
            effective_date = datetime.datetime.combine(change.get('EffectiveDate'), datetime.datetime.min.time())

            # 加入成分股的
            if stats in self.stats_addition:
                # 判断该条记录在目标数据库中是否存在
                record = {"CompType": 3, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    inner_code = get_zh_inner_code(secu_code)
                    update_time = change.get("Time")
                    record.update({"InnerCode": inner_code, "Flag": 1})
                    logger.info("新增一条记录: {}".format(record))

            elif stats in self.stats_recover:
                record = {"CompType": 3, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    logger.info("新增一条恢复记录{}".format(record))

            # 移除成分股的
            elif stats in self.stats_transfer:
                record = {"CompType": 3, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    logger.info("新增一条移除记录{}".format(record))

            elif stats in self.stats_removal:
                record = {"CompType": 3, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    is_in_list = self.is_in_list(secu_code)
                    if is_in_list:
                        logger.info("需要剔除记录 {}".format(record))
                    else:
                        # logger.info("此条剔除记录已被记录{}".format(record))
                        pass
            else:
                logger.warning("其他状态: {}".format(stats))

    def check_zh_list(self):
        def get_juyuan_zh_list():
            juyuan = self.init_sql_pool(self.juyuan_cfg)
            sql = 'select SecuCode from {} where CompType = 3 and Flag = 1;'.format(self.juyuan_table_name)
            ret = juyuan.select_all(sql)
            juyuan.dispose()
            zh_list = [r.get("SecuCode") for r in ret]
            return zh_list

        def get_spider_zh_list():
            spider_source = self.init_sql_pool(self.source_cfg)
            sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
                self.zh_list_table_name, self.zh_list_table_name)
            ret = spider_source.select_all(sql)
            spider_source.dispose()
            zh_list = [r.get("SSESCode") for r in ret]
            return zh_list

        def get_target_zh_list():
            target = self.init_sql_pool(self.target_cfg)
            sql = 'select SecuCode from {} where CompType = 3 and Flag = 1;'.format(self.target_table_name)
            ret = target.select_all(sql)
            target.dispose()
            zh_list = [r.get("SecuCode") for r in ret]
            return zh_list

        juyuan_zh_list = set(get_juyuan_zh_list())
        spider_zh_list = set(get_spider_zh_list())
        target_zh_list = set(get_target_zh_list())
        logger.info(juyuan_zh_list == spider_zh_list)
        logger.info(spider_zh_list == target_zh_list)

    def check_hk_list(self):
        def get_juyuan_hk_list():
            juyuan = self.init_sql_pool(self.juyuan_cfg)
            sql = 'select SecuCode from {} where CompType = 4 and Flag = 1;'.format(self.juyuan_table_name)
            ret = juyuan.select_all(sql)
            juyuan.dispose()
            hk_list = [r.get("SecuCode") for r in ret]
            return hk_list

        def get_spider_hk_list():
            spider_source = self.init_sql_pool(self.source_cfg)
            sql = 'select distinct(SecuCode) from {} where Time = (select max(Time) from {});'.format(
                self.hk_list_table_name, self.hk_list_table_name)
            ret = spider_source.select_all(sql)
            spider_source.dispose()
            hk_list = [r.get("SecuCode") for r in ret]
            return hk_list

        def get_target_hk_list():
            target = self.init_sql_pool(self.target_cfg)
            sql = 'select SecuCode from {} where CompType = 4 and Flag = 1;'.format(self.target_table_name)
            ret = target.select_all(sql)
            target.dispose()
            hk_list = [r.get("SecuCode") for r in ret]
            return hk_list

        juyuan_hk_list = set(get_juyuan_hk_list())
        spider_hk_list = set(get_spider_hk_list())
        target_hk_list = set(get_target_hk_list())
        logger.info(juyuan_hk_list == spider_hk_list)
        logger.info(spider_hk_list == target_hk_list)

    def get_hk_changes(self):
        # 获取深港通中港的更改
        spider_source = self.init_sql_pool(self.source_cfg)
        sql = 'select * from {}; '.format(self.hk_change_table_name)
        hk_changes = spider_source.select_all(sql)
        spider_source.dispose()
        return hk_changes

    def process_hk_changes(self, hk_changes):
        def get_hk_inner_code(secu_code):
            juyuan = self.init_sql_pool(self.juyuan_cfg)
            sql = 'select * from hk_secumain where SecuCode = "{}";'.format(secu_code)
            ret = juyuan.select_all(sql)
            juyuan.dispose()
            if len(ret) != 1:
                print("请检查: {} {}".format(secu_code, len(ret)))
            else:
                inner_code = ret[0].get("InnerCode")
                return inner_code

        for change in hk_changes:
            secu_code = change.get("SecuCode")
            effective_date = datetime.datetime.combine(change.get('AdjustTime'), datetime.datetime.min.time())

            stats = change.get("AdjustContent")
            if stats == '调入':
                record = {"CompType": 4, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    inner_code = get_hk_inner_code(secu_code)
                    update_time = change.get("Time")
                    record.update({"InnerCode": inner_code, "Flag": 1})
                    logger.info("需要新增一条调入记录 {}".format(record))
            elif stats == '调出':
                record = {"CompType": 4, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    logger.info("需要新增一条调出记录{}".format(record))

    def _create_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS `hkland_sgcomponent` (
          `ID` bigint AUTO_INCREMENT COMMENT 'ID',
          `CompType` int NOT NULL COMMENT '成分股类别',
          `InnerCode` int NOT NULL COMMENT '证券内部编码',
          `SecuCode` varchar(10) DEFAULT NULL COMMENT '证券代码',
          `InDate` datetime NOT NULL COMMENT '调入日期',
          `OutDate` datetime DEFAULT NULL COMMENT '调出日期',
          `Flag` int DEFAULT NULL COMMENT '资讯级别',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY `IX_JZ_ZHSCComponent` (`CompType`,`SecuCode`,`InDate`),
          UNIQUE KEY `PK_JZ_ZHSCComponent` (`ID`)
        ) ENGINE=InnoDB DEFAULT CHARSET=gbk COMMENT='深港通成分股变更表'; 
        """
        target = self.init_sql_pool(self.target_cfg)
        target.insert(sql)
        target.dispose()

    def sync_juyuan_table(self, table, target_cli, target_table_name, fields=None):
        """
        将聚源数据库中的有效字段导出
        :param table:
        :param target_cli:
        :param target_table_name:
        :param fields:
        :return:
        """
        fields_str = ",".join(fields)
        # print(fields_str)   # CompType,InnerCode,SecuCode,InDate,OutDate,Flag

        juyuan = self.init_sql_pool(self.juyuan_cfg)
        sql = 'select {} from {}; '.format(fields_str, table)
        # print(sql)   # select CompType,InnerCode,SecuCode,InDate,OutDate,Flag from lc_zhsccomponent;
        datas = juyuan.select_all(sql)
        juyuan.dispose()

        # print(len(datas))    # 1984
        # print(datas[0])

        data = datas[0]
        fields = sorted(data.keys())
        columns = ", ".join(fields)
        placeholders = ', '.join(['%s'] * len(data))
        insert_sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (target_table_name, columns, placeholders)
        print(insert_sql)
        values = []
        for data in datas:
            value = tuple(data.get(field) for field in fields)
            values.append(value)
        try:
            count = target_cli.insert_many(insert_sql, values)
        except:
            logger.warning("导入聚源历史数据失败 ")
            traceback.print_exc()
        else:
            logger.info("深港通成分股历史数据插入数量: {}".format(count))
        target_cli.dispose()

    def _start(self):
        if LOCAL:
            self._create_table()

        target = self.init_sql_pool(self.target_cfg)
        fields = ["CompType", "InnerCode", "SecuCode", "InDate", "OutDate", "Flag"]
        self.sync_juyuan_table(self.juyuan_table_name, target, self.target_table_name, fields=fields)

        zh_changes = self.get_zh_changes()
        logger.info("len(hu_changes): {} ".format(len(zh_changes)))
        self.process_zh_changes(zh_changes)
        self.check_zh_list()

        hk_changes = self.get_hk_changes()
        logger.info("len(hk_changes): {}".format(len(hk_changes)))
        self.process_hk_changes(hk_changes)
        self.check_hk_list()


if __name__ == "__main__":
    tool = ZHMergeTools()

    tool.start()

# select SSESCode, EffectiveDate, Ch_ange from hkex_lgt_change_of_szse_securities_lists where SSESCode = '000043' order by  EffectiveDate;
# select * from lc_zhsccomponent where SecuCode = '000043';

# select SecuCode, AdjustTime, AdjustContent from lgt_szse_underlying_securities_adjustment where SecuCode = '02981';
# select * from lc_zhsccomponent where SecuCode = '02981';
