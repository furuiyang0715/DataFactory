import datetime
import pickle
import sys
import traceback

from hkland_component.configs import LOCAL
from hkland_component.merge_tools import MergeTools
from hkland_component.my_log import logger


class SHMergeTools(MergeTools):
    def __init__(self):
        super(SHMergeTools, self).__init__()

        self.juyuan_table_name = 'lc_shsccomponent'    # 对标【聚源-沪港通成分股】
        self.target_table_name = 'hkland_hgcomponent'

        # 沪港通中的沪股通成分和更改
        self.hu_list_table_name = 'hkex_lgt_sse_securities'
        self.hu_change_table_name = 'hkex_lgt_change_of_sse_securities_lists'

        # 沪港通中的港股通中的成分以及更改
        self.hk_list_table_name = 'hkex_lgt_sse_list_of_eligible_securities'
        self.hk_change_table_name = 'lgt_sse_underlying_securities_adjustment'

        #  不对成分股记录产生影响的状态
        self.stats_todonothing = [
            'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
            'Remove from List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
            'SSE Stock Code and Stock Name are changed to 601360 and 360 SECURITY TECHNOLOGY respectively',
            'SSE Stock Code and Stock Name are changed from 601313 and SJEC respectively',
            'Buy orders suspended',
            'Buy orders resumed',
            'Addition to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
            # 详情见 601200
        ]
        self.stats_addition = ['Addition']
        self.stats_recover = [
            'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))']  # 从移除中恢复的状态

        self.stats_removal = ['Removal']
        self.stats_transfer = [
            'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
        ]  # Transfer to 加入这个名单; Addition to 从这个名单移除

    def get_hu_changes(self):
        # 获取爬虫数据库中的沪港通 (沪) 变更记录
        spider_source = self.init_sql_pool(self.source_cfg)
        sql = 'select * from {} where Time = (select max(Time) from {});'.format(self.hu_change_table_name, self.hu_change_table_name)
        hu_changes = spider_source.select_all(sql)
        spider_source.dispose()
        return hu_changes

    def get_hk_changes(self):
        # 获取爬虫数据库中的沪港通（港）变更记录
        spider_source = self.init_sql_pool(self.source_cfg)
        sql = 'select * from {} where Time = (select max(Time) from {}); '.format(self.hk_change_table_name, self.hk_change_table_name)
        hk_changes = spider_source.select_all(sql)
        spider_source.dispose()
        return hk_changes

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

    def process_hu_changes(self, hu_changes):
        def get_hu_inner_code(secu_code):
            juyuan = self.init_sql_pool(self.juyuan_cfg)
            sql = 'select * from secumain where SecuMarket = 83 and SecuCode = "{}";'.format(secu_code)
            ret = juyuan.select_all(sql)
            juyuan.dispose()
            if len(ret) != 1:
                print("请检查: {} {}".format(secu_code, len(ret)))  # 请检查: 601313 0
            else:
                inner_code = ret[0].get("InnerCode")
                return inner_code

        for change in hu_changes:
            stats = change.get("Ch_ange")
            # 无影响的
            if stats in self.stats_todonothing:
                continue

            secu_code = change.get("SSESCode")
            effective_date = datetime.datetime.combine(change.get('EffectiveDate'), datetime.datetime.min.time())

            # 加入成分股的
            if stats in self.stats_addition:
                # 判断这条记录是否存在
                record = {"CompType": 2, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    # logger.info("此条添加记录已经存在 {}".format(record))
                    pass
                else:
                    inner_code = get_hu_inner_code(secu_code)
                    update_time = change.get("Time")
                    record.update({"InnerCode": inner_code, "Flag": 1})
                    logger.info("新记录: {}".format(record))
                    # self.target_insert([record])

            elif stats in self.stats_recover:
                record = {"CompType": 2, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    # logger.info("此条恢复记录已经存在 {}".format(record))
                    pass
                else:
                    logger.info("从移除中恢复: {}".format(record))

            # 移除成分股的
            elif stats in self.stats_transfer:
                record = {"CompType": 2, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    # logger.info("此条移除到只能卖出记录已经存在 {}".format(record))
                    pass
                else:
                    logger.info("移除到只能卖出: {}".format(record))
            elif stats in self.stats_removal:
                record = {"CompType": 2, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    # logger.info("剔除记录已经存在 {}".format(record))
                    pass
                else:
                    # 判读这只最新的状态
                    is_in_list = self.is_in_list(secu_code)
                    if is_in_list:
                        logger.info("需剔除 {}".format(record))
                    else:
                        # logger.info("此条剔除记录已被记录{}".format(record))
                        pass
                        # 剔除之前如果有移除 这条剔除不会留下记录

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
                record = {"CompType": 1, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    inner_code = get_hk_inner_code(secu_code)
                    record.update({"InnerCode": inner_code, "Flag": 1})
                    logger.info("需要新增一条调入记录 {}".format(record))
            elif stats == '调出':
                record = {"CompType": 1, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    logger.info("需要新增一条调出记录{}".format(record))

    def check_hu_list(self):
        def get_spider_hu_list():
            # 从爬虫成分股列表里面获取的直接成分股
            spider_source = self.init_sql_pool(self.source_cfg)
            sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(self.hu_list_table_name, self.hu_list_table_name)
            ret = spider_source.select_all(sql)
            spider_source.dispose()
            hu_list = [r.get("SSESCode") for r in ret]
            return hu_list

        def get_target_hu_list():
            # 获取目标数据库的沪成分股
            target = self.init_sql_pool(self.target_cfg)
            sql = 'select SecuCode from {} where CompType = 2 and Flag = 1;'.format(self.target_table_name)
            ret = target.select_all(sql)
            hu_list = [r.get("SecuCode") for r in ret]
            target.dispose()
            return hu_list

        spider_hu_list = set(get_spider_hu_list())
        target_hu_list = set(get_target_hu_list())
        logger.info(spider_hu_list == target_hu_list)

    def check_hk_list(self):
        def get_spider_hk_list():
            spider_source = self.init_sql_pool(self.source_cfg)
            sql = 'select distinct(SecuCode) from {} where Time = (select max(Time) from {} );'.format(
                self.hk_list_table_name, self.hk_list_table_name)
            ret = spider_source.select_all(sql)
            spider_source.dispose()
            hk_list = [r.get("SecuCode") for r in ret]
            return hk_list

        def get_target_hk_list():
            target = self.init_sql_pool(self.target_cfg)
            sql = 'select SecuCode from {} where CompType = 1 and Flag = 1;'.format(self.target_table_name)
            ret = target.select_all(sql)
            target.dispose()
            hk_list = [r.get("SecuCode") for r in ret]
            return hk_list

        spider_hk_list = set(get_spider_hk_list())
        target_hk_list = set(get_target_hk_list())
        logger.info(spider_hk_list == target_hk_list)

    def _create_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS `hkland_hgcomponent` (
          `ID` bigint AUTO_INCREMENT COMMENT 'ID',
          `CompType` int NOT NULL COMMENT '成分股类别',
          `InnerCode` int NOT NULL COMMENT '证券内部编码',
          `SecuCode` varchar(10) DEFAULT NULL COMMENT '证券代码',
          `InDate` datetime NOT NULL COMMENT '调入日期',
          `OutDate` datetime DEFAULT NULL COMMENT '调出日期',
          `Flag` int DEFAULT NULL COMMENT '资讯级别',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY `IX_JZ_SHSCComponent` (`CompType`,`SecuCode`,`InDate`),
          UNIQUE KEY `PK_JZ_SHSCComponent` (`ID`)
        ) ENGINE=InnoDB DEFAULT CHARSET=gbk COMMENT='沪港通成分股变更表'; 
        """
        target = self.init_sql_pool(self.target_cfg)
        target.insert(sql)
        target.dispose()

    def sync_juyuan_table(self, table, target_cli, target_table_name, fields=None):
        """
        将聚源数据库中的有效字段导出
        """
        fields_str = ",".join(fields)
        juyuan = self.init_sql_pool(self.juyuan_cfg)
        sql = 'select {} from {}; '.format(fields_str, table)
        datas = juyuan.select_all(sql)
        juyuan.dispose()

        data = datas[0]
        fields = sorted(data.keys())
        columns = ", ".join(fields)
        placeholders = ', '.join(['%s'] * len(data))
        insert_sql = "REPLACE INTO %s ( %s ) VALUES ( %s )" % (target_table_name, columns, placeholders)
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
            logger.info("沪港通成分股历史数据插入数量: {}".format(count))
        target_cli.dispose()

    def save_hu_changes(self, hu_changes):
        with open("HU_CHANGES.pickle", "wb") as f:
            f.write(pickle.dumps(hu_changes))

    def save_hk_changes(self, hk_changes):
        with open("HK_CHANGES_H.pickle", "wb") as f:
            f.write(pickle.dumps(hk_changes))

    def loads_hu_changes(self):
        try:
            with open("HU_CHANGES.pickle", "rb") as f:
                content = f.read()
                hu_changes = pickle.loads(content)
        except:
            return []
        return hu_changes

    def loads_hk_changes(self):
        try:
            with open("HK_CHANGES_H.pickle", "rb") as f:
                content = f.read()
                hk_changes = pickle.loads(content)
        except:
            return []
        return hk_changes

    def _start(self):
        # 建表
        if LOCAL:
            self._create_table()

        # 导入聚源数据（在聚源是一直更新的情况下
        target = self.init_sql_pool(self.target_cfg)
        fields = ["CompType", "InnerCode", "SecuCode", "InDate", "OutDate", "Flag"]
        self.sync_juyuan_table(self.juyuan_table_name, target, self.target_table_name, fields=fields)

        # 处理沪港通 沪变更
        hu_changes = self.get_hu_changes()
        last_hu_changes = self.loads_hu_changes()

        new_hu_changes = []
        for change in hu_changes:
            if change not in last_hu_changes:
                new_hu_changes.append(change)

        self.save_hu_changes(hu_changes)

        logger.info("len(new_hu_changes): {}".format(len(new_hu_changes)))
        self.process_hu_changes(new_hu_changes)
        self.check_hu_list()

        # 处理沪港通 港变更
        hk_changes = self.get_hk_changes()
        last_hk_changes = self.loads_hk_changes()

        new_hk_changes = []
        for change in hk_changes:
            if not change in last_hk_changes:
                new_hk_changes.append(change)

        self.save_hk_changes(hk_changes)

        logger.info("len(new_hk_changes): {}".format(len(new_hk_changes)))
        self.process_hk_changes(new_hk_changes)
        self.check_hk_list()


if __name__ == "__main__":
    tool = SHMergeTools()
    tool.start()
