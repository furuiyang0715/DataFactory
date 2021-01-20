import datetime
import os
import sys

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)
from hkland_component.base import BaseSpider, logger


class SHSCComponent(BaseSpider):
    def get_shhk_diff_changes(self, _type):
        """获得两次时间点之间的差异记录"""
        assert _type in ("sh", "hk")
        if _type == "sh":
            table_name = self.sh_change_table_name
        else:
            table_name = self.hk_change_table_name

        self._spider_init()
        client = self.spider_client

        # 从爬虫数据库中获取到要比较的两个时间点
        sql = """select distinct(Time) from {}; """.format(table_name)
        ret = client.select_all(sql)
        times = [r.get("Time") for r in ret]
        t1 = times[0]   # 第一次
        t2 = times[-1]   # 最后一次
        logger.info(f"沪港通两个需要比较的时间点分别是 {t1} 和 {t2}")

        sql1 = '''select * from {} where Time = '{}' ;'''.format(table_name, t1)
        sql2 = '''select * from {} where Time = '{}' ;'''.format(table_name, t2)

        datas1 = client.select_all(sql1)
        drop_fields = ["id", "Time", "ItemID", "CREATETIMEJZ", "UPDATETIMEJZ"]

        if t1 == t2:
            for data in datas1:
                for field in drop_fields:
                    data.pop(field)
            return datas1, []
        else:
            datas2 = client.select_all(sql2)
            for datas in (datas1, datas2):
                for data in datas:
                    for field in drop_fields:
                        data.pop(field)

            to_add = []
            to_delete = []

            for record in datas1:
                if not record in datas2:
                    to_delete.append(record)
            for record in datas2:
                if not record in datas1:
                    to_add.append(record)
            return to_add, to_delete

    def __init__(self):
        super(SHSCComponent, self).__init__()
        self.juyuan_table_name = 'lc_shsccomponent'
        self.dc_table_name = 'hkland_hgcomponent'
        self.target_table_name = 'hkland_hgcomponent'
        self.fields = ["CompType", "InnerCode", "SecuCode", "InDate", "OutDate", "Flag"]
        self.sh_change_table_name = 'hkex_lgt_change_of_sse_securities_lists'
        self.sh_list_table_name = 'hkex_lgt_sse_securities'
        self.hk_change_table_name = 'lgt_sse_underlying_securities_adjustment'
        self.hk_list_table_name = 'hkex_lgt_sse_list_of_eligible_securities'

        self.stats_todonothing = [
            'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
            'Remove from List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
            'SSE Stock Code and Stock Name are changed to 601360 and 360 SECURITY TECHNOLOGY respectively',
            'SSE Stock Code and Stock Name are changed from 601313 and SJEC respectively',
            'Buy orders suspended',
            'Buy orders resumed',
            'Addition to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
        ]

        self.stats_addition = ['Addition']
        self.stats_recover = [
            'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))',
        ]
        self.stats_removal = ['Removal']
        self.stats_transfer = [
            'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
        ]
        self.tool_table_name = 'base_table_updatetime'
        self.ding_info = ''

    def refresh_update_time(self):
        """在工具表中刷新更新时间"""
        self._product_init()
        sql = '''select max(UPDATETIMEJZ) as max_dt from {}; '''.format(self.target_table_name)
        max_dt = self.product_client.select_one(sql).get("max_dt")
        logger.info("最新的更新时间是{}".format(max_dt))
        refresh_sql = '''replace into {} (id,TableName, LastUpdateTime,IsValid) values (2, 'hkland_hgcomponent', '{}', 1);
        '''.format(self.tool_table_name, max_dt)
        self.product_client.update(refresh_sql)

    def check_target_exist(self, record):
        """检查生成的记录在目标库中是否存在"""
        self._product_init()
        client = self.product_client
        if record.get("InDate"):
            is_exist = client.select_one(
                "select * from {} where CompType = {} and SecuCode = '{}' and InDate = '{}'; ".format(
                    self.target_table_name, record.get("CompType"), record.get("SecuCode"), record.get("InDate")
                ))
        else:
            is_exist = client.select_one(
                "select * from {} where CompType = {} and SecuCode = '{}' and OutDate = '{}';".format(
                    self.target_table_name, record.get("CompType"), record.get("SecuCode"), record.get("OutDate")
                ))
        if is_exist:
            return True
        else:
            return False

    def check_hk_list(self):
        self._spider_init()
        self._product_init()

        def get_spider_hk_list():
            sql = 'select distinct(SecuCode) from {} where Time = (select max(Time) from {} );'.format(
                self.hk_list_table_name, self.hk_list_table_name)

            ret = self.spider_client.select_all(sql)
            hk_list = [r.get("SecuCode") for r in ret]
            return hk_list

        def get_target_hk_list():
            sql = 'select SecuCode from {} where CompType = 1 and Flag = 1;'.format(self.target_table_name)
            ret = self.product_client.select_all(sql)
            hk_list = [r.get("SecuCode") for r in ret]
            return hk_list

        spider_hk_list = set(get_spider_hk_list())
        target_hk_list = set(get_target_hk_list())
        logger.info(spider_hk_list == target_hk_list)
        print(spider_hk_list - target_hk_list)
        print(target_hk_list - spider_hk_list)
        return spider_hk_list == target_hk_list

    def check_sh_list(self):
        self._spider_init()
        self._dc_init()

        def get_spider_sh_list():
            sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
                self.sh_list_table_name, self.sh_list_table_name)
            ret = self.spider_client.select_all(sql)
            sh_list = [r.get("SSESCode") for r in ret]
            return sh_list

        def get_target_sh_list():
            sql = 'select SecuCode from {} where CompType = 2 and Flag = 1;'.format(self.target_table_name)
            ret = self.dc_client.select_all(sql)
            sh_list = [r.get("SecuCode") for r in ret]
            return sh_list

        spider_hu_list = set(get_spider_sh_list())
        target_hu_list = set(get_target_sh_list())
        logger.info(spider_hu_list == target_hu_list)
        print("爬虫比目标表多 : ", spider_hu_list - target_hu_list)
        print("目标比爬虫表多 : ", target_hu_list - spider_hu_list)
        return spider_hu_list == target_hu_list

    def process_sh_changes(self, hu_changes):
        # client 是与 dc 一致的测试库
        self._product_init()
        client = self.product_client
        add_items = []
        recover_items = []
        transfer_items = []
        removal_items = []

        for change in hu_changes:
            stats = change.get("Ch_ange")
            if stats in self.stats_todonothing:
                continue

            secu_code = change.get("SSESCode")
            effective_date = datetime.datetime.combine(change.get('EffectiveDate'), datetime.datetime.min.time())

            if stats in self.stats_addition:
                record = {"CompType": 2, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    logger.debug("新增记录已存在")
                    continue
                inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
                record.update({"InnerCode": inner_code, "Flag": 1})
                logger.info("新记录: {}".format(record))
                info = '沪股成分变更: 需要新增一条记录{} \n'.format(record)
                self.ding_info += info
                add_items.append(record)

            elif stats in self.stats_recover:
                record = {"CompType": 2, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    logger.debug("恢复记录已存在")
                    continue
                inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
                record.update({"InnerCode": inner_code, "Flag": 1})
                logger.info("从移除中恢复: {}".format(record))
                info = "沪股成分变更: 需要新增一条恢复记录{} \n".format(record)
                self.ding_info += info
                recover_items.append(record)

            # 移除成分股的
            elif stats in self.stats_transfer:
                record = {"CompType": 2, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    logger.debug("移除记录已存在")
                    continue

                inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
                sql = 'select  InDate from {} where CompType = 2 and SecuCode = {} and Flag = 1; '.format(
                    self.target_table_name, secu_code)
                in_date = client.select_one(sql)
                if not in_date:
                    continue
                else:
                    in_date = in_date.get("InDate")

                record.update({"InnerCode": inner_code, "Flag": 2, "InDate": in_date})
                transfer_items.append(record)

                info = '沪股成分变更: 需要新增一条移除记录{} \n'.format(record)
                self.ding_info += info

            elif stats in self.stats_removal:
                record = {"CompType": 2, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    logger.info("removal 记录已存在")
                    continue
                inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
                sql = 'select  InDate from {} where CompType = 2 and SecuCode = {} and Flag = 1; '.format(
                    self.target_table_name, secu_code)
                is_exist = client.select_one(sql)
                if not is_exist:
                    logger.debug(f"removal: {secu_code}")
                    continue
                else:
                    in_date = is_exist.get("InDate")
                record.update({"InnerCode": inner_code, "Flag": 2, "InDate": in_date})
                removal_items.append(record)
                logger.info("需剔除 {}".format(record))
                info = '沪股成分变更:需要新增一条删除记录:{}\n'.format(record)
                self.ding_info += info
            else:
                logger.warning("其他状态: {}".format(stats))
                info = "沪股成分变更: 存在未知的其他状态 {} \n".format(stats)
                self.ding_info += info

        for items in (add_items, recover_items, transfer_items, removal_items):
            if items:
                self._batch_save(client, items, self.target_table_name, self.fields)
                client.end()

    def process_hk_changes(self, hk_changes):
        def get_hk_inner_code(secu_code):
            self._juyuan_init()
            sql = 'select * from hk_secumain where SecuCode = "{}";'.format(secu_code)
            ret = self.juyuan_client.select_one(sql)
            if ret:
                inner_code = ret.get("InnerCode")
                return inner_code
            else:
                return None

        self._product_init()
        client = self.product_client
        add_items = []
        delete_items = []

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
                    add_items.append(record)
                    logger.info("需要新增一条调入记录 {}".format(record))
                    info = "港股(沪)成分变更: 需要新增一条调入记录{}\n".format(record)
                    self.ding_info += info

            elif stats == '调出':
                record = {"CompType": 1, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    inner_code = get_hk_inner_code(secu_code)
                    # 1-港股通(沪); 2-沪股通。
                    sql = 'select  InDate from {} where CompType = 1 and SecuCode = {} and Flag = 1; '.format(
                        self.target_table_name, secu_code)
                    logger.debug(sql)
                    ret = client.select_one(sql)
                    if not ret:
                        logger.debug(secu_code)
                        continue
                    else:
                        in_date = ret.get("InDate")
                    record.update({"InnerCode": inner_code, "Flag": 2, "InDate": in_date})
                    delete_items.append(record)
                    logger.info("需要新增一条调出记录{}".format(record))
                    info = "港股(深)成分变更: 需要新增一条调出记录{}\n".format(record)
                    self.ding_info += info

        logger.debug("# " * 20)
        logger.debug(add_items)
        logger.debug(delete_items)
        for items in (add_items, delete_items):
            if items:
                self._batch_save(client, items, self.target_table_name, self.fields)

    def start(self):
        to_add, to_delete = self.get_shhk_diff_changes("sh")
        if to_add:
            self.process_sh_changes(to_add)

        ret1 = self.check_sh_list()
        info = '沪股的核对结果: {}\n'.format(ret1)
        self.ding_info += info

        to_add, to_delete = self.get_shhk_diff_changes("hk")
        if to_add:
            self.process_hk_changes(to_add)

        ret2 = self.check_hk_list()
        info = '港股(沪)的核对结果: {}\n'.format(ret2)
        self.ding_info += info

        self.ding(self.ding_info)
        logger.info(self.ding_info)
        self.refresh_update_time()

'''
核对流程: 
检查该条记录在爬虫清单表(hkex_lgt_sse_securities)中是否存在： 
select distinct(SSESCode) from hkex_lgt_sse_securities where Date = (select max(Date) from hkex_lgt_sse_securities); 
select * from hkex_lgt_sse_securities where Date = (select max(Date) from hkex_lgt_sse_securities) and SSESCode = '601995'; 

在爬虫变更表(hkex_lgt_change_of_sse_securities_lists) 拿到最新历史:
select * from hkex_lgt_change_of_sse_securities_lists where EffectiveDate = (select max(EffectiveDate) from hkex_lgt_change_of_sse_securities_lists) and SSESCode = '601995'; 

'''