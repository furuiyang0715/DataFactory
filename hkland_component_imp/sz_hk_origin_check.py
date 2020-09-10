import datetime
import os
import sys

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from hkland_component_imp.base import BaseSpider, logger
from hkland_component_imp.configs import LOCAL


class SZSCComponent(BaseSpider):
    # def juyuan_stats(self):
    #     """获取聚源数据库中的数据【聚源库已停止更新】"""
    #     self._juyuan_init()
    #     fields_str = ",".join(self.fields)
    #     sql = 'select {} from {}; '.format(fields_str, self.juyuan_table_name)
    #     datas = self.juyuan_client.select_all(sql)
    #     return datas

    # def dc_stats(self):
    #     """获取现在正式的 dc 数据库中的情况 """
    #     self._dc_init()
    #     fields_str = ",".join(self.fields)
    #     sql = 'select {} from {}; '.format(fields_str, self.dc_table_name)
    #     datas = self.dc_client.select_all(sql)
    #     return datas

    # def juyuan_dc_same_check(self):
    #     """检查聚源以及 dc 数据库的一致性"""
    #     juyuan_datas = self.juyuan_stats()
    #     dc_datas = self.dc_stats()
    #     logger.debug(len(juyuan_datas))
    #     logger.debug(len(dc_datas))
    #
    #     for data in juyuan_datas:
    #         assert data in dc_datas
    #     for data in dc_datas:
    #         assert data in juyuan_datas

    def get_szhk_diff_changes(self, _type):
        """获得两次时间点之间的记录差异"""
        assert _type in ("sz", "hk")
        if _type == "sz":
            table_name = self.sz_change_table_name
        else:
            table_name = self.hk_change_table_name

        self._test_init()
        self._spider_init()

        if self.is_local:
            client = self.test_client
        else:
            client = self.spider_client

        sql1 = 'select * from {} where Time = (select min(Time) from {});'.format(table_name, table_name)
        sql2 = 'select * from {} where Time = (select max(Time) from {});'.format(table_name, table_name)

        datas1 = client.select_all(sql1)
        datas2 = client.select_all(sql2)
        drop_fields = ["id", "Time", "ItemID", "CREATETIMEJZ", "UPDATETIMEJZ"]

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
        super(SZSCComponent, self).__init__()
        self.is_local = LOCAL
        self.juyuan_table_name = 'lc_zhsccomponent'
        self.dc_table_name = 'hkland_sgcomponent'
        self.target_table_name = 'hkland_sgcomponent'
        self.fields = ["CompType", "InnerCode", "SecuCode", "InDate", "OutDate", "Flag"]
        self.sz_change_table_name = 'hkex_lgt_change_of_szse_securities_lists'
        self.sz_list_table_name = 'hkex_lgt_szse_securities'
        self.hk_change_table_name = 'lgt_szse_underlying_securities_adjustment'
        self.hk_list_table_name = 'hkex_lgt_szse_list_of_eligible_securities'
        self.stats_todonothing = [
            'Addition to List of Eligible SZSE Securities for Margin Trading and List of Eligible SZSE Securities for Short Selling',
            'Remove from List of Eligible SZSE Securities for Margin Trading and List of Eligible SZSE Securities for Short Selling',
            'SZSE Stock Code and Stock Name are changed from 22 and SHENZHEN CHIWAN WHARF HOLDINGS respectively',
            'SZSE Stock Code and Stock Name are changed to 1872 and CHINA MERCHANTS PORT GROUP respectively',
            'SZSE Stock Code and Stock Name are changed to 001914 and CHINA MERCHANTS PPTY OPERATION&SERVICE respectively',
            'SZSE Stock Code and Stock Name are changed from 000043 and AVIC SUNDA HOLDING respectively',
            'Buy orders suspended',
            'Buy orders resumed',
        ]
        self.stats_addition = ['Addition']
        self.stats_recover = [
            'Addition (from List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only))',
        ]
        self.stats_removal = ['Removal']
        self.stats_transfer = [
            'Transfer to List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)'
        ]
        self.tool_table_name = 'base_table_updatetime'
        self.ding_info = ''

    def refresh_update_time(self):
        """更新工具库的更新时间"""
        self._product_init()
        sql = '''select max(UPDATETIMEJZ) as max_dt from {}; '''.format(self.target_table_name)
        max_dt = self.product_client.select_one(sql).get("max_dt")
        logger.info("最新的更新时间是{}".format(max_dt))
        refresh_sql = '''replace into {} (id,TableName, LastUpdateTime,IsValid) values (7, 'hkland_sgcomponent', '{}', 1); 
        '''.format(self.tool_table_name, max_dt)
        self.product_client.update(refresh_sql)

    def check_target_exist(self, record):
        """判断移入或者移出的数据 在目标库中是否存在"""
        self._test_init()
        self._product_init()

        if self.is_local:
            client = self.test_client
        else:
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

    def is_in_list(self, code):
        """判断是否在目标库的当前列表中 """
        self._test_init()
        self._product_init()

        if self.is_local:
            client = self.test_client
        else:
            client = self.product_client

        ret = client.select_all(
            "select flag from {} where SecuCode = '{}' and InDate = (select max(InDate) from {} where SecuCode = '{}'); ".format(
                self.target_table_name, code, self.target_table_name, code))[0]
        if ret.get("flag") == 1:
            return True
        else:
            return False

    def check_hk_list(self):
        self._spider_init()
        self._test_init()
        self._product_init()

        def get_spider_hk_list():
            sql = 'select distinct(SecuCode) from {} where Time = (select max(Time) from {});'.format(
                self.hk_list_table_name, self.hk_list_table_name)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.spider_client.select_all(sql)
            hk_list = [r.get("SecuCode") for r in ret]
            return hk_list

        def get_target_hk_list():
            sql = 'select SecuCode from {} where CompType = 4 and Flag = 1;'.format(self.target_table_name)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.product_client.select_all(sql)
            hk_list = [r.get("SecuCode") for r in ret]
            return hk_list

        spider_hk_list = set(get_spider_hk_list())
        target_hk_list = set(get_target_hk_list())
        logger.info(spider_hk_list == target_hk_list)
        print(spider_hk_list - target_hk_list)
        print(target_hk_list - spider_hk_list)
        return spider_hk_list == target_hk_list

    def check_zh_list(self):
        self._spider_init()
        self._test_init()
        self._product_init()

        def get_spider_zh_list():
            sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
                self.sz_list_table_name, self.sz_list_table_name)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.spider_client.select_all(sql)
            zh_list = [r.get("SSESCode") for r in ret]
            return zh_list

        def get_target_zh_list():
            sql = 'select SecuCode from {} where CompType = 3 and Flag = 1;'.format(self.target_table_name)
            if self.is_local:
                ret = self.test_client.select_all(sql)
            else:
                ret = self.product_client.select_all(sql)
            zh_list = [r.get("SecuCode") for r in ret]
            return zh_list

        spider_zh_list = set(get_spider_zh_list())
        target_zh_list = set(get_target_zh_list())
        logger.info(spider_zh_list == target_zh_list)
        print(spider_zh_list - target_zh_list)
        print(target_zh_list - spider_zh_list)
        return spider_zh_list == target_zh_list

    def process_zh_changes(self, zh_changes):
        self._test_init()
        self._product_init()

        if self.is_local:
            client = self.test_client
        else:
            client = self.product_client

        add_items = []
        recover_items = []
        transfer_items = []
        removal_items = []

        for change in zh_changes:
            stats = change.get("Ch_ange")
            if stats in self.stats_todonothing:
                continue

            secu_code = change.get("SSESCode")
            effective_date = datetime.datetime.combine(change.get('EffectiveDate'), datetime.datetime.min.time())

            # 需要进行填值的字段: CompType | InnerCode | SecuCode | InDate | OutDate | Flag
            if stats in self.stats_addition:
                record = {"CompType": 3, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    logger.info("记录已存在")
                    continue
                inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
                record.update({"InnerCode": inner_code, "Flag": 1})
                logger.info("新增一条记录: {}".format(record))
                info = "深股成分股变更: 需要新增一条记录{} \n".format(record)
                self.ding_info += info
                add_items.append(record)

            elif stats in self.stats_recover:
                record = {"CompType": 3, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    logger.info("记录已存在")
                    continue
                inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
                record.update({"InnerCode": inner_code, "Flag": 1})
                logger.info("新增一条恢复记录{}".format(record))
                info = '深股成分股变更: 需要新增一条恢复记录{} \n'.format(record)
                self.ding_info += info
                recover_items.append(record)

            elif stats in self.stats_transfer:
                record = {"CompType": 3, "SecuCode": secu_code, "OutDate": effective_date}
                # 移除记录要检查之前的移除效果是否存在 因为 OutDate 不是唯一的联合主键
                is_exist = self.check_target_exist(record)
                if is_exist:
                    logger.info("记录已存在")
                    continue
                inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)

                sql = 'select  InDate from {} where CompType = 3 and SecuCode = {} and Flag = 1; '.format(self.target_table_name, secu_code)
                in_date = client.select_one(sql)
                if not in_date:
                    # 已经处理过的数据 eg. 002681, 002176
                    print(secu_code)
                    continue
                else:
                    in_date = in_date.get("InDate")

                record.update({"InnerCode": inner_code, "Flag": 2, "InDate": in_date})
                transfer_items.append(record)
                logger.info("新增一条移除记录{}".format(record))

                info = '深股成分股变更: 需要新增一条移除记录{} \n'.format(record)
                self.ding_info += info

            elif stats in self.stats_removal:
                record = {"CompType": 3, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    logger.info("记录已存在")
                    continue

                inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
                sql = 'select  InDate from {} where CompType = 3 and SecuCode = {} and Flag = 1; '.format(self.target_table_name, secu_code)
                is_exist = client.select_one(sql)
                if not is_exist:
                    # 前期已经移除过或者不对状态造成影响的
                    print(secu_code)
                    continue
                else:
                    in_date = is_exist.get("InDate")
                record.update({"InnerCode": inner_code, "Flag": 2, "InDate": in_date})
                removal_items.append(record)
                logger.info("需要剔除记录 {}".format(record))
                info = '深股成分变更: 需要剔除一条记录: {}\n'.format(record)
                self.ding_info += info

            else:
                logger.warning("其他状态: {}".format(stats))
                info = "深股成分变更: 存在未知的其他状态 {} \n".format(stats)
                self.ding_info += info

        for items in (add_items, recover_items, transfer_items, removal_items):
            if items:
                self._batch_save(client, items, self.target_table_name, self.fields)

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

        self._test_init()
        self._product_init()

        if self.is_local:
            client = self.test_client
        else:
            client = self.product_client
        add_items = []
        delete_items = []
        for change in hk_changes:
            secu_code = change.get("SecuCode")
            effective_date = datetime.datetime.combine(change.get('AdjustTime'), datetime.datetime.min.time())
            stats = change.get("AdjustContent")

            if stats == '调入':
                record = {"CompType": 4, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    logger.info("记录已存在")
                    continue
                inner_code = get_hk_inner_code(secu_code)
                record.update({"InnerCode": inner_code, "Flag": 1})
                add_items.append(record)
                logger.info("需要新增一条调入记录 {}".format(record))
                info = "港股(深)成分变更: 需要新增一条调入记录{}\n".format(record)
                self.ding_info += info

            elif stats == '调出':
                record = {"CompType": 4, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    logger.info("记录已存在")
                    continue
                inner_code = get_hk_inner_code(secu_code)
                sql = 'select  InDate from {} where CompType = 4 and SecuCode = {} and Flag = 1; '.format(self.target_table_name, secu_code)
                ret = client.select_one(sql)
                if not ret:
                    print(secu_code)
                    continue
                else:
                    in_date = ret.get("InDate")
                record.update({"InnerCode": inner_code, "Flag": 2, "InDate": in_date})
                delete_items.append(record)
                logger.info("需要新增一条调出记录{}".format(record))
                info = "港股(深)成分变更: 需要新增一条调出记录{}\n".format(record)
                self.ding_info += info

        for items in (add_items, delete_items):
            if items:
                self._batch_save(client, items, self.target_table_name, self.fields)

    def start(self):
        to_add, to_delete = self.get_szhk_diff_changes("sz")
        if to_add:
            self.process_zh_changes(to_add)

        ret1 = self.check_zh_list()
        info = '深股的核对结果: {}\n'.format(ret1)
        self.ding_info += info

        to_add, to_delete = self.get_szhk_diff_changes("hk")
        if to_add:
            self.process_hk_changes(to_add)
        ret2 = self.check_hk_list()
        info = '港股(深)的核对结果: {}\n'.format(ret2)
        self.ding_info += info

        self.ding(self.ding_info)
        self.refresh_update_time()


if __name__ == "__main__":
    sz = SZSCComponent()
    sz.start()
