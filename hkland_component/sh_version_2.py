import datetime

from hkland_component.configs import (JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB, DATACENTER_HOST,
                                      DATACENTER_PORT, DATACENTER_USER, DATACENTER_PASSWD, DATACENTER_DB, SPIDER_HOST,
                                      SPIDER_PORT, SPIDER_USER, SPIDER_PASSWD, SPIDER_DB, TARGET_HOST, TARGET_PORT,
                                      TARGET_USER, TARGET_PASSWD, TARGET_DB)
from hkland_component.my_log import logger
from hkland_component.sql_pool import PyMysqlPoolBase


class ZHSCComponent(object):
    juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,
    }

    dc_cfg = {
        "host": DATACENTER_HOST,
        "port": DATACENTER_PORT,
        "user": DATACENTER_USER,
        "password": DATACENTER_PASSWD,
        "db": DATACENTER_DB,
    }

    source_cfg = {
        "host": SPIDER_HOST,
        "port": SPIDER_PORT,
        "user": SPIDER_USER,
        "password": SPIDER_PASSWD,
        "db": SPIDER_DB,
    }

    target_cfg = {
        "host": TARGET_HOST,
        "port": TARGET_PORT,
        "user": TARGET_USER,
        "password": TARGET_PASSWD,
        "db": TARGET_DB,
    }

    def init_sql_pool(self, sql_cfg: dict):
        pool = PyMysqlPoolBase(**sql_cfg)
        return pool

    def __init__(self):
        #  ID  | CompType | InnerCode | SecuCode | InDate | OutDate | Flag | CREATETIMEJZ | UPDATETIMEJZ | CMFID | CMFTime |
        #  ID  | CompType | InnerCode | SecuCode | InDate | OutDate | Flag | UpdateTime   | JSID
        self.juyuan_table_name = 'lc_zhsccomponent'
        self.dc_table_name = 'hkland_sgcomponent'
        self.target_table_name = 'hkland_sgcomponent'
        self.fields = ["CompType", "InnerCode", "SecuCode", "InDate", "OutDate", "Flag"]
        self.zh_change_table_name = 'hkex_lgt_change_of_szse_securities_lists'
        self.zh_list_table_name = 'hkex_lgt_szse_securities'
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

    def juyuan_stats(self):
        fields_str = ",".join(self.fields)
        juyuan = self.init_sql_pool(self.juyuan_cfg)
        sql = 'select {} from {}; '.format(fields_str, self.juyuan_table_name)
        datas = juyuan.select_all(sql)
        juyuan.dispose()
        return datas

    def dc_stats(self):
        fields_str = ",".join(self.fields)
        dc = self.init_sql_pool(self.dc_cfg)
        sql = 'select {} from {}; '.format(fields_str, self.dc_table_name)
        datas = dc.select_all(sql)
        dc.dispose()
        return datas

    def get_zh_changes(self):
        # 获取深股通的更改
        spider_source = self.init_sql_pool(self.source_cfg)
        sql = 'select * from {} where Time = (select max(Time) from {});'.format(
            self.zh_change_table_name, self.zh_change_table_name)
        zh_changes = spider_source.select_all(sql)
        spider_source.dispose()
        return zh_changes

    def get_hk_changes(self):
        # 获取深港通中港的更改
        spider_source = self.init_sql_pool(self.source_cfg)
        sql = 'select * from {} where Time = (select max(Time) from {}); '.format(
            self.hk_change_table_name, self.hk_change_table_name)
        hk_changes = spider_source.select_all(sql)
        spider_source.dispose()
        return hk_changes

    def check_target_exist(self, record):
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
        target = self.init_sql_pool(self.target_cfg)
        ret = target.select_all(
            "select flag from {} where SecuCode = '{}' and InDate = (select max(InDate) from {} where SecuCode = '{}'); ".format(
                self.target_table_name, code, self.target_table_name, code))[0]
        if ret.get("flag") == 1:
            return True
        else:
            return False

    def check_hk_list(self):
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

        spider_hk_list = set(get_spider_hk_list())
        target_hk_list = set(get_target_hk_list())
        logger.info(spider_hk_list == target_hk_list)
        print(spider_hk_list - target_hk_list)
        print(target_hk_list - spider_hk_list)
        return spider_hk_list == target_hk_list

    def check_zh_list(self):
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

        spider_zh_list = set(get_spider_zh_list())
        target_zh_list = set(get_target_zh_list())
        logger.info(spider_zh_list == target_zh_list)
        return spider_zh_list == target_zh_list

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
            # print(">>> ", change)
            stats = change.get("Ch_ange")
            if stats in self.stats_todonothing:
                continue

            secu_code = change.get("SSESCode")
            effective_date = datetime.datetime.combine(change.get('EffectiveDate'), datetime.datetime.min.time())

            # 加入成分股的
            if stats in self.stats_addition:
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

    def start(self):
        juyuan_datas = self.juyuan_stats()
        dc_datas = self.dc_stats()

        # for data in juyuan_datas:
        #     assert data in dc_datas
        # for data in dc_datas:
        #     assert data in juyuan_datas

        zh_changes = self.get_zh_changes()
        self.process_zh_changes(zh_changes)
        self.check_zh_list()

        hk_changes = self.get_hk_changes()
        self.process_hk_changes(hk_changes)
        self.check_hk_list()


if __name__ == "__main__":
    tool = ZHSCComponent()
    tool.start()



'''记录
2020.04.20 
新增一条恢复记录{'CompType': 3, 'SecuCode': '000043', 'InDate': datetime.datetime(2019, 12, 16, 0, 0)} 
新增一条记录: {'CompType': 3, 'SecuCode': '000022', 'InDate': datetime.datetime(2018, 1, 2, 0, 0), 'InnerCode': None, 'Flag': 1}

需要新增一条调入记录 {'CompType': 4, 'SecuCode': '00697', 'InDate': datetime.datetime(2020, 4, 15, 0, 0), 'InnerCode': 1000543, 'Flag': 1}  


'''