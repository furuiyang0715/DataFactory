import datetime
import sys
import time

from hkland_component import tools
from hkland_component.configs import (JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB, DATACENTER_HOST,
                                      DATACENTER_PORT, DATACENTER_USER, DATACENTER_PASSWD, DATACENTER_DB, SPIDER_HOST,
                                      SPIDER_PORT, SPIDER_USER, SPIDER_PASSWD, SPIDER_DB, TARGET_HOST, TARGET_PORT,
                                      TARGET_USER, TARGET_PASSWD, TARGET_DB)
from hkland_component.my_log import logger
from hkland_component.sql_pool import PyMysqlPoolBase


nnow = lambda: time.time()


class SHSCComponent(object):
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
        # ID | CompType | InnerCode | SecuCode | InDate| OutDate | Flag | UpdateTime   | JSID
        # ID | CompType | InnerCode | SecuCode | InDate| OutDate | Flag | CREATETIMEJZ | UPDATETIMEJZ | CMFID | CMFTime |
        self.juyuan_table_name = 'lc_shsccomponent'
        self.dc_table_name = 'hkland_hgcomponent'
        self.target_table_name = 'hkland_hgcomponent'
        self.fields = ["CompType", "InnerCode", "SecuCode", "InDate"]
        self.sh_change_table_name = 'hkex_lgt_change_of_sse_securities_lists'
        self.sh_list_table_name = 'hkex_lgt_sse_securities'
        self.hk_change_table_name = 'lgt_sse_underlying_securities_adjustment'
        self.hk_list_table_name = 'hkex_lgt_sse_list_of_eligible_securities'

        #  不对成分股记录产生影响的状态
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
        self.ding_info = ''

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

    def get_sh_changes(self):
        # 获取沪股的更改
        spider_source = self.init_sql_pool(self.source_cfg)
        sql = 'select * from {} where Time = (select max(Time) from {});'.format(
            self.sh_change_table_name, self.sh_change_table_name)
        sh_changes = spider_source.select_all(sql)
        spider_source.dispose()
        return sh_changes

    def get_hk_changes(self):
        # 获取爬虫数据库中的沪港通（港）变更记录
        spider_source = self.init_sql_pool(self.source_cfg)
        sql = 'select * from {} where Time = (select max(Time) from {}); '.format(
            self.hk_change_table_name, self.hk_change_table_name)
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
        return spider_hk_list == target_hk_list

    def check_sh_list(self):
        def get_spider_hu_list():
            # 从爬虫成分股列表里面获取的直接成分股
            spider_source = self.init_sql_pool(self.source_cfg)
            sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
                self.sh_list_table_name, self.sh_list_table_name)
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
        return spider_hu_list == target_hu_list

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
                    info = "港股(沪)成分变更: 需要新增一条调入记录{}\n".format(record)
                    self.ding_info += info

            elif stats == '调出':
                record = {"CompType": 1, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    logger.info("需要新增一条调出记录{}".format(record))
                    info = "港股(深)成分变更: 需要新增一条调出记录{}\n".format(record)
                    self.ding_info += info

    def process_sh_changes(self, hu_changes):
        def get_sh_inner_code(secu_code):
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
                    pass
                else:
                    inner_code = get_sh_inner_code(secu_code)
                    update_time = change.get("Time")
                    record.update({"InnerCode": inner_code, "Flag": 1})
                    logger.info("新记录: {}".format(record))
                    info = '沪股成分变更: 需要新增一条记录{} \n'.format(record)
                    self.ding_info += info

            elif stats in self.stats_recover:
                record = {"CompType": 2, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    # logger.info("此条恢复记录已经存在 {}".format(record))
                    pass
                else:
                    logger.info("从移除中恢复: {}".format(record))
                    info = "沪股成分变更: 需要新增一条恢复记录{} \n".format(record)
                    self.ding_info += info

            # 移除成分股的
            elif stats in self.stats_transfer:
                record = {"CompType": 2, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if is_exist:
                    # logger.info("此条移除到只能卖出记录已经存在 {}".format(record))
                    pass
                else:
                    logger.info("移除到只能卖出: {}".format(record))
                    info = '沪股成分变更: 需要新增一条移除记录{} \n'.format(record)
                    self.ding_info += info

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
                        info = '沪股成分变更:需要新增一条删除记录:{}\n'.format(record)
                        self.ding_info += info
                    else:
                        # logger.info("此条剔除记录已被记录{}".format(record))
                        pass
                        # 剔除之前如果有移除 这条剔除不会留下记录
            else:
                logger.warning("其他状态: {}".format(stats))
                info = "沪股成分变更: 存在未知的其他状态 {} \n".format(stats)
                self.ding_info += info

    def start(self):
        juyuan_datas = self.juyuan_stats()
        dc_datas = self.dc_stats()

        for data in juyuan_datas:
            assert data in dc_datas
        for data in dc_datas:
            assert data in juyuan_datas

        sh_changes = self.get_sh_changes()
        self.process_sh_changes(sh_changes)
        ret1 = self.check_sh_list()
        info = '沪股的核对结果: {}\n'.format(ret1)
        self.ding_info += info

        hk_changes = self.get_hk_changes()
        self.process_hk_changes(hk_changes)
        ret2 = self.check_hk_list()
        info = '港股(沪)的核对结果: {}\n'.format(ret2)
        self.ding_info += info

        tools.ding_msg(self.ding_info)


if __name__ == "__main__":
    t1 = nnow()
    tool = SHSCComponent()
    tool.start()
    print("用时 {} 秒".format(nnow() - t1))
    sys.exit(0)


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

        self.ding_info = ''

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
        # 获取深股的更改
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
                    info = "深股成分股变更: 需要新增一条记录{} \n".format(record)
                    self.ding_info += info

            elif stats in self.stats_recover:
                record = {"CompType": 3, "SecuCode": secu_code, "InDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    logger.info("新增一条恢复记录{}".format(record))
                    info = '深股成分股变更: 需要新增一条恢复记录{} \n'.format(record)
                    self.ding_info += info

            elif stats in self.stats_transfer:
                record = {"CompType": 3, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    logger.info("新增一条移除记录{}".format(record))
                    info = '深股成分股变更: 需要新增一条移除记录{} \n'.format(record)
                    self.ding_info += info

            elif stats in self.stats_removal:
                record = {"CompType": 3, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    is_in_list = self.is_in_list(secu_code)
                    if is_in_list:
                        logger.info("需要剔除记录 {}".format(record))
                        info = '深股成分变更: 需要剔除一条记录: {}\n'.format(record)
                        self.ding_info += info
                    else:
                        # logger.info("此条剔除记录已被记录{}".format(record))
                        pass
            else:
                logger.warning("其他状态: {}".format(stats))
                info = "深股成分变更: 存在未知的其他状态 {} \n".format(stats)
                self.ding_info += info

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
                    info = "港股(深)成分变更: 需要新增一条调入记录{}\n".format(record)
                    self.ding_info += info

            elif stats == '调出':
                record = {"CompType": 4, "SecuCode": secu_code, "OutDate": effective_date}
                is_exist = self.check_target_exist(record)
                if not is_exist:
                    logger.info("需要新增一条调出记录{}".format(record))
                    info = "港股(深)成分变更: 需要新增一条调出记录{}\n".format(record)
                    self.ding_info += info

    def start(self):
        juyuan_datas = self.juyuan_stats()
        dc_datas = self.dc_stats()

        for data in juyuan_datas:
            assert data in dc_datas
        for data in dc_datas:
            assert data in juyuan_datas

        zh_changes = self.get_zh_changes()
        self.process_zh_changes(zh_changes)
        ret1 = self.check_zh_list()
        info = '深股的核对结果: {}\n'.format(ret1)
        self.ding_info += info

        hk_changes = self.get_hk_changes()
        self.process_hk_changes(hk_changes)
        ret2 = self.check_hk_list()
        info = '港股(深)的核对结果: {}\n'.format(ret2)
        self.ding_info += info

        tools.ding_msg(self.ding_info)


if __name__ == "__main__":
    t1 = nnow()
    tool = ZHSCComponent()
    tool.start()
    print("用时 {} 秒".format(nnow() - t1))
