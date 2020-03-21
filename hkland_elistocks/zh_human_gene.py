import traceback

import pymysql

from hkland_elistocks.common import CommonHumamTools
from hkland_elistocks.my_log import logger


class ZHHumanTools(CommonHumamTools):
    def __init__(self):
        super(ZHHumanTools, self).__init__()
        self.buy_and_sell_list = self.get_buy_and_sell_list()
        self.buy_margin_list = self.get_buy_margin_trading_list()
        self.short_sell_list = self.get_short_sell_list()
        self.only_sell_list = self.get_only_sell_list()
        self.table_name = 'hkland_sgelistocks'  # 深港通合资格股
        self.change_table_name = 'hkex_lgt_change_of_szse_securities_lists'
        self.market = 90

        self.stats_todonothing = [
            # 'SSE Stock Code and Stock Name are changed to 601360 and 360 SECURITY TECHNOLOGY respectively',
            # 'SSE Stock Code and Stock Name are changed from 601313 and SJEC respectively',
            # 'Buy orders suspended',
            # 'Buy orders resumed',
            'SZSE Stock Code and Stock Name are changed from 000043 and AVIC SUNDA HOLDING respectively',
            'SZSE Stock Code and Stock Name are changed to 1872 and CHINA MERCHANTS PORT GROUP respectively',
            'SZSE Stock Code and Stock Name are changed from 22 and SHENZHEN CHIWAN WHARF HOLDINGS respectively',
        ]
        # self.stats_add_only_sell = 'Addition to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)'
        #
        self.stats_transfer = 'Transfer to List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)'
        self.stats_recover = 'Addition (from List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only))'
        #
        self.stats_removal = 'Removal'
        self.stats_addition = "Addition"
        #
        self.stats_add_margin_and_shortsell = "Addition to List of Eligible SZSE Securities for Margin Trading and List of Eligible SZSE Securities for Short Selling"
        self.stats_remove_margin_and_shortsell = 'Remove from List of Eligible SZSE Securities for Margin Trading and List of Eligible SZSE Securities for Short Selling'
        #
        self.sentense1 = 'This stock is also added to the List of Eligible SZSE Securities for Margin Trading and the List of Eligible SZSE Securities for Short Selling as it is also included in SZSE stock list for margin trading and shortselling.'
        self.sentense2 = 'This stock will also be added to the List of Eligible SZSE Securities for Margin Trading and the List of Eligible SZSE Securities for Short Selling as it is also included in SZSE stock list for margin trading and shortselling.'

        # self.sentense1 = 'This stock will also be added to the List of Eligible SSE Securities for Margin Trading and the List of Eligible SSE Securities for Short Selling as it is also included in SSE stock list for margin trading and shortselling.'
        # self.sentense2 = 'Initial list of securities eligible for buy and sell'

        # self.sentense3 = 'This stock will also be removed from the List of Eligible SSE Securities for Margin Trading and the List of Eligible SSE Securities for Short Selling.'
        self.sentense3 = 'This stock will also be removed from the List of Eligible SZSE Securities for Margin Trading and the List of Eligible SZSE Securities for Short Selling.'

    def get_only_sell_list(self):
        # 只可卖出清单
        spider = self.init_sql_pool(self.spider_cfg)
        sql = 'select distinct(SSESCode) from hkex_lgt_special_szse_securities where Date = (select max(Date) from hkex_lgt_special_szse_securities);'
        ret = spider.select_all(sql)
        spider.dispose()
        lst = [r.get("SSESCode") for r in ret]
        return lst

    def get_buy_and_sell_list(self):
        # 可买入以及卖出清单
        spider = self.init_sql_pool(self.spider_cfg)
        sql = 'select distinct(SSESCode) from hkex_lgt_szse_securities where Date = (select max(Date) from hkex_lgt_szse_securities);'
        ret = spider.select_all(sql)
        spider.dispose()
        lst = [r.get("SSESCode") for r in ret]
        return lst

    def get_buy_margin_trading_list(self):
        # 可进行保证金交易的清单
        spider = self.init_sql_pool(self.spider_cfg)
        sql = 'select distinct(SSESCode) from hkex_lgt_special_szse_securities_for_margin_trading where Date = (select max(Date) from hkex_lgt_special_szse_securities_for_margin_trading);'
        ret = spider.select_all(sql)
        spider.dispose()
        lst = [r.get("SSESCode") for r in ret]
        return lst

    def get_short_sell_list(self):
        # 可进行担保卖空的清单
        spider = self.init_sql_pool(self.spider_cfg)
        sql = 'select distinct(SSESCode) from hkex_lgt_special_szse_securities_for_short_selling where Date = (select max(Date) from hkex_lgt_special_szse_securities_for_short_selling);'
        ret = spider.select_all(sql)
        spider.dispose()
        lst = [r.get("SSESCode") for r in ret]
        return lst

    # @property
    # def inner_code_map(self):
    #     secu_codes = self.get_distinct_spider_secucode()
    #     inner_code_map = self.get_inner_code_map(secu_codes)
    #     return inner_code_map
    #
    # @property
    # def css_code_map(self):
    #     secu_codes = self.get_distinct_spider_secucode()
    #     css_code_map = self.get_css_code_map(secu_codes)
    #     return css_code_map

    # def get_distinct_spider_secucode(self):
    #     spider = self.init_sql_pool(self.spider_cfg)  # hkex_lgt_change_of_szse_securities_lists
    #     sql = 'select distinct(SSESCode) from {}; '.format(self.change_table_name)
    #     ret = spider.select_all(sql)
    #     spider.dispose()
    #     ret = [r.get("SSESCode") for r in ret]
    #     return ret

    # def get_inner_code_map(self, secu_codes):
    #     juyuan = self.init_sql_pool(self.juyuan_cfg)
    #     sql = 'select SecuCode, InnerCode, SecuAbbr from secumain where SecuMarket = {} and SecuCode in {};'.format(self.market, tuple(secu_codes))
    #     ret = juyuan.select_all(sql)
    #     juyuan.dispose()
    #     info = {}
    #     for r in ret:
    #         key = r.get("SecuCode")
    #         value = (r.get('InnerCode'), r.get("SecuAbbr"))
    #         info[key] = value
    #     return info

    def get_css_code_map(self, secu_codes):    # hkex_lgt_special_szse_securities
        sql = 'select SSESCode, CCASSCode, FaceValue from hkex_lgt_special_szse_securities where SSESCode in {}; '.format(tuple(secu_codes))
        spider = self.init_sql_pool(self.spider_cfg)
        ret = spider.select_all(sql)
        spider.dispose()
        info = {}
        for r in ret:
            key = r.get("SSESCode")
            value = (r.get('CCASSCode'), r.get("FaceValue"))
            info[key] = value
        return info

    def create_target_table(self):
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `ID` bigint(20) AUTO_INCREMENT COMMENT 'ID',
          `TradingType` int(11) NOT NULL COMMENT '交易方向',
          `TargetCategory` int(11) NOT NULL COMMENT '标的类别',
          `InnerCode` int(11) NOT NULL COMMENT '证券内部编码',
          `SecuCode` varchar(50) DEFAULT NULL COMMENT '证券代码',
          `SecuAbbr` varchar(50) DEFAULT NULL COMMENT '证券简称',
          `InDate` datetime NOT NULL COMMENT '调入日期',
          `OutDate` datetime DEFAULT NULL COMMENT '调出日期',
          `Flag` int(11) DEFAULT NULL COMMENT '资讯级别',
          `CCASSCode` varchar(50) DEFAULT NULL COMMENT 'CCASS股份编码',
          `ParValue` varchar(50) DEFAULT NULL COMMENT '面值(人民币)',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY `IX_JZ_ZHSCEliStocks` (`SecuCode`,`TradingType`,`TargetCategory`, `InDate`),
          UNIQUE KEY `IX_JZ_ZHSCEliStocks_ID` (`ID`)
        ) ENGINE=InnoDB DEFAULT CHARSET=gbk COMMENT '深港通合资格成分股变更表';
        '''.format(self.table_name)

        target = self.init_sql_pool(self.target_cfg)
        ret = target.insert(sql)
        target.dispose()

    def insert(self, data):
        fields = sorted(data.keys())
        columns = ", ".join(fields)
        placeholders = ', '.join(['%s'] * len(data))
        insert_sql = "INSERT INTO %s ( %s ) VALUES ( %s ); " % (self.table_name, columns, placeholders)
        value = tuple(data.get(field) for field in fields)
        target = self.init_sql_pool(self.target_cfg)

        # print(insert_sql)
        # print(value)
        try:
            count = target.insert(insert_sql, value)
        except pymysql.err.IntegrityError:
            logger.warning("重复数据 ")
            # traceback.print_exc()
        except Exception as e:
            traceback.print_exc()
        else:
            pass
        finally:
            target.dispose()

    def select_spider_records_with_a_num(self, num):
        sql = 'select SSESCode from hkex_lgt_change_of_szse_securities_lists group by SSESCode having count(1) = {}; '.format(num)
        spider = self.init_sql_pool(self.spider_cfg)
        ret = spider.select_all(sql)
        spider.dispose()
        ret = [r.get('SSESCode') for r in ret]
        return ret

    def show_code_spider_records(self, code):
        sql = '''select SSESCode, EffectiveDate, Ch_ange, Remarks from hkex_lgt_change_of_szse_securities_lists\
 where SSESCode = "{}" and Time = (select max(Time) from hkex_lgt_change_of_szse_securities_lists) order by EffectiveDate;
        '''.format(code)
        spider = self.init_sql_pool(self.spider_cfg)
        ret = spider.select_all(sql)
        spider.dispose()
        return ret

    # def get_juyuan_inner_code(self, secu_code):
    #     ret = self.inner_code_map.get(secu_code)
    #     if ret:
    #         return ret
    #     else:
    #         return None, None

    def get_ccas_code(self, secu_code):
        ret = self.css_code_map.get(secu_code)
        if ret:
            return ret
        else:
            return None, None

    def assert_stats(self, stats, secu_code):
        """
        判断当前状态与清单是否一致
        :param stats:
        :param secu_code:
        :return:
        """
        if stats.get("s1"):
            assert secu_code in self.buy_and_sell_list
        else:
            assert secu_code not in self.buy_and_sell_list

        if stats.get("s2"):
            assert secu_code in self.only_sell_list
        else:
            assert secu_code not in self.only_sell_list

        if stats.get("s3"):
            assert secu_code in self.buy_margin_list
        else:
            assert secu_code not in self.buy_margin_list

        if stats.get("s4"):
            assert secu_code in self.short_sell_list
        else:
            assert secu_code not in self.short_sell_list

    def sisth_process(self):
        codes = self.select_spider_records_with_a_num(6)
        logger.info("len-6: {}".format(codes))   # ['000043']
        spider_changes = self.show_code_spider_records(codes[0])
        for change in spider_changes:
            print(change)
        print()

        change = spider_changes[0]
        _change = change.get("Ch_ange")
        remarks = change.get("Remarks")
        effective_date = change.get("EffectiveDate")

        secu_code = change.get("SSESCode")
        inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
        ccass_code, face_value = self.get_ccas_code(secu_code)

        record1 = {
            "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
            'CCASSCode': ccass_code, 'ParValue': face_value}
        record2 = {
            "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
            'CCASSCode': ccass_code, 'ParValue': face_value}
        record3 = {
            "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
            'CCASSCode': ccass_code, 'ParValue': face_value}

        change_1 = spider_changes[1]
        _change = change_1.get("Ch_ange")
        remarks = change_1.get("Remarks")
        effective_date = change_1.get("EffectiveDate")

        record1.update({"OutDate": effective_date, 'Flag': 2})
        record2.update({"OutDate": effective_date, 'Flag': 2})
        record3.update({"OutDate": effective_date, 'Flag': 2})
        record4 = {
            "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
            'CCASSCode': ccass_code, 'ParValue': face_value}

        change_2 = spider_changes[2]
        _change = change_2.get("Ch_ange")
        remarks = change_2.get("Remarks")
        effective_date = change_2.get("EffectiveDate")

        record5 = {
            "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
            'CCASSCode': ccass_code, 'ParValue': face_value}
        record6 = {
            "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
            'CCASSCode': ccass_code, 'ParValue': face_value}
        record7 = {
            "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
            'CCASSCode': ccass_code, 'ParValue': face_value}
        record4.update({"OutDate": effective_date, 'Flag': 2})

        change_3 = spider_changes[3]
        _change = change_3.get("Ch_ange")
        remarks = change_3.get("Remarks")
        effective_date = change_3.get("EffectiveDate")

        record5.update({"OutDate": effective_date, 'Flag': 2})
        record6.update({"OutDate": effective_date, 'Flag': 2})
        record7.update({"OutDate": effective_date, 'Flag': 2})
        record8 = {
            "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
            'CCASSCode': ccass_code, 'ParValue': face_value}

        change_4 = spider_changes[4]
        _change = change_4.get("Ch_ange")
        remarks = change_4.get("Remarks")
        effective_date = change_4.get("EffectiveDate")

        # SZSE Stock Code and Stock Name are changed to 001914 and CHINA MERCHANTS PPTY OPERATION&SERVICE respectively
        # Change is resulted from the announcement issued on the SZSE website on 6 December 2019 by that listed company. For details, please refer to http://www.szse.cn/disclosure/listed/bulletinDetail/index.html?401ef940-f509-4c81-a73c-75b60dd905b9 (Chinese Version Only).
        # 此处将 secu_code 改为 001914
        # 所以最后一次插入的代码是 001914
        secu_code = '001914'

        change_5 = spider_changes[5]
        _change = change_5.get("Ch_ange")
        remarks = change_5.get("Remarks")
        effective_date = change_5.get("EffectiveDate")

        record8.update({"OutDate": effective_date, 'Flag': 2})
        record9 = {
            "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
            'CCASSCode': ccass_code, 'ParValue': face_value}
        record10 = {
            "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
            'CCASSCode': ccass_code, 'ParValue': face_value}
        record11 = {
            "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
            'CCASSCode': ccass_code, 'ParValue': face_value}

        for r in [record1, record2, record3, record4, record5, record6, record7, record8, record9, record10, record11]:
            print(r)
            self.insert(r)

        stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
        self.assert_stats(stats, secu_code)

    def fifth_process(self):
        codes = self.select_spider_records_with_a_num(5)
        logger.info("len-5: {}".format(len(codes)))
        for code in codes:
            if code in ["000931", "300236", '000918', '002477', '000418']:
                print()
                print(code)
                spider_changes = self.show_code_spider_records(code)
                assert len(spider_changes) == 5
                change = spider_changes[0]
                _change = change.get("Ch_ange")
                remarks = change.get("Remarks")
                effective_date = change.get("EffectiveDate")

                secu_code = change.get("SSESCode")
                inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
                ccass_code, face_value = self.get_ccas_code(secu_code)

                # 仅加入 1
                assert _change == self.stats_addition and self.sentense1 not in remarks
                record1 = {
                    "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                change_1 = spider_changes[1]
                _change = change_1.get("Ch_ange")
                remarks = change_1.get("Remarks")
                effective_date = change_1.get("EffectiveDate")

                # print(_change)
                # print(remarks)

                if code in ["000931", "300236", "000918"]:    # 将 1 移除 生成 2
                    assert _change == self.stats_transfer and self.sentense3 not in remarks
                    record1.update({"OutDate": effective_date, 'Flag': 2})
                    record2 = {
                        "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    change_2 = spider_changes[2]
                    _change = change_2.get("Ch_ange")
                    remarks = change_2.get("Remarks")
                    effective_date = change_2.get("EffectiveDate")

                    # print(_change)
                    # print(remarks)

                    # 结束 2， 将 1 恢复
                    assert _change == self.stats_recover and self.sentense1 not in remarks
                    record2.update({"OutDate": effective_date, 'Flag': 2})
                    record3 = {
                        "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    change_3 = spider_changes[3]
                    _change = change_3.get("Ch_ange")
                    remarks = change_3.get("Remarks")
                    effective_date = change_3.get("EffectiveDate")

                    # print(_change)
                    # print(remarks)

                    if code in ("000931", "300236"):  # 结束 1， 生成 2
                        assert _change == self.stats_transfer and self.sentense3 not in remarks
                        record3.update({"OutDate": effective_date, 'Flag': 2})
                        record4 = {
                            "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        change_4 = spider_changes[4]
                        _change = change_4.get("Ch_ange")
                        remarks = change_4.get("Remarks")
                        effective_date = change_4.get("EffectiveDate")

                        # print(_change)
                        # print(remarks)
                        # 加上 1 3 4 结束 2
                        assert _change == self.stats_recover
                        assert self.sentense1 in remarks or self.sentense2 in remarks
                        record4.update({"OutDate": effective_date, 'Flag': 2})
                        record5 = {
                            "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        record6 = {
                            "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        record7 = {
                            "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                        self.assert_stats(stats, secu_code)
                        for r in (record1, record2, record3, record4, record5, record6, record7):
                            print(r)
                            self.insert(r)

                    elif code in ("000918"):    # 加上 3 4
                        # print(_change)
                        # print(remarks)
                        assert _change == self.stats_add_margin_and_shortsell
                        record4 = {
                            "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        record5 = {
                            "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        change_4 = spider_changes[4]
                        _change = change_4.get("Ch_ange")
                        remarks = change_4.get("Remarks")
                        effective_date = change_4.get("EffectiveDate")

                        # print(_change)
                        # print(remarks)   # 移除 1 3 4 添加一条 2

                        assert _change == self.stats_transfer and self.sentense3 in remarks
                        record3.update({"OutDate": effective_date, 'Flag': 1})
                        record4.update({"OutDate": effective_date, 'Flag': 1})
                        record5.update({"OutDate": effective_date, 'Flag': 1})

                        record6 = {
                            "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                        self.assert_stats(stats, secu_code)

                        for r in (record1, record2, record3, record4, record5, record6):
                            print(r)
                            self.insert(r)

                elif code in ('002477', '000418'):   # 加入 3 4
                    # print(_change)
                    # print(remarks)
                    assert _change == self.stats_add_margin_and_shortsell
                    record2 = {
                        "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    record3 = {
                        "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    change_2 = spider_changes[2]
                    _change = change_2.get("Ch_ange")
                    remarks = change_2.get("Remarks")
                    effective_date = change_2.get("EffectiveDate")

                    # print(_change)   # 移除 3 4
                    # print(remarks)
                    assert _change == self.stats_remove_margin_and_shortsell

                    record2.update({"OutDate": effective_date, 'Flag': 2})
                    record3.update({"OutDate": effective_date, 'Flag': 2})

                    change_3 = spider_changes[3]
                    _change = change_3.get("Ch_ange")
                    remarks = change_3.get("Remarks")
                    effective_date = change_3.get("EffectiveDate")

                    # print(_change)  # 移除 1 生成 2
                    # print(remarks)
                    assert _change == self.stats_transfer and self.sentense1 not in remarks
                    record1.update({"OutDate": effective_date, 'Flag': 2})
                    record4 = {
                        "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    change_4 = spider_changes[4]
                    _change = change_4.get("Ch_ange")
                    remarks = change_4.get("Remarks")
                    effective_date = change_4.get("EffectiveDate")
                    # print(_change)  # 最后移除 2
                    # print(remarks)

                    assert _change == self.stats_removal
                    record4.update({"OutDate": effective_date, 'Flag': 2})
                    stats = {"date": effective_date, "s1": 0, "s2": 0, "s3": 0, "s4": 0}
                    self.assert_stats(stats, secu_code)
                    for r in (record1, record2, record3, record4):
                        print(r)
                        self.insert(r)

            elif code in ['000422']:
                print()
                print(code)
                spider_changes = self.show_code_spider_records(code)
                # print(pprint.pformat(spider_changes))
                assert len(spider_changes) == 5
                change = spider_changes[0]
                _change = change.get("Ch_ange")
                remarks = change.get("Remarks")
                effective_date = change.get("EffectiveDate")

                secu_code = change.get("SSESCode")
                inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
                ccass_code, face_value = self.get_ccas_code(secu_code)

                record1 = {
                    "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record2 = {
                    "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record3 = {
                    "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                # (2) 加上 2 移除 1 3 4
                record4 = {
                    "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record1.update({"OutDate": effective_date, 'Flag': 2})
                record2.update({"OutDate": effective_date, 'Flag': 2})
                record3.update({"OutDate": effective_date, 'Flag': 2})

                # (3) 恢复 1 3 4 结束 2
                record4 = {
                    "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record5 = {
                    "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record6 = {
                    "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record4.update({"OutDate": effective_date, 'Flag': 2})

                # （4） 移除 3 4
                record5.update({"OutDate": effective_date, 'Flag': 2})
                record6.update({"OutDate": effective_date, 'Flag': 2})

                # (5) 移除 1 开始 2
                record4.update({"OutDate": effective_date, 'Flag': 2})

                record7 = {
                    "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                # stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}   # TODO
                self.assert_stats(stats, secu_code)
                for r in (record1, record2, record3, record4, record5, record6, record7):
                    print(r)
                    self.insert(r)

            else:
                raise Exception

    def fourth_process(self):
        codes = self.select_spider_records_with_a_num(4)

        for code in codes:
            if code in ("002367", "001696", "002210"):
                print()
                print(code)

                #（1) 加入 1
                # (2) 加入 2 3
                # (3) 002367, 001696 移出 1 3 4 加上 2  （4) 恢复 1 3 4 结束 2
                    # 002210 移出 3 4 （4）移出 1 生成 2

                spider_changes = self.show_code_spider_records(code)
                change = spider_changes[0]
                _change = change.get("Ch_ange")
                remarks = change.get("Remarks")
                effective_date = change.get("EffectiveDate")

                secu_code = change.get("SSESCode")
                inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
                ccass_code, face_value = self.get_ccas_code(secu_code)

                assert _change == self.stats_addition and self.sentense1 not in remarks and self.sentense2 not in remarks

                record1 = {
                    "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                change_1 = spider_changes[1]
                _change = change_1.get("Ch_ange")
                remarks = change_1.get("Remarks")
                effective_date = change_1.get("EffectiveDate")

                assert _change == self.stats_add_margin_and_shortsell

                record2 = {
                    "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record3 = {
                    "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                change_2 = spider_changes[2]
                _change = change_2.get("Ch_ange")
                remarks = change_2.get("Remarks")
                effective_date = change_2.get("EffectiveDate")

                if code in ("002367", "001696"):
                    assert _change == self.stats_transfer and self.sentense3 in remarks
                    record4 = {
                        "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    record1.update({"OutDate": effective_date, 'Flag': 2})
                    record2.update({"OutDate": effective_date, 'Flag': 2})
                    record3.update({"OutDate": effective_date, 'Flag': 2})

                    change_3 = spider_changes[3]
                    _change = change_3.get("Ch_ange")
                    remarks = change_3.get("Remarks")
                    effective_date = change_3.get("EffectiveDate")

                    assert _change == self.stats_recover and (self.sentense1 in remarks or self.sentense2 in remarks)

                    record4.update({"OutDate": effective_date, 'Flag': 2})
                    record5 = {
                        "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    record6 = {
                        "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    record7 = {
                        "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                    self.assert_stats(stats, secu_code)
                    for r in (record1, record2, record3, record4, record5, record6, record7):
                        print(r)
                        self.insert(r)

                elif code in ("002210"):
                    print()
                    print(code)
                    assert _change == self.stats_remove_margin_and_shortsell
                    record2.update({"OutDate": effective_date, 'Flag': 2})
                    record3.update({"OutDate": effective_date, 'Flag': 2})

                    change_3 = spider_changes[3]
                    _change = change_3.get("Ch_ange")
                    remarks = change_3.get("Remarks")
                    effective_date = change_3.get("EffectiveDate")

                    assert _change == self.stats_transfer and self.sentense3 not in remarks
                    record1.update({"OutDate": effective_date, 'Flag': 2})
                    record4 = {
                        "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                    for r in (record1, record2, record3, record4):
                        print(r)
                        self.insert(r)

            elif code in {
                    '002210',  '000652', '002045',  '002135', '002334', '002649',
                    '002342', '002083', '002510', '002564', '000523', '002021',
                      "000498", "002192", "000016", "000070", "000301", "000861", "000936", "002100",
                      "002124", "002446", "002567", "002792",'300348', '300352', '002741', "002782",
                      "000652", "002045", "002135", '002334', '002649', "002342",
                      "000532",  "002564", "002510"
            }:
                print()
                print(code)
                #（1） 加入 1 （2）移除 1 生成 2 (3) 恢复 1 结束 2
                spider_changes = self.show_code_spider_records(code)

                change = spider_changes[0]
                _change = change.get("Ch_ange")
                remarks = change.get("Remarks")
                effective_date = change.get("EffectiveDate")

                secu_code = change.get("SSESCode")
                inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
                ccass_code, face_value = self.get_ccas_code(secu_code)

                # print("1")
                # print(_change)
                # print(remarks)

                assert _change in self.stats_addition
                assert self.sentense1 not in remarks
                record1 = {
                    "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                change_1 = spider_changes[1]
                _change = change_1.get("Ch_ange")
                remarks = change_1.get("Remarks")
                effective_date = change_1.get("EffectiveDate")

                # print("2")
                # print(_change)
                # print(remarks)
                assert _change in self.stats_transfer
                assert self.sentense3 not in remarks
                record1.update({"OutDate": effective_date, 'Flag': 2})
                record2 = {
                    "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                change_2 = spider_changes[2]
                _change = change_2.get("Ch_ange")
                remarks = change_2.get("Remarks")
                effective_date = change_2.get("EffectiveDate")

                # print("3")
                # print(_change)
                # print(remarks)
                assert _change == self.stats_recover

                record3 = {
                    "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record2.update({"OutDate": effective_date, 'Flag': 2})

                change_3 = spider_changes[3]
                _change = change_3.get("Ch_ange")
                remarks = change_3.get("Remarks")
                effective_date = change_3.get("EffectiveDate")

                # print("4")   # （1） 结束 1 增加 2  （2） 增加 3 4
                # print(_change)
                # print(remarks)
                if _change == self.stats_transfer:
                    record3.update({"OutDate": effective_date, 'Flag': 2})
                    record4 = {
                        "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                    self.assert_stats(stats, secu_code)
                    for r in (record1, record2, record3, record4):
                        self.insert(r)
                        print(r)
                elif _change == self.stats_add_margin_and_shortsell:
                    record4 = {
                        "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    record5 = {
                        "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                    self.assert_stats(stats, secu_code)
                    for r in (record1, record2, record3, record4, record5):
                        self.insert(r)
                        print(r)
                else:
                    raise Exception("请检查数据")

            elif code in ['000426', '000979', '002079', '000680', '002490', '300152']:
                print()
                print(code)
                spider_changes = self.show_code_spider_records(code)
                change = spider_changes[0]
                _change = change.get("Ch_ange")
                remarks = change.get("Remarks")
                effective_date = change.get("EffectiveDate")

                secu_code = change.get("SSESCode")
                inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
                ccass_code, face_value = self.get_ccas_code(secu_code)

                # print("1")
                # print(_change)
                # print(remarks)

                # 第一步是加入 1 3 4
                assert _change in self.stats_addition
                assert self.sentense1 in remarks

                change_1 = spider_changes[1]
                _change = change_1.get("Ch_ange")
                remarks = change_1.get("Remarks")
                effective_date = change_1.get("EffectiveDate")

                record1 = {
                    "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record2 = {
                    "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record3 = {
                    "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                # print("2")
                # print(_change)
                # print(remarks)
                if _change == self.stats_remove_margin_and_shortsell:
                    # 移出  3 4
                    # print("移出 3 4 ")
                    change_2 = spider_changes[2]
                    _change = change_2.get("Ch_ange")
                    remarks = change_2.get("Remarks")
                    effective_date = change_2.get("EffectiveDate")

                    record2.update({"OutDate": effective_date, 'Flag': 2})
                    record3.update({"OutDate": effective_date, 'Flag': 2})

                    # print("3")
                    # print(_change)
                    # print(remarks)
                    if _change == self.stats_add_margin_and_shortsell:
                        # print("加上 3 4 ")
                        record4 = {
                            "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        record5 = {
                            "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        change_3 = spider_changes[3]
                        _change = change_3.get("Ch_ange")
                        remarks = change_3.get("Remarks")
                        effective_date = change_3.get("EffectiveDate")
                        # print(_change)
                        # print(remarks)
                        assert _change == self.stats_transfer and self.sentense3 in remarks   # 移出 1 3 4 生成 2
                        record1.update({"OutDate": effective_date, 'Flag': 2})
                        record4.update({"OutDate": effective_date, 'Flag': 2})
                        record5.update({"OutDate": effective_date, 'Flag': 2})
                        record6 = {
                            "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        stats = {"date": effective_date, "s1": 0, "s2": 1, "s3":0, "s4": 0}
                        self.assert_stats(stats, secu_code)
                        for r in (record1, record2, record3, record4, record5, record6):
                            print(r)
                            self.insert(r)

                    elif _change == self.stats_transfer:
                        assert self.sentense3 not in remarks
                        # print("移出 1, 生成 2")
                        record1.update({"OutDate": effective_date, 'Flag': 2})
                        record4 = {
                            "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        change_3 = spider_changes[3]
                        _change = change_3.get("Ch_ange")
                        remarks = change_3.get("Remarks")
                        effective_date = change_3.get("EffectiveDate")
                        # print(_change)
                        # print(remarks)

                        if _change == self.stats_removal:
                            # print("移出 2 ")
                            record4.update({"OutDate": effective_date, 'Flag': 2})
                            stats = {"date": effective_date, "s1":0, "s2":0, "s3":0, "s4": 0}
                            self.assert_stats(stats, secu_code)
                            for r in (record1, record2, record3, record4):
                                self.insert(r)
                                print(r)
                        else:
                            assert _change == self.stats_recover
                            assert self.sentense1 not in remarks
                            # print("恢复 1 , 结束 2")
                            record5 = {
                                "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                                "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                'CCASSCode': ccass_code, 'ParValue': face_value}
                            stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 0, "s4": 0}
                            self.assert_stats(stats, secu_code)
                            for r in (record1, record2, record3, record4, record5):
                                self.insert(r)
                                print(r)
                    else:
                        raise Exception("error")
                elif _change == self.stats_transfer and self.sentense3 in remarks:
                    # print("移出 1 3 4 生成 2 ")
                    change_2 = spider_changes[2]
                    _change = change_2.get("Ch_ange")
                    remarks = change_2.get("Remarks")
                    effective_date = change_2.get("EffectiveDate")
                    # print("3")
                    # print(_change)
                    # print(remarks)
                    record1.update({"OutDate": effective_date, 'Flag': 2})
                    record2.update({"OutDate": effective_date, 'Flag': 2})
                    record3.update({"OutDate": effective_date, 'Flag': 2})
                    record4 = {
                        "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    assert _change == self.stats_recover
                    assert self.sentense1 not in remarks
                    # print("恢复 1, 结束 2")
                    record5 = {
                        "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    record4.update({"OutDate": effective_date, 'Flag': 2})

                    change_3 = spider_changes[3]
                    _change = change_3.get("Ch_ange")
                    remarks = change_3.get("Remarks")
                    effective_date = change_3.get("EffectiveDate")

                    # print("4")
                    # print(_change)
                    # print(remarks)
                    assert _change == self.stats_transfer    # 移出 1 生成 2
                    record5.update({"OutDate": effective_date, 'Flag': 2})
                    record6 = {
                        "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                    self.assert_stats(stats, secu_code)
                    for r in (record1, record2, record3, record4, record5, record6):
                        print(r)
                        self.insert(r)
                else:
                    raise Exception("请检查数据")

            else:
                raise Exception

    def third_process(self):
        codes = self.select_spider_records_with_a_num(3)
        print(len(codes))  # 99
        codes = set(codes) - {"002008"}
        for code in codes:
            print()
            print(code)
            spider_changes = self.show_code_spider_records(code)
            # print(pprint.pformat(spider_changes))

            change = spider_changes[0]
            _change = change.get("Ch_ange")
            remarks = change.get("Remarks")
            effective_date = change.get("EffectiveDate")

            secu_code = change.get("SSESCode")
            inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
            ccass_code, face_value = self.get_ccas_code(secu_code)

            change_1 = spider_changes[1]
            _change_1 = change_1.get("Ch_ange")
            remarks_1 = change_1.get("Remarks")
            effective_date_1 = change_1.get("EffectiveDate")

            change_2 = spider_changes[2]
            _change_2 = change_2.get("Ch_ange")
            remarks_2 = change_2.get("Remarks")
            effective_date_2 = change_2.get("EffectiveDate")

            # print(_change, "\n", _change_1, "\n", _change_2)
            assert _change == self.stats_addition
            if self.sentense1 in remarks or self.sentense2 in remarks:
                # print("step 1, add 1 3 4")
                r1 = {"TargetCategory": 1, 'InDate': effective_date, "OutDate": None, 'Flag': 1}
                r2 = {"TargetCategory": 3, 'InDate': effective_date, "OutDate": None, 'Flag': 1}
                r3 = {"TargetCategory": 4, 'InDate': effective_date, "OutDate": None, 'Flag': 1}

                if _change_1 == self.stats_transfer and self.sentense3 in remarks_1:
                    # print("step 2, del 1 3 4, add 2")
                    r1.update({"OutDate": effective_date_1, 'Flag': 2})
                    r2.update({"OutDate": effective_date_1, 'Flag': 2})
                    r3.update({"OutDate": effective_date_1, 'Flag': 2})
                    r4 = {"TargetCategory": 2, 'InDate': effective_date_1, "OutDate": None, 'Flag': 1}

                    if _change_2 == self.stats_recover and self.sentense1 in remarks_2 or self.sentense2 in remarks_2:
                        # print("step 3, stop 2, add 1 3 4")
                        r4.update({"OutDate": effective_date_2, 'Flag': 2})
                        r5 = {"TargetCategory": 1, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}
                        r6 = {"TargetCategory": 3, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}
                        r7 = {"TargetCategory": 4, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}

                        stats = {"date": effective_date_2, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                        self.assert_stats(stats, secu_code)

                        for r in (r1, r2, r3, r4, r5, r6, r7):
                            r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                            print(r)
                            self.insert(r)

                    elif _change_2 == self.stats_recover and self.sentense1 not in remarks_2 and self.sentense2 not in remarks_2:
                        # print("step 3, stop 2, add 1")
                        r4.update({"OutDate": effective_date_2, 'Flag': 2})
                        r5 = {"TargetCategory": 1, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}

                        stats = {"date": effective_date_2, "s1": 1, "s2": 0, "s3": 0, "s4": 0}
                        # stats = {"date": effective_date_2, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                        self.assert_stats(stats, secu_code)

                        for r in (r1, r2, r3, r4, r5):
                            r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                                      "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                            print(r)
                            self.insert(r)

                    elif _change_2 == self.stats_removal:
                        # print("step 3, del 2")
                        r4.update({"OutDate": effective_date_2, 'Flag': 2})

                        for r in (r1, r2, r3, r4):
                            r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                                      "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                            print(r)
                            self.insert(r)
                    else:
                        # print(_change_2)
                        raise Exception
                elif _change_1 == self.stats_transfer and self.sentense3 not in remarks_1:
                    # print("step 2, del 1, add 2")
                    # r1.update({"OutDate": effective_date_1, 'Flag': 2})
                    # r4 = {"TargetCategory": 2, 'InDate': effective_date_1, "OutDate": None, 'Flag': 1}
                    raise Exception("1 是否是 3 4 的必要条件")
                elif _change_1 == self.stats_remove_margin_and_shortsell:
                    # print("step 2, del 3 4")
                    r2.update({"OutDate": effective_date_1, 'Flag': 2})
                    r3.update({"OutDate": effective_date_1, 'Flag': 2})
                    if _change_2 == self.stats_transfer:
                        # print("step 3, del 1, add 2")
                        r1.update({"OutDate": effective_date_2, 'Flag': 2})
                        r4 = {"TargetCategory": 2, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}
                        stats = {"date": effective_date_2, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                        self.assert_stats(stats, secu_code)
                        for r in (r1, r2, r3, r4):
                            r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                            print(r)
                            self.insert(r)
                    elif _change_2 == self.stats_add_margin_and_shortsell:
                        # print("step 3, add 3 4 ")
                        r4 = {"TargetCategory": 3, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}
                        r5 = {"TargetCategory": 4, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}
                        stats = {"date": effective_date_2, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                        self.assert_stats(stats, secu_code)
                        for r in (r1, r2, r3, r4, r5):
                            r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                            print(r)
                            self.insert(r)
                    else:
                        raise Exception
                else:
                    # print(_change_1)
                    raise Exception
            else:
                # print("step 1, add 1 only")
                r1 = {"TargetCategory": 1, 'InDate': effective_date, "OutDate": None, 'Flag': 1}
                if _change_1 == self.stats_transfer and self.sentense3 not in remarks_1:
                    # print("step 2, del 1, add 2")
                    r1.update({"OutDate": effective_date_1, 'Flag': 2})
                    r2 = {"TargetCategory": 2, 'InDate': effective_date_1, "OutDate": None, 'Flag': 1}
                    if _change_2 == self.stats_recover and self.sentense1 in remarks_2 or self.sentense2 in remarks_2:
                        # print("step 3, add 1 3 4 , del 2")
                        r3 = {"TargetCategory": 1, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}
                        r4 = {"TargetCategory": 3, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}
                        r5 = {"TargetCategory": 4, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}
                        r2.update({"OutDate": effective_date_2, 'Flag': 2})
                        stats = {"date": effective_date_2, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                        self.assert_stats(stats, secu_code)
                        for r in (r1, r2, r3, r4, r5):
                            r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                                      "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                            print(r)
                            self.insert(r)
                    elif _change_2 == self.stats_removal:
                        # print("step 3, del2")
                        r2.update({"OutDate": effective_date_2, 'Flag': 2})
                        stats = {"date": effective_date_2, "s1": 0, "s2": 0, "s3": 0, "s4": 0}
                        self.assert_stats(stats, secu_code)
                        for r in (r1, r2):
                            r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                                      "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                            print(r)
                            self.insert(r)
                    elif _change_2 == self.stats_recover:
                        # print("step3 add 1, del2")
                        # print(_change_2)
                        # print(remarks_2)
                        # print(code)
                        r3 = {"TargetCategory": 1, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}
                        r2.update({"OutDate": effective_date_2, 'Flag': 2})
                        stats = {"date": effective_date_2, "s1": 1, "s2": 0, "s3": 0, "s4": 0}
                        self.assert_stats(stats, secu_code)
                        for r in (r1, r2, r3):
                            r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                                      "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                            print(r)
                            self.insert(r)

                elif _change_1 == self.stats_add_margin_and_shortsell:
                    # print("step 2, add 3 4")
                    r2 = {"TargetCategory": 3, 'InDate': effective_date_1, "OutDate": None, 'Flag': 1}
                    r3 = {"TargetCategory": 4, 'InDate': effective_date_1, "OutDate": None, 'Flag': 1}

                    assert _change_2 == self.stats_transfer
                    if self.sentense3 in remarks_1:
                        # print("step 3, del 1 3 4, add 2")
                        r1.update({"OutDate": effective_date_2, 'Flag': 2})
                        r2.update({"OutDate": effective_date_2, 'Flag': 2})
                        r3.update({"OutDate": effective_date_2, 'Flag': 2})
                        r2 = {"TargetCategory": 2, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}
                        stats = {"date": effective_date_2, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                        self.assert_stats(stats, secu_code)
                        for r in (r1, r2, r3):
                            r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                                      "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                            print(r)
                            self.insert(r)
                    else:
                        # print("step 3, del 1, add 2")
                        assert self.sentense3 in remarks_2
                        r1.update({"OutDate": effective_date_2, 'Flag': 2})
                        r2.update({"OutDate": effective_date_2, 'Flag': 2})
                        r3.update({"OutDate": effective_date_2, 'Flag': 2})
                        r2 = {"TargetCategory": 2, 'InDate': effective_date_2, "OutDate": None, 'Flag': 1}
                        stats = {"date": effective_date_2, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                        self.assert_stats(stats, secu_code)
                        for r in (r1, r2, r3):
                            r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                                      "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                            print(r)
                            self.insert(r)
                else:
                    print(_change_1)
                    raise Exception

    def second_process(self):
        lst = []
        codes = self.select_spider_records_with_a_num(2)
        print(len(codes))    # 743

        # codes = {"000333",
        #          "000022"}
        codes = set(codes) - {"000333", "000022"}
        for code in codes:
            print()
            print(code)
            spider_changes = self.show_code_spider_records(code)
            # print(pprint.pformat(spider_changes))

            change = spider_changes[0]
            _change = change.get("Ch_ange")
            remarks = change.get("Remarks")
            effective_date = change.get("EffectiveDate")

            secu_code = change.get("SSESCode")
            inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
            ccass_code, face_value = self.get_ccas_code(secu_code)

            change_1 = spider_changes[1]
            _change_1 = change_1.get("Ch_ange")
            remarks_1 = change_1.get("Remarks")
            effective_date_1 = change_1.get("EffectiveDate")
            # print(_change)

            if _change == self.stats_addition:
                if self.sentense1 in remarks or self.sentense2 in remarks:
                    print("1: add 1 3 4")
                    assert _change_1 == self.stats_transfer
                    print("2: del 1 3 4; add 2")

                    r1 = {"TargetCategory": 1, 'InDate': effective_date, "OutDate": None, 'Flag': 1}
                    r2 = {"TargetCategory": 3, 'InDate': effective_date, "OutDate": None, 'Flag': 1}
                    r3 = {"TargetCategory": 4, 'InDate': effective_date, "OutDate": None, 'Flag': 1}

                    r1.update({"OutDate": effective_date_1, 'Flag': 2})
                    r2.update({"OutDate": effective_date_1, 'Flag': 2})
                    r3.update({"OutDate": effective_date_1, 'Flag': 2})
                    r4 = {"TargetCategory": 2, 'InDate': effective_date_1, "OutDate": None, 'Flag': 1}

                    stats = {"date": effective_date_1, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                    self.assert_stats(stats, secu_code)
                    for r in (r1, r2, r3, r4):
                        r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                                  "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                        print(r)
                        self.insert(r)
                else:
                    print("1: add just 1")
                    r1 = {"TargetCategory": 1, 'InDate': effective_date, "OutDate": None, 'Flag': 1}
                    if _change_1 == self.stats_add_margin_and_shortsell:
                        print("2: add 3 4")
                        r2 = {"TargetCategory": 3, 'InDate': effective_date, "OutDate": None, 'Flag': 1}
                        r3 = {"TargetCategory": 4, 'InDate': effective_date, "OutDate": None, 'Flag': 1}
                        stats = {"date": effective_date_1, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                        self.assert_stats(stats, secu_code)
                        for r in (r1, r2, r3):
                            r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                                      "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                            print(r)
                            self.insert(r)
                    elif _change_1 == self.stats_transfer:
                        print("2: del 1 add 2 ")
                        r1.update({"OutDate": effective_date_1, 'Flag': 2})
                        r2 = {"TargetCategory": 2, 'InDate': effective_date_1, "OutDate": None, 'Flag': 1}
                        stats = {"date": effective_date_1, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                        self.assert_stats(stats, secu_code)
                        for r in (r1, r2):
                            r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                                      "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                            print(r)
                            self.insert(r)
                    elif _change_1 == self.stats_removal:
                        print("2: del 1")
                        r1.update({"OutDate": effective_date_1, 'Flag': 2})
                        stats = {"date": effective_date_1, "s1": 0, "s2": 0, "s3": 0, "s4": 0}
                        self.assert_stats(stats, secu_code)
                        r1.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                                  "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                        print(r1)
                        self.insert(r1)
                    elif _change_1 in self.stats_todonothing:
                        lst.append(code)
                        self.insert(r1)
                    else:
                        raise Exception
            elif _change == self.stats_transfer:
                raise Exception
            elif _change == self.stats_add_margin_and_shortsell:
                raise Exception
            else:
                raise Exception
        print(lst)

    def first_process(self):
        lst = []
        codes = self.select_spider_records_with_a_num(1)
        print(len(codes))  # 362
        for code in set(codes):
            print()
            print(code)
            spider_changes = self.show_code_spider_records(code)
            change = spider_changes[0]
            _change = change.get("Ch_ange")
            remarks = change.get("Remarks")
            effective_date = change.get("EffectiveDate")

            secu_code = change.get("SSESCode")
            inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
            ccass_code, face_value = self.get_ccas_code(secu_code)

            if _change == self.stats_addition:
                if self.sentense1 in remarks or self.sentense2 in remarks:
                    r1 = {"TargetCategory": 1, 'InDate': effective_date, "OutDate": None, 'Flag': 1}
                    r2 = {"TargetCategory": 3, 'InDate': effective_date, "OutDate": None, 'Flag': 1}
                    r3 = {"TargetCategory": 4, 'InDate': effective_date, "OutDate": None, 'Flag': 1}
                    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                    self.assert_stats(stats, secu_code)
                    for r in (r1, r2, r3):
                        r.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                                  "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                        print(r)
                        self.insert(r)
                else:
                    r1 = {"TargetCategory": 1, 'InDate': effective_date, "OutDate": None, 'Flag': 1}
                    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 0, "s4": 0}
                    try:
                        self.assert_stats(stats, secu_code)
                    except:
                        lst.append(code)
                    r1.update({"TradingType": 3, "SecuCode": secu_code, "InnerCode": inner_code,
                              "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value})
                    print(r1)
                    self.insert(r1)
            elif _change in self.stats_todonothing:
                print(_change, "\n", remarks)   # TODO
            else:
                raise

        print(lst)

    def process(self):
        self.sisth_process()

        self.fifth_process()

        self.fourth_process()

        self.third_process()

        self.second_process()

        self.first_process()
