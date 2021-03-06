import pprint

from hkland_elistocks.common import CommonHumamTools
from hkland_elistocks.my_log import logger


class SHHumanTools(CommonHumamTools):
    def __init__(self):
        super(SHHumanTools, self).__init__()
        self.only_sell_list_table = 'hkex_lgt_special_sse_securities'
        self.buy_and_sell_list_table = 'hkex_lgt_sse_securities'
        self.buy_margin_trading_list_table = 'hkex_lgt_special_sse_securities_for_margin_trading'
        self.short_sell_list_table = 'hkex_lgt_special_sse_securities_for_short_selling'
        self.special_codes = {
            # second
            "601200",

            # third
            '601313', "601360",
            "600546",
            # '600368', '600736', '600123', '600282', '600378', '603508', '600702',
            
            # fourth 
            "600009",

        }

        self.table_name = 'hkland_hgelistocks'   # 沪港通合资格股
        self.change_table_name = 'hkex_lgt_change_of_sse_securities_lists'
        self.market = 83
        self.trade_type = 1
        self.tool_table_name = 'base_table_updatetime'

        self.stats_todonothing = [
            'SSE Stock Code and Stock Name are changed to 601360 and 360 SECURITY TECHNOLOGY respectively',
            'SSE Stock Code and Stock Name are changed from 601313 and SJEC respectively',
            'Buy orders suspended',
            'Buy orders resumed',
        ]
        self.stats_add_only_sell = 'Addition to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)'

        self.stats_transfer = 'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)'
        self.stats_recover = 'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))'

        self.stats_removal = 'Removal'
        self.stats_addition = "Addition"

        self.stats_add_margin_and_shortsell = "Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling"
        self.stats_remove_margin_and_shortsell = 'Remove from List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling'

        self.sentense1 = 'This stock will also be added to the List of Eligible SSE Securities for Margin Trading and the List of Eligible SSE Securities for Short Selling as it is also included in SSE stock list for margin trading and shortselling.'
        # self.sentense4 = 'This stock will also be added to the List of Eligible SSE Securities for Margin Trading and the List of Eligible SSE Securities for Short Selling as it is also included in SSE stock list for margin trading and shortselling.'
        self.sentense2 = 'Initial list of securities eligible for buy and sell'
        self.sentense3 = 'This stock will also be removed from the List of Eligible SSE Securities for Margin Trading and the List of Eligible SSE Securities for Short Selling.'

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
          UNIQUE KEY `IX_JZ_SHSCEliStocks` (`SecuCode`,`TradingType`,`TargetCategory`,`InDate`),
          UNIQUE KEY `IX_JZ_SHSCEliStocks_ID` (`ID`)
        ) ENGINE=InnoDB DEFAULT CHARSET=gbk COMMENT '沪港通合资格成分股变更表'; 
        '''.format(self.table_name)
        target = self.init_sql_pool(self.target_cfg)
        ret = target.insert(sql)
        target.dispose()

    def _first_process(self, change):
        """
        处理第一条的改变后得到的 记录 以及 当前对应的状态
        :param change:
        :return:
        """
        _change = change.get("Ch_ange")
        remarks = change.get("Remarks")

        secu_code = change.get("SSESCode")
        inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
        ccass_code, face_value = self.get_ccas_code(secu_code)
        effective_date = change.get("EffectiveDate")

        if _change == self.stats_addition:
            if self.sentense1 in remarks:    # 添加 1 3 4
                record1 = {
                    "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
                    'ParValue': face_value}
                record3 = {
                    "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
                    'ParValue': face_value}
                record4 = {
                    "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
                    'ParValue': face_value}
                records = [record1, record3, record4]
                stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
            else:    # 仅仅初始化 1
                record1 = {
                    "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
                    'ParValue': face_value}
                records = [record1]
                stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 0, "s4": 0}
            return records, stats

        elif _change == self.stats_add_margin_and_shortsell:
            record1 = {
                "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
                'ParValue': face_value}
            record2 = {
                "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
                'ParValue': face_value}
            stats = {"date": effective_date, "s1": 0, "s2": 0, "s3": 1, "s4": 1}
            return [record1, record2], stats

        elif _change == self.stats_add_only_sell:
            record1 = {
                "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                'CCASSCode': ccass_code, 'ParValue': face_value}
            stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
            return [record1], stats
        else:
            logger.warning("其他情况 请检查 ")
            logger.info(_change)
            logger.info(remarks)
            return None, None

    def first_process(self):
        appear_1_codes = self.select_spider_records_with_a_num(1)
        logger.info("SH-LEN-1 {}".format(len(appear_1_codes)))  # 128 全部是 addition 的
        for code in appear_1_codes:
            print()
            logger.info(code)
            spider_records = self.show_code_spider_records(code)
            assert len(spider_records) == 1
            spider_change = spider_records[0]
            logger.info("{} \n {}".format(spider_change.get("Ch_ange"), spider_change.get("Remarks")))
            records, stats = self._first_process(spider_change)
            logger.info(records)
            logger.info(stats)
            self.assert_stats(stats, code)
            # for record in records:
            #     self.insert(record)
            self.update_code_info(code, records)

    def _second_process(self, change, first_records: list, first_stats):
        """
        根据首次生成的记录以及首次之后的状态
        生成第二次处理后的总记录 以及 当前状态
        """
        _change = change.get("Ch_ange")
        remarks = change.get("Remarks")

        secu_code = change.get("SSESCode")
        inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
        ccass_code, face_value = self.get_ccas_code(secu_code)
        effective_date = change.get("EffectiveDate")

        if _change == self.stats_transfer:
            if first_stats.get("s1") == 1 and first_stats.get("s3") == 0:
                if self.sentense3 not in remarks:    # 仅仅将 1 移出 同时生成 2
                    second_record = first_records[0]
                    second_record.update({"OutDate": effective_date, 'Flag': 2})
                    second_stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                    record_new = {"TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code,
                                  'InDate': effective_date, "OutDate": None, 'Flag': 1, "InnerCode": inner_code,
                                  "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value}
                    return [second_record, record_new], second_stats
                else:   # 将 1 3 4 均移出 不合理的数据 因为第一步仅仅加进了 1
                    raise Exception
            elif first_stats.get("s1") == 1 and first_stats.get("s3") == 1:
                if self.sentense3 in remarks:  # 将 1 3 4 均移出 同时生成 2
                    for record in first_records:
                        record.update({"OutDate": effective_date, 'Flag': 2})
                    record_new = {"TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code,
                                  'InDate': effective_date, "OutDate": None, 'Flag': 1, "InnerCode": inner_code,
                                  "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code, 'ParValue': face_value}
                    first_records.append(record_new)
                    second_stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                    return [first_records, second_stats]
                else:
                    raise Exception

        elif _change == self.stats_add_margin_and_shortsell:   # 将 3 4 加入
            assert first_stats.get("s1") == 1 and first_stats.get("s3") == 0 and first_stats.get("s4") == 0
            # 第一次将 1 加入, 第二次 将 3、4 加入的情况
            record3 = {
                "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
                'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
                'ParValue': face_value}
            record4 = {
                "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
                'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
                'ParValue': face_value}
            first_records.extend([record3, record4])
            second_stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
            return first_records, second_stats
        else:
            return None, None

    def second_process(self):
        appear_2_codes = self.select_spider_records_with_a_num(2)
        logger.info("SH-LEN-2: {}".format(len(appear_2_codes)))  # 576
        appear_2_codes = set(appear_2_codes) - self.special_codes
        for code in appear_2_codes:
            print()
            logger.info(code)
            spider_changes = self.show_code_spider_records(code)
            spider_change = spider_changes[0]
            assert spider_change.get("Ch_ange") == self.stats_addition
            assert len(spider_changes) == 2
            first_records, first_stats = self._first_process(spider_changes[0])
            assert first_records
            assert first_stats
            second_records, second_stats = self._second_process(spider_changes[1], first_records, first_stats)
            self.assert_stats(second_stats, code)
            assert second_records
            assert second_stats
            logger.info(second_stats)
            for record in second_records:
                logger.info(record)
                # self.insert(record)
            self.update_code_info(code, second_records)

    def third_process(self):
        appear_3_codes = self.select_spider_records_with_a_num(3)
        logger.info("SH-LEN-3: {}".format(len(appear_3_codes)))  # 166
        appear_3_codes = set(appear_3_codes) - self.special_codes
        for code in appear_3_codes:
            print()
            logger.info(code)
            spider_changes = self.show_code_spider_records(code)
            assert len(spider_changes) == 3

            change = spider_changes[0]
            _change = change.get("Ch_ange")
            remarks = change.get("Remarks")
            effective_date = change.get("EffectiveDate")

            secu_code = change.get("SSESCode")
            inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
            ccass_code, face_value = self.get_ccas_code(secu_code)

            assert _change in self.stats_addition
            if self.sentense1 in remarks:   # 第一步加入 1 3 4   '601872', '600604', '600155'
                record1 = {
                    "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record2 = {
                    "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record3 = {
                    "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                change_1 = spider_changes[1]
                _change = change_1.get("Ch_ange")
                remarks = change_1.get("Remarks")
                effective_date = change_1.get("EffectiveDate")
                assert _change == self.stats_transfer and self.sentense3 in remarks
                # 第二步 移出 1 3 4 生成 2
                record1.update({"OutDate": effective_date, 'Flag': 2})
                record2.update({"OutDate": effective_date, 'Flag': 2})
                record3.update({"OutDate": effective_date, 'Flag': 2})
                record4 = {
                    "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                # 第三步  结束 2 生成 1 3 4
                change_2 = spider_changes[2]
                _change = change_2.get("Ch_ange")
                remarks = change_2.get("Remarks")
                effective_date = change_2.get("EffectiveDate")
                assert _change == self.stats_recover and self.sentense1 in remarks
                record4.update({"OutDate": effective_date, 'Flag': 2})
                record5 = {
                    "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record6 = {
                    "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record7 = {
                    "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                self.assert_stats(stats, secu_code)
                logger.info(stats)
                for r in (record1, record2, record3, record4, record5, record6, record7):
                    logger.info(r)

                self.update_code_info(secu_code, [record1, record2, record3, record4, record5, record6, record7])

            else:    # 第一步只加入 1
                record1 = {
                    "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                change_1 = spider_changes[1]
                _change = change_1.get("Ch_ange")
                remarks = change_1.get("Remarks")
                effective_date = change_1.get("EffectiveDate")
                if _change == self.stats_add_margin_and_shortsell:  # 加上  3 4
                    record2 = {
                        "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    record3 = {
                        "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    change_2 = spider_changes[2]
                    _change = change_2.get("Ch_ange")
                    remarks = change_2.get("Remarks")
                    effective_date = change_2.get("EffectiveDate")
                    assert _change in (self.stats_transfer, self.stats_removal)
                    if _change == self.stats_transfer:
                        if self.sentense3 in remarks:  # 移除 1 3 4 加上 2
                            for r in [record1, record2, record3]:
                                r.update({"OutDate": effective_date, 'Flag': 2})
                            record4 = {
                                "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                                "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                'CCASSCode': ccass_code, 'ParValue': face_value}
                            stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                            logger.info(stats)
                            self.assert_stats(stats, secu_code)
                            for r in [record1, record2, record3, record4]:
                                logger.info(r)
                            self.update_code_info(secu_code, [record1, record2, record3, record4])
                        else:     # 只移除 1 加上 2 这种情况是不合理的
                            raise Exception
                    else:  # Removal
                        record1.update({"OutDate": effective_date, 'Flag': 2})
                        record2.update({"OutDate": effective_date, 'Flag': 2})
                        record3.update({"OutDate": effective_date, 'Flag': 2})
                        stats = {"date": effective_date, "s1": 0, "s2": 0, "s3": 0, "s4": 0}
                        logger.info(stats)
                        self.assert_stats(stats, secu_code)
                        for r in [record1, record2, record3]:
                            logger.info(r)

                        self.update_code_info(secu_code, [record1, record2, record3])

                elif _change == self.stats_transfer:  # 移除
                    if self.sentense3 in remarks:   # 第一步未加入 1 就移除了 1
                        raise Exception

                    else:  # 移除 1  加上 2
                        record1.update({"OutDate": effective_date, 'Flag': 2})
                        record2 = {
                            "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        change_2 = spider_changes[2]
                        _change = change_2.get("Ch_ange")
                        remarks = change_2.get("Remarks")
                        effective_date = change_2.get("EffectiveDate")

                        if _change == self.stats_recover:
                            if self.sentense1 in remarks:
                                logger.info("结束 2, 需要恢复 1 3 4")
                                record2.update({"OutDate": effective_date, 'Flag': 2})
                                record3 = {
                                    "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code,
                                    'InDate': effective_date,
                                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                    'CCASSCode': ccass_code, 'ParValue': face_value}
                                record4 = {
                                    "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code,
                                    'InDate': effective_date,
                                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                    'CCASSCode': ccass_code, 'ParValue': face_value}
                                record5 = {
                                    "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code,
                                    'InDate': effective_date,
                                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                    'CCASSCode': ccass_code, 'ParValue': face_value}

                                stats = {'date': effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                                logger.info(stats)
                                self.assert_stats(stats, secu_code)
                                for r in [record1, record2, record3, record4, record5]:
                                    logger.info(r)
                                self.update_code_info(secu_code, [record1, record2, record3, record4, record5])

                            else:
                                logger.info("结束 2，仅仅恢复 1")
                                record2.update({"OutDate": effective_date, 'Flag': 2})
                                record3 = {
                                    "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code,
                                    'InDate': effective_date,
                                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                    'CCASSCode': ccass_code, 'ParValue': face_value}
                                stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 0, "s4": 0}
                                logger.info(stats)
                                self.assert_stats(stats, secu_code)
                                for r in [record1, record2, record3]:
                                    logger.info(r)
                                self.update_code_info(secu_code, [record1, record2, record3])

                        elif _change == self.stats_removal:  # 移除 2
                            record2.update({"OutDate": effective_date, 'Flag': 2})
                            stats = {"date": effective_date, "s1": 0, "s2": 0, "s3": 0, "s4": 0}
                            logger.info(stats)
                            self.assert_stats(stats, secu_code)
                            for r in [record1, record2]:
                                logger.info(r)
                            self.update_code_info(secu_code, [record1, record2])
                        else:
                            raise Exception("inner more choices ")
                else:
                    raise Exception("more choices")

    def fourth_process(self):
        appear_4_codes = self.select_spider_records_with_a_num(4)
        logger.info("SH-LEN-4:{}".format(len(appear_4_codes)))  # 52
        codes = set(appear_4_codes) - self.special_codes
        for code in codes:
            print()
            logger.info(code)
            spider_changes = self.show_code_spider_records(code)
            # print(pprint.pformat(spider_changes))
            assert len(spider_changes) == 4
            change = spider_changes[0]
            _change = change.get("Ch_ange")
            remarks = change.get("Remarks")
            effective_date = change.get("EffectiveDate")

            secu_code = change.get("SSESCode")
            inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
            ccass_code, face_value = self.get_ccas_code(secu_code)

            # print(_change, "\n", remarks)
            assert _change == self.stats_addition
            assert self.sentense1 not in remarks  # (1)--> 全部只加入 1
            record1 = {
                "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                'CCASSCode': ccass_code, 'ParValue': face_value}

            change_1 = spider_changes[1]
            _change = change_1.get("Ch_ange")
            remarks = change_1.get("Remarks")
            effective_date = change_1.get("EffectiveDate")

            # print(_change, "\n", remarks)
            if _change == self.stats_add_margin_and_shortsell:
                # (2) --> 将 3 4 添加上
                record2 = {
                    "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}
                record3 = {
                    "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                change_2 = spider_changes[2]
                _change = change_2.get("Ch_ange")
                remarks = change_2.get("Remarks")
                effective_date = change_2.get("EffectiveDate")

                # print(_change, "\n", remarks)
                if _change == self.stats_transfer:
                    assert self.sentense3 in remarks
                    # (3) ---> 将 1  3  4 全部移出, 生成 2
                    record1.update({"OutDate": effective_date, 'Flag': 2})
                    record2.update({"OutDate": effective_date, 'Flag': 2})
                    record3.update({"OutDate": effective_date, 'Flag': 2})
                    record4 = {
                        "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    change_3 = spider_changes[3]
                    _change = change_3.get("Ch_ange")
                    remarks = change_3.get("Remarks")
                    effective_date = change_3.get("EffectiveDate")
                    # print(_change, "\n", remarks)

                    if _change == self.stats_recover:
                        assert self.sentense1 in remarks
                        # (4) ---> 将 2 终止 将 1 3 4 加入
                        record4.update({"OutDate": effective_date, 'Flag': 2})
                        record5 = {
                            "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code,
                            'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        record6 = {
                            "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code,
                            'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        record7 = {
                            "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code,
                            'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                        logger.info(stats)
                        self.assert_stats(stats, secu_code)
                        for r in [record1, record2, record3, record4, record5, record6, record7]:
                            logger.info(r)
                        self.update_code_info(secu_code, [record1, record2, record3, record4, record5, record6, record7])

                    elif _change == self.stats_removal:
                        # (4) ---> 将最后的 2 也移除
                        record4.update({"OutDate": effective_date, 'Flag': 2})
                        stats = {"date": effective_date, "s1": 0, "s2": 0, "s3": 0, "s4": 0}
                        logger.info(stats)
                        self.assert_stats(stats, secu_code)
                        for r in [record1, record2, record3, record4]:
                            logger.info(r)
                        self.update_code_info(secu_code, [record1, record2, record3, record4])
                    else:
                        raise Exception("其他情况", _change)
                elif _change == self.stats_remove_margin_and_shortsell:
                    # （3） ---> 结束 3 4
                    record2.update({"OutDate": effective_date, 'Flag': 2})
                    record3.update({"OutDate": effective_date, 'Flag': 2})

                    change_3 = spider_changes[3]
                    _change = change_3.get("Ch_ange")
                    remarks = change_3.get("Remarks")
                    effective_date = change_3.get("EffectiveDate")
                    # print(_change, "\n", remarks)
                    assert _change == self.stats_removal
                    # (4) ---> 结束 1
                    record1.update({"OutDate": effective_date, 'Flag': 2})
                    stats = {"date": effective_date, "s1": 0, "s2": 0, "s3": 0, "s4": 0}
                    logger.info(stats)
                    self.assert_stats(stats, secu_code)
                    for r in [record1, record2, record3]:
                        logger.info(r)
                    self.update_code_info(secu_code, [record1, record2, record3])
                else:
                    raise Exception

            elif _change == self.stats_transfer:
                assert self.sentense3 not in remarks
                # (2) 将 1 移出, 生成 2
                record1.update({"OutDate": effective_date, 'Flag': 2})
                record2 = {
                    "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                change_2 = spider_changes[2]
                _change = change_2.get("Ch_ange")
                remarks = change_2.get("Remarks")
                effective_date = change_2.get("EffectiveDate")

                assert _change in self.stats_recover   # （1） 加入1 -- (2) 移除 1 生成 2 --（3） 恢复1 结束 2
                assert self.sentense1 not in remarks
                # 只加入 1  结束 2
                record2.update({"OutDate": effective_date, 'Flag': 2})
                record3 = {
                    "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                change_3 = spider_changes[3]
                _change = change_3.get("Ch_ange")
                remarks = change_3.get("Remarks")
                effective_date = change_3.get("EffectiveDate")
                # print(_change, "\n", remarks)

                if _change == self.stats_transfer:
                    assert self.sentense3 not in remarks
                    # （4） 将 1 移除  生成 2
                    record3.update({"OutDate": effective_date, 'Flag': 2})
                    record4 = {
                        "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                    logger.info(stats)
                    self.assert_stats(stats, secu_code)
                    for r in [record1, record2, record3, record4]:
                        logger.info(r)
                    self.update_code_info(secu_code, [record1, record2, record3, record4])
                elif _change == self.stats_add_margin_and_shortsell:
                    # （4） 加入 3 4
                    record4 = {
                        "TradingType": 1, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    record5 = {
                        "TradingType": 1, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                    logger.info(stats)
                    self.assert_stats(stats, secu_code)
                    for r in [record1, record2, record3, record4, record5]:
                        logger.info(r)
                    self.update_code_info(secu_code, [record1, record2, record3, record4, record5])
                else:
                    raise Exception("其他的 {}".format(_change))

            else:
                raise Exception("其他 {}".format(_change))

    def fifth_process(self):
        appear_5_codes = self.select_spider_records_with_a_num(5)
        logger.info("SH-LEN-5: {}".format(len(appear_5_codes)))  # 10
        for code in appear_5_codes:
            print()
            logger.info(code)
            spider_changes = self.show_code_spider_records(code)
            assert len(spider_changes) == 5
            change = spider_changes[0]
            _change = change.get("Ch_ange")
            remarks = change.get("Remarks")
            effective_date = change.get("EffectiveDate")

            secu_code = change.get("SSESCode")
            inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
            ccass_code, face_value = self.get_ccas_code(secu_code)

            if _change == self.stats_addition:
                assert self.sentense1 not in remarks
                record1 = {
                    "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                change_1 = spider_changes[1]
                _change = change_1.get("Ch_ange")
                remarks = change_1.get("Remarks")
                effective_date = change_1.get("EffectiveDate")

                if _change == self.stats_add_margin_and_shortsell:
                    # 将 3 4  也添加进来
                    record2 = {
                        "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}
                    record3 = {
                        "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    change_2 = spider_changes[2]
                    _change = change_2.get("Ch_ange")
                    remarks = change_2.get("Remarks")
                    effective_date = change_2.get("EffectiveDate")

                    if _change == self.stats_transfer:
                        assert self.sentense3 in remarks  # 将 1 3 4 全部移出 添加 2
                        record1.update({"OutDate": effective_date, 'Flag': 2})
                        record2.update({"OutDate": effective_date, 'Flag': 2})
                        record3.update({"OutDate": effective_date, 'Flag': 2})
                        record4 = {
                            "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        change_3 = spider_changes[3]
                        _change = change_3.get("Ch_ange")
                        remarks = change_3.get("Remarks")
                        effective_date = change_3.get("EffectiveDate")

                        if _change == self.stats_recover:
                            # 将 1 3 4 全部恢复, 2 结束
                            if self.sentense1 in remarks:
                                record5 = {
                                    "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code,
                                    'InDate': effective_date,
                                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                    'CCASSCode': ccass_code, 'ParValue': face_value}
                                record6 = {
                                    "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code,
                                    'InDate': effective_date,
                                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                    'CCASSCode': ccass_code, 'ParValue': face_value}
                                record7 = {
                                    "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code,
                                    'InDate': effective_date,
                                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                    'CCASSCode': ccass_code, 'ParValue': face_value}
                                record4.update({"OutDate": effective_date, 'Flag': 2})

                                change_4 = spider_changes[4]
                                _change = change_4.get("Ch_ange")
                                remarks = change_4.get("Remarks")
                                effective_date = change_4.get("EffectiveDate")
                                if _change == self.stats_transfer:
                                    assert self.sentense3 in remarks
                                    record5.update({"OutDate": effective_date, 'Flag': 2})
                                    record6.update({"OutDate": effective_date, 'Flag': 2})
                                    record7.update({"OutDate": effective_date, 'Flag': 2})
                                    record8 = {
                                        "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code,
                                        'InDate': effective_date,
                                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                        'CCASSCode': ccass_code, 'ParValue': face_value}
                                    stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                                    logger.info(stats)
                                    self.assert_stats(stats, secu_code)
                                    for r in [record1, record2, record3, record4, record5, record6, record7, record8]:
                                        logger.info(r)
                                    self.update_code_info(secu_code, [record1, record2, record3, record4, record5, record6, record7, record8])
                            else:   # 将 1 恢复 2 结束 3 4 不变 # 600675 601005 603188
                                record5 = {
                                    "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code,
                                    'InDate': effective_date,
                                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                    'CCASSCode': ccass_code, 'ParValue': face_value}
                                record4.update({"OutDate": effective_date, 'Flag': 2})

                                change_4 = spider_changes[4]
                                _change = change_4.get("Ch_ange")
                                remarks = change_4.get("Remarks")
                                effective_date = change_4.get("EffectiveDate")

                                # 结束 1  生成 2   # 603188
                                if _change == self.stats_transfer:
                                    record5.update({"OutDate": effective_date, 'Flag': 2})
                                    record6 = {
                                        "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code,
                                        'InDate': effective_date,
                                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                        'CCASSCode': ccass_code, 'ParValue': face_value}
                                    stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                                    logger.info(stats)
                                    self.assert_stats(stats, secu_code)
                                    for r in (record1, record2, record3, record4, record5, record6):
                                        logger.info(r)
                                    self.update_code_info(secu_code, [record1, record2, record3, record4, record5, record6])

                                elif _change == self.stats_add_margin_and_shortsell:    # 添加 3 4
                                    record6 = {
                                        "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code,
                                        'InDate': effective_date,
                                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                        'CCASSCode': ccass_code, 'ParValue': face_value}
                                    record7 = {
                                        "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code,
                                        'InDate': effective_date,
                                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                        'CCASSCode': ccass_code, 'ParValue': face_value}
                                    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                                    logger.info(stats)
                                    self.assert_stats(stats, secu_code)
                                    for r in [record1, record2, record3, record4, record5, record6, record7]:
                                        logger.info(r)
                                    self.update_code_info(secu_code, [record1, record2, record3, record4, record5, record6, record7])
                                else:
                                    raise Exception("5 more choice")
                        else:
                            raise Exception("4 more choice")

                    elif _change == self.stats_remove_margin_and_shortsell:  # 600005   # 将 3 4 移除
                        record2.update({"OutDate": effective_date, 'Flag': 2})
                        record3.update({"OutDate": effective_date, 'Flag': 2})

                        change_3 = spider_changes[3]
                        _change = change_3.get("Ch_ange")
                        remarks = change_3.get("Remarks")
                        effective_date = change_3.get("EffectiveDate")

                        assert _change == self.stats_transfer    # 将 1 移除 生成 2
                        record1.update({"OutDate": effective_date, 'Flag': 2})
                        record4 = {
                            "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        change_4 = spider_changes[4]
                        _change = change_4.get("Ch_ange")
                        remarks = change_4.get("Remarks")
                        effective_date = change_4.get("EffectiveDate")

                        assert _change == self.stats_removal     # 将 2 移除
                        record4.update({"OutDate": effective_date, 'Flag': 2})
                        stats = {"date": effective_date, "s1": 0, "s2": 0, "s3": 0, "s4": 0}
                        logger.info(stats)
                        self.assert_stats(stats, secu_code)
                        for r in [record1, record2, record3, record4]:
                            logger.info(r)
                        self.update_code_info(secu_code, [record1, record2, record3, record4])
                    else:
                        raise Exception("3 more choice")
                elif _change == self.stats_transfer:
                    record1.update({"OutDate": effective_date, 'Flag': 2})
                    record2 = {
                        "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    change_2 = spider_changes[2]
                    _change = change_2.get("Ch_ange")
                    remarks = change_2.get("Remarks")
                    effective_date = change_2.get("EffectiveDate")

                    assert _change == self.stats_recover
                    if self.sentense1 in remarks:  # 将 1 3 4  全部加入  结束 2
                        record2.update({"OutDate": effective_date, 'Flag': 2})
                        record3 = {
                            "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code,
                            'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        record4 = {
                            "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code,
                            'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        record5 = {
                            "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code,
                            'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        change_3 = spider_changes[3]
                        _change = change_3.get("Ch_ange")
                        remarks = change_3.get("Remarks")
                        effective_date = change_3.get("EffectiveDate")

                        # print(_change, "\n", remarks)
                        assert _change == self.stats_transfer
                        assert self.sentense3 in remarks
                        # 将 1 3 4 全部移出 生成 2
                        record3.update({"OutDate": effective_date, 'Flag': 2})
                        record4.update({"OutDate": effective_date, 'Flag': 2})
                        record5.update({"OutDate": effective_date, 'Flag': 2})
                        record6 = {
                            "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        change_4 = spider_changes[4]
                        _change = change_4.get("Ch_ange")
                        remarks = change_4.get("Remarks")
                        effective_date = change_4.get("EffectiveDate")
                        # print(_change, "\n", remarks)
                        assert _change == self.stats_recover
                        assert self.sentense1 in remarks
                        # 恢复     1 3 4  结束 2
                        record6.update({"OutDate": effective_date, 'Flag': 2})
                        record7 = {
                            "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code,
                            'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        record8 = {
                            "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code,
                            'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        record9 = {
                            "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code,
                            'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
                        logger.info(stats)
                        self.assert_stats(stats, secu_code)

                        for r in [record1, record2, record3, record4, record5, record6, record7, record8, record9]:
                            logger.info(r)
                        self.update_code_info(secu_code, [record1, record2, record3, record4, record5, record6, record7, record8, record9])
                    else:
                        # 仅将 1 加入 结束 2
                        record2.update({"OutDate": effective_date, 'Flag': 2})
                        record3 = {
                            "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code,
                            'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        change_3 = spider_changes[3]
                        _change = change_3.get("Ch_ange")
                        remarks = change_3.get("Remarks")
                        effective_date = change_3.get("EffectiveDate")

                        # print(_change, "\n", remarks)
                        assert _change == self.stats_add_margin_and_shortsell
                        # 将 3 4 加入
                        record4 = {
                            "TradingType": self.trade_type, "TargetCategory": 3, "SecuCode": secu_code,
                            'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}
                        record5 = {
                            "TradingType": self.trade_type, "TargetCategory": 4, "SecuCode": secu_code,
                            'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        change_4 = spider_changes[4]
                        _change = change_4.get("Ch_ange")
                        remarks = change_4.get("Remarks")
                        effective_date = change_4.get("EffectiveDate")
                        # print(_change, "\n", remarks)

                        assert _change == self.stats_transfer
                        assert self.sentense3 in remarks
                        # 将 1 3 4 全部移出 生成 2
                        record3.update({"OutDate": effective_date, 'Flag': 2})
                        record4.update({"OutDate": effective_date, 'Flag': 2})
                        record5.update({"OutDate": effective_date, 'Flag': 2})
                        record6 = {
                            "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
                        logger.info(stats)
                        self.assert_stats(stats, secu_code)
                        for r in [record1, record2, record3, record4, record5, record6]:
                            logger.info(r)
                        self.update_code_info(secu_code, [record1, record2, record3, record4, record5, record6])
                else:
                    raise Exception("2 more choices")
            else:
                raise Exception("first no addition")

    def sixth_process(self):
        appear_6_codes = self.select_spider_records_with_a_num(6)
        logger.info("SH-LEN-6: {}".format(len(appear_6_codes)))   # 1
        code = appear_6_codes[0]
        spider_changes = self.show_code_spider_records(code)
        for c in spider_changes:
            print(c)

        print()
        change = spider_changes[0]

        _change = change.get("Ch_ange")
        remarks = change.get("Remarks")
        secu_code = change.get("SSESCode")
        inner_code, secu_abbr = self.get_juyuan_inner_code(secu_code)
        ccass_code, face_value = self.get_ccas_code(secu_code)
        effective_date = change.get("EffectiveDate")
        if _change == self.stats_addition:
            record1 = {
                "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                'CCASSCode': ccass_code, 'ParValue': face_value}

            change_1 = spider_changes[1]
            _change = change_1.get("Ch_ange")
            remarks = change_1.get("Remarks")
            effective_date = change_1.get("EffectiveDate")
            if _change == self.stats_transfer:
                # 将 1 移出
                record1.update({"OutDate": effective_date, 'Flag': 2})
                record2 = {
                    "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                    'CCASSCode': ccass_code, 'ParValue': face_value}

                change_2 = spider_changes[2]
                # print(change_2)
                _change = change_2.get("Ch_ange")
                remarks = change_2.get("Remarks")
                effective_date = change_2.get("EffectiveDate")
                if _change == self.stats_recover:
                    # print(">>> ", remarks)
                    # 将 1 恢复
                    record2.update({"OutDate": effective_date, "Flag": 2})
                    record3 = {
                        "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                        'CCASSCode': ccass_code, 'ParValue': face_value}

                    change_3 = spider_changes[3]
                    # print(change_3)
                    _change = change_3.get("Ch_ange")
                    remarks = change_3.get("Remarks")
                    effective_date = change_3.get("EffectiveDate")
                    if _change == self.stats_transfer:
                        # print(remarks)
                        # 将 1 移出
                        record3.update({"OutDate": effective_date, "Flag": 2})
                        record4 = {
                            "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date,
                            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                            'CCASSCode': ccass_code, 'ParValue': face_value}

                        change_4 = spider_changes[4]
                        _change = change_4.get("Ch_ange")
                        remarks = change_4.get("Remarks")
                        effective_date = change_4.get("EffectiveDate")
                        if _change == self.stats_recover:
                            record4.update({"OutDate": effective_date, "Flag": 2})
                            record5 = {
                                "TradingType": self.trade_type, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
                                "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                'CCASSCode': ccass_code, 'ParValue': face_value}

                            change_5 = spider_changes[5]
                            _change = change_5.get("Ch_ange")
                            remarks = change_5.get("Remarks")
                            effective_date = change_5.get("EffectiveDate")
                            if _change == self.stats_transfer:
                                record5.update({"OutDate": effective_date, 'Flag': 2})
                                record6 = {
                                    "TradingType": self.trade_type, "TargetCategory": 2, "SecuCode": secu_code,
                                    'InDate': effective_date,
                                    "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr,
                                    'CCASSCode': ccass_code, 'ParValue': face_value}
                                self.assert_stats({"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0},
                                                  secu_code)
                                for record in [record1, record2, record3, record4, record5, record6]:
                                    logger.info(record)
                                self.update_code_info(secu_code, [record1, record2, record3, record4, record5, record6])

    def refresh_update_time(self):
        target = self.init_sql_pool(self.target_cfg)
        sql = '''select max(UPDATETIMEJZ) as max_dt from {}; '''.format(self.table_name)
        max_dt = target.select_one(sql).get("max_dt")
        logger.info("最新的更新时间是{}".format(max_dt))
        refresh_sql = '''replace into {} (id,TableName, LastUpdateTime,IsValid) values (3, 'hkland_hgelistocks', '{}', 1); 
        '''.format(self.tool_table_name,
                   # self.target_table_name,
                   max_dt)
        logger.info(refresh_sql)
        count = target.update(refresh_sql)
        logger.info(count)  # 1 首次插入 2 替换插入
        target.dispose()

    def _process(self):
        self.first_process()

        self.second_process()

        self.third_process()

        self.fourth_process()

        self.fifth_process()

        self.sixth_process()

        pass
