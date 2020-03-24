import pprint

from hkland_elistocks.sh_human_gene import SHHumanTools
from hkland_elistocks.zh_human_gene import ZHHumanTools

'''     
        self.special_codes = {
            # second
            "601200",  # 首次是加入 2  已处理 

            # third
            '601313',   # 改名为 "601360", 已处理 
            "600546",   # 移除 1 的时候, 即使不明说 也要移除 3 4, 已处理 
            '600368', '600736', '600123', '600282', '600378', '603508', '600702',  # 没啥逻辑硬伤不知道为啥单独拿出来了 已处理 
            
            # fourth 
            "600009",  # 已处理 含有状态 5 

        }
        
        self.special_codes = {
            # first
            '001914',   # 000043 --> 001914  已处理 
            '001872',   # 000022 --> 001872  已处理 
            # second
            "000333",   # 已处理 含有状态 5 
            # third
            "002008",   # 已处理 
        }
'''
# select * from hkex_lgt_change_of_sse_securities_lists where  SSESCode = 601200 and Time = '2020-03-18' order by EffectiveDate\G
# select * from  LC_SHSCEliStocks where SecuCode = 601200\G

# select * from hkex_lgt_change_of_szse_securities_lists where  SSESCode = 000333 and Time = '2020-03-18' order by EffectiveDate \G
# select * from  LC_ZHSCEliStocks where SecuCode = 000333\G


def process_601200():
    # 首次是加入 2
    sh = SHHumanTools()
    spider_changes = sh.show_code_spider_records("601200")
    print(pprint.pformat(spider_changes))
    change = spider_changes[0]
    _change = change.get("Ch_ange")
    remarks = change.get("Remarks")
    secu_code = change.get("SSESCode")
    inner_code, secu_abbr = sh.get_juyuan_inner_code(secu_code)
    ccass_code, face_value = sh.get_ccas_code(secu_code)
    effective_date = change.get("EffectiveDate")
    # 加入 2
    record1 = {
        "TradingType": 1, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # 结束 2 加入 1
    change = spider_changes[1]
    effective_date = change.get("EffectiveDate")
    record1.update({"OutDate": effective_date, 'Flag': 2})
    record2 = {
        "TradingType": 1, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 0, "s4": 0}
    sh.assert_stats(stats, secu_code)
    for r in (record1, record2):
        print(r)
        sh.insert(r)


def process_601313():
    sh = SHHumanTools()
    spider_changes = sh.show_code_spider_records("601313")
    print(pprint.pformat(spider_changes))
    change = spider_changes[0]
    _change = change.get("Ch_ange")
    remarks = change.get("Remarks")
    secu_code = change.get("SSESCode")
    inner_code, secu_abbr = sh.get_juyuan_inner_code(secu_code)
    ccass_code, face_value = sh.get_ccas_code(secu_code)
    effective_date = change.get("EffectiveDate")
    # 加入 1
    record1 = {
        "TradingType": 1, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # 结束 1 生成 2
    change = spider_changes[1]
    effective_date = change.get("EffectiveDate")
    record1.update({"OutDate": effective_date, "Flag": 2})
    record2 = {
        "TradingType": 1, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
    # sh.assert_stats(stats, secu_code)

    # 改名为 601360
    # 改名之后, Flag 的状态始终置为 2
    record2.update({'Flag': 2})
    secu_code = "601360"
    inner_code, secu_abbr = sh.get_juyuan_inner_code(secu_code)
    ccass_code, face_value = sh.get_ccas_code(secu_code)
    spider_changes = sh.show_code_spider_records("601360")
    # print(pprint.pformat(spider_changes))

    change = spider_changes[1]
    effective_date = change.get("EffectiveDate")
    # 恢复 1 结束 2
    record3 = {
        "TradingType": 1, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record2.update({"OutDate": effective_date, "Flag": 2})

    # 加上 3 4
    change = spider_changes[2]
    effective_date = change.get("EffectiveDate")
    record4 = {
        "TradingType": 1, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record5 = {
        "TradingType": 1, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    for r in [record1, record2, record3, record4, record5]:
        r.update({"InnerCode": inner_code, "SecuAbbr": secu_abbr})
        print(r)
        sh.insert(r)

    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
    sh.assert_stats(stats, secu_code)


def process_600546():
    sh = SHHumanTools()
    spider_changes = sh.show_code_spider_records("600546")
    print(pprint.pformat(spider_changes))
    change = spider_changes[0]
    _change = change.get("Ch_ange")
    remarks = change.get("Remarks")
    secu_code = change.get("SSESCode")
    inner_code, secu_abbr = sh.get_juyuan_inner_code(secu_code)
    ccass_code, face_value = sh.get_ccas_code(secu_code)
    effective_date = change.get("EffectiveDate")
    # 加1
    record1 = {
        "TradingType": 1, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    # 加3 4
    change = spider_changes[1]
    effective_date = change.get("EffectiveDate")
    record2 = {
        "TradingType": 1, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record3 = {
        "TradingType": 1, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    # 移除 1 生成 2 (这里也要移除 3 4 ）
    change = spider_changes[2]
    effective_date = change.get("EffectiveDate")
    record1.update({"OutDate": effective_date, "Flag": 2})
    record2.update({"OutDate": effective_date, "Flag": 2})
    record3.update({"OutDate": effective_date, "Flag": 2})
    record4 = {
        "TradingType": 1, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    stats = {"date": effective_date, "s1": 0, "s2": 1, "s3": 0, "s4": 0}
    sh.assert_stats(stats, secu_code)
    for r in (record1, record2, record3, record4):
        print(r)
        sh.insert(r)


def process_600368():
    sh = SHHumanTools()
    for code in ('600368', '600736', '600123', '600282', '600378', '603508', '600702'):
        spider_changes = sh.show_code_spider_records("600368")
        print("\n", code)
        print(pprint.pformat(spider_changes))
        change = spider_changes[0]
        _change = change.get("Ch_ange")
        remarks = change.get("Remarks")
        secu_code = change.get("SSESCode")
        inner_code, secu_abbr = sh.get_juyuan_inner_code(secu_code)
        ccass_code, face_value = sh.get_ccas_code(secu_code)
        effective_date = change.get("EffectiveDate")
        # 加1
        record1 = {
            "TradingType": 1, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
            'ParValue': face_value}

        # 移除 1 生成 2
        change = spider_changes[1]
        effective_date = change.get("EffectiveDate")
        record1.update({"OutDate": effective_date, "Flag": 2})
        record2 = {
            "TradingType": 1, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
            'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
            'ParValue': face_value}

        # 加入 1 3 4 结束 2
        change = spider_changes[2]
        effective_date = change.get("EffectiveDate")
        record2.update({"OutDate": effective_date, "Flag": 2})
        record3 = {
            "TradingType": 1, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
            "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
            'ParValue': face_value}
        record4 = {
            "TradingType": 1, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
            'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
            'ParValue': face_value}
        record5 = {
            "TradingType": 1, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
            'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
            'ParValue': face_value}

        stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
        sh.assert_stats(stats, secu_code)
        for r in (record1, record2, record3, record4, record5):
            print(r)
            sh.insert(r)


def process_600009():
    sh = SHHumanTools()
    spider_changes = sh.show_code_spider_records("600009")
    print(pprint.pformat(spider_changes))
    change = spider_changes[0]
    _change = change.get("Ch_ange")
    remarks = change.get("Remarks")
    secu_code = change.get("SSESCode")
    inner_code, secu_abbr = sh.get_juyuan_inner_code(secu_code)
    ccass_code, face_value = sh.get_ccas_code(secu_code)
    effective_date = change.get("EffectiveDate")
    # 加1
    record1 = {
        "TradingType": 1, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    # 加 3 4
    change = spider_changes[1]
    effective_date = change.get("EffectiveDate")
    record2 = {
        "TradingType": 1, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record3 = {
        "TradingType": 1, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # 'Ch_ange': 'Buy orders suspended',   5-触发持股比例限制暂停买入
    change = spider_changes[2]
    effective_date = change.get("EffectiveDate")
    record4 = {
        "TradingType": 1, "TargetCategory": 5, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # {'Ch_ange': 'Buy orders resumed'
    change = spider_changes[3]
    effective_date = change.get("EffectiveDate")
    record4.update({"OutDate": effective_date, "Flag": 2})

    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
    sh.assert_stats(stats, secu_code)
    for r in (record1, record2, record3, record4):
        print(r)
        sh.insert(r)


def run_000333():
    zh = ZHHumanTools()
    spider_changes = zh.show_code_spider_records("000333")
    print(pprint.pformat(spider_changes))
    change = spider_changes[0]
    _change = change.get("Ch_ange")
    remarks = change.get("Remarks")
    secu_code = change.get("SSESCode")
    inner_code, secu_abbr = zh.get_juyuan_inner_code(secu_code)
    ccass_code, face_value = zh.get_ccas_code(secu_code)
    effective_date = change.get("EffectiveDate")

    # 加 1 3 4
    record1 = {
        "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record2 = {
        "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record3 = {
        "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # 'Buy orders suspended',
    change = spider_changes[1]
    _change = change.get("Ch_ange")
    effective_date = change.get("EffectiveDate")
    record4 = {
        "TradingType": 3, "TargetCategory": 5, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # 'Buy orders resumed',
    change = spider_changes[2]
    _change = change.get("Ch_ange")
    effective_date = change.get("EffectiveDate")
    record4.update({"OutDate": effective_date, "Flag": 2})

    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
    zh.assert_stats(stats, secu_code)
    for r in (record1, record2, record3, record4):
        print(r)
        zh.insert(r)


def run_000022():
    zh = ZHHumanTools()
    spider_changes = zh.show_code_spider_records("000022")
    print(pprint.pformat(spider_changes))
    change = spider_changes[0]
    _change = change.get("Ch_ange")
    remarks = change.get("Remarks")
    secu_code = change.get("SSESCode")
    inner_code, secu_abbr = zh.get_juyuan_inner_code(secu_code)
    ccass_code, face_value = zh.get_ccas_code(secu_code)
    effective_date = change.get("EffectiveDate")
    # (1) 加 1 3 4
    record1 = {
        "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record2 = {
        "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record3 = {
        "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # (2) 改名 SZSE Stock Code and Stock Name are changed to 1872
    spider_changes = zh.show_code_spider_records("001872")
    change = spider_changes[0]
    secu_code = change.get("SSESCode")
    inner_code, secu_abbr = zh.get_juyuan_inner_code(secu_code)
    effective_date = change.get("EffectiveDate")
    # 将之前代码的状态转移后结束
    record1.update({"OutDate": effective_date, "Flag": 2})
    record2.update({"OutDate": effective_date, "Flag": 2})
    record3.update({"OutDate": effective_date, "Flag": 2})
    # 转移后生成的新状态
    record_1 = {
        "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record_2 = {
        "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record_3 = {
        "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
    zh.assert_stats(stats, secu_code)
    for r in (record1, record2, record3):
        # 内部编码更新为聚源最新的
        r.update({"InnerCode": inner_code, "SecuAbbr": secu_abbr})
        print(r)
        zh.insert(r)

    for r in (record_1, record_2, record_3):
        print(r)
        zh.insert(r)


def run_002008():
    zh = ZHHumanTools()
    spider_changes = zh.show_code_spider_records("002008")
    print(pprint.pformat(spider_changes))
    change = spider_changes[0]
    _change = change.get("Ch_ange")
    remarks = change.get("Remarks")
    secu_code = change.get("SSESCode")
    inner_code, secu_abbr = zh.get_juyuan_inner_code(secu_code)
    ccass_code, face_value = zh.get_ccas_code(secu_code)
    effective_date = change.get("EffectiveDate")
    # 加 1 3 4
    record1 = {
        "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record2 = {
        "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record3 = {
        "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    change = spider_changes[1]
    _change = change.get("Ch_ange")
    effective_date = change.get("EffectiveDate")

    record4 = {
        "TradingType": 3, "TargetCategory": 5, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    change = spider_changes[2]
    _change = change.get("Ch_ange")
    effective_date = change.get("EffectiveDate")
    record4.update({ "OutDate": effective_date, "Flag": 2})

    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 1, "s4": 1}
    zh.assert_stats(stats, secu_code)
    for r in (record1, record2, record3, record4):
        print(r)
        zh.insert(r)


def run_000043():
    zh = ZHHumanTools()
    spider_changes = zh.show_code_spider_records("000043")
    print(pprint.pformat(spider_changes))
    change = spider_changes[0]
    _change = change.get("Ch_ange")
    remarks = change.get("Remarks")
    secu_code = change.get("SSESCode")
    inner_code, secu_abbr = zh.get_juyuan_inner_code(secu_code)
    ccass_code, face_value = zh.get_ccas_code(secu_code)
    effective_date = change.get("EffectiveDate")
    # 加 1 3 4
    record1 = {
        "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record2 = {
        "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record3 = {
        "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # 移出 1 3 4 生成 2
    change = spider_changes[1]
    effective_date = change.get("EffectiveDate")
    record1.update({"OutDate": effective_date, "Flag": 2})
    record2.update({"OutDate": effective_date, "Flag": 2})
    record3.update({"OutDate": effective_date, "Flag": 2})
    record4 = {
        "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # 恢复 1 3 4 结束 2
    change = spider_changes[2]
    effective_date = change.get("EffectiveDate")
    record4.update({"OutDate": effective_date, "Flag": 2})
    record5 = {
        "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record6 = {
        "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record7 = {
        "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # 移出 1 3 4 生成 2
    change = spider_changes[3]
    effective_date = change.get("EffectiveDate")
    record5.update({"OutDate": effective_date, "Flag": 2})
    record6.update({"OutDate": effective_date, "Flag": 2})
    record7.update({"OutDate": effective_date, "Flag": 2})
    record8 = {
        "TradingType": 3, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # 恢复 1 3 4 结束 2
    change = spider_changes[4]
    effective_date = change.get("EffectiveDate")
    record8.update({"OutDate": effective_date, "Flag": 2})
    record9 = {
        "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record10 = {
        "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record11 = {
        "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    # SZSE Stock Code and Stock Name are changed to 001914
    # 同一天 先恢复再更名
    change = spider_changes[5]
    effective_date = change.get("EffectiveDate")
    secu_code = '001914'
    inner_code, secu_abbr = zh.get_juyuan_inner_code(secu_code)
    record_1 = {
        "TradingType": 3, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date,
        "OutDate": None, 'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record_2 = {
        "TradingType": 3, "TargetCategory": 3, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    record_3 = {
        "TradingType": 3, "TargetCategory": 4, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    stats = {"date": effective_date, 's1': 1, "s2": 0, "s3": 1, "s4": 1}
    zh.assert_stats(stats, secu_code)
    for r in (record_1, record_2, record_3):
        print(r)
        zh.insert(r)

    # 将最后 9 10 11 的状态更新为 2 因为将其状态转义给了新的编码 001914 就将其上一个状态结束
    record9.update({'Flag': 2, "OutDate": effective_date})
    record10.update({'Flag': 2, "OutDate": effective_date})
    record11.update({'Flag': 2, "OutDate": effective_date})

    for r in (record1, record2, record3, record4, record5, record6, record7, record8, record9, record10, record11):
        # 将之前的聚源内部编码以及简称进行更新 因为现在的可能找不到了
        r.update({"InnerCode": inner_code, "SecuAbbr": secu_abbr})
        print(r)
        zh.insert(r)


def fix():
    # process_600368()
    # process_600546()
    # process_601313()
    # process_601200()
    # process_600009()

    # run_002008()
    # run_000333()
    # run_000022()
    # run_000043()

    pass


fix()