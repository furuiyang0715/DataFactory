# 需要进行单独处理的
''' 感觉可能会是 某个更名的
mysql> select * from hkex_lgt_change_of_sse_securities_lists where  SSESCode = 601200\G
*************************** 1. row ***************************
           id: 4182985
EffectiveDate: 2017-12-11
     SSESCode: 601200
    StockName: SHANGHAI ENVIRONMENT GROUP
      Ch_ange: Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))
         Time: 2020-02-03
      Remarks: Change is resulted from the change in SSE 180 / 380 Index as announced on 27 November 2017. For details, please refer to http://www.sse.com.cn/market/sseindex/diclosure/c/c_20171127_4424760.shtml (Chinese Version Only).
       ItemID: ced04b8bcb2727def97703375cf4caaa
 CREATETIMEJZ: 2020-03-02 16:13:00
 UPDATETIMEJZ: 2020-03-09 15:41:20
*************************** 2. row ***************************
           id: 4183275
EffectiveDate: 2017-03-31
     SSESCode: 601200
    StockName: SHANGHAI ENVIRONMENT GROUP
      Ch_ange: Addition to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)
         Time: 2020-02-03
      Remarks: Change is resulted from a spin-off from an existing SSE-Listed China Connect Security. The announcement from the new-listed company is issued on the SSE website on 30 March 2017. For details, please refer to http://static.sse.com.cn/disclosure/listedinfo/announcement/c/2017-03-30/601200_20170330_1.pdf (Chinese Version Only).
       ItemID: bf90cc5848c97102afb7807e7614a3b2
 CREATETIMEJZ: 2020-03-02 16:13:04
 UPDATETIMEJZ: 2020-03-09 15:41:25
2 rows in set (0.01 sec)

'''


# （1） 之前的名称是 601313 现在的名称是 601360
# （2） 600368 （1）加入1 （2） 移出 1 （3） 恢复 1 （这时显示恢复 3 4 但是之前未加入和移出3 4）
#  (3) 600546  (1)  加入1 （2）加入 3 4 （3） 移出了1 （只移出 1 但是未移出 3 4 的情况是否合理）
# (4)  '600368', '600736', '600123', '600282', '600378', '603508', '600702' (1)加入1 （2）移出1 加入2 （3） 结束 2 将 1 恢复 （这时有了 3 4 但是未加入过 3 4 ）


# （1）600009 状态有 Buy orders suspended 和 Buy orders resumed


# 深
# （1）000422 最后一条状态未核对上
# (2) 002008  "000333" 状态有 Buy orders suspended 和 Buy orders resumed
# (3) "000022" 改名 001872 嗯
# (4) 000043 改名 001914
import pprint

from hkland_elistocks.sh_human_gene import SHHumanTools

'''     
        self.special_codes = {
            # second
            "601200",  # 已处理 

            # third
            '601313', # 改名为 "601360",
            "600546",
            '600368', '600736', '600123', '600282', '600378', '603508', '600702',
            
            # fourth 
            "600009",

        }
        
        self.special_codes = {
            # first
            '001914',   # 000043 --> 001914
            '001872',   # 000022 --> 001872
            # second
            "000333",
            "000022",
            # third
            "002008",
        }
'''
# select * from hkex_lgt_change_of_sse_securities_lists where  SSESCode = 601200\G
# select * from  LC_SHSCEliStocks where SecuCode = 601200\G

# select * from hkex_lgt_change_of_szse_securities_lists where  SSESCode = 601200\G


def process_601313():
    sh = SHHumanTools()
    spider_changes = sh.show_code_spider_records("601313")
    # print(pprint.pformat(spider_changes))
    change = spider_changes[0]
    _change = change.get("Ch_ange")
    remarks = change.get("Remarks")
    secu_code = change.get("SSESCode")
    inner_code, secu_abbr = sh.get_juyuan_inner_code(secu_code)
    print(inner_code)
    ccass_code, face_value = sh.get_ccas_code(secu_code)
    effective_date = change.get("EffectiveDate")
    # 加入 1
    record1 = {
        "TradingType": 1, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    change = spider_changes[1]
    # _change = change.get("Ch_ange")
    # remarks = change.get("Remarks")
    effective_date = change.get("EffectiveDate")

    # 结束 1 生成 2
    record1.update({"OutDate": effective_date, "Flag": 2})
    record2 = {
        "TradingType": 1, "TargetCategory": 2, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}

    stats = {"date": effective_date, "s1": 0, "s2": 0, "s3": 0, "s4": 0}
    sh.assert_stats(stats, secu_code)

    # 改名 改为 601360
    secu_code = "601360"
    inner_code, secu_abbr = sh.get_juyuan_inner_code(secu_code)
    print(inner_code)
    ccass_code, face_value = sh.get_ccas_code(secu_code)
    spider_changes = sh.show_code_spider_records("601360")
    # print(pprint.pformat(spider_changes))

    change = spider_changes[1]
    effective_date = change.get("EffectiveDate")
    # 恢复 1
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


def process_601200():
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

    change = spider_changes[1]
    # _change = change.get("Ch_ange")
    # remarks = change.get("Remarks")
    effective_date = change.get("EffectiveDate")
    # 结束 2 加入 1
    record1.update({"OutDate": effective_date, 'Flag': 2})
    record2 = {
        "TradingType": 1, "TargetCategory": 1, "SecuCode": secu_code, 'InDate': effective_date, "OutDate": None,
        'Flag': 1, "InnerCode": inner_code, "SecuAbbr": secu_abbr, 'CCASSCode': ccass_code,
        'ParValue': face_value}
    stats = {"date": effective_date, "s1": 1, "s2": 0, "s3": 0, "s4": 0}
    print(stats)
    sh.assert_stats(stats, secu_code)
    print(record1)
    print(record2)
    sh.insert(record1)
    sh.insert(record2)





# process_601200()
# process_601313()
