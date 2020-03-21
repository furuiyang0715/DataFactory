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