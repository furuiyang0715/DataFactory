## 沪港通成分表
### 对标聚源表   LC_SHSCComponent
### 聚源表详情链接  https://dd.gildata.com/#/tableShow/124/column/// 
### 爬虫数据源： 
（1） 沪股通成分： 
上交所证券/中华通证券清单： https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities_xls/SSE_Securities.xls?la=en
更改上交所证券/中华通证券名单： https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities_xls/Change_of_SSE_Securities_Lists.xls?la=en
(2) 港股通(沪)
港股通股票名单: http://www.sse.com.cn/services/hkexsc/disclo/eligible/ 
港股通股票调整信息: http://www.sse.com.cn/services/hkexsc/disclo/eligiblead/

## 深港通成分股
### 对标聚源表： LC_ZHSCComponent
### 聚源表详情链接： https://dd.gildata.com/#/tableShow/1067/column/// 
### 爬虫数据源： 
（1） 深股通成分： 
深交所证券/中华通证券清单（可同时买卖的股票): https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities_xls/SZSE_Securities.xls?la=en
更改深交所证券/中华通证券名单: https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities_xls/Change_of_SZSE_Securities_Lists.xls?la=en
(2) 港股通(深)
港股通标的证券名单： http://www.szse.cn/szhk/hkbussiness/underlylist/index.html 
港股通标的证券调整： http://www.szse.cn/szhk/hkbussiness/underlyadjust/index.html 



### 逻辑 
沪股通的几种更新逻辑： 
mysql> select distinct(Ch_ange) from hkex_lgt_change_of_sse_securities_lists;
+----------------------------------------------------------------------------------------------------------------------+
| Ch_ange                                                                                                              |
+----------------------------------------------------------------------------------------------------------------------+
| Removal                                                                                                              |
| Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling |
| Addition                                                                                                             |
| Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))      |
| Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)          |
| Remove from List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling |
| SSE Stock Code and Stock Name are changed from 601313 and SJEC respectively                                          |
| SSE Stock Code and Stock Name are changed to 601360 and 360 SECURITY TECHNOLOGY respectively                         |
| Addition to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)          |
| Buy orders resumed                                                                                                   |
| Buy orders suspended                                                                                                 |
+----------------------------------------------------------------------------------------------------------------------+
11 rows in set (0.01 sec)

英文	    中文简义
"c1" =  Removal	    从成分股剔除
"c9" = Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)	移动到只能卖出名单
"c2" = Addition	加入到成分股
"c3" = Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))	从移除名单中恢复

"c6" = Addition to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)	【加入(特殊上证所证券/特殊中华通证券清单（仅适合出售的股票）)】

"c7" =  SSE Stock Code and Stock Name are changed from 601313 and SJEC respectively	上交所股票代码和股票名称分别从601313和SJEC更改
"c8" =  SSE Stock Code and Stock Name are changed to 601360 and 360 SECURITY TECHNOLOGY respectively	上交所股票代码和股票名称分别改为601360和360安全技术
"c4" = Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling	加入到 可进行保证金交易 和 可进行担保卖空 成分股
"c5" =  Remove from List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling	从可进行保证金交易及可进行担保卖空的名单中剔除
"c10" =  Buy orders suspended	暂停买入资格
"c11" =  Buy orders resumed	    恢复买入资格


## c2, c4 是不被剔除的 
## c1, c9 是被剔除的 










根据骆博士的意见， 成分股是可以同时间进行买卖的, 所以将其添加进入的情况是 (Addition)

将其从成分股剔除的情况是(
(3) 
(5)
(10)
(11)
)


# 测试 
## 说明 (10) (11) 未计入记录 
## 说明 (4) (5) 不计入记录 
select EffectiveDate, SSESCode, Ch_ange from hkex_lgt_change_of_sse_securities_lists where SSESCode = '600009' order by EffectiveDate; 
select * from lc_shsccomponent where SecuCode = '600009';

## 说明 Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only) 当做移除计算 
## Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)) 作为以上的恢复 
select EffectiveDate, SSESCode, Ch_ange from hkex_lgt_change_of_sse_securities_lists where SSESCode = '600123' order by EffectiveDate;
select * from lc_shsccomponent where SecuCode = '600123'; 


## 
select * from lc_shsccomponent where SecuCode = '601200';
select EffectiveDate, SSESCode, Ch_ange from hkex_lgt_change_of_sse_securities_lists where SSESCode = '601200' order by EffectiveDate;



## 检查变更数量是 1 的条目 
select SSESCode from hkex_lgt_change_of_sse_securities_lists group by SSESCode having count(*) = 1;


'''
docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_component:v0.0.1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_component:v0.0.1
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_component:v0.0.1
sudo docker run -itd --name kland_component registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_component:v0.0.1

'''