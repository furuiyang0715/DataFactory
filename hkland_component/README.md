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


### 部署
docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_component:v0.0.1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_component:v0.0.1
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_component:v0.0.1
sudo docker run -itd --name trade_days --env LOCAL=0 registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_component:v0.0.1
