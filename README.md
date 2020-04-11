## 爬虫数据加工 


### 陆股通 

#### 陆港通持股记录 
文件夹同表名: hkland_shares 

#### 陆股通成分股  
文件夹同表名: hkland_component

#### 陆股通合资格股
文件夹同表名: hkland_elistocks 

#### 陆股通交易日表 
文件夹同表名: hkland_shszhktradingday 

### 陆股通实时资金流向 
文件夹同表名: hkland_flow 
网站数据源:
东财数据中心：http://data.eastmoney.com/hsgt/index.html
港交所官网 https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/Mutual-Market/Stock-Connect?sc_lang=zh-CN 
同花顺数据中心  http://data.10jqka.com.cn/hgt/hgtb/ 

### 陆港通实时资金流向汇总 
文件夹同表名: hkland_historytradestat 
网站数据源: 
港交所官网 https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/Mutual-Market/Stock-Connect?sc_lang=zh-CN （数据推算） 
同花顺数据中心 http://data.10jqka.com.cn/hgt/hgtb/ 
东财数据中心http://data.eastmoney.com/hsgt/index.html 

### 陆港通十大成交股
文件夹同表名: hkland_toptrade
网站数据源: 
港交所 https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Statistics/Historical-Daily?sc_lang=zh-HK#select4=1&select5=0&select3=0&select2=2&select1=19 
东财数据中心http://data.eastmoney.com/hsgt/index.html 
同花顺数据中心：http://data.10jqka.com.cn/hgt/hgtb/ 


### 说明
#### 陆股通 
属于北向资金。 
港资买卖 A 股的股票（包括：沪股通以及深股通） 
####  港股通 
属于南向资金 
陆资买卖港股的股票（包括：港股通(沪),港股通(深))
#### 陆港通 
陆股通以及港股通的合并叫法。
####  A 股的交易时间 
集合竞价的时间区间： 9:15-9:25 (9:15 ~ 9:20 可以申报和撤销; 9:20 ~9:25 可以申报，不可以撤销)
连续竞价区间：上午 9:30-11:30, 下午 13:00-15:00（我们平时所说的早晨开盘通常是指 9:30 ）
####  港股交易时间 
上午： 9:00-12:00
下午：13:00-16:10 
