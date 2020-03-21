## lc_shscelistocks


# 什么是CCASS股份编码  https://zh.wikipedia.org/wiki/%E4%B8%AD%E5%A4%AE%E7%B5%90%E7%AE%97%E5%8F%8A%E4%BA%A4%E6%94%B6%E7%B3%BB%E7%B5%B1
'''
中央结算及交收系统（The Central Clearing And Settlement System，缩写为CCASS），简称中央结算系统，
是一个专为在香港联交所上市的证券买卖而设的电脑化账面交收系统；中央结算系统原本的设计只为让香港交易所辖下的香港中央结算有限公司(结算公司)可向市场中介人士，
例如经纪及托管商，提供结算所服务，再由中介人士向其客户提供结算服务。现时，鉴于投资者希望享有账面结算及交收系统的优点及对他们的股份拥实质的操控权，个人及公司投资者现在可于中央结算系统开设投资者户口。

投资者户口实际上是一个存管股票的户口。投资者户口持有人仍须透过经纪或托管商(同时为中央结算系统参与者)进行股份买卖及有关交收;
投资者户口持有人仍要自行管理本身在交收过程中所涉及的交收风险。
'''


# select max(UpdateTime) from lc_shscelistocks; # 2018-10-22 07:30:39
# select SecuCode from lc_shscelistocks group by SecuCode  having count(1) > 8;    # 600094
# > 7; # 600433 600770

# select distinct(Remarks) from hkex_lgt_change_of_sse_securities_lists;
# 有点信息可能隐藏在 Remarks 里面 ..

# 什么是合资格股票
# select distinct(TargetCategory)  from lc_shscelistocks where Flag = 1; # 1, 2, 3, 4
# select *  from lc_shscelistocks where TargetCategory = 5; # 空

# select * from hkex_lgt_change_of_sse_securities_lists where Remarks = 'Initial list of securities eligible for buy and sell';
# select distinct(EffectiveDate) from hkex_lgt_change_of_sse_securities_lists where Remarks = 'Initial list of securities eligible for buy and sell';


# select distinct(EffectiveDate)  from hkex_lgt_change_of_sse_securities_lists where Remarks = 'Initial list of Eligible SSE Securities for Margin Trading and initial list of Eligible SSE Securities for Short Selling';




'''沪港通合资格股份
CREATE TABLE `lc_shscelistocks` (
  `ID` bigint(20) NOT NULL COMMENT 'ID',
  `TradingType` int(11) NOT NULL COMMENT '交易方向',      # 1-沪股通，2-港股通（沪）
  `TargetCategory` int(11) NOT NULL COMMENT '标的类别',   # 1-可买入及卖出，2-只可卖出，3-可进行保证金交易，4-可进行担保卖空，5-触发持股比例限制暂停买入。
  `InnerCode` int(11) NOT NULL COMMENT '证券内部编码',     # 证券内部编码（InnerCode）：当TradingType=1时，与“证券主表（SecuMain）”中的“内部编码（InnerCode）”关联，得到股票的交易代码、简称等；当TradingType=2时，与“港股证券主表（HK_SecuMain）”中的“内部编码（InnerCode）”关联，得到股票的交易代码、简称等；
  `SecuCode` varchar(50) DEFAULT NULL COMMENT '证券代码',
  `SecuAbbr` varchar(50) DEFAULT NULL COMMENT '证券简称',
  `InDate` datetime NOT NULL COMMENT '调入日期',
  `OutDate` datetime DEFAULT NULL COMMENT '调出日期',
  `Flag` int(11) DEFAULT NULL COMMENT '资讯级别',          # 该字段固定以下常量：1-是；2-否 应该是标识当前的状态: 是否是合资格股票
  `CCASSCode` varchar(50) DEFAULT NULL COMMENT 'CCASS股份编码',    # CCASS Stock Code
  `ParValue` varchar(50) DEFAULT NULL COMMENT '面值(人民币)',  # 面值
  `UpdateTime` datetime NOT NULL COMMENT '更新时间',
  `JSID` bigint(20) NOT NULL COMMENT 'JSID',
  UNIQUE KEY `IX_LC_SHSCEliStocks` (`InnerCode`,`TradingType`,`TargetCategory`,`InDate`),
  UNIQUE KEY `IX_LC_SHSCEliStocks_ID` (`ID`),
  UNIQUE KEY `IX_LC_SHSCEliStocks_JSID` (`JSID`)
) ENGINE=InnoDB DEFAULT CHARSET=gbk;
'''

# 可进行保证金交易 # 历史数据中无  3 的记录

# 可进行买入以及卖出 (Addition) 即可进行买入以及卖出;
# 可进行担保卖空 (Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling)
# 只可卖出 (Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)
# 恢复可买入以及卖出（Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))） 之前有过担保卖空的 也在这一天恢复可进行担保卖空
# 1-可买入及卖出，2-只可卖出，3-可进行保证金交易，4-可进行担保卖空，5-触发持股比例限制暂停买入。