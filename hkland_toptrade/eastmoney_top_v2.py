import copy
import datetime
import json
import re
import time

import requests

# api = f'http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?\
# callback=jQuery112307259899045837717_{int(time.time()* 1000)}\
# &st=DetailDate%2CRank\
# &sr=1\
# &ps=10\
# &p=1\
# &type=HSGTCJB\
# &token=70f12f2f4f091e459a279469fe49eca5\
# &filter=(MarketType%3D1)\
# &sty=HGT'

api = 'http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?'

base_post_data = {
    'callback': f'jQuery112307259899045837717_{int(time.time()* 1000)}',
    'st': 'DetailDate,Rank',
    'sr': 1,
    'ps': 10,
    'p': 1,
    'type': 'HSGTCJB',
    'token': '70f12f2f4f091e459a279469fe49eca5',

} 

# 沪股通十大成交股参数
base_post_data.update(
    {
        'filter': '(MarketType=1)',
        'sty': 'HGT',
    }
)
hk_sh_post_data = copy.deepcopy(base_post_data)

# 深股通十大成交股参数
base_post_data.update(
    {
        'filter': '(MarketType=3)',
        'sty': 'SGT',
    }
)
hk_sz_post_data = copy.deepcopy(base_post_data)

# 港股通（沪）
base_post_data.update(
    {
        'filter': '(MarketType=2)',
        'sty': 'GGT',
    }
)
sh_hk_post_data = copy.deepcopy(base_post_data)

# 港股通（深）
base_post_data.update(
    {
        'filter': '(MarketType=4)',
        'sty': 'GGT',
    }
)
sz_hk_post_data = copy.deepcopy(base_post_data)


loop_info = {
    'HG': hk_sh_post_data,      # 沪股通
    'GGh':  sh_hk_post_data,    # 港股通(沪)
    'SG': hk_sz_post_data,      # 深股通
    'GGs': sz_hk_post_data,     # 港股通(深)
}


for category, post_data in loop_info.items():
    resp = requests.get(api, params=post_data)
    if resp.status_code == 200:
        body = resp.text
        string_data = re.findall(r'jQuery\d{21}_\d{13}\((.*)\)', body)[0]
        json_data = json.loads(string_data)
        for data in json_data:
            print(data)
            item = dict()
            item["Date"] = datetime.datetime.strptime(data['DetailDate'], '%Y-%m-%dT00:00:00')
            item['SecuCode'] = data['Code']
            # item['InnerCode'] =
            if data['MarketType'] == 1:    # 市场类型 1 沪股通 2 港股通（沪） 3 深股通 4 港通股（深）
                item['CategoryCode'] = 'HG'



                pass

'''
CREATE TABLE `hkland_toptrade` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `Date` date NOT NULL COMMENT '时间',
  `SecuCode` varchar(10) COLLATE utf8_bin NOT NULL COMMENT '证券代码',
  `InnerCode` int(11) NOT NULL COMMENT '内部编码',
  `SecuAbbr` varchar(20) COLLATE utf8_bin NOT NULL COMMENT '股票简称',
  `Close` decimal(19,3) DEFAULT NULL COMMENT '收盘价',
  `ChangePercent` decimal(19,5) DEFAULT NULL COMMENT '涨跌幅',
  `TJME` decimal(19,3) NOT NULL COMMENT '净买额（元/港元）',
  `TMRJE` decimal(19,3) NOT NULL COMMENT '买入金额（元/港元）',
  `TCJJE` decimal(19,3) NOT NULL COMMENT '成交金额（元/港元）',
  `CategoryCode` varchar(10) COLLATE utf8_bin DEFAULT NULL COMMENT '类别代码:GGh: 港股通(沪), GGs: 港股通(深), HG: 沪股通, SG: 深股通',
  `CMFID` bigint(20) NOT NULL COMMENT '来源ID',
  `CMFTime` datetime NOT NULL COMMENT '来源日期',
  `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
  `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un` (`SecuCode`,`Date`,`CategoryCode`) USING BTREE,
  UNIQUE KEY `un2` (`InnerCode`,`Date`,`CategoryCode`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆港通十大成交股'; 

'''


'''
# 沪股通 
{
'MarketType': 1.0,    # 市场类型 1 沪股通 2 港股通（沪） 3 深股通 4 港通股（深）
'DetailDate': '2021-01-26T00:00:00',    # 日期 
'Rank': 10.0,       # 排名 Rank 和 Rank1 使用数据完整的一个 
'Rank1': '-',
'Code': '600887',   # 证券代码 
'Name': '伊利股份',   # 证券简称 
'Close': 46.99,    # 收盘价 
'ChangePercent': -3.7682,    # 涨跌幅 
'HGTJME': -495485737.0,    # 沪股通净买额 
'HGTMRJE': 257070723.0,    # 沪股通买入金额 
'HGTMCJE': 752556460.0,    # 沪股通卖出金额 
'HGTCJJE': 1009627183.0,    # 沪股通成交金额 
}

# 港股通(沪） 
{
'MarketType': 2.0,    # 市场类型
'DetailDate': '2021-01-26T00:00:00', 
'Rank': 1.0, 
'Rank1': 1.0, 
'Code': '00700', 
'Name': '腾讯控股', 
'Close': 718.5, 
'ChangePercent': -6.2622, 
'GGTHJME': 5898204811.0,    # 港股通沪净买额 
'GGTHMRJE': 7830191950.0,   # 港股通沪买入金额 
'GGTHMCJE': 1931987139.0,   # 港股通沪卖出金额
'GGTHCJJE': 9762179089.0,   # 港股通沪成交金额 
'GGTSJME': 3044508450.0,    # 港股通深净买额 
'GGTSMRJE': 3971414450.0,   # 港股通深买入金额
'GGTSMCJE': 926906000.0,    # 港股通深卖出金额 
'GGTSCJJE': 4898320450.0,   # 港股通深成交金额 
'GGTJME': 8942713261.0,     # 港股通净买额=港股通沪净买额+港股通深净买额
'GGTCJL': 24422678628.0,    # ???
}


# 深股通
{
'MarketType': 3.0, 
'DetailDate': '2021-01-26T00:00:00', 
'Rank': 1.0, 
'Rank1': '-', 
'Code': '300059', 
'Name': '东方财富', 
'Close': 36.62, 
'ChangePercent': -7.4317, 
'SGTJME': 512309701.0,     # 深股通净买额
'SGTMRJE': 1481200004.0,   # 深股通买入金额 
'SGTMCJE': 968890303.0,    # 深股通卖出金额
'SGTCJJE': 2450090307.0,   # 深股通成交金额 
}

# 港股通（深）
{
'MarketType': 4.0, 
'DetailDate': '2021-01-26T00:00:00', 
'Rank': '-', 
'Rank1': 7.0, 
'Code': '03800', 
'Name': '保利协鑫能源', 
'Close': 2.65, 
'ChangePercent': 3.1128, 
'GGTHJME': '-', 
'GGTHMRJE': '-', 
'GGTHMCJE': '-', 
'GGTHCJJE': '-', 
'GGTSJME': 373444830.0, 
'GGTSMRJE': 874173000.0, 
'GGTSMCJE': 500728170.0, 
'GGTSCJJE': 1374901170.0, 
'GGTJME': 373444830.0, 
'GGTCJL': 1374901170.0, 
}

'''