## 数据源
东财网站[http://data.eastmoney.com/hsgt/index.html]

### 数据接口
资金净流入模块
- json api: http://push2.eastmoney.com/api/qt/kamt.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55,f56&ut=b2884a393a59ad64002292a3e90d46a5&cb=jQuery183041256596489447617_1600069691969&_=1600069692968
- 所需字段:   
    - ShHkFlow    # 沪股通/港股通（沪）当日净流入 
    - ShHkBalance # 沪股通/港股通(沪) 当日余额 
    - SzHkFlow    # 深股通/港股通(深) 当日净流入
    - SzHkBalance # 深股通/港股通(深) 当日余额
    - Netinflow   # 北/南向资金当日净流入

资金净买额模块 
- json api: http://push2.eastmoney.com/api/qt/kamtbs.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f54,f52,f58,f53,f62,f56,f57,f60,f61&ut=b2884a393a59ad64002292a3e90d46a5&cb=jQuery183041256596489447617_1600069691970&_=1600069692969
- 所需字段: 
    - ShHkNetBuyAmount    # '沪股通/港股通(沪)净买额（万）
    - ShHkBuyAmount       # '沪股通/港股通(沪) 买入额（万）
    - ShHkSellAmount      # '沪股通/港股通(沪) 卖出额（万）
    - SzHkNetBuyAmount    # '深股通/港股通(深)净买额（万）
    - SzHkBuyAmount       # '深股通/港股通(深) 买入额（万）
    - SzHkSellAmount      # '深股通/港股通(深) 卖出额（万）
    - TotalNetBuyAmount   # '北向/南向净买额（万）
    - TotalBuyAmount      # '北向/南向买入额（万）
    - TotalSellAmount     # '北向/南向卖出额（万）

