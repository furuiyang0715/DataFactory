'''
self.sh_change_table_name = 'hkex_lgt_change_of_sse_securities_lists'
self.sh_list_table_name = 'hkex_lgt_sse_securities'
self.hk_change_table_name = 'lgt_sse_underlying_securities_adjustment'
self.hk_list_table_name = 'hkex_lgt_sse_list_of_eligible_securities'


沪股通成分
    - 上交所证券/中华通证券清单： https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities_xls/SSE_Securities.xls?la=en
    - 更改上交所证券/中华通证券名单： https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities_xls/Change_of_SSE_Securities_Lists.xls?la=en
港股通(沪)
    - 港股通股票名单: http://www.sse.com.cn/services/hkexsc/disclo/eligible/
    - 港股通股票调整信息: http://www.sse.com.cn/services/hkexsc/disclo/eligiblead/

深股通成分：
    - 深交所证券/中华通证券清单（可同时买卖的股票): https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities_xls/SZSE_Securities.xls?la=en
    - 更改深交所证券/中华通证券名单: https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities_xls/Change_of_SZSE_Securities_Lists.xls?la=en
港股通(深)
    - 港股通标的证券名单： http://www.szse.cn/szhk/hkbussiness/underlylist/index.html
    - 港股通标的证券调整： http://www.szse.cn/szhk/hkbussiness/underlyadjust/index.html

'''


class ComponentSpider(object):
    """陆股通成分股爬虫表"""
    def __init__(self):
        self.sh_change_table_name = 'hkex_lgt_change_of_sse_securities_lists'
        self.sh_list_table_name = 'hkex_lgt_sse_securities'
        self.hk_change_table_name = 'lgt_sse_underlying_securities_adjustment'
        self.hk_list_table_name = 'hkex_lgt_sse_list_of_eligible_securities'

    def start(self):
        pass
