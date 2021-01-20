import os
import sys

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from hkland_elistocks.base import BaseSpider


class DailyUpdate(BaseSpider):
    def __init__(self):
        super(DailyUpdate, self).__init__()
        # sh
        self.sh_only_sell_list_table = 'hkex_lgt_special_sse_securities'
        self.sh_buy_and_sell_list_table = 'hkex_lgt_sse_securities'
        self.sh_buy_margin_trading_list_table = 'hkex_lgt_special_sse_securities_for_margin_trading'
        self.sh_short_sell_list_table = 'hkex_lgt_special_sse_securities_for_short_selling'

        # sz
        self.sz_only_sell_list_table = 'hkex_lgt_special_szse_securities'
        self.sz_buy_and_sell_list_table = 'hkex_lgt_szse_securities'
        self.sz_buy_margin_trading_list_table = 'hkex_lgt_special_szse_securities_for_margin_trading'
        self.sz_short_sell_list_table = 'hkex_lgt_special_szse_securities_for_short_selling'

    def sh_buy_and_sell_list(self):
        self._spider_init()
        self._product_init()

        sql = '''select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 1 and Flag = 1 and InDate <= now(); '''
        ret = self.product_client.select_all(sql)
        datas = set([r.get("SecuCode") for r in ret])

        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(self.sh_buy_and_sell_list_table, self.sh_buy_and_sell_list_table)
        ret = self.spider_client.select_all(sql)
        lst = set([r.get("SSESCode") for r in ret])
        assert datas
        assert lst
        print(f'sh buyandsell: 变更-列表{datas - lst}')
        print(f'sh buyandsell: 列表-变更{lst - datas}')
        return (not (datas - lst)) and (not (lst - datas))

    def sz_buy_and_sell_list(self):
        self._spider_init()
        self._product_init()

        sql = '''select SecuCode from hkland_sgelistocks where TradingType = 3 and TargetCategory = 1 and Flag = 1 and InDate <= now(); '''
        ret = self.product_client.select_all(sql)
        datas = set([r.get("SecuCode") for r in ret])

        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
            self.sz_buy_and_sell_list_table, self.sz_buy_and_sell_list_table)
        ret = self.spider_client.select_all(sql)
        lst = set([r.get("SSESCode") for r in ret])
        assert datas
        assert lst
        print(f'sz buyandsell: 变更-列表{datas - lst}')
        print(f'sz buyandsell: 列表-变更{lst - datas}')
        return (not (datas - lst)) and (not (lst - datas))

    def sh_only_sell_list(self):
        # self._test_init()
        self._spider_init()
        self._product_init()

        sql = '''select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 2 and Flag = 1 and InDate <= now() ; '''
        ret = self.product_client.select_all(sql)
        datas = set([r.get("SecuCode") for r in ret])

        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(self.sh_only_sell_list_table, self.sh_only_sell_list_table)
        ret = self.spider_client.select_all(sql)

        lst = set([r.get("SSESCode") for r in ret])
        assert datas
        assert lst
        print(f'sh onlysell: 变更-列表{datas - lst}')
        print(f'sh onlysell: 列表-变更{lst - datas}')
        return (not (datas - lst)) and (not (lst - datas))

    def sz_only_sell_list(self):
        self._spider_init()
        self._product_init()
        sql = '''select SecuCode from hkland_sgelistocks where TradingType = 3 and TargetCategory = 2 and Flag = 1 and InDate <= now(); '''
        ret = self.product_client.select_all(sql)
        datas = set([r.get("SecuCode") for r in ret])

        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(self.sz_only_sell_list_table, self.sz_only_sell_list_table)
        ret = self.spider_client.select_all(sql)

        lst = set([r.get("SSESCode") for r in ret])
        assert datas
        assert lst
        print(f'sz onlysell: 变更-列表{datas - lst}')
        print(f'sz onlysell: 列表-变更{lst - datas}')
        return (not (datas - lst)) and (not (lst - datas))

    def sh_buy_margin_trading_list(self):
        self._spider_init()
        self._product_init()

        sql = '''select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 3 and Flag = 1 and InDate <= now(); '''
        ret = self.product_client.select_all(sql)
        datas = set([r.get("SecuCode") for r in ret])

        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
            self.sh_buy_margin_trading_list_table, self.sh_buy_margin_trading_list_table)
        ret = self.spider_client.select_all(sql)

        lst = set([r.get("SSESCode") for r in ret])
        assert datas
        assert lst
        print(f'sh buymargintrading: 变更-列表{datas - lst}')
        print(f'sh buymargintrading: 列表-变更{lst - datas}')
        return (not (datas - lst)) and (not (lst - datas))

    def sz_buy_margin_trading_list(self):
        self._spider_init()
        self._product_init()

        sql = '''select SecuCode from hkland_sgelistocks where TradingType = 3 and TargetCategory = 3 and Flag = 1 and InDate <= now(); '''
        ret = self.product_client.select_all(sql)
        datas = set([r.get("SecuCode") for r in ret])

        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(self.sz_buy_margin_trading_list_table, self.sz_buy_margin_trading_list_table)
        ret = self.spider_client.select_all(sql)

        lst = set([r.get("SSESCode") for r in ret])
        assert lst
        assert datas
        print(f'sz buymargintrading: 变更-列表{datas - lst}')
        print(f'sz buymargintrading: 列表-变更{lst - datas}')
        return (not (datas - lst)) and (not (lst - datas))

    def sh_short_sell_list(self):
        self._spider_init()
        self._product_init()

        sql = '''select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 4 and Flag = 1 and InDate <= now(); '''
        ret = self.product_client.select_all(sql)
        datas = set([r.get("SecuCode") for r in ret])

        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(
            self.sh_short_sell_list_table, self.sh_short_sell_list_table)
        ret = self.spider_client.select_all(sql)
        lst = set([r.get("SSESCode") for r in ret])
        assert datas
        assert lst
        print(f'sh short sell: 变更-列表{datas - lst}')
        print(f'sh short sell: 列表-变更{lst - datas}')
        return (not (datas - lst)) and (not (lst - datas))

    def sz_short_sell_list(self):
        self._spider_init()
        self._product_init()

        sql = '''select SecuCode from hkland_sgelistocks where TradingType = 3 and TargetCategory = 4 and Flag = 1 and InDate <= now(); '''
        ret = self.product_client.select_all(sql)
        datas = set([r.get("SecuCode") for r in ret])

        sql = 'select distinct(SSESCode) from {} where Date = (select max(Date) from {});'.format(self.sz_short_sell_list_table, self.sz_short_sell_list_table)
        ret = self.spider_client.select_all(sql)

        lst = set([r.get("SSESCode") for r in ret])
        assert datas
        assert lst
        print(f'sz short sell: 变更-列表{datas - lst}')
        print(f'sz short sell: 列表-变更{lst - datas}')
        return (not (datas - lst)) and (not (lst - datas))
