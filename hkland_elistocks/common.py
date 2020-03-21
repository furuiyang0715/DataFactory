from hkland_elistocks.configs import TARGET_HOST, TARGET_PORT, TARGET_USER, TARGET_PASSWD, TARGET_DB, SPIDER_HOST, \
    SPIDER_PORT, SPIDER_USER, SPIDER_PASSWD, SPIDER_DB, JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB
from hkland_elistocks.sql_pool import PyMysqlPoolBase


class CommonHumamTools(object):
    target_cfg = {
        "host": TARGET_HOST,
        "port": TARGET_PORT,
        "user": TARGET_USER,
        "password": TARGET_PASSWD,
        "db": TARGET_DB,
    }

    local_cfg = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "password": 'ruiyang',
        "db": 'test_db',
    }

    spider_cfg = {
        "host": SPIDER_HOST,
        "port": SPIDER_PORT,
        "user": SPIDER_USER,
        "password": SPIDER_PASSWD,
        "db": SPIDER_DB,
    }

    juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,
    }

    related_tables = [
        "hkex_lgt_change_of_sse_securities_lists",  # 更改上交所证券/中华通证券名单
        'hkex_lgt_sse_securities',  # 上交所证券/中华通证券清单（可同时买卖的股票）
        'hkex_lgt_special_sse_securities',  # 特殊上证所证券/特殊中华通证券清单（仅适合出售的股票）  # 含有面值等信息
        'hkex_lgt_special_sse_securities_for_margin_trading',  # 保证金交易合格的上证所证券清单
        'hkex_lgt_special_sse_securities_for_short_selling',  # 合格卖空的上证所证券清单

        'hkex_lgt_sse_list_of_eligible_securities',  # 沪港通进行南向交易的合格证券清单（可同时买卖的股票）
        'lgt_sse_underlying_securities_adjustment',  # 新增

        'hkex_lgt_change_of_szse_securities_lists',  # 更改深交所证券/中华通证券名单（2020年1月）
        'hkex_lgt_szse_securities',  # 深交所证券/中华通证券清单（可同时买卖的股票）
        'hkex_lgt_special_szse_securities',  # 特殊深交所证券/特殊中华通证券清单（只限出售股票）
        'hkex_lgt_special_szse_securities_for_margin_trading',  # 合格保证金交易的深交所证券清单
        'hkex_lgt_special_szse_securities_for_short_selling',  # 合格的深交所合格股票清单

        'hkex_lgt_szse_list_of_eligible_securities',  # 深港通南向交易合资格证券一览表（可买卖的股票）
        'lgt_szse_underlying_securities_adjustment',  # 新增
    ]

    def init_sql_pool(self, sql_cfg: dict):
        pool = PyMysqlPoolBase(**sql_cfg)
        return pool

    @property
    def inner_code_map(self):
        secu_codes = self.get_distinct_spider_secucode()
        inner_code_map = self.get_inner_code_map(secu_codes)
        return inner_code_map

    @property
    def css_code_map(self):
        secu_codes = self.get_distinct_spider_secucode()
        css_code_map = self.get_css_code_map(secu_codes)
        return css_code_map

    def get_distinct_spider_secucode(self):
        spider = self.init_sql_pool(self.spider_cfg)
        sql = 'select distinct(SSESCode) from {}; '.format(self.change_table_name)
        ret = spider.select_all(sql)
        spider.dispose()
        ret = [r.get("SSESCode") for r in ret]
        return ret

    def get_inner_code_map(self, secu_codes):
        juyuan = self.init_sql_pool(self.juyuan_cfg)
        sql = 'select SecuCode, InnerCode, SecuAbbr from secumain where SecuMarket = {} and SecuCode in {};'.format(self.market, tuple(secu_codes))
        ret = juyuan.select_all(sql)
        juyuan.dispose()
        info = {}
        for r in ret:
            key = r.get("SecuCode")
            value = (r.get('InnerCode'), r.get("SecuAbbr"))
            info[key] = value
        return info

    def get_juyuan_inner_code(self, secu_code):
        ret = self.inner_code_map.get(secu_code)
        if ret:
            return ret
        else:
            return None, None
