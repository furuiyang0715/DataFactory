
from hkland_elistocks.configs import TARGET_HOST, TARGET_PORT, TARGET_USER, TARGET_PASSWD, TARGET_DB, SPIDER_HOST, \
    SPIDER_PORT, SPIDER_USER, SPIDER_PASSWD, SPIDER_DB, JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB
from hkland_elistocks.sh_human_gene import SHHumanTools
from hkland_elistocks.sql_pool import PyMysqlPoolBase
from hkland_elistocks.zh_human_gene import ZHHumanTools

target_cfg = {
    "host": TARGET_HOST,
    "port": TARGET_PORT,
    "user": TARGET_USER,
    "password": TARGET_PASSWD,
    "db": TARGET_DB,
}

# 爬虫数据库
spider_cfg = {
    "host": SPIDER_HOST,
    "port": SPIDER_PORT,
    "user": SPIDER_USER,
    "password": SPIDER_PASSWD,
    "db": SPIDER_DB,
}

# 聚源数据库
juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,
}


def demo_1():
    sh = SHHumanTools()
    list_1 = set(sh.buy_and_sell_list)
    target = PyMysqlPoolBase(**target_cfg)
    sql = '''
    select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 1 and Flag = 1; 
    '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    print(ret - list_1)
    print(list_1 - ret)


def demo_2():
    sh = SHHumanTools()
    list_1 = set(sh.only_sell_list)

    target = PyMysqlPoolBase(**target_cfg)
    sql = '''select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 2 and Flag = 1; '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    print(ret - list_1)
    print(list_1 - ret)


def demo_3():
    sh = SHHumanTools()
    list_1 = set(sh.buy_margin_trading_list)
    target = PyMysqlPoolBase(**target_cfg)
    sql = '''select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 3 and Flag = 1; '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    print(ret - list_1)
    print(list_1 - ret)


def demo_4():
    sh = SHHumanTools()
    list_1 = set(sh.short_sell_list)
    target = PyMysqlPoolBase(**target_cfg)
    sql = '''
    select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 4 and Flag = 1; 
    '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    print(ret - list_1)
    print(list_1 - ret)


def run_1():
    sh = ZHHumanTools()
    list_1 = set(sh.buy_and_sell_list)
    target = PyMysqlPoolBase(**target_cfg)
    sql = '''select SecuCode from hkland_sgelistocks where TradingType = 3 and TargetCategory = 1 and Flag = 1; '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    print(ret - list_1)
    print(list_1 - ret)


def run_2():
    sh = ZHHumanTools()
    list_1 = set(sh.only_sell_list)
    target = PyMysqlPoolBase(**target_cfg)
    sql = '''select SecuCode from hkland_sgelistocks where TradingType = 3 and TargetCategory = 2 and Flag = 1; '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    print(ret - list_1)
    print(list_1 - ret)


def run_3():
    sh = ZHHumanTools()
    list_1 = set(sh.buy_margin_trading_list)
    target = PyMysqlPoolBase(**target_cfg)
    sql = '''select SecuCode from hkland_sgelistocks where TradingType = 3 and TargetCategory = 3 and Flag = 1; '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    print(ret - list_1)
    print(list_1 - ret)


def run_4():
    sh = ZHHumanTools()
    list_1 = set(sh.short_sell_list)
    target = PyMysqlPoolBase(**target_cfg)
    sql = '''select SecuCode from hkland_sgelistocks where TradingType = 3 and TargetCategory = 4 and Flag = 1; '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    print(ret - list_1)
    print(list_1 - ret)


def list_check():
    demo_1()
    print()

    demo_2()
    print()

    demo_3()
    print()

    demo_4()
    print()

    run_1()
    print()

    run_2()
    print()

    run_3()
    print()

    run_4()
    print()


if __name__ == "__main__":
    list_check()
