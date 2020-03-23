from hkland_component.sql_pool import PyMysqlPoolBase
from hkland_elistocks.configs import TARGET_HOST, TARGET_PORT, TARGET_USER, TARGET_PASSWD, TARGET_DB, SPIDER_HOST, \
    SPIDER_PORT, SPIDER_USER, SPIDER_PASSWD, SPIDER_DB, JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB
from hkland_elistocks.sh_human_gene import SHHumanTools


# 目标数据库 意思是规整完成的数据要插入的数据库
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
    # 1 的爬虫清单
    list_1 = set(sh.buy_and_sell_list)
    # print(list_1)
    # print(len(list_1))

    target = PyMysqlPoolBase(**target_cfg)
    sql = '''
    select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 1 and Flag = 1; 
    '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    # print(ret)
    # print(len(ret))

    print(ret - list_1)   # set()
    print(list_1 - ret)   # {'600378', '600736', '600282', '600123', '600702', '603508'}


    # # 600378 600736 600282 600123 600702 603508 无记录 可能是某个改名来的 TODO
    # sql2 = '''select * from hkex_lgt_change_of_sse_securities_lists where  SSESCode = '600378' and Time = '2020-03-18' order by EffectiveDate;'''
    # # 查看一下全部改名的记录
    # sql3 = '''select distinct(Remarks) from hkex_lgt_change_of_sse_securities_lists; '''
    #
    # spider = PyMysqlPoolBase(**spider_cfg)
    # res = spider.select_all(sql3)
    # print(res)
    # with open("test.py", "w") as f:
    #     f.write("{}".format(res))
    #
    # # 注： 成分直接拿聚源的数据。聚源里面有这个的成分信息。
    # # 但是合资格是采用的爬虫数据，爬虫里面是没有这个信息的。
    # # TODO 从聚源库中补上


def demo_2():
    sh = SHHumanTools()
    # 2 的爬虫清单
    list_1 = set(sh.only_sell_list)
    # print(list_1)
    # print(len(list_1))

    target = PyMysqlPoolBase(**target_cfg)
    sql = '''
        select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 2 and Flag = 1; 
        '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    # print(ret)
    # print(len(ret))  # 334

    print(ret - list_1)   # set()
    print(list_1 - ret)   # set()


def demo_3():
    sh = SHHumanTools()
    # 3 的爬虫清单
    list_1 = set(sh.buy_margin_trading_list)
    # print(list_1)
    # print(len(list_1))

    target = PyMysqlPoolBase(**target_cfg)
    sql = '''
            select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 3 and Flag = 1; 
            '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    # print(ret)
    # print(len(ret))

    print(ret - list_1)   # set()
    print(list_1 - ret)   # {'600282', '600378', '600702', '600123', '603508', '600736'}   # TODO 与 demo_1 的问题一样


def demo_4():
    sh = SHHumanTools()
    # 4 的爬虫清单
    list_1 = set(sh.short_sell_list)
    # print(list_1)
    # print(len(list_1))

    target = PyMysqlPoolBase(**target_cfg)
    sql = '''
                select SecuCode from hkland_hgelistocks where TradingType = 1 and TargetCategory = 4 and Flag = 1; 
                '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    # print(ret)
    # print(len(ret))

    print(ret - list_1)  # set()
    print(list_1 - ret)  # {'600282', '600378', '600702', '600123', '603508', '600736'}   # TODO 与 demo_1 的问题一样


def run_1():
    sh = ZHHumanTools()
    # 1 的爬虫清单
    list_1 = set(sh.buy_and_sell_list)
    # print(list_1)
    # print(len(list_1))

    target = PyMysqlPoolBase(**target_cfg)
    sql = '''select SecuCode from hkland_sgelistocks where TradingType = 3 and TargetCategory = 1 and Flag = 1; '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    # print(ret)
    # print(len(ret))

    # print()

    print(ret - list_1)    # {'000043', '000022'}   000043 --> 001914
    # print("001914" in ret)    # True   # TODO 讲道理 000043 改名之后 就应该将其从名单中移除
    # print("001914" in list_1)  # True
    print(list_1 - ret)    # {'001872'}    001872 --> 000022


def run_2():
    sh = ZHHumanTools()
    # 2 的爬虫清单
    list_1 = set(sh.only_sell_list)
    # print(list_1)
    # print(len(list_1))

    target = PyMysqlPoolBase(**target_cfg)
    sql = '''select SecuCode from hkland_sgelistocks where TradingType = 3 and TargetCategory = 2 and Flag = 1; '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    # print(ret)
    # print(len(ret))

    # print()

    print(ret - list_1)  # set()
    print(list_1 - ret)   # set()


def run_3():
    sh = ZHHumanTools()
    # 3 的爬虫清单
    list_1 = set(sh.buy_margin_trading_list)
    # print(list_1)
    # print(len(list_1))

    target = PyMysqlPoolBase(**target_cfg)
    sql = '''select SecuCode from hkland_sgelistocks where TradingType = 3 and TargetCategory = 3 and Flag = 1; '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    # print(ret)
    # print(len(ret))

    # print()

    print(ret - list_1)   # {'000043', '000022'}
    print(list_1 - ret)   # {'001872'}  同 1


def run_4():
    sh = ZHHumanTools()
    # 4 的爬虫清单
    list_1 = set(sh.short_sell_list)
    # print(list_1)
    # print(len(list_1))

    target = PyMysqlPoolBase(**target_cfg)
    sql = '''select SecuCode from hkland_sgelistocks where TradingType = 3 and TargetCategory = 4 and Flag = 1; '''
    ret = target.select_all(sql)
    ret = set([r.get("SecuCode") for r in ret])
    # print(ret)
    # print(len(ret))

    # print()

    print(ret - list_1)  # {'000043', '000022'}  同 1
    print(list_1 - ret)   # {'001872'}


def sync_from_juyuan(codes, source_table):
    juyuan = PyMysqlPoolBase(**juyuan_cfg)
    for code in codes:
        sql = "select * from {} where SecuCode = '{}'; ".format(source_table, code)
        ret = juyuan.select_all(sql)
        for r in ret:
            record = {
                "TradingType": r.get("TradingType"),
                "TargetCategory": r.get("TargetCategory"),
                "SecuCode": r.get('SecuCode'),
                'InDate': r.get('InDate'),
                "OutDate": None,
                'Flag': 1,
                "InnerCode": r.get('InnerCode'),
                "SecuAbbr": r.get("SecuAbbr"),
                'CCASSCode': r.get('CCASSCode'),
                'ParValue': r.get('ParValue'),
            }
            # print(record)
            sh = SHHumanTools()
            sh.insert(record)
    juyuan.dispose()


if __name__ == "__main__":
    # codes = {'600378', '600736', '600282', '600123', '600702', '603508'}
    # source_table = 'lc_shscelistocks'
    # sync_from_juyuan(codes, source_table)

    # demo_1()
    # print()
    #
    # demo_2()
    # print()
    #
    # demo_3()
    # print()
    #
    # demo_4()
    # print()

    run_1()
    print()

    run_2()
    print()

    run_3()
    print()

    run_4()
    print()


# TODO {'000333', '002008', '000022', '000043'} 这几个港股的 TradingType = 1 应该是 3
# 直接在数据库中更新
# select * from  hkland_sgelistocks where SecuCode = '000333';
# update hkland_sgelistocks set TradingType = 3 where SecuCode = '000333';
# 是单独插入的处理 没改成 3 线上删除重跑吧


# TODO  zh 的 000918 需要删除重新生成
# TODO  zh 的 000418 需要删除重新生成
