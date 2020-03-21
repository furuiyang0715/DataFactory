import sys

from ganggutong_list.sql_pool import PyMysqlPoolBase


local_cfg = {
        "host": '127.0.0.1',
        "port": 3306,
        "user": 'root',
        "password": 'ruiyang',
        "db": 'test_db',
    }


def init_sql_pool(sql_cfg: dict):
    pool = PyMysqlPoolBase(**sql_cfg)
    return pool


local = init_sql_pool(local_cfg)


def is_in_list(code):
    # 判断当前是否成分股
    ret = local.select_all("select flag from lc_shsccomponent where SecuCode = '{}' and InDate = (select max(InDate) from lc_shsccomponent where SecuCode = '{}'); ".format(code, code))[0]
    if ret.get("flag") == 1:
        return True
    else:
        return False


def get_codes_with_appearcount(count=1):
    # 获取指定出现次数的证券列表
    ret = local.select_all("select SSESCode from hkex_lgt_change_of_sse_securities_lists group by SSESCode having count(*) = {};".format(count))
    ret = [r.get("SSESCode") for r in ret]
    return ret


def check_juyuan_exist(record):
    if record.get("OutDate"):
        is_exist = local.select_one(
            "select * from lc_shsccomponent where CompType = {} and SecuCode = '{}' and InDate = '{}' and OutDate = '{}' and Flag = {}; ".format(
                record.get("CompType"), record.get("SecuCode"), record.get("InDate"), record.get("OutDate"), record.get("Flag")
            ))
    else:
        is_exist = local.select_one(
            "select * from lc_shsccomponent where CompType = {} and SecuCode = '{}' and InDate = '{}' and Flag = {}; ".format(
                record.get("CompType"), record.get("SecuCode"), record.get("InDate"), record.get("Flag")
            ))
    if is_exist:
        return True
    else:
        return False


def each_history():
    once_codes = get_codes_with_appearcount(1)
    for code in once_codes:
        origin = local.select_all('select EffectiveDate, SSESCode, Ch_ange from hkex_lgt_change_of_sse_securities_lists where SSESCode = {} order by EffectiveDate; '.format(code))
        assert len(origin) == 1
        record = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "Flag": 1}
        is_exist = check_juyuan_exist(record)
        assert is_exist

    twice_codes = get_codes_with_appearcount(2)
    for code in twice_codes:
        origin = local.select_all('select EffectiveDate, SSESCode, Ch_ange from hkex_lgt_change_of_sse_securities_lists where SSESCode = {} order by EffectiveDate; '.format(code))
        assert len(origin) == 2
        history = [r.get("Ch_ange") for r in origin]
        h1 = ["Addition", 'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)']
        h2 = ['Addition', 'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling']
        h3 = ['Addition to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
              'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))']

        if history not in [h1, h2, h3]:
            print(history)
        if history == h1:
            record = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[1].get("EffectiveDate"), "Flag": 2}
            assert check_juyuan_exist(record)
        elif history == h2:
            record = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "Flag": 1}
            assert check_juyuan_exist(record)
        elif history == h3:
            record = {"CompType": 2, "SecuCode": code, "InDate": origin[1].get("EffectiveDate"), "Flag": 1}
            assert check_juyuan_exist(record)

    codes_3_times = get_codes_with_appearcount(3)
    h1 = ['Addition',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))']
    h2 = ['Addition',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)']
    h3 = ['Addition',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'Removal']
    h4 = ['Addition',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
          'Removal']
    h5 = ['SSE Stock Code and Stock Name are changed from 601313 and SJEC respectively',
          'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling']
    h6 = ['Addition',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'SSE Stock Code and Stock Name are changed to 601360 and 360 SECURITY TECHNOLOGY respectively']
    for code in codes_3_times:
        origin = local.select_all('select EffectiveDate, SSESCode, Ch_ange from hkex_lgt_change_of_sse_securities_lists where SSESCode = {} order by EffectiveDate; '.format(code))
        history = [r.get("Ch_ange") for r in origin]
        if history not in (h1, h2, h3, h4, h5, h6):
            print(history)
        if history == h1:
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[1].get("EffectiveDate"), "Flag": 2}
            record2 = {"CompType": 2, "SecuCode": code, "InDate": origin[2].get("EffectiveDate"), "Flag": 1}
            assert check_juyuan_exist(record1)
            assert check_juyuan_exist(record2)
        elif history == h2:
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[2].get("EffectiveDate"), "Flag": 2}
            assert check_juyuan_exist(record1)
        elif history == h3:
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[1].get("EffectiveDate"), "Flag": 2}
            assert check_juyuan_exist(record1)
        elif history == h4:
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[2].get("EffectiveDate"), "Flag": 2}
            assert check_juyuan_exist(record1)
        elif history == h5:
            # TODO
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[1].get("EffectiveDate"), "Flag": 1}
            assert check_juyuan_exist(record1)
        elif history == h6:
            # TODO
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[1].get("EffectiveDate"),  "Flag": 2}
            assert check_juyuan_exist(record1)

    codes_4_times = get_codes_with_appearcount(4)
    h1 = ['Addition',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'Removal']

    h2 = ['Addition',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))']

    h3 = ['Addition',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling']

    h4 = ['Addition',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
          'Remove from List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
          'Removal']

    h5 = ['Addition',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
          'Buy orders suspended', 'Buy orders resumed']

    h6 = ['Addition',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)']

    for code in codes_4_times:
        origin = local.select_all('select EffectiveDate, SSESCode, Ch_ange from hkex_lgt_change_of_sse_securities_lists where SSESCode = {} order by EffectiveDate; '.format(code))
        assert len(origin) == 4
        history = [r.get("Ch_ange") for r in origin]
        if history not in (h1, h2, h3, h4, h5, h6):
            print(history)
        if history == h1:
            record = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[2].get("EffectiveDate"), "Flag": 2}
            assert check_juyuan_exist(record)
        elif history == h2:
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[2].get("EffectiveDate"), "Flag": 2}
            record2 = {"CompType": 2, "SecuCode": code, "InDate": origin[3].get("EffectiveDate"), "Flag": 1}
            assert check_juyuan_exist(record1)
            assert check_juyuan_exist(record2)
        elif history == h3:
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[1].get("EffectiveDate"), "Flag": 2}
            record2 = {"CompType": 2, "SecuCode": code, "InDate": origin[2].get("EffectiveDate"), "Flag": 1}
            assert check_juyuan_exist(record1)
            assert check_juyuan_exist(record2)
        elif history == h4:
            record = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[3].get("EffectiveDate"), "Flag": 2}
            assert check_juyuan_exist(record)
        elif history == h5:
            record = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "Flag": 1}
            assert check_juyuan_exist(record)
        elif history == h6:
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[1].get("EffectiveDate"), "Flag": 2}
            record2 = {"CompType": 2, "SecuCode": code, "InDate": origin[2].get("EffectiveDate"), "OutDate": origin[3].get("EffectiveDate"), "Flag": 2}
            assert check_juyuan_exist(record1)
            assert check_juyuan_exist(record2)

    codes_5_times = get_codes_with_appearcount(5)
    h1 = ['Addition',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)']

    h2 = ['Addition',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling']

    h3 = ['Addition',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))']

    h4 = ['Addition',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)']

    h5 = ['Addition',
          'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
          'Remove from List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling',
          'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',
          'Removal']

    for code in codes_5_times:
        origin = local.select_all('select EffectiveDate, SSESCode, Ch_ange from hkex_lgt_change_of_sse_securities_lists where SSESCode = {} order by EffectiveDate; '.format(code))
        assert len(origin) == 5
        history = [r.get("Ch_ange") for r in origin]
        if history not in (h1, h2, h3, h4, h5):
            print(history)
        if history == h1:
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[2].get("EffectiveDate"), "Flag": 2}
            record2 = {"CompType": 2, "SecuCode": code, "InDate": origin[3].get("EffectiveDate"), "OutDate": origin[4].get("EffectiveDate"), "Flag": 2}
            assert check_juyuan_exist(record1)
            assert check_juyuan_exist(record2)
        elif history == h2:
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[2].get("EffectiveDate"), "Flag": 2}
            record2 = {"CompType": 2, "SecuCode": code, "InDate": origin[3].get("EffectiveDate"), "Flag": 1}
            assert check_juyuan_exist(record1)
            assert check_juyuan_exist(record2)
        elif history == h3:
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[1].get("EffectiveDate"), "Flag": 2}
            record2 = {"CompType": 2, "SecuCode": code, "InDate": origin[2].get("EffectiveDate"), "OutDate": origin[3].get("EffectiveDate"), "Flag": 2}
            record3 = {"CompType": 2, "SecuCode": code, "InDate": origin[4].get("EffectiveDate"), "Flag": 1}
            assert check_juyuan_exist(record1)
            assert check_juyuan_exist(record2)
            assert check_juyuan_exist(record3)
        elif history == h4:
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[1].get("EffectiveDate"), "Flag": 2}
            record2 = {"CompType": 2, "SecuCode": code, "InDate": origin[2].get("EffectiveDate"), "OutDate": origin[4].get("EffectiveDate"), "Flag": 2}
            assert check_juyuan_exist(record1)
            assert check_juyuan_exist(record2)
        elif history == h5:
            record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[3].get("EffectiveDate"), "Flag": 2}
            assert check_juyuan_exist(record1)

    codes_6_times = get_codes_with_appearcount(6)
    for code in codes_6_times:
        origin = local.select_all('select EffectiveDate, SSESCode, Ch_ange from hkex_lgt_change_of_sse_securities_lists where SSESCode = {} order by EffectiveDate; '.format(code))
        assert len(origin) == 6
        history = [r.get("Ch_ange") for r in origin]

        h1 = ['Addition',
              'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',

              'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))',
              'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)',

              'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))',
              'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)']
        assert history in [h1]
        record1 = {"CompType": 2, "SecuCode": code, "InDate": origin[0].get("EffectiveDate"), "OutDate": origin[1].get("EffectiveDate"), "Flag": 2}
        record2 = {"CompType": 2, "SecuCode": code, "InDate": origin[2].get("EffectiveDate"), "OutDate": origin[3].get("EffectiveDate"), "Flag": 2}
        record3 = {"CompType": 2, "SecuCode": code, "InDate": origin[4].get("EffectiveDate"), "OutDate": origin[5].get("EffectiveDate"), "Flag": 2}
        assert check_juyuan_exist(record1)
        assert check_juyuan_exist(record2)
        assert check_juyuan_exist(record3)

    # local.dispose()
    # sys.exit(0)


each_history()

try:
    local.dispose()
except:
    pass
