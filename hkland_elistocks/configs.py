import configparser
import os

env = os.environ

cf = configparser.ConfigParser()
thisdir = os.path.dirname(__file__)
cf.read(os.path.join(thisdir, '.conf'))

# 是否本地 默认是在本地运行
LOCAL = int(env.get("LOCAL", 1))

# 是否是首次运行
FIRST = int(env.get("FIRST", 0))

# 聚源
JUY_HOST = env.get("JUY_HOST", cf.get('juyuan', 'JUY_HOST'))
JUY_PORT = int(env.get("JUY_PORT", cf.get('juyuan', 'JUY_PORT')))
JUY_USER = env.get("JUY_USER", cf.get('juyuan', 'JUY_USER'))
JUY_PASSWD = env.get("JUY_PASSWD", cf.get('juyuan', 'JUY_PASSWD'))
JUY_DB = env.get("JUY_DB", cf.get('juyuan', 'JUY_DB'))

# 数据中心
DATACENTER_HOST = env.get("DATACENTER_HOST", cf.get('datacenter', 'DATACENTER_HOST'))
DATACENTER_PORT = int(env.get("DATACENTER_PORT", cf.get('datacenter', 'DATACENTER_PORT')))
DATACENTER_USER = env.get("DATACENTER_USER", cf.get('datacenter', 'DATACENTER_USER'))
DATACENTER_PASSWD = env.get("DATACENTER_PASSWD", cf.get('datacenter', 'DATACENTER_PASSWD'))
DATACENTER_DB = env.get("DATACENTER_DB", cf.get('datacenter', 'DATACENTER_DB'))

# 测试库
TEST_HOST = env.get("TEST_HOST", cf.get('test', 'TEST_HOST'))
TEST_PORT = env.get("TEST_PORT", cf.get('test', 'TEST_PORT'))
TEST_USER = env.get("TEST_USER", cf.get('test', 'TEST_USER'))
TEST_PASSWD = env.get("TEST_PASSWD", cf.get('test', 'TEST_PASSWD'))
TEST_DB = env.get("TEST_DB", cf.get('test', 'TEST_DB'))

# 爬虫数据源
if not LOCAL:
    SPIDER_HOST = env.get("SPIDER_HOST", cf.get('spider', 'SPIDER_HOST'))
    SPIDER_PORT = int(env.get("SPIDER_PORT", cf.get('spider', 'SPIDER_PORT')))
    SPIDER_USER = env.get("SPIDER_USER", cf.get('spider', 'SPIDER_USER'))
    SPIDER_PASSWD = env.get("SPIDER_PASSWD", cf.get('spider', 'SPIDER_PASSWD'))
    SPIDER_DB = env.get("SPIDER_DB", cf.get('spider', 'SPIDER_DB'))
else:
    SPIDER_HOST = TEST_HOST
    SPIDER_PORT = TEST_PORT
    SPIDER_USER = TEST_USER
    SPIDER_PASSWD = TEST_PASSWD
    SPIDER_DB = TEST_DB

# 目标数据库
if not LOCAL:
    TARGET_HOST = env.get("TARGET_HOST", cf.get('target', 'TARGET_HOST'))
    TARGET_PORT = int(env.get("TARGET_PORT", cf.get('target', 'TARGET_PORT')))
    TARGET_USER = env.get("TARGET_USER", cf.get('target', 'TARGET_USER'))
    TARGET_PASSWD = env.get("TARGET_PASSWD", cf.get('target', 'TARGET_PASSWD'))
    TARGET_DB = env.get("TARGET_DB", cf.get('target', 'TARGET_DB'))
else:
    TARGET_HOST = TEST_HOST
    TARGET_PORT = TEST_PORT
    TARGET_USER = TEST_USER
    TARGET_PASSWD = TEST_PASSWD
    TARGET_DB = TEST_DB


if __name__ == "__main__":
    import sys
    mod = sys.modules[__name__]
    attrs = dir(mod)
    attrs = [attr for attr in attrs if not attr.startswith("__") and attr.isupper()]
    for attr in attrs:
        print(attr, ":", getattr(mod, attr))
