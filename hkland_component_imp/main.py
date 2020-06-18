import os
import sys
import time

import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from hkland_component_imp.sh_hk_origin_check import SHSCComponent
from hkland_component_imp.sz_hk_origin_check import SZSCComponent


def task():
    SHSCComponent().start()
    SZSCComponent().start()


def main():
    task()

    schedule.every().day.at("05:00").do(task)

    while True:
        schedule.run_pending()
        time.sleep(180)


if __name__ == "__main__":
    main()

'''
docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_component:v0.0.1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_component:v0.0.1
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_component:v0.0.1
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name  component_imp --env LOCAL=0 registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_component:v0.0.1
'''