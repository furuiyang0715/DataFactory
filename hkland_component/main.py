import sys
import time
import schedule

sys.path.insert(0, './../')

from hkland_component.merge_lc_shsccomponent import SHMergeTools
from hkland_component.merge_lc_zhsccomponent import ZHMergeTools


def task():
    sh = SHMergeTools()
    sh.start()

    zh = ZHMergeTools()
    zh.start()
    print()


def main():
    task()

    schedule.every().day.at("05:00").do(task)

    while True:
        schedule.run_pending()
        time.sleep(180)


main()
