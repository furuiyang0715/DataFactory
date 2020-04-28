import time
import traceback

import schedule


# test1
# def task():
#     print("start")
#     count = 2
#     while True:
#         try:
#             1/0
#         except:
#             print("no")
#             count -= 1
#             if count < 0:
#                 # 向上抛出异常
#                 # 主程序捕获异常退出
#                 # traceback.print_exc()
#                 raise
#         else:
#             print("ok")
#             break
#
#
# def main():
#     schedule.every(20).seconds.do(task)
#
#     while True:
#         print("当前调度系统中的任务列表是{}".format(schedule.jobs))
#         schedule.run_pending()
#         time.sleep(1)
#
#
# main()


# test2
def task():
    for i in range(10):
        try:
            # 1 / 0
            0 / 1
        except:
            print("retry")
            time.sleep(0.1)
        else:
            print("ok")
            break


task()