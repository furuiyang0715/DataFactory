import time


def ding_msg():
    raise ConnectionError


def _start():
    # 业务程序 可能出各种错 这次不行就下次重试
    raise TimeoutError


def start():
    try:
        _start()
    except:
        # 发送每次的错误原因
        ding_msg()


def main():
    while True:
        start()
        time.sleep(3)


main()
