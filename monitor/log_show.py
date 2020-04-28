import os
# https://www.cnblogs.com/ruiy/p/6422586.html
# https://docs.python.org/2/library/commands.html

from monitor import tools


def show_docker_container_logs():
    output = os.popen('docker logs --tail 1000 shares_spider')
    ret = output.read()
    print(">>\n", ret)
    tools.ding_msg(ret)