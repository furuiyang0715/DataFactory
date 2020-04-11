import urllib.parse

data = {"name": "小笨猪", "age": "/", "addr": "&&&"}
print(urllib.parse.urlencode(data))
# name=%E5%B0%8F%E7%AC%A8%E7%8C%AA&age=%2F&addr=%26%26%26


print(urllib.parse.quote("瑞阳*&￥"))
# %E7%91%9E%E9%98%B3%2A%26%EF%BF%A5

print(urllib.parse.unquote("%E7%91%9E%E9%98%B3%2A%26%EF%BF%A5"))
# 瑞阳*&￥

