# https://www.tutorialspoint.com/python/time_strftime.htm

import datetime


_format = "%a, %d %b %Y %I:%M:%S %p %Z"
_str = "Wed, 30 Apr 2008 00:00:00 GMT"
ret = datetime.datetime.strptime(_str, _format)
print(ret)
