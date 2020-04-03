import execjs


class SFLgthisdataspiderSpider(object):
    def __init__(self):
        self.headers = {
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            # 'hexin-v': '',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
            'Accept': 'text/html, */*; q=0.01',
            'Referer': 'http://data.10jqka.com.cn/hgt/ggtb/',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
        }

    @property
    def cookie(self):
        """由于同花顺生成cookie的文件是不变的, 所以直接用文件保存方便获取"""
        with open('jqka.js', 'r') as f:
            jscont = f.read()
        cont = execjs.compile(jscont)
        cookie_v = cont.call('v')
        return cookie_v


if __name__ == "__main__":
    sf = SFLgthisdataspiderSpider()
    print(sf.cookie)

    pass
