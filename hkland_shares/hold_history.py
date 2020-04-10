import requests as re

datas = {
    "__VIEWSTATE": "/wEPDwUJNjIxMTYzMDAwZGQ79IjpLOM+JXdffc28A8BMMA9+yg==",
    "__VIEWSTATEGENERATOR": "EC4ACD6F",
    "__EVENTVALIDATION": "/wEdAAdtFULLXu4cXg1Ju23kPkBZVobCVrNyCM2j+bEk3ygqmn1KZjrCXCJtWs9HrcHg6Q64ro36uTSn/Z2SUlkm9HsG7WOv0RDD9teZWjlyl84iRMtpPncyBi1FXkZsaSW6dwqO1N1XNFmfsMXJasjxX85jz8PxJxwgNJLTNVe2Bh/bcg5jDf8=",
    "today": "20200409",
    "sortBy": "stockcode",
    "sortDirection": "asc",
    "alertMsg": '',
    "txtShareholdingDate": "2020/01/08",
    "btnSearch": "搜尋",
}
'''
__VIEWSTATE: /wEPDwUJNjIxMTYzMDAwZGQ79IjpLOM+JXdffc28A8BMMA9+yg==
__VIEWSTATEGENERATOR: EC4ACD6F
__EVENTVALIDATION: /wEdAAdtFULLXu4cXg1Ju23kPkBZVobCVrNyCM2j+bEk3ygqmn1KZjrCXCJtWs9HrcHg6Q64ro36uTSn/Z2SUlkm9HsG7WOv0RDD9teZWjlyl84iRMtpPncyBi1FXkZsaSW6dwqO1N1XNFmfsMXJasjxX85jz8PxJxwgNJLTNVe2Bh/bcg5jDf8=
today: 20200409
sortBy: stockcode
sortDirection: asc
alertMsg: 
txtShareholdingDate: 2020/01/08
btnSearch: 搜尋

'''

url = 'https://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t=hk'
resp = re.post(url, data=datas)
body = resp.text
print(body)