import requests
import json
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from bs4 import BeautifulSoup
from vgdb_general import smart_http_request
import re


def show_cadnum_old(x, y):
    with requests.Session() as s:
        
        pass
        headers = {
            "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            "Accept-Encoding": 'gzip, deflate, br, zstd',
            "Accept-Language": 'ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7',
            "Connection": 'keep-alive',
            "Origin": "https://pkk.rosreestr.ru/",
            "DNT": '1',
            "Sec-Fetch-Dest": 'document',
            "Sec-Fetch-Mode": 'navigate',
            "Sec-Fetch-Site": 'none',
            "Sec-Fetch-User": '?1',
            "Upgrade-Insecure-Requests": '1',
            "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0',
            "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Microsoft Edge";v="128"',
            "sec-ch-ua-mobile": '?0',
            "sec-ch-ua-platform": '"Windows"'
        }
        s.get('https://egrp365.ru', headers=headers)
        pass
        s.get('https://dadata.egrp365.org/suggestions/api/4_1/rs/status/address', headers=headers)
        pass
        s.get('https://pkk.rosreestr.ru', headers=headers, timeout=(5, 4), allow_redirects=False, verify=False, stream=True)
        pass
        url1 = 'https://pkk.rosreestr.ru/api/features/1/'
        url2 = f"https://pkk.rosreestr.ru/api/features/5?sq=%7B%22type%22%3A%22Point%22%2C%22coordinates%22%3A%5B{y}%2C{x}%5D%7D&tolerance=1&limit=11"
        url3 = '''https://pkk.rosreestr.ru/api/features/5?sq=%7B%22type%22%3A%22Point%22%2C%22coordinates%22%3A%5B37.50992596149445%2C55.744983632156774%5D%7D&tolerance=1&limit=11'''
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ru",
            "Connection": "keep-alive",
            "DNT": "1",
            # "host": "pkk5-rosreestr.ru",
            # "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Microsoft Edge";v="128"',
            # "sec-ch-ua-mobile": "?0",
            # "sec-ch-ua-platform": "Windows",
            # "sec-fetch-dest": "document",
            # "sec-fetch-mode": "navigate",
            # "sec-fetch-site": "none",
            # "sec-fetch-user": "?1",
            "Upgrade-Insecure-Requests": "0",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0"
            # "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36"
        }
        headers = {"Accept": "*/*",
                   "Accept-Encoding": "gzip, deflate, br, zstd",
                   "Accept-Language": "ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
                   "Connection": "keep-alive",
                   "DNT": "1",
                   "Host": "pkk.rosreestr.ru",
                   "Origin": "https://egrp365.ru",
                   "Sec-Fetch-Dest": "empty",
                   "Sec-Fetch-Mode": "cors",
                   "Sec-Fetch-Site": "cross-site",
                   "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
                   "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Microsoft Edge";v="128"',
                   "sec-ch-ua-mobile": "?0",
                   "sec-ch-ua-platform": "Windows"}
        params = {
            "sq": '{"type":"Point","coordinates":[' + x + ',' + y + ']}',
            "tolerance": '1',
            "limit": '11'
        }
        # params = json.dumps(params)
        # {"type":"Point","coordinates":[37.50992596149445,55.744983632156774]}
        s.headers = headers
        s.params = params
        # cookies = s.cookies
        # cookies['_cookies']['session-cookie'] = '17f4c937f5c964d6b13db1b2beb261f5746b91864f558112593f227b9af0f70026090bc1f0dea8e9bac44bb663258b06'
        # s.cookies = cookies
        cookies = {'session-cookie': '17f4c937f5c964d6b13db1b2beb261f5746b91864f558112593f227b9af0f70026090bc1f0dea8e9bac44bb663258b06'}
        try:
            # result = s.get(url1, headers=headers, params=params, verify=False)
            result = s.get(url1)
            pass
        except Exception as err:
            pass

def show_cadnum_new(x, y):
    
    with requests.Session() as s:
        url = 'https://pkk.rosreestr.ru'
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ru",
            "Connection": "keep-alive",
            "DNT": "1",
            "Host": "pkk.rosreestr.ru",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
            "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Microsoft Edge";v="128"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "Windows",
        }
        result = s.get(url, headers=headers, verify=False)

        url = 'https://pkk.rosreestr.ru/api/features/'
        params = {
            "text": f"{x} {y}",
            "tolerance": "4",
            "types": "[2,3,4,1,21,5]"
            }
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
            "Connection": "keep-alive",
            "Cookie": "session-cookie=17f4c040edb068adb13db1b2beb261f56894a3789295c18e104a286d9f2673e34f8868a9d9be91979cfa9e255ef1e4fe; _ga=GA1.2.959209876.1726215947; _gid=GA1.2.618891204.1726215947; sputnik_session=1726215955296|15; _gat_gtag_UA_15707457_7=1; _ga_78LLLS27C6=GS1.1.1726215947.1.1.1726216119.49.0.0",
            "DNT": "1",
            "Host": "pkk.rosreestr.ru",
            "Referer": "https://pkk.rosreestr.ru"
        }
        code = 0
        tries = 0
        data1 = None
        while code != 200 and tries < 10:
            try:
                result = s.get(url, params=params, headers=headers, verify=False)
                code = result.status_code
                tries += 1
                data1 = result.json()
            except Exception as err:
                message = 'Ошибка запроса кадастрового номера'
        if data1:
            message = ''
            for r in data1.get('results'):
                if r.get('type') == 1:
                    message += r.get('attrs').get('cn') + '\n'
        
        print(message)

def show_cadnum_sel(x, y):
    driver_exe = 'edgedriver'
    options = Options()
    # options.add_argument("--headless")

    # Eldar Mamedov's Selenium options:
    options.add_argument("--headless=new")  # Запуск браузера в безголовом режиме
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-infobars")
    options.add_argument("--start-maximized")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    # driver = webdriver.Edge(options=options)
    driver = webdriver.Chrome(options=options)
    # driver.get('https://egrp365.ru')
    url = 'https://pkkn.egrp365.org/api/features/1?'
    url += 'sq=%7B%22type%22%3A%22Point%22%2C%22'
    url += f'coordinates%22%3A%5B{x}%2C{y}'
    url += '%5D%7D&tolerance=1&limit=11'
    # driver.get(f'https://pkk.rosreestr.ru/api/features/1/?sq=%7B%22type%22%3A%22Point%22%2C%22coordinates%22%3A%5B{x}%2C{y}%5D%7D&tolerance=1&limit=11')
    driver.get(url)
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    data = soup.find('div', attrs={'hidden': 'true'})
    data = json.loads(data.text)
    message = ''
    for f in data.get('features'):
        message += f.get('attrs').get('cn') + '\n'
    if not message:
        message = 'Ошибка запроса кадастрового номера'
    print(message)


def show_cadnum_nspd(x, y):
    with requests.Session() as s:
        url = 'https://nspd.gov.ru/api/aeggis/v3/36048/wms'
        headers = {
            "Accept": '*/*',
            "Accept-Encoding": 'gzip, deflate, br, zstd; charset=utf-8',
            "Content-Type": 'text/html; charset=utf-8',
            "Accept-Language": 'en,ru;q=0.9,en-GB;q=0.8,en-US;q=0.7',
            "Connection": 'keep-alive',
            "Origin": "https://nspd.gov.ru/",
            "DNT": '1',
            "cache-control": 'no-cache',
            "Pragma": 'no-cache',
            "Priority": 'u=1, i',
            "Referer": f'https://nspd.gov.ru/map?' \
                        f'thematic=PKK' \
                        f'&zoom=13.866602508125357' \
                        f'&coordinate_x={str(x)}' \
                        f'&coordinate_y={str(y)}' \
                        f'&theme_id=1' \
                        f'&is_copy_url=true' \
                        f'&active_layers=%E8%B3%90',
            "sec-ch-ua": '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": '?0',
            "Sec-ch-ua-platform": '"Windows"',
            "Sec-fetch-dest": 'empty',
            "Sec-fetch-mode": 'cors',
            "Sec-Fetch-Site": 'same-origin',
            "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0'
        }
        # headers = {
        #     "Referer": f'https://nspd.gov.ru/map?' \
        #                 f'thematic=PKK' \
        #                 f'&zoom=13.866602508125357' \
        #                 f'&coordinate_x={str(x)}' \
        #                 f'&coordinate_y={str(y)}' \
        #                 f'&theme_id=1' \
        #                 f'&is_copy_url=true' \
        #                 f'&active_layers=%E8%B3%90',
        # }

        params = {
            "REQUEST": 'GetFeatureInfo',
            "QUERY_LAYERS": '36048',
            "SERVICE": 'WMS',
            "VERSION": '1.3.0',
            "FORMAT": 'image/png',
            "STYLES": '',
            "TRANSPARENT": 'true',
            "LAYERS": '36048',
            "RANDOM": '0.10847854968801807',
            "INFO_FORMAT": 'application/json',
            "FEATURE_COUNT": '10',
            "I": '228',
            "J": '150',
            "WIDTH": '512',
            "HEIGHT": '512',
            "CRS": 'EPSG:3857',
            "BBOX": f'{str(float(x) - 4267.64188619)},{str(float(y) - 1481.34839063)},{str(float(x) + 624.327924059)},{str(float(y) + 3410.57141961)}'
        }

        status, result = smart_http_request(s=s, url=url, params=params, headers=headers, verify=False)
        if status == 200:
            jresult = result.json()
            if jresult.get('features'):
                return jresult['features'][0]['properties']['descr']
            # return jresult
        return None


if __name__ == '__main__':
    # show_cadnum('57.02722347329609', '47.219388818706435')
    # show_cadnum_old('57.02722347329609', '47.219388818706435')
    
    # show_cadnum_sel('46.286645', '52.7779')

    # show_cadnum_nspd(6672022.493258687, 9876046.983278478)
    num = show_cadnum_nspd(6667330.92232106, 9876142.067073278)
    pass