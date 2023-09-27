import requests, json, psycopg2, os
from datetime import datetime, timedelta
from vgdb_general import send_to_telegram, log_message



def download_torgi_gov_ru(size=100, log_bot_info=('token', 'chatid'), report_bot_info=('token', 'chatid'), logfile='torgi_gov_ru/logfile.txt'):
      url = 'https://torgi.gov.ru/new/api/public/lotcards/search' \
            '?chars=&chars=sl-mineralResource:hydrocarbon' \
            '&catCode=609' \
            '&byFirstVersion=true' \
            '&withFacets=true' \
            f'&size={size}' \
            f'&sort=firstVersionPublicationDate,desc'
      headers = {'accept': 'application/json, text/plain, */*',
               'accept-encoding': 'gzip, deflate, br',
               'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,en-GB;q=0.6',
               'Connection': 'keep-alive',
               # 'Cookie:': 'SESSION=NGVlZjQxNTYtMDUxYi00YmIxLTgyYTAtNWQxMGEzZjVhOGU0',
               'authorization': 'Bearer NoAuth',
               'content-type': 'application/json',
               'dnt': '1',
               'Referer': 'https://torgi.gov.ru/new/public/lots/reg'
               # 'Traceparent': '00-7da9ccb214edb92f16dda26cc382acbe-894a255635a77534-01'
               }

      with open(logfile, 'a', encoding='utf-8') as logf, requests.Session() as s:
            message = 'Запущена загрузка данных об аукционах на участки УВС с сайта torgi.gov.ru'
            log_message(s, logf, log_bot_info, message)
            status_code = 0
            i = 1
            while status_code != 200 and i <= 10:
                  try:
                        response = s.get(url, headers=headers, verify=False)
                        status_code = response.status_code
                  except:
                        pass
                  i += 1
            if status_code != 200:
                  message = f'Ошибка: не удалось загрузить данные с сайта torgi.gov.ru после {str(i)} попыток'
                  log_message(s, logf, log_bot_info, message)
                  return False
            else:
                  data = response.json()
                  pass