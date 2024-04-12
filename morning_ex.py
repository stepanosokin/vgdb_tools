import requests, json
from vgdb_general import send_to_telegram
from datetime import datetime

with open('bot_info_vgdb_bot_toJerks.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    bot_info = (jdata['token'], jdata['chatid'])

with open('jerks.log', 'w', encoding='utf-8') as f, requests.Session() as s:
    secs = datetime.now().isocalendar().week * 10 + 140
    if secs > 240:
        secs = 240
    message = f'Пора на зарядку!\n\n' \
              f'https://www.youtube.com/watch?v=lGMSpgrv6Jw\n\n' \
              f'Планка {str(secs // 60)}:{str(secs % 60)}'
    send_to_telegram(s, f, bot_info=bot_info, message=message, logdateformat='%Y-%m-%d %H:%M:%S')