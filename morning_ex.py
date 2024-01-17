import requests, json
from vgdb_general import send_to_telegram

with open('bot_info_vgdb_bot_toJerks.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    bot_info = (jdata['token'], jdata['chatid'])

with open('jerks.log', 'w', encoding='utf-8') as f, requests.Session() as s:
    message = 'Пора на зарядку!\nhttps://open.spotify.com/track/0jpk88zk40MjQ63ljrq7V2?si=cePmfWfpTcuqDM4G-XIvKA'
    send_to_telegram(s, f, bot_info=bot_info, message=message, logdateformat='%Y-%m-%d %H:%M:%S')