from vgdb_torgi_gov_ru import *
from datetime import datetime, timedelta

# # This is telegram credentials to send message to stepanosokin
with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    log_bot_info = (jdata['token'], jdata['chatid'])
    report_bot_info = (jdata['token'], jdata['chatid'])

download = download_torgi_gov_ru(log_bot_info=log_bot_info)