from vgdb_torgi_gov_ru import *
from datetime import datetime, timedelta

# read the postgres login credentials in dsn format from file
with open('.pgdsn', encoding='utf-8') as dsnf:
    dsn = dsnf.read().replace('\n', '')

# This is telegram credentials to send message to the 'VG Database Techinfo' group
with open('bot_info_vgdb_bot_toAucGroup.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    report_bot_info = (jdata['token'], jdata['chatid'])

# # This is telegram credentials to send message to stepanosokin
with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    log_bot_info = (jdata['token'], jdata['chatid'])
    report_bot_info = (jdata['token'], jdata['chatid'])

# # This is telegram credentials to send message to teams
with open('2023_blocks_nr_ne.webhook', 'r', encoding='utf-8') as f:
    nr_ne_webhook_2023 = f.read().replace('\n', '')

# refresh_lotcards(dsn=dsn, log_bot_info=log_bot_info, report_bot_info=report_bot_info, webhook=nr_ne_webhook_2023)
refresh_lotcards(dsn=dsn, log_bot_info=log_bot_info, report_bot_info=report_bot_info)