from vgdb_torgi_gov_ru import *
from datetime import datetime, timedelta
from synchro_evergis import *
from time import sleep
import json

sleep(120)

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
    # report_bot_info = (jdata['token'], jdata['chatid'])

# # This is telegram credentials to send message to teams
with open('2025_blocks_nr_ne.webhook', 'r', encoding='utf-8') as f:
    nr_ne_webhook_2025 = f.read().replace('\n', '')

with open('.egssh', 'r', encoding='utf-8') as f:
    egssh = json.load(f)

with open('.mapbox_token', encoding='utf-8') as mbtkf:
    mb_token = mbtkf.read().replace('\n', '')

refresh_lotcards(dsn=dsn, log_bot_info=log_bot_info, 
                 report_bot_info=log_bot_info, 
                 webhook=nr_ne_webhook_2025, 
                 mapbox_token=mb_token, 
                 webhostssh='.regrussh')

# refresh_lotcards(dsn=dsn, log_bot_info=log_bot_info, 
#                  report_bot_info=report_bot_info, 
#                  webhook='', 
#                  mapbox_token=mb_token)

# refresh_lotcards(dsn=dsn, log_bot_info=log_bot_info, report_bot_info=report_bot_info)

refresh_old_lotcards(dsn=dsn, log_bot_info=log_bot_info, 
                 report_bot_info=log_bot_info, 
                 webhook=nr_ne_webhook_2025, 
                 mapbox_token=mb_token, 
                 webhostssh='.regrussh')

# synchro_table([('torgi_gov_ru', ['lotcards'])], '.pgdsn', '.ext_pgdsn', ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=log_bot_info)
