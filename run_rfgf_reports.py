import json
from vgdb_reports_rfgf import *

# # read the telegram bot credentials
with open('bot_info_vgdb_bot_toReportsGroup.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    report_bot_info = (jdata['token'], jdata['chatid'])

# read the telegram bot credentials
# with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
#     jdata = json.load(f)
#     report_bot_info = (jdata['token'], jdata['chatid'])

# read the telegram bot credentials
with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    log_bot_info = (jdata['token'], jdata['chatid'])

# read the postgres login credentials for gdal from file
with open('.pgdsn', encoding='utf-8') as gdalf:
    dsn = gdalf.read().replace('\n', '')

refresh_rfgf_reports(dsn, pages_pack_size=5000, report_bot_info=report_bot_info, log_bot_info=log_bot_info, send_updates=True)