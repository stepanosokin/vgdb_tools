import json
from vgdb_license_blocks_rfgf import *
from synchro_evergis import *

# read the telegram bot credentials
# with open('bot_info_vgdb_bot_toGroup.json', 'r', encoding='utf-8') as f:
#     jdata = json.load(f)
#     bot_info = (jdata['token'], jdata['chatid'])

# read the telegram bot credentials
with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    bot_info = (jdata['token'], jdata['chatid'])

# read the postgres login credentials for gdal from file
with open('.pggdal', encoding='utf-8') as gdalf:
    gdalpgcs = gdalf.read().replace('\n', '')

with open('license_blocks_general.webhook', 'r', encoding='utf-8') as f:
    lb_general_webhook = f.read().replace('\n', '')

with open('.ext_pgdsn', encoding='utf-8') as f:
    ext_pgdsn = f.read()

with open('.pgdsn', encoding='utf-8') as f:
    local_pgdsn = f.read()

with open('.egssh', 'r', encoding='utf-8') as f:
    egssh = json.load(f)

# download the license blocks data from Rosgeolfond
if download_rfgf_blocks('rfgf_request_noFilter_300000.json', 'rfgf_result_300000.json', bot_info=bot_info):
    # parse the blocks from downloaded json
    if parse_rfgf_blocks('rfgf_result_300000.json', bot_info=bot_info):
        # update license blocks on server
        if update_postgres_table(gdalpgcs, bot_info=bot_info, webhook=lb_general_webhook):
            fix_selected_geometry()
            synchro_layer([('rfgf', ['license_blocks_rfgf'])], local_pgdsn, ext_pgdsn, ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)
