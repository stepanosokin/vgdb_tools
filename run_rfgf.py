import json
from vgdb_license_blocks_rfgf import *

# read the telegram bot credentials
with open('bot_info_vgdb_bot_toGroup.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    bot_info = (jdata['token'], jdata['chatid'])

# read the postgres login credentials for gdal from file
with open('.pggdal', encoding='utf-8') as gdalf:
    gdalpgcs = gdalf.read().replace('\n', '')

# download the license blocks data from Rosgeolfond
if download_rfgf_blocks('rfgf_request_noFilter_300000.json', 'rfgf_result_300000.json', bot_info=bot_info):
    # parse the blocks from downloaded json
    if parse_rfgf_blocks('rfgf_result_300000.json', bot_info=bot_info):
        # update license blocks on server
        update_postgres_table(gdalpgcs, bot_info=bot_info)
