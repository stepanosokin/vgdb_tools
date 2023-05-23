from download_auction_lic_from_rosnedra import *
from datetime import datetime, timedelta
import json, psycopg2

# read the postgres login credentials in dsn format from file
with open('.pgdsn', encoding='utf-8') as dsnf:
    dsn = dsnf.read().replace('\n', '')

# read the postgres login credentials for gdal from file
with open('.pggdal', encoding='utf-8') as gdalf:
    gdalpgcs = gdalf.read().replace('\n', '')

# get the latest rosnedra order announce date from postgres
with psycopg2.connect(dsn) as pgconn:
    startdt = get_latest_order_date_from_synology(pgconn) + timedelta(days=1)

# # This is telegram credentials to send message to stepanosokin
# with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
#     jdata = json.load(f)
#     bot_info = (jdata['token'], jdata['chatid'])

# This is telegram credentials to send message to the 'VG Database Techinfo' group
with open('bot_info_vgdb_bot_toGroup.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    bot_info = (jdata['token'], jdata['chatid'])

# clear any previous results from folder
clear_folder('rosnedra_auc')

# download newly announced Rosnedra orders since last loaded to database
download_orders(start=startdt - timedelta(days=10), end=datetime.now(), search_string='Об утверждении Перечня участков недр', folder='rosnedra_auc', bot_info=bot_info)

# parse the blocks from order announcements to geopackage
parse_blocks_from_orders(folder='rosnedra_auc', gpkg='rosnedra_result.gpkg', bot_info=bot_info)

# load new blocks to the database
update_synology_table(gdalpgcs, folder='rosnedra_auc', bot_info=bot_info)