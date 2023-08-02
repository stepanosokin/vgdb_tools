from vgdb_auctions_rosnedra import *
from datetime import datetime, timedelta
import json, psycopg2
from psycopg2.extras import *

# read the postgres login credentials in dsn format from file
with open('.pgdsn', encoding='utf-8') as dsnf:
    dsn = dsnf.read().replace('\n', '')

# read the postgres login credentials for gdal from file
with open('.pggdal', encoding='utf-8') as gdalf:
    gdalpgcs = gdalf.read().replace('\n', '')

# # This is telegram credentials to send message to stepanosokin
with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    bot_info = (jdata['token'], jdata['chatid'])



with psycopg2.connect(dsn, cursor_factory=DictCursor) as pgconn:
    lastdt_result = get_latest_auc_result_date_from_synology(pgconn)
    if lastdt_result[0]:
        startdt = lastdt_result[1] + timedelta(days=1)
        clear_folder('rosnedra_auc_results')
        if download_auc_results(start=startdt, end=datetime.now(), search_string='Информация об итогах проведения аукциона на право пользования недрами', folder='rosnedra_auc_results', bot_info=bot_info):
            update_postgres_auc_results_table(pgconn, bot_info=bot_info)
            pass