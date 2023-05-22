from download_auction_lic_from_rosnedra import *
from datetime import datetime, timedelta

with open('.pgdsn', encoding='utf-8') as dsnf:
    dsn = dsnf.read().replace('\n', '')

with open('.pggdal', encoding='utf-8') as gdalf:
    gdalpgcs = gdalf.read().replace('\n', '')

with psycopg2.connect(dsn) as pgconn:
    startdt = get_latest_order_date_from_synology(pgconn) + timedelta(days=1)

# bot_info = ('5576469760:AAGs39cBmZM-lfhzolRdT7N-fvK0hsjrdTc', '165098508')
bot_info = ('5576469760:AAGs39cBmZM-lfhzolRdT7N-fvK0hsjrdTc', '-987181064')

clear_folder('rosnedra_auc')

download_orders(start=startdt, end=datetime.now(), search_string='Об утверждении Перечня участков недр', folder='rosnedra_auc', bot_info=bot_info)

parse_blocks_from_orders(folder='rosnedra_auc', gpkg='rosnedra_result.gpkg', bot_info=bot_info)
#
# # this is a test comment #3
update_synology_table(gdalpgcs, folder='rosnedra_auc', bot_info=bot_info)