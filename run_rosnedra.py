from vgdb_auctions_rosnedra import *
from datetime import datetime, timedelta
import json, psycopg2
from synchro_evergis import *

# read the postgres login credentials in dsn format from file
with open('.pgdsn', encoding='utf-8') as dsnf:
    dsn = dsnf.read().replace('\n', '')

with open('.ext_pgdsn', encoding='utf-8') as edsnf:
    ext_dsn = edsnf.read().replace('\n', '')

# read the postgres login credentials for gdal from file
with open('.pggdal', encoding='utf-8') as gdalf:
    gdalpgcs = gdalf.read().replace('\n', '')

# # This is telegram credentials to send message to stepanosokin
with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    bot_info = (jdata['token'], jdata['chatid'])

# This is telegram credentials to send message to the 'VG Database Techinfo' group
# with open('bot_info_vgdb_bot_toGroup.json', 'r', encoding='utf-8') as f:
#     jdata = json.load(f)
#     bot_info = (jdata['token'], jdata['chatid'])

# This is telegram credentials to send message to the 'VG Database Techinfo' group
with open('bot_info_vgdb_bot_toAucGroup.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    report_bot_info = (jdata['token'], jdata['chatid'])

# # This is telegram credentials to send message to teams
with open('2025_blocks_nr_ne.webhook', 'r', encoding='utf-8') as f:
    blocks_nr_ne_webhook = f.read().replace('\n', '')

with open('2025_blocks_np.webhook', 'r', encoding='utf-8') as f:
    blocks_np_webhook = f.read().replace('\n', '')

with open('.egssh', 'r', encoding='utf-8') as f:
    egssh = json.load(f)




current_dir = os.getcwd()
proj_path = os.path.join(current_dir, '.venv', 'Lib', 'site-packages', 'osgeo', 'data', 'proj')
gdal_path = os.path.join(current_dir, '.venv', 'Lib', 'site-packages', 'osgeo', 'data', 'gdal')
if os.path.isdir(proj_path):
    os.environ["PROJ_LIB"] = os.path.join(current_dir, '.venv', 'Lib', 'site-packages', 'osgeo', 'data', 'proj')
else:
    proj_path = os.path.join(current_dir, '.venv', 'lib', 'python3.12', 'site-packages', 'osgeo', 'data', 'proj')
    if os.path.isdir(proj_path):
        os.environ["PROJ_LIB"] = proj_path
if os.path.isdir(gdal_path):
    os.environ["GDAL_DATA"] = os.path.join(current_dir, '.venv', 'Lib', 'site-packages', 'osgeo', 'data', 'gdal')
else:
    gdal_path = os.path.join(current_dir, '.venv', 'lib', 'python3.12', 'site-packages', 'osgeo', 'data', 'gdal')
    if os.path.isdir(proj_path):
        os.environ["GDAL_DATA"] = proj_path



# get the latest rosnedra order announce date from postgres
# pgconn = psycopg2.connect(dsn)
# with psycopg2.connect(dsn) as pgconn:
update1, update2 = False, False
lastdt_result = get_latest_order_date_from_synology(dsn, field='order_date')
# lastdt_result = (True, datetime.strptime('2025-01-01', '%Y-%m-%d'))
if lastdt_result[0]:
    startdt = lastdt_result[1] + timedelta(days=1)
    # clear any previous results from folder
    clear_folder('rosnedra_auc')
    # # download newly announced Rosnedra orders since last loaded to database
    # if search_download_orders(start=startdt, end=datetime.now(), search_string='Об утверждении Перечня участков недр', folder='rosnedra_auc', bot_info=bot_info):
    if docs_download_orders(start=startdt, end=datetime.now(), folder='rosnedra_auc', bot_info=bot_info, npages=5):
    # if True:
        # # parse the blocks from order announcements to geopackage
        if parse_blocks_from_orders(folder='rosnedra_auc', gpkg='rosnedra_result.gpkg',
                                    bot_info=bot_info, report_bot_info=bot_info,
                                    blocks_np_webhook=blocks_np_webhook,
                                    blocks_nr_ne_webhook=blocks_nr_ne_webhook,
                                    dsn=dsn):
            # # load new blocks to the database
            update1 = update_postgres_table(gdalpgcs, folder='rosnedra_auc', bot_info=bot_info)
# update2 = update_rfgf_gos_reg_num(dsn, bot_info=bot_info, report_bot_info=report_bot_info)
update2 = update_rfgf_gos_reg_num(dsn, bot_info=bot_info, report_bot_info=bot_info)


# if update1 or update2:
#     synchro_layer([('rosnedra', ['license_blocks_rosnedra_orders'])], dsn, ext_dsn, ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)










#lastdt_result = get_latest_auc_result_date_from_synology(pgconn)
#if lastdt_result[0]:
#    startdt = lastdt_result[1] + timedelta(days=1)
#    clear_folder('rosnedra_auc_results')
#    if download_auc_results(start=startdt, end=datetime.now(),
#                            search_string='Информация об итогах проведения аукциона на право пользования недрами',
#                            folder='rosnedra_auc_results', bot_info=bot_info):
#        update_postgres_auc_results_table(pgconn, bot_info=bot_info)
