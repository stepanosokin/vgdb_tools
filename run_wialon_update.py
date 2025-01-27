import requests
from vgdb_general import smart_http_request
import psycopg2
import json
from vgdb_wialon import *
from synchro_evergis import *

w = wialon_session()

if w:
    units = get_units(w)
    if units:
        update_units_pg('.pgdsn', units)
        if add_units_to_session(w=w, units=units):
            pass
            # for _ in range(500):
            shrink_events_pg('.pgdsn')
    w.core_logout()

with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
    jdata = json.load(f)
    log_bot_info = (jdata['token'], jdata['chatid'])

with open('.egssh', 'r', encoding='utf-8') as f:
    egssh = json.load(f)

synchro_table([('wialon', ['wialon_units', 'wialon_evts'])], '.pgdsn', '.ext_pgdsn', ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=log_bot_info)

