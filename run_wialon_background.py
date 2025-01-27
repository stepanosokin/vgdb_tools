import requests
from vgdb_general import smart_http_request
import psycopg2
import json
from vgdb_wialon import *

w = wialon_session()

if w:
    units = get_units(w)
    if units:
        update_units_pg('.pgdsn', units)
        if add_units_to_session(w=w, units=units):
            pass
            # for _ in range(500):
            shrink_events_pg('.pgdsn')
            while True:
                data = w.avl_evts()
                if data.get('events'):
                    events=[x for x in data['events'] if x['t'] == 'm']
                    update_events_pg('.pgdsn', tm=data['tm'], events=events)
                sleep(1)

    w.core_logout()