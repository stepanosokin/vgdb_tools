import requests
from vgdb_general import smart_http_request
import psycopg2
import json
from vgdb_wialon import *




while True:
    try:
        w = wialon_session()
        if w:
            units = get_units(w)
            if units:
                update_units_pg('.pgdsn', units)
                if add_units_to_session(w=w, units=units):
                    pass                
                    shrink_events_pg('.pgdsn')
                    for _ in range(1000):
                    # while True:
                        try:
                            data = w.avl_evts()
                            if data.get('events'):
                                events=[x for x in data['events'] if x['t'] == 'm']
                                update_events_pg('.pgdsn', tm=data['tm'], events=events)
                            sleep(1)
                        except:
                            try:
                                w.core_logout()
                            except:
                                pass
                            w = wialon_session()
                            add_units_to_session(w=w, units=units)
        w.core_logout()
    except:
        pass