# First you need to create long-lasting or infinite token to access the api.
# The easiest way is to use any http client to generate infinite token.
# For this solution the Postman app was used.
# To receive the token from Postman:
# 1. Create a POST request
# 2. use URL https://hst-api.wialon.host/login.html
# 3. go to the Authorization tab
# 4. Choose:
#       Auth Type - OAuth 2.0
#       under Configure New Token:
#           Token - type any name. I used 'vgdb-token'
#           Grant type - Implicit
#           Auth URL - https://hosting.wialon.host/login.html
#           Client ID - type any name for the app. I used 'python-wialon'
#       under Advanced / Auth Request:
#           client_id - type any name for the app. I used 'python-wialon'
#           access_type - 0x100
#           duration - 0
#           user - tpgk (login received from Wialon on registration)
#           response_type - token
# 5. Press the orange button Get New Access Token at the bottom
# 6. the popup window appears. Type in the password and press Login.
# 7. After succesful login, the token value will appear at the top in the Token field.
#    Copy it and paste to the .wialon_token file.
#
# Wialon API documentation: https://sdk.wialon.host/wiki/ru/sidebar/remoteapi/codesamples/login
#
#
# workflow to install pip package python-wialon on conda environment if it cannot be installed by conda directly:
# https://stackoverflow.com/questions/41060382/using-pip-to-install-packages-to-anaconda-environment
# conda env list
# conda activate <env name>
# conda install pip
# <path_to_conda_env>\Scripts\pip install python-wialon


import requests
from vgdb_general import smart_http_request, log_message
import psycopg2
import json
from synchro_evergis import get_free_port

# https://github.com/wialon/python-wialon
# https://forum.wialon.com/viewtopic.php?id=4661
# https://help.wialon.com/help/api/ru/user-guide/api-reference
# https://forum.wialon.com/viewtopic.php?id=4661
from wialon import Wialon, WialonError, flags      

from time import sleep
from fabric import Connection
import os

def login(s: requests.Session, tokenfile='.wialon_token'):
    with open(tokenfile, encoding='utf-8') as f:
        token = f.read()
    with s:
        url = 'https://hst-api.wialon.host/wialon/ajax.html'
        params = {
            "svc": "token/login",
            "params": "{\"token\":\"" + token + "\"}"
            }
        response, status = smart_http_request(s=s, url=url, params=params)
        # response = s.post(url, params=params)
        eid = None
        if status == 200:
            try:
                eid = response.json().get('eid')
            except:
                pass
        return eid
        pass


def wialon_session(host='hst-api.wialon.host', tokenpath='.wialon_token'):
    # https://github.com/wialon/python-wialon
    with open(tokenpath, encoding='utf-8') as f:
        token = f.read()
    try:
        wialon_api = Wialon(host=host)
        result = wialon_api.token_login(token=token, appName='python-wialon')
        wialon_api.sid = result['eid']
        return wialon_api
    except:
        return None


def get_units(w: Wialon):
    # https://sdk.wialon.host/wiki/en/sidebar/remoteapi/apiref/core/search_items
    # https://sdk.wialon.host/wiki/en/sidebar/remoteapi/codesamples/search#search_items_by_property
    # https://help.wialon.com/help/api/ru/user-guide/api-reference/core/search_items?q=null&start=0&scroll-translations:language-key=ru
    spec = {
            "itemsType": "avl_unit",
            "propName": "sys_name",
            "propValueMask": "*",
            "sortType": "sys_name"
            }
    interval = {"from": 0, "to": 0}
    try:
        units = w.core_search_items(spec=spec, force=1, flags=flags.ITEM_DATAFLAG_BASE, **interval)    # список доступных юнитов
        return units
    except:
        return None


def update_units_pg(dsn: str, units: dict):
    with open(dsn, encoding='utf-8') as f:
        pgdsn = f.read()
    pgconn = None    
    i = 1
    while not pgconn and i < 10:
        i += 1
        try:
            pgconn = psycopg2.connect(pgdsn)
        except:
            pass
    if pgconn:
        with pgconn:
            with pgconn.cursor() as cur:
                if units:
                    for unit in units.get('items'):
                        id = unit.get('id')
                        if id:
                            sql = f"select id from wialon.wialon_units;"
                            cur.execute(sql)
                            message = cur.statusmessage
                            id_list = [x[0] for x in cur.fetchall()]
                            if id not in id_list:
                                values = [str(unit.get('id')), 
                                          f"'{str(unit.get('nm'))}'", 
                                          str(unit.get('cls')), 
                                          str(unit.get('mu')),
                                          str(unit.get('uacl'))]
                                sql = f"insert into wialon.wialon_units(id, nm, cls, mu, uacl) " \
                                      f"values({', '.join(values)});"
                                cur.execute(sql)
                                pgconn.commit()
        pgconn.close()


def add_units_to_session(w: Wialon, units: dict):    
    try:
        spec = [{
                    "type": "col",
                    "data": [x.get('id') for x in units.get('items')],
                    "flags": 4294967295, 
                    "mode": 1,
                    "max_items": 0
                }]
        result = w.core_update_data_flags(spec=spec, flags=flags.ITEM_DATAFLAG_BASE)
        return True
    except:
        return False


def update_events_pg(dsn, tm: int, events: list):
    with open(dsn, encoding='utf-8') as f:
        pgdsn = f.read()
    pgconn = None    
    i = 1
    while not pgconn and i < 10:
        i += 1
        try:
            pgconn = psycopg2.connect(pgdsn)
        except:
            pass
    if pgconn:
        with pgconn:
            with pgconn.cursor() as cur:
                for evt in events:
                    tm = tm
                    id = evt['i']
                    t = evt['d']['t']
                    tp = evt['d'].get('tp')
                    if tp == 'ud':
                        x = evt['d']['pos']['x']
                        y = evt['d']['pos']['y']
                        z = evt['d']['pos']['z']
                        s = evt['d']['pos']['s']
                    elif tp == 'evt':
                        x = evt['d']['x']
                        y = evt['d']['y']
                        z = None
                        s = None
                    data = f"'{json.dumps(evt)}'"
                    if tp in ['ud', 'evt']:
                        sql = f"insert into wialon.wialon_evts(tm, id, t, x, y, z, s, tp, data) " \
                              f"values({str(tm)}, {str(id)}, {str(t)}, {str(x)}, {str(y)}, {str(z)}, {str(s)}, '{tp}', {data});"
                        cur.execute(sql)
                        pgconn.commit()
                        pass


def update_events_ssh_pg(ext_pgdsn_path, local_pgdsn_path, table, ssh_host='', ssh_user='',
                         bot_info=('token', 'id'), folder='wialon', log=False):
    result = False
    local_pgconn = None
    k = 1
    while not local_pgconn and k <= 5:
        try:
            k += 1
            with open(local_pgdsn_path, encoding='utf-8') as f:
                local_pgdsn = f.read()
            local_pgconn = psycopg2.connect(local_pgdsn)
        except:
            pass
    today_events = None
    if local_pgconn:
        sql = 'select tm, id, x, y, z, s, data, tp, t from wialon.wialon_evts where date(to_timestamp(t)) = date(now());'
        with local_pgconn.cursor() as cur:
            cur.execute(sql)
            today_events = cur.fetchall()
        local_pgconn.close()
    if today_events:
        pass
        with open(ext_pgdsn_path, encoding='utf-8') as f:
            ext_pgdsn = f.read()
        ext_pgdsn_dict = dict([x.split('=') for x in ext_pgdsn.split(' ')])
        current_directory = os.getcwd()
        log_file = os.path.join(current_directory, folder, 'ssh_logfile.txt')
        with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:
            if log:
                log_message(s, logf, bot_info, '<send_to_ssh_postgres> Начинаю синхронизацию таблиц с Evergis...')
            j = 1
            ssh_conn = None
            while not ssh_conn and j <= 2:
                if log:
                    log_message(s, logf, bot_info, f'<send_to_ssh_postgres> Установка подключения к удаленному серверу по SSH, попытка {str(j)}...', to_telegram=False)
                try:
                    j += 1
                    local_port_for_ext_pg = get_free_port((5433, 5440))
                    if local_port_for_ext_pg:
                        new_ext_pgdsn = ext_pgdsn.replace(f"port={ext_pgdsn_dict['port']}",
                                                        f"port={str(local_port_for_ext_pg)}")
                        new_ext_pgdsn_dict = dict([x.split('=') for x in new_ext_pgdsn.split(' ')])
                        new_ext_pgpass = os.path.join(current_directory, folder, '.new_ext_pgpass')
                        with open(new_ext_pgpass, 'w', encoding='utf-8') as f:
                            f.write(
                                f"{new_ext_pgdsn_dict['host']}:{new_ext_pgdsn_dict['port']}:{new_ext_pgdsn_dict['dbname']}:{new_ext_pgdsn_dict['user']}:{new_ext_pgdsn_dict['password']}")
                        os.chmod(new_ext_pgpass, 0o600)
                        ssh_conn = Connection(ssh_host, user=ssh_user, connect_kwargs={"banner_timeout": 60}).forward_local(local_port_for_ext_pg, remote_port=int(ext_pgdsn_dict['port']))
                except:
                    if log:
                        log_message(s, logf, bot_info, f'<send_to_ssh_postgres> Ошибка подключения к удаленному серверу по SSH (попытка {str(j)})', to_telegram=False)
            if not ssh_conn:
                if log:
                    log_message(s, logf, bot_info, '<send_to_ssh_postgres> Ошибка подключения к удаленному серверу по SSH')
                result = False
                return result
            with ssh_conn:
                if log:
                    log_message(s, logf, bot_info, f'<send_to_ssh_postgres> Подключение SSH установлено')
                my_env = os.environ.copy()
                my_env["PGPASSFILE"] = new_ext_pgpass
                i = 1
                pgconn = None
                while not pgconn and i <= 2:
                    i += 1
                    try:
                        new_ext_pgdsn = ext_pgdsn.replace(f"port={ext_pgdsn_dict['port']}",
                                                        f"port={str(local_port_for_ext_pg)}")
                        pgconn = psycopg2.connect(new_ext_pgdsn)
                    except:
                        pass
                if pgconn:
                    with pgconn.cursor() as cur:
                        sql = f'delete from {table} where date(to_timestamp(t)) < date(now());'
                        cur.execute(sql)
                        pgconn.commit()
                        sql = f'select max(t) from {table};'
                        cur.execute(sql)
                        latest_t = 0
                        dataresult = cur.fetchall()
                        if dataresult:
                            if dataresult[0][0]:
                                latest_t = dataresult[0][0]
                        today_events = [x for x in today_events if x[8] > latest_t]
                        if today_events:
                            values_to_insert = "(" + "), (".join([str(x[0]) + ", " + str(x[1]) + ", " + str(x[2]) + ", " + str(x[3]) + ", " + str(x[4]) + ", " + str(x[5]) + ", '" + str(x[6]).replace('\'', '\"') + "', '" + str(x[7]) + "', " + str(x[8]) for x in today_events]) + ")"
                            sql = f"insert into {table}(tm, id, x, y, z, s, data, tp, t) values{values_to_insert};"
                            pass
                            cur.execute(sql)
                            pgconn.commit()
                            message = cur.statusmessage
                            if 'INSERT' in message:
                                result = True
                            else:
                                result = False
                    pgconn.close()
            ssh_conn = None
    return result


def shrink_events_pg(dsn):
    with open(dsn, encoding='utf-8') as f:
        pgdsn = f.read()
    pgconn = None    
    i = 1
    while not pgconn and i < 10:
        i += 1
        try:
            pgconn = psycopg2.connect(pgdsn)
        except:
            pass
    if pgconn:
        with pgconn:
            with pgconn.cursor() as cur:
                sql = 'select count(*) from wialon.wialon_evts;' 
                cur.execute(sql)
                count = cur.fetchall()[0][0]
                extra = count - 10000
                if extra > 0:
                    sql = f"delete from wialon.wialon_evts where gid in (select gid from wialon.wialon_evts order by t ASC limit {str(extra)});"
                    cur.execute(sql)
                    pgconn.commit()
        pgconn.close()



if __name__ == '__main__':
    
    # w = wialon_session()

    # if w:
    #     units = get_units(w)
    #     if units:
    #         update_units_pg('.pgdsn', units)
    #         if add_units_to_session(w=w, units=units):
    #             pass
    #             # for _ in range(500):
    #             shrink_events_pg('.pgdsn')
    #             while True:
    #                 try:
    #                     data = w.avl_evts()
    #                     if data.get('events'):
    #                         events=[x for x in data['events'] if x['t'] == 'm']
    #                         update_events_pg('.pgdsn', tm=data['tm'], events=events)
    #                     sleep(1)
    #                 except:
    #                     try:
    #                         w.core_logout()
    #                     except:
    #                         pass
    #                     w = wialon_session()
    #                     add_units_to_session(w=w, units=units)



    # w.core_logout()


    with open('.egssh', 'r', encoding='utf-8') as f:
        egssh = json.load(f)
    with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
        jdata = json.load(f)
        bot_info = (jdata['token'], jdata['chatid'])
    update_events_ssh_pg('.ext_pgdsn','.pgdsn', 'wialon.wialon_evts', ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)


    
    if False:
        # https://sdk.wialon.host/wiki/en/local/remoteapi1904/codesamples/update_datafalags?s[]=avl&s[]=evts
        
        pause = 1
        try:
            wialon_api = Wialon(host='hst-api.wialon.host')
            with open('.wialon_token', encoding='utf-8') as f:
                token = f.read()
            result = wialon_api.token_login(token=token, appName='python-wialon')    
            wialon_api.sid = result['eid']       
            
            # https://sdk.wialon.host/wiki/en/sidebar/remoteapi/apiref/core/search_items
            # https://sdk.wialon.host/wiki/en/sidebar/remoteapi/codesamples/search#search_items_by_property
            # https://help.wialon.com/help/api/ru/user-guide/api-reference/core/search_items?q=null&start=0&scroll-translations:language-key=ru
            spec = {
                'itemsType': 'avl_unit',
                'propName': 'sys_name',
                'propValueMask': '*',
                'sortType': 'sys_name'
                }
            interval = {"from": 0, "to": 0}
            units = wialon_api.core_search_items(spec=spec, force=1, flags=flags.ITEM_DATAFLAG_BASE, **interval)    # список доступных юнитов
            print(units)
            
            # https://sdk.wialon.host/wiki/en/kit/remoteapi/apiref/core/update_data_flags
            # https://help.wialon.com/help/api/ru/user-guide/api-reference/core/update_data_flags
            spec = [{
                "type": "col",
                "data": [x['id'] for x in units['items']],
                "flags": 4294967295, 
                "mode": 1,
                "max_items": 0
            }]
            result = wialon_api.core_update_data_flags(spec=spec, flags=flags.ITEM_DATAFLAG_BASE)   # добавление всех доступных юнитов в сессию
            
            # https://sdk.wialon.host/wiki/en/local/remoteapi1904/apiref/requests/avl_evts
            # https://help.wialon.com/help/api/ru/user-guide/api-reference/requests/avl_evts
            data = wialon_api.avl_evts()        # получить новое событие
            sleep(pause)
            
            for _ in range(30):
                print(data)
                data = wialon_api.avl_evts()
                sleep(pause)
            
            wialon_api.core_logout()
            pass
        except WialonError as e:
            print(e)
            pass

        # Description of 'pos' element
        # https://sdk.wialon.host/wiki/en/sidebar/remoteapi/apiref/format/unit#position

        # В ответ на avl_evts приходит json с событиями. 
        # {
        #     "tm": <uint>, /* message time (UTC) */
        #     "events": [....]    /* список СОБЫТИЙ */
        # }
        # 
        # Каждое СОБЫТИЕ имеет структуру:
        # {
        #     "i": <uint>,        /* идентификатор объекта */
        #     "t": 'm',           /* здесь не уверен, скорее всего тип события - СООБЩЕНИЕ */
        #     "d": {MESSAGE}      /* Это само сообщение - MESSAGE */
        # }
        # 
        # данные, вложенные в событие - это MESSAGE.
        # Типы разных MESSAGE описаны тут: https://sdk.wialon.host/wiki/en/local/remoteapi1904/apiref/format/messages
        # Нас интересуют два типа MESSAGE: 'Message with data' и 'Event'.
        # 
        # Формат Message with data - https://sdk.wialon.host/wiki/en/local/remoteapi1904/apiref/format/messages#message_with_data
        # {
        #     "t":<uint>,		/* message time (UTC) */
        #     "f":<uint>,		/* flags (see below)*/
        #     "tp":"ud",		/* message type (ud - message with data) */
        #     "pos":{			/* position */
        #         "y":<double>,		/* latitude */
        #         "x":<double>,		/* longitude */
        #         "z":<int>,		/* altitude */
        #         "s":<uint>		/* speed */
        #         "c":<uint>,		/* course */
        #         "sc":<ubyte>		/* satellites count */
        #     },
        #     "i":<uint>,		/* input data */
        #     "o":<uint>,		/* output data */
        #     "p":{			/* parameters */
        #         <text>:<double>,
        #         ...	
        #     }
        # }
        # 
        # формат Event - https://sdk.wialon.host/wiki/en/local/remoteapi1904/apiref/format/messages#event
        # {
        #     "t":<uint>,	/* message time (UTC) */
        #     "f":<uint>,	/* flags (see below) */
        #     "tp":"evt",	/* message type (evt - event) */
        #     "et":<text>,	/* text of event */
        #     "x":<double>,	/* longitude */
        #     "y":<double>,	/* latitude */
        #     "p":{}		/* parameters */
        # }
        #
        # Содержание объекта pos /* position */:
        # https://sdk.wialon.host/wiki/en/sidebar/remoteapi/apiref/format/unit#position
        # {
        #     "pos":{			/* last known position */
        #         "t":<uint>,		/* time (UTC) */
        #         "y":<double>,		/* latitude */
        #         "x":<double>,		/* longitude */
        #         "z":<double>,		/* altitude */
        #         "s":<int>,		/* speed */
        #         "c":<int>,		/* course */
        #         "sc":<int>		/* satellites count */
        #     }
        # }