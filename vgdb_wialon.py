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
from vgdb_general import smart_http_request
import psycopg2

# https://github.com/wialon/python-wialon
# https://forum.wialon.com/viewtopic.php?id=4661
# https://help.wialon.com/help/api/ru/user-guide/api-reference
# https://forum.wialon.com/viewtopic.php?id=4661
from wialon import Wialon, WialonError, flags      

from time import sleep

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


if __name__ == '__main__':
    
    w = wialon_session()
    if w:
        units = get_units(w)
        if units:
            update_units_pg('.pgdsn', units)
    w.core_logout()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    # # https://sdk.wialon.host/wiki/en/local/remoteapi1904/codesamples/update_datafalags?s[]=avl&s[]=evts
    
    # pause = 1
    # try:
    #     wialon_api = Wialon(host='hst-api.wialon.host')
    #     with open('.wialon_token', encoding='utf-8') as f:
    #         token = f.read()
    #     result = wialon_api.token_login(token=token, appName='python-wialon')    
    #     wialon_api.sid = result['eid']       
        
    #     # https://sdk.wialon.host/wiki/en/sidebar/remoteapi/apiref/core/search_items
    #     # https://sdk.wialon.host/wiki/en/sidebar/remoteapi/codesamples/search#search_items_by_property
    #     # https://help.wialon.com/help/api/ru/user-guide/api-reference/core/search_items?q=null&start=0&scroll-translations:language-key=ru
    #     spec = {
    #         'itemsType': 'avl_unit',
    #         'propName': 'sys_name',
    #         'propValueMask': '*',
    #         'sortType': 'sys_name'
    #         }
    #     interval = {"from": 0, "to": 0}
    #     units = wialon_api.core_search_items(spec=spec, force=1, flags=flags.ITEM_DATAFLAG_BASE, **interval)    # список доступных юнитов
    #     print(units)
        
    #     # https://sdk.wialon.host/wiki/en/kit/remoteapi/apiref/core/update_data_flags
    #     # https://help.wialon.com/help/api/ru/user-guide/api-reference/core/update_data_flags
    #     spec = [{
    #         "type": "col",
    #         "data": [x['id'] for x in units['items']],
    #         "flags": 4294967295, 
    #         "mode": 1,
    #         "max_items": 0
    #     }]
    #     result = wialon_api.core_update_data_flags(spec=spec, flags=flags.ITEM_DATAFLAG_BASE)   # добавление всех доступных юнитов в сессию
        
    #     # https://sdk.wialon.host/wiki/en/local/remoteapi1904/apiref/requests/avl_evts
    #     # https://help.wialon.com/help/api/ru/user-guide/api-reference/requests/avl_evts
    #     data = wialon_api.avl_evts()        # получить новое событие
    #     sleep(pause)
        
    #     for _ in range(30):
    #         print(data)
    #         data = wialon_api.avl_evts()
    #         sleep(pause)
        
    #     wialon_api.core_logout()
    #     pass
    # except WialonError as e:
    #     print(e)
    #     pass

    # # Description of 'pos' element
    # # https://sdk.wialon.host/wiki/en/sidebar/remoteapi/apiref/format/unit#position

    # # В ответ на avl_evts приходит json с событиями. 
    # # {
    # #     "tm": <uint>, /* message time (UTC) */
    # #     "events": [....]    /* список СОБЫТИЙ */
    # # }
    # # 
    # # Каждое СОБЫТИЕ имеет структуру:
    # # {
    # #     "i": <uint>,        /* идентификатор объекта */
    # #     "t": 'm',           /* здесь не уверен, скорее всего тип события - СООБЩЕНИЕ */
    # #     "d": {MESSAGE}      /* Это само сообщение - MESSAGE */
    # # }
    # # 
    # # данные, вложенные в событие - это MESSAGE.
    # # Типы разных MESSAGE описаны тут: https://sdk.wialon.host/wiki/en/local/remoteapi1904/apiref/format/messages
    # # Нас интересуют два типа MESSAGE: 'Message with data' и 'Event'.
    # # 
    # # Формат Message with data - https://sdk.wialon.host/wiki/en/local/remoteapi1904/apiref/format/messages#message_with_data
    # # {
    # #     "t":<uint>,		/* message time (UTC) */
    # #     "f":<uint>,		/* flags (see below)*/
    # #     "tp":"ud",		/* message type (ud - message with data) */
    # #     "pos":{			/* position */
    # #         "y":<double>,		/* latitude */
    # #         "x":<double>,		/* longitude */
    # #         "z":<int>,		/* altitude */
    # #         "s":<uint>		/* speed */
    # #         "c":<uint>,		/* course */
    # #         "sc":<ubyte>		/* satellites count */
    # #     },
    # #     "i":<uint>,		/* input data */
    # #     "o":<uint>,		/* output data */
    # #     "p":{			/* parameters */
    # #         <text>:<double>,
    # #         ...	
    # #     }
    # # }
    # # 
    # # формат Event - https://sdk.wialon.host/wiki/en/local/remoteapi1904/apiref/format/messages#event
    # # {
    # #     "t":<uint>,	/* message time (UTC) */
    # #     "f":<uint>,	/* flags (see below) */
    # #     "tp":"evt",	/* message type (evt - event) */
    # #     "et":<text>,	/* text of event */
    # #     "x":<double>,	/* longitude */
    # #     "y":<double>,	/* latitude */
    # #     "p":{}		/* parameters */
    # # }
    # #
    # # Содержание объекта pos /* position */:
    # # https://sdk.wialon.host/wiki/en/sidebar/remoteapi/apiref/format/unit#position
    # # {
    # #     "pos":{			/* last known position */
    # #         "t":<uint>,		/* time (UTC) */
    # #         "y":<double>,		/* latitude */
    # #         "x":<double>,		/* longitude */
    # #         "z":<double>,		/* altitude */
    # #         "s":<int>,		/* speed */
    # #         "c":<int>,		/* course */
    # #         "sc":<int>		/* satellites count */
    # #     }
    # # }