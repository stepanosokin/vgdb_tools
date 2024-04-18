import json, requests, psycopg2

def download_wells_geometry():
    '''
    информация по загрузке данных с сайта kern.vnigni.ru почерпнута с сайта https://kern.vnigni.ru/well/catalog.
    Включаем dev mode. Переходим в режим Карта. Видим HTTP POST запрос wellsgeometry, который запрашивает
    данные с адреса https://kern.vnigni.ru/api/kern/objects/wellsgeometry с отправкой JSON-структуры.
    Далее информацию по керну каждой скважины можно получить, приписав guid скважины к адресу
    https://kern.vnigni.ru/well/catalog/
    :return: JSON
    '''
    # data = {"filters":{"attr": [],"fulltext": None}}
    data = '{"filters":{"attr":[],"fulltext":null}}'
    url = 'https://kern.vnigni.ru/api/kern/objects/wellsgeometry'
    headers = {"Accept": "application/json, text/plain, */*",
               "Accept-Encoding": "gzip, deflate, br, zstd",
               "Accept-Language": "ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
               "Connection": "keep-alive",
               "Content-Length": "39",
               "Content-Type": "application/json",
               "Cookie": "_pk_id.7.0d49=e77bba19d5d73e49.1713438233.1.1713438657.1713438233.",
               "Dnt": "1",
               "Host": "kern.vnigni.ru",
               "Origin": "https://kern.vnigni.ru",
               "Referer": "https://kern.vnigni.ru/well/catalog",
               "Sec-Ch-Ua": '"Microsoft Edge";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
               "Sec-Ch-Ua-Mobile": "?0",
               "Sec-Ch-Ua-Platform": "Windows",
               "Sec-Fetch-Dest": "empty",
               "Sec-Fetch-Mode": "cors",
               "Sec-Fetch-Site": "same-origin",
               "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
               "X-Requested-With": "XMLHttpRequest"}
    with requests.Session() as s:
        code = 0
        tries = 1
        while code != 200 and tries <= 10:
            try:
                tries += 1
                response = s.post(url, headers=headers, data=data)
                code = response.status_code
                pass
            except:
                print(f"Ошибка загрузки данных с kern.vnigni.ru (попытка {str(tries)})")
    if code == 200:
        result = response.json()
        if isinstance(result, list) and len(result) > 0:
            return result
        elif isinstance(result, dict):
            info = result.get('info')
            message = result.get('message')
            if info and message:
                notice = info.get('notice')
                if notice:
                    print(f"{notice['type']}: {message}")
            return None


def load_wells_to_postgis(wellsgeom, pgdsn, table='kern_vnigni_ru.wellsgeom'):
    sql = f"CREATE TABLE IF NOT EXISTS {table}" \
          f"(" \
          f"gid serial, " \
          f"well character varying, " \
          f"well_guid character varying, " \
          f"depth numeric, " \
          f"type json, " \
          f"type_guid character varying, " \
          f"type_name character varying, " \
          f"lat numeric, " \
          f"lon numeric, " \
          f"geom geometry, " \
          f"CONSTRAINT wellsgeom_pkey PRIMARY KEY (gid)" \
          f");"
    i = 1
    pgconn = None
    while not pgconn and i <= 10:
        i += 1
        try:
            pgconn = psycopg2.connect(pgdsn)
        except:
            print(f"Ошибка подключения к postgres (попытка {str(i)})")
    if pgconn:
        with pgconn:
            with pgconn.cursor() as cur:
                cur.execute(sql)
                sql = f"delete from {table};"
                cur.execute(sql)
                sql = f"insert into {table}(well, well_guid, depth, type, type_guid, type_name, lat, lon, geom) values"

                values = []
                for x in wellsgeom:
                    row = '('
                    row += (chr(39) + x['well'] + chr(39) + ', ')
                    row += (chr(39) + x['well_guid'] + chr(39) + ', ')
                    row += (str(x['depth']) + ', ')
                    row += (chr(39) + json.dumps(x['type'], ensure_ascii=False) + chr(39) + ', ')
                    row += (chr(39) + str(x['type'].get('guid', None)) + chr(39) + ', ')
                    row += (chr(39) + str(x['type'].get('name', None)) + chr(39) + ', ')
                    row += (str(x['lat']) + ', ')
                    row += (str(x['lon']) + ', ')
                    if x['lon'] and x['lat'] and x['lon'] != 0 and x['lat'] != 0:
                        row += ('st_geomfromtext(' + chr(39) + 'POINT(' + str(x['lat']) + ' ' + str(x['lon']) + ')' + chr(39) + ', 4326' + ')')
                    else:
                        row += 'NULL'
                    row += ')'
                    row = row.replace('None', 'NULL')
                    values.append(row)

                sql += ', '.join(values)
                sql += ';'
                cur.execute(sql)
                # result = cur.fetchall()
                pass
        pgconn.close()


if __name__ == '__main__':
    wellsgeom = download_wells_geometry()
    if wellsgeom:
        with open('.pgdsn', encoding='utf-8') as dsnf:
            pgdsn = dsnf.read().replace('\n', '')
        load_wells_to_postgis(wellsgeom, pgdsn)