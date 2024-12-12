import requests, json, psycopg2
from vgdb_general import smart_http_request


def download_license_blocks(ofile):
    with requests.Session() as s:
        url = 'https://uvspwa.sgp72.ru/search?extent=-2091831.35892269;3123966.9171396266;-2088777.3004859157;3127020.9755764008'
        headers = {
            'Dnt': '1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Aser-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0'
        }
        status1, result = smart_http_request(s, url=url, headers=headers)
        if status1 == 200:
            # url = 'https://uvspwa.sgp72.ru/api/map/0/0/0'
            jdata = []
            # for x in range(840, 851):
            for y in range(726, 918):
                # for y in range(960, 971):
                for x in range(820, 1196):
                    url = f'https://uvspwa.sgp72.ru/api/map/11/{str(y)}/{str(x)}'
                    status2, result = smart_http_request(s, url=url, headers=headers)
                    if status2 == 200:
                        jresult = result.json()
                        if jresult:
                            jdata += jresult
                            pass
            if jdata:
                with open(ofile, 'w', encoding='utf-8') as f:
                    json.dump(jdata, f, ensure_ascii=False)
                    pass


def send_license_blocks_to_postgres(ifile, pgdsn):
    with open(ifile, 'r', encoding='utf-8') as f:
        data = json.load(f)
    data = {r['objectGuid']: r for r in data}.values()
    i = 1
    pgconn = None
    while not pgconn and i <= 10:
        try:
            i += 1
            pgconn = psycopg2.connect(pgdsn)
        except:
            pass
    if pgconn:
        with pgconn.cursor() as cur:
            sql = 'delete from uvswpa_sgp72_ru.uvswpa_license_blocks;'
            cur.execute(sql)
            pgconn.commit()
            for block in data:
                if block['type'] == 'licenses':
                    try:
                        # sql = f"insert into uvswpa_sgp72_ru.uvswpa_license_blocks" \
                        #     f"(\"objectGuid\", name, type, geometry, geom) " \
                        sql = f"insert into uvswpa_sgp72_ru.uvswpa_license_blocks" \
                            f"(\"objectGuid\", name, type, geometry) " \
                            f"values('{block['objectGuid']}', " \
                            f"'{block['name']}', " \
                            f"'{block['type']}', " \
                            f"'{block['geometry']}');" \
                            
                            # f"st_geomfromwkb(decode('{block['geometry']}', 'hex'), 102027));"
                        cur.execute(sql)
                    except:
                        pass
        pgconn.commit()
        pgconn.close()
            


if __name__ == '__main__':
    with open('.pgdsn', encoding='utf-8') as f:
        pgdsn = f.read()

    # download_license_blocks('uvswpa_sgp72_ru/license_blocks.json')
    send_license_blocks_to_postgres('uvswpa_sgp72_ru/license_blocks.json', pgdsn)