import requests, json, psycopg2, os
from datetime import datetime, timedelta
from vgdb_general import send_to_telegram, log_message, send_to_teams
from psycopg2.extras import *
from synchro_evergis import *
# from mapbox import Static
# import polyline
from requests.utils import quote
import paramiko
from scp import SCPClient
import locale


def download_lotcards(size=1000, log_bot_info=('token', 'chatid'), report_bot_info=('token', 'chatid'), logfile='torgi_gov_ru/logfile.txt'):
      url = 'https://torgi.gov.ru/new/api/public/lotcards/search' \
            '?chars=&chars=sl-mineralResource:hydrocarbon' \
            '&catCode=609' \
            '&byFirstVersion=true' \
            '&withFacets=true' \
            f'&size={size}' \
            f'&sort=firstVersionPublicationDate,desc'
      headers = {'accept': 'application/json, text/plain, */*',
               'accept-encoding': 'gzip, deflate, br',
               'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,en-GB;q=0.6',
               'Connection': 'keep-alive',
               # 'Cookie:': 'SESSION=NGVlZjQxNTYtMDUxYi00YmIxLTgyYTAtNWQxMGEzZjVhOGU0',
               'authorization': 'Bearer NoAuth',
               'content-type': 'application/json',
               'dnt': '1',
               'Referer': 'https://torgi.gov.ru/new/public/lots/reg'
               # 'Traceparent': '00-7da9ccb214edb92f16dda26cc382acbe-894a255635a77534-01'
               }

      with open(logfile, 'a', encoding='utf-8') as logf, requests.Session() as s:
            message = 'torgi_gov_ru: Запущена загрузка лотов на участки УВС с сайта torgi.gov.ru'
            log_message(s, logf, log_bot_info, message)
            status_code = 0
            i = 1
            while status_code != 200 and i <= 10:
                  try:
                        response = s.get(url, headers=headers, verify=False)
                        status_code = response.status_code
                  except:
                        pass
                  i += 1
            if status_code != 200:
                  message = f'Ошибка: не удалось загрузить данные с сайта torgi.gov.ru после {str(i)} попыток'
                  log_message(s, logf, log_bot_info, message)
                  return False
            else:
                  message = f"torgi_gov_ru: Выполнена загрузка лотов на участки УВС с сайта torgi.gov.ru. Загружено {len(list(response.json()['content']))} лотов"
                  log_message(s, logf, log_bot_info, message)
                  return response.json()['content']


def parse_lotcard(lotcard):
    lotcard_id = lotcard.get('id')
    if lotcard_id:
        lotcard_dict = {"id": f"'{lotcard_id}'"}
        if lotcard.get('noticeNumber'):
            noticeNumber = lotcard['noticeNumber'].replace("'", "''")
            lotcard_dict['noticeNumber'] = f"'{noticeNumber}'"
        if lotcard.get('lotNumber'):
            lotcard_dict['lotNumber'] = lotcard['lotNumber']
        if lotcard.get('lotStatus'):
            lotStatus = lotcard['lotStatus'].replace("'", "''")
            lotcard_dict['lotStatus'] = f"'{lotStatus}'"
        if lotcard.get('biddType'):
            if lotcard['biddType'].get('code'):
                biddType_code = lotcard['biddType']['code'].replace("'", "''")
                lotcard_dict['biddType_code'] = f"'{biddType_code}'"
            if lotcard['biddType'].get('name'):
                biddType_name = lotcard['biddType']['name'].replace("'", "''")
                lotcard_dict['biddType_name'] = f"'{biddType_name}'"
        if lotcard.get('biddForm'):
            if lotcard['biddForm'].get('code'):
                biddForm_code = lotcard['biddForm']['code'].replace("'", "''")
                lotcard_dict['biddForm_code'] = f"'{biddForm_code}'"
            if lotcard['biddForm'].get('name'):
                biddForm_name = lotcard['biddForm']['name'].replace("'", "''")
                lotcard_dict['biddForm_name'] = f"'{biddForm_name}'"
        if lotcard.get('lotName'):
            lotName = lotcard['lotName'].replace("'", "''")
            lotcard_dict['lotName'] = f"'{lotName}'"
        if lotcard.get('lotDescription'):
            lotDescription = lotcard['lotDescription'].replace("'", "''")
            lotcard_dict['lotDescription'] = f"'{lotDescription}'"
        if lotcard.get('priceMin'):
            lotcard_dict['priceMin'] = lotcard['priceMin']
        if lotcard.get('priceFin'):
            lotcard_dict['priceFin'] = lotcard['priceFin']
        if lotcard.get('biddEndTime'):
            lotcard_dict[
                'biddEndTime'] = f"'{datetime.fromisoformat(lotcard['biddEndTime']).strftime('%Y-%m-%d %H:%M:%S')}'"
        if lotcard.get('characteristics'):
            for charstic in lotcard['characteristics']:
                if charstic.get('code') == 'mineralResource':
                    if charstic['characteristicValue'].get('value'):
                        mineralResource = charstic['characteristicValue']['value'].replace("'", "''")
                        lotcard_dict['mineralResource'] = f"'{mineralResource}'"
                if charstic.get('code') == 'resourceCategory':
                    if charstic['characteristicValue'].get('value'):
                        resourceCategory = charstic['characteristicValue']['value'].replace("'", "''")
                        lotcard_dict['resourceCategory'] = f"'{resourceCategory}'"
                if charstic.get('code') == 'squareMR':
                    if charstic.get('characteristicValue'):
                        lotcard_dict['squareMR'] = charstic['characteristicValue']
                if charstic.get('code') == 'resourcePotential':
                    if charstic.get('characteristicValue'):
                        resourcePotential = charstic['characteristicValue'].replace("'", "''")
                        lotcard_dict['resourcePotential'] = f"'{resourcePotential}'"
                if charstic.get('code') == 'resourceAreaId':
                    if charstic.get('characteristicValue'):
                        if isinstance(charstic['characteristicValue'], str):
                            lotcard_dict['resourceAreaId'] = charstic['characteristicValue']
                        elif charstic['characteristicValue'].get('value'):
                            lotcard_dict['resourceAreaId'] = charstic['characteristicValue']['value']
        if lotcard.get('currencyCode'):
            currencyCode = lotcard['currencyCode'].replace("'", "''")
            lotcard_dict['currencyCode'] = f"'{currencyCode}'"
        if lotcard.get('etpCode'):
            etpCode = lotcard['etpCode'].replace("'", "''")
            lotcard_dict['etpCode'] = f"'{etpCode}'"
        if lotcard.get('createDate'):
            lotcard_dict[
                'createDate'] = f"'{datetime.fromisoformat(lotcard['createDate']).strftime('%Y-%m-%d %H:%M:%S')}'"
        if lotcard.get('timeZoneName'):
            timeZoneName = lotcard['timeZoneName'].replace("'", "''")
            lotcard_dict['timeZoneName'] = f"'{timeZoneName}'"
        if lotcard.get('timezoneOffset'):
            timezoneOffset = lotcard['timezoneOffset'].replace("'", "''")
            lotcard_dict['timeZoneOffset'] = f"'{timezoneOffset}'"
        if 'hasAppeals' in lotcard.keys():
            lotcard_dict['hasAppeals'] = f"'{lotcard['hasAppeals']}'"
        if 'isStopped' in lotcard.keys():
            lotcard_dict['isStopped'] = f"'{lotcard['isStopped']}'"
        if 'isAnnuled' in lotcard.keys():
            lotcard_dict['isAnnuled'] = f"'{lotcard['isAnnuled']}'"
        if lotcard.get('attributes'):
            for attr in lotcard['attributes']:
                if attr.get('code') == 'miningSiteName_EA(N)':
                    if attr.get('value'):
                        miningSiteName = attr['value'].replace("'", "''")
                        lotcard_dict['miningSiteName_EA(N)'] = f"'{miningSiteName}'"
                if attr.get('code') == 'resourceTypeUse_EA(N)':
                    if attr.get('value'):
                        if attr.get('value').get('name'):
                            resourceTypeUse = attr['value']['name'].replace("'", "''")
                            lotcard_dict['resourceTypeUse_EA(N)'] = f"'{resourceTypeUse}'"
                if attr.get('code') == 'resourceLocation_EA(N)':
                    if attr.get('value'):
                        resourceLocation = ', '.join([x.get('name').replace("'", "''") for x in attr['value']])
                        lotcard_dict['resourceLocation_EA(N)'] = f"'{resourceLocation}'"
                if attr.get('code') == 'conditionsOfUse_EA(N)':
                    if attr.get('value'):
                        conditionsOfUse = attr['value'].replace("'", "''")
                        lotcard_dict['conditionsOfUse_EA(N)'] = f"'{conditionsOfUse}'"
                if attr.get('code') == 'licensePeriod_EA(N)':
                    if attr.get('value'):
                        licensePeriod = attr['value'].replace("'", "''")
                        lotcard_dict['licensePeriod_EA(N)'] = f"'{licensePeriod}'"
                if attr.get('code') == 'licenseFeeAmount_EA(N)':
                    if attr.get('value'):
                        lotcard_dict['licensePeriod_EA(N)'] = attr['value']
                if attr.get('code') == 'licenseProcedureMaking_EA(N)':
                    if attr.get('value'):
                        licenseProcedureMaking = attr['value'].replace("'", "''")
                        lotcard_dict['licenseProcedureMaking_EA(N)'] = f"'{licenseProcedureMaking}'"
                if attr.get('code') == 'participationFee_EA(N)':
                    if attr.get('value'):
                        lotcard_dict['participationFee_EA(N)'] = attr['value']
                if attr.get('code') == 'feeProcedureMaking_EA(N)':
                    if attr.get('value'):
                        feeProcedureMaking = attr['value'].replace("'", "''")
                        lotcard_dict['feeProcedureMaking_EA(N)'] = f"'{feeProcedureMaking}'"
                if attr.get('code') == 'oneTimePaymentProcedure_EA(N)':
                    if attr.get('value'):
                        oneTimePaymentProcedure = attr['value'].replace("'", "''")
                        lotcard_dict['oneTimePaymentProcedure_EA(N)'] = f"'{oneTimePaymentProcedure}'"
                if attr.get('code') == 'depositTimeAndRules_EA(N)':
                    if attr.get('value'):
                        depositTimeAndRules = attr['value'].replace("'", "''")
                        lotcard_dict['depositTimeAndRules_EA(N)'] = f"'{depositTimeAndRules}'"
                if attr.get('code') == 'depositRefund_EA(N)':
                    if attr.get('value'):
                        depositRefund = attr['value'].replace("'", "''")
                        lotcard_dict['depositRefund_EA(N)'] = f"'{depositRefund}'"
                lotcard_dict['lot_data'] = f"'{json.dumps(lotcard, ensure_ascii=False)}'"
        return lotcard_dict
    else:
        return False


def check_lotcard(pgconn, lotcard, table='torgi_gov_ru.lotcards', log_bot_info=('token', 'chatid'),
                  report_bot_info=('token', 'chatid'), logfile='torgi_gov_ru/logfile.txt',
                  webhook='', mapbox_token=''):
    status_dict = {
        "PUBLISHED": 'Опубликован',
        "APPLICATIONS_SUBMISSION": 'Прием заявок',
        "DETERMINING_WINNER": 'Определение победителя',
        "FAILED": 'Не состоялся',
        "SUCCEED": 'Состоялся'
    }
    updates = (0, 0)
    lotcard_dict = parse_lotcard(lotcard)
    if lotcard_dict:
        sql = f"select * from {table} where \"id\" = {lotcard_dict['id']};"
        with pgconn:
            with pgconn.cursor() as cur:
                cur.execute(sql)
                result = cur.fetchall()
                if result:
                    fields_to_check = ['lotStatus', 'priceMin', 'biddEndTime']
                    changes = []
                    for field, val in lotcard_dict.items():
                        if type(val) == str and field != 'squareMR':
                            val = val[1:-1]
                            val.replace("''", "'")
                        if field in ['lotNumber', 'licensePeriod_EA(N)']:
                            val = str(val)
                        if field in ['biddEndTime', 'createDate']:
                            val = datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
                        if field in ['hasAppeals', 'isStopped', 'isAnnuled']:
                            val = json.loads(val.lower())
                        if field == 'lot_data':
                            val = json.loads(val)

                        dbval = result[0][field]
                        if field in ['squareMR', 'priceFin', 'priceMin']:
                            if dbval:
                                dbval = float(dbval)
                                val = float(val)
                            pass
                        if val != dbval:
                            # changes.append({"id": result[0]['id'], "lotName": lotcard_dict['lotName'][1:-1], "field": field, "old": result[0][field], "new": val})
                            changes.append({"id": result[0]['id'], "lotName": lotcard_dict['lotName'][1:-1], "field": field, "old": dbval, "new": val})
                            
                            # message = f"lotcard {result[0]['id']} change detected: {field} -> {val}"
                            # with open(logfile, 'a', encoding='utf-8') as logf, requests.Session() as s:
                            #     log_message(s, logf, log_bot_info, message)
                    
                    if changes:
                        # print(changes)
                        updates = (0, 1)
                        fields_to_update = []
                        values_to_insert = []
                        for change in list(changes):
                            fields_to_update.append('"' + change['field'] + '"')
                            values_to_insert.append(str(lotcard_dict[change['field']]))

                        sql = f"update {table} set {', '.join([x[0] + ' = ' + x[1] for x in zip(fields_to_update, values_to_insert)])} " \
                              f"where \"id\" = {lotcard_dict['id']};"

                        cur.execute(sql)
                        pgconn.commit()
                        lotcard_dict = dict(zip(list(lotcard_dict.keys()), [str(x).replace("'", "") for x in lotcard_dict.values()]))
                        message = ''
                        chfieldsdict = {"lotStatus": 'Статус', "priceMin": 'Нач.цена', "biddEndTime": 'Заявки до'}
                        for i, change in enumerate([x for x in list(changes) if x['field'] in ['lotStatus', 'priceMin', 'biddEndTime']]):
                            # logmessage = f"logging lotcard change: {change['id']} -> {change['field']} -> {str(change['new'])}"
                            # with open(logfile, 'a', encoding='utf-8') as logf, requests.Session() as s:
                            #     log_message(s, logf, log_bot_info, logmessage)
                            if i == 0:
                                message = f"Изменен лот на участок УВС на torgi.gov.ru: \n\"{change['lotName']}\""
                                if lotcard_dict.get('resourceLocation_EA(N)'):
                                    resourceLocation = str(lotcard_dict['resourceLocation_EA(N)'])
                                    message += f" ({resourceLocation})"
                                message += ':'
                            val = change['new']
                            if change['field'] == 'biddEndTime':
                                if lotcard_dict.get('timeZoneOffset'):
                                    offset = lotcard_dict['timeZoneOffset']
                                    val = val + timedelta(minutes=int(offset))
                                val = val.strftime('%d.%m.%Y %H:%M')
                                if lotcard_dict.get('timeZoneName'):
                                    tz = lotcard_dict['timeZoneName']
                                    val += f' {tz}'
                            elif change['field'] == 'lotStatus':
                                message += f"\n{chfieldsdict[change['field']]}: {status_dict[str(val)]}"
                            elif change['field'] == 'priceMin':
                                locale.setlocale(locale.LC_ALL, ('ru_RU', 'UTF-8'))
                                message += f"\n{chfieldsdict[change['field']]}: {locale.currency(float(val), grouping=True)}"
                                locale.setlocale(locale.LC_ALL, (''))
                            else:
                                message += f"\n{chfieldsdict[change['field']]}: {str(val)}"
                        if message:
                            message += f"; \n<a href=\"https://torgi.gov.ru/new/public/lots/lot/{lotcard_dict['id']}/(lotInfo:info)?fromRec=false\">Карточка лота</a>"
                            with open(logfile, 'a', encoding='utf-8') as logf, requests.Session() as s:                                
                                is_png = get_lot_on_mapbox_png(lotcard_dict['noticeNumber'], 'torgi_gov_ru/lot.png', mapbox_token)                                
                                is_html = generate_lot_mapbox_html(lot=lotcard_dict['noticeNumber'], ofile=f"torgi_gov_ru/{str(lotcard_dict['noticeNumber'])}.htm", token=mapbox_token)
                                if is_html:
                                    message += f"; \n<a href=\"http://195.2.79.9:8080/{lotcard_dict['noticeNumber']}.htm\">Отобразить на карте</a>"
                                    os.remove(f"torgi_gov_ru/{str(lotcard_dict['noticeNumber'])}.htm")
                                log_message(s, logf, report_bot_info, message, to_telegram=False)
                                    # message += f'\n[Landing (VG VPN)](http://192.168.117.3:5000/collections/license_hcs_lotcards/items/{lotcard_dict['id']})'
                                if is_png:
                                    send_to_telegram(s, logf, bot_info=report_bot_info, message=message, photo='torgi_gov_ru/lot.png')
                                elif is_html:
                                    send_to_telegram(s, logf, bot_info=report_bot_info, message=message)                                
                                else:
                                    log_message(s, logf, report_bot_info, message, to_telegram=True)
                                # if generate_lot_mapbox_html(lot=lotcard_dict['id'], ofile=f"torgi_gov_ru/{str(lotcard_dict['miningSiteName_EA(N)'])}.htm", token=mapbox_token):
                                #     if send_to_telegram(s, logf, bot_info=report_bot_info, message='Откройте файл в браузере для отображения на карте', document=f"torgi_gov_ru/{str(lotcard_dict['miningSiteName_EA(N)'])}.htm"):
                                #         os.remove(f"torgi_gov_ru/{str(lotcard_dict['miningSiteName_EA(N)'])}.htm")
                                if webhook:
                                    send_to_teams(webhook, message, logf)
                                
                    pass
                else:
                    updates = (1, 0)
                    fields_to_update = ['"' + x + '"' for x in lotcard_dict.keys()]
                    values_to_insert = lotcard_dict.values()
                    sql = f"insert into {table}({', '.join(fields_to_update)}) values({', '.join([str(x) for x in values_to_insert])});"
                    cur.execute(sql)
                    pgconn.commit()
                    lotcard_dict = dict(zip(list(lotcard_dict.keys()), [str(x).replace("'", "") for x in lotcard_dict.values()]))
                    message = f"Новый лот на участок УВС на torgi.gov.ru:\n{lotcard_dict['lotName']}"
                    message += f"\nСтатус: {status_dict.get(lotcard_dict['lotStatus'], lotcard_dict['lotStatus'])}"
                    if lotcard_dict.get('resourceLocation_EA(N)'):
                        resourceLocation = str(lotcard_dict['resourceLocation_EA(N)']).replace("'", "")
                        message += f"; \nРасположение: {resourceLocation}"
                    if lotcard_dict.get('priceMin'):
                        locale.setlocale(locale.LC_ALL, ('ru_RU', 'UTF-8'))                        
                        # message += f"; \nНач.цена: {str(lotcard_dict['priceMin'])}"
                        message += f"; \nНач.цена: {locale.currency(float(lotcard_dict['priceMin']), grouping=True)}"
                        locale.setlocale(locale.LC_ALL, '')
                    if lotcard_dict.get('priceFin'):
                        locale.setlocale(locale.LC_ALL, ('ru_RU', 'UTF-8'))
                        # message += f"; \nИтог.цена: {str(lotcard_dict['priceFin'])}"
                        message += f"; \nИтог.цена: {locale.currency(float(lotcard_dict['priceFin']), grouping=True)}"
                        locale.setlocale(locale.LC_ALL, '')
                    if lotcard_dict.get('biddEndTime'):
                        endtime = datetime.strptime(lotcard_dict['biddEndTime'], "%Y-%m-%d %H:%M:%S")
                        if lotcard_dict.get('timeZoneOffset'):
                            endtime = endtime + timedelta(minutes=int(lotcard_dict['timeZoneOffset']))
                        message += f"; \nЗаявки до: {endtime.strftime('%d.%m.%Y %H:%M')}"
                        if lotcard_dict.get('timeZoneName'):
                            message += f" {lotcard_dict['timeZoneName']}"
                    if lotcard_dict.get('resourcePotential'):
                        message += f"; \nРесурсы: {lotcard_dict['resourcePotential']}"
                    message += f"; \n<a href=\"https://torgi.gov.ru/new/public/lots/lot/{lotcard_dict['id']}/(lotInfo:info)?fromRec=false\">Карточка лота</a>"
                    
                    with open(logfile, 'a', encoding='utf-8') as logf, requests.Session() as s:
                        is_png = get_lot_on_mapbox_png(lotcard_dict['noticeNumber'], 'torgi_gov_ru/lot.png', mapbox_token)                                
                        is_html = generate_lot_mapbox_html(lot=lotcard_dict['noticeNumber'], ofile=f"torgi_gov_ru/{str(lotcard_dict['noticeNumber'])}.htm", token=mapbox_token)
                        if is_html:
                            message += f"; \n<a href=\"http://195.2.79.9:8080/{lotcard_dict['noticeNumber']}.htm\">Отобразить на карте</a>"
                            os.remove(f"torgi_gov_ru/{str(lotcard_dict['noticeNumber'])}.htm")
                        log_message(s, logf, report_bot_info, message, to_telegram=False)
                            # message += f'\n[Landing (VG VPN)](http://192.168.117.3:5000/collections/license_hcs_lotcards/items/{lotcard_dict['id']})'
                        if is_png:
                            send_to_telegram(s, logf, bot_info=report_bot_info, message=message, photo='torgi_gov_ru/lot.png')
                        elif is_html:
                            send_to_telegram(s, logf, bot_info=report_bot_info, message=message)                                
                        else:
                            log_message(s, logf, report_bot_info, message, to_telegram=True)
                        # if generate_lot_mapbox_html(lot=lotcard_dict['id'], ofile=f"torgi_gov_ru/{str(lotcard_dict['miningSiteName_EA(N)'])}.htm", token=mapbox_token):
                        #     if send_to_telegram(s, logf, bot_info=report_bot_info, message='Откройте файл в браузере для отображения на карте', document=f"torgi_gov_ru/{str(lotcard_dict['miningSiteName_EA(N)'])}.htm"):
                        #         os.remove(f"torgi_gov_ru/{str(lotcard_dict['miningSiteName_EA(N)'])}.htm")
                        if webhook:
                            send_to_teams(webhook, message, logf)
    else:
        message = f"Ошибка: отсутствует lotcard id"
        with open(logfile, 'a', encoding='utf-8') as logf, requests.Session() as s:
            log_message(s, logf, log_bot_info, message)
    return updates


def refresh_lotcards(dsn='', log_bot_info=('token', 'chatid'), report_bot_info=('token', 'chatid'), 
                     logfile='torgi_gov_ru/logfile.txt', webhook='', mapbox_token=''):
    lotcards = download_lotcards(log_bot_info=log_bot_info, logfile=logfile)
    if lotcards and dsn:
        new, updated = 0, 0
        pgconn = psycopg2.connect(dsn, cursor_factory=DictCursor)
        for lotcard in lotcards:
            updates = check_lotcard(pgconn, lotcard,
                                    log_bot_info=log_bot_info, report_bot_info=report_bot_info, logfile=logfile,
                                    webhook=webhook, mapbox_token=mapbox_token)
            new, updated = [x + y for x, y in zip((new, updated), updates)]
        pgconn.commit()
        pgconn.close()
        message = f"Новых лотов: {str(new)}\nИзменено лотов: {str(updated)}"
        with open(logfile, 'a', encoding='utf-8') as logf, requests.Session() as s:
            log_message(s, logf, log_bot_info, message)


def generate_lot_mapbox_html(lot, ofile, token):
    with requests.Session() as s:
        i = 1
        status = 0
        response = None
        jd = None
        while (not response) and i <= 10 and status != 200:
            try:
                i += 1
                response = s.get(f'http://192.168.117.3:5000/collections/license_hcs_lotcards/items/{lot}?f=json')
                status = response.status_code
                jd = response.json()
            except:
                pass
        if jd and status == 200:
            pass
            xmin = min([point[0] for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]) - 0.01
            xmax = max([point[0] for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]) + 0.01
            ymin = min([point[1] for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]) - 0.01
            ymax = max([point[1] for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]) + 0.01
            xcenter = (xmin + xmax) / 2
            ycenter = (ymin + ymax) / 2
            # jd2 = {"type": "Polygon", "coordinates": jd["geometry"]["coordinates"][0]}
            jd3 = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {k.replace(' ', '_'): v for k, v in jd["properties"].items()}, "geometry": {"type": "Polygon", "coordinates": crds}} for crds in jd["geometry"]["coordinates"]]}
            html = '''
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>'''
            html += jd3['features'][0]['properties']['Название_лота']
            html += '''</title>
<meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no">
<link href="https://api.mapbox.com/mapbox-gl-js/v3.7.0/mapbox-gl.css" rel="stylesheet">
<script src="https://api.mapbox.com/mapbox-gl-js/v3.7.0/mapbox-gl.js"></script>
<style>
body { margin: 0; padding: 0; }
#map { position: absolute; top: 0; bottom: 0; width: 100%; }
</style>
</head>
<body>
<style>
    .mapboxgl-popup {
        max-width: 400px;
        font:
            12px/20px 'Helvetica Neue',
            Arial,
            Helvetica,
            sans-serif;
    }
</style>
<div id="map"></div>
<script>
	// TO MAKE THE MAP APPEAR YOU MUST
	// ADD YOUR ACCESS TOKEN FROM
	// https://account.mapbox.com
	mapboxgl.accessToken = '''
            html += f"'{token}'"
            html += ''';
    const map = new mapboxgl.Map({
        container: 'map', // container ID
        // Choose from Mapbox's core styles, or make your own style with Mapbox Studio
        style: 'mapbox://styles/mapbox/light-v11', // style URL
        center: [
            '''
            html += str((xmin + xmax) / 2)
            html += ', '
            html += str((ymin + ymax) / 2)
            html += '''], // starting position
        zoom: 7 // starting zoom
    });

    map.on('load', () => {
        // Add a data source containing GeoJSON data.
        map.addSource('lotcard', {
            'type': 'geojson',
            'data':'''
            # jds = json.dumps(jd3, indent=4)
            jds = json.dumps(jd3, ensure_ascii=False)
            html += ' ' * 1 + jds
            html += '''
            })

        // Add a new layer to visualize the polygon.
        map.addLayer({
            'id': 'lot-fill',
            'type': 'fill',
            'source': 'lotcard', // reference the data source
            'layout': {},
            'paint': {
                'fill-color': '#0080ff', // blue color fill
                'fill-opacity': 0.5
            }
        });
        // Add a black outline around the polygon.
        map.addLayer({
            'id': 'lot-outline',
            'type': 'line',
            'source': 'lotcard',
            'layout': {},
            'paint': {
                'line-color': '#000',
                'line-width': 3
            }
        });
        // When a click event occurs on a feature in the places layer, open a popup at the
        // location of the feature, with description HTML from its properties.
        map.on('click', 'lot-fill', (e) => {
            // Copy coordinates array.
            // const coordinates = e.features[0].geometry.coordinates.slice();

            //var description = '<table>';
            //for (const prop in e.features[0].properties) {
            //  description += '<tr><td><b>' + prop + '</b></td><td>' + e.features[0].properties.prop + '</td></tr>'
            //}
            //description += '</table>'

            var description = '<h3>' + e.features[0].properties.Наименование_участка_недр + '</h3>'
            description += '<table>'
            description += '<tr><td colspan=2>' + e.features[0].properties.Название_лота + '</td></tr>'
            description += '<tr><td colspan=2>' + e.features[0].properties.Сведения_о_запасах_и_ресурсах + '</td></tr>'
            description += '<tr><td>Срок подачи:</td><td>' + e.features[0].properties.Срок_подачи_заявок + '</td></tr>'
            description += '<tr><td>Нач.цена:</td><td>' + parseInt(e.features[0].properties.Начальная_цена).toLocaleString('ru-RU', {
              style: 'currency', 
              currency: 'RUB', 
              currencyDisplay : 'symbol', 
              useGrouping: true
            }) + '</td></tr>'
            description += '<tr><td>Статус:</td><td>' + e.features[0].properties.Статус + '</td></tr>'
            description += '<tr><td colspan=2><a href="' + e.features[0].properties.url + '">Карточка лота</a></td></tr>'
            description += '</table>'
            
            // description = e.features[0].properties.Название_лота;


            const coords = ['''
            html += f"{str(xcenter)}, {str(ycenter)}"
            html +='''];

            // Ensure that if the map is zoomed out such that multiple
            // copies of the feature are visible, the popup appears
            // over the copy being pointed to.
            if (['mercator', 'equirectangular'].includes(map.getProjection().name)) {
                while (Math.abs(e.lngLat.lng - coordinates[0]) > 180) {
                    coordinates[0] += e.lngLat.lng > coordinates[0] ? 360 : -360;
                }
            }

            new mapboxgl.Popup()
                .setLngLat(coords)
                .setHTML(description)
                .addTo(map);
        });

        // Change the cursor to a pointer when the mouse is over the places layer.
        map.on('mouseenter', 'lot-fill', () => {
            map.getCanvas().style.cursor = 'pointer';
        });

        // Change it back to a pointer when it leaves.
        map.on('mouseleave', 'lot-fill', () => {
            map.getCanvas().style.cursor = '';
        });
    });
</script>

</body>
</html>
            '''
            with open(ofile, 'w', encoding='utf-8') as output:
                _ = output.write(html)

            def createSSHClient(server, port, user):
                client = paramiko.SSHClient()
                client.load_system_host_keys()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(server, port, user)
                return client
            try:
                ssh = createSSHClient('195.2.79.9', '22', 'stepan')
                scp = SCPClient(ssh.get_transport())
                scp.put(ofile, recursive=True, remote_path='/home/stepan/apache/htdocs')
                scp.close()
                return True
            except:
                pass
            
    return False
    




def get_lot_on_mapbox_png(lot, ofile, token, size=400, padding=100):
    # https://docs.mapbox.com/api/maps/static-images/
    # Пример на перспективу - как встраивать MapBox в HTML. Может, можно такой вставить в Telegram сообщение?
    # https://docs.mapbox.com/mapbox-gl-js/example/simple-map/
    # https://docs.mapbox.com/mapbox-gl-js/example/external-geojson/
    success = False
    with requests.Session() as s:
        # lot = '22000033960000000024'
        i = 1
        status = 0
        response = None
        jd = None
        while (not response) and i <= 10 and status != 200:
            try:
                i += 1
                response = s.get(f'http://192.168.117.3:5000/collections/license_hcs_lotcards/items/{lot}?f=json')
                status = response.status_code
                jd = response.json()
            except:
                pass
        if jd and status == 200:
            # jd = {"type": jd["type"], "properties": {k: v for k, v in jd['properties'].items() if k == 'Название лота'}, "geometry": jd["geometry"]}
            # xmin = min([point[0] for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]) - 0.01
            # xmax = max([point[0] for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]) + 0.01
            # ymin = min([point[1] for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]) - 0.01
            # ymax = max([point[1] for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]) + 0.01
            if jd.get('geometry'):
                # Вариант 1 - GeoJSON без изменений структуры, но с отбором атрибутов
                jd = {"type": jd["type"], "properties": {k: v for k, v in jd['properties'].items() if k == 'Название лота'}, "geometry": jd["geometry"]}
                
                # Вариант 2 - Если предположить, что в мультиполигоне всегда один полигон
                jd2 = {"type": "Polygon", "coordinates": jd["geometry"]["coordinates"][0]}
                
                # Вариант 3 - Попытка сделать пригодный для MapBox Мультиполигон (не работает)
                # https://docs.mapbox.com/api/maps/static-images/#example-request-retrieve-a-static-map-with-a-geojson-featurecollection-overlay
                jd3 = {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Polygon", "coordinates": crds}} for crds in jd["geometry"]["coordinates"]]}
                
                jds =json.dumps(jd2)    # Пока что будем пользоваться вариантом 2
                encoded = quote(jds.replace(' ', ''))
                
                # Это для варианта 1. Делаем из GeoJSON просто цепочку координат всех колец полигона. Не работает, если в полигоне есть дырки.
                points = [point for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]
                
                # https://docs.mapbox.com/api/maps/styles/
                styles = {'streets': 'streets-v12', 'dark': 'dark-v11', 'light': 'light-v11', 'sat': 'satellite-v9', 
                        'sat-str': 'satellite-streets-v12', 'out': 'outdoors-v12'}
                
                # # Вариант 1 - формируем URL для отображения path
                # points_tuples = [(point[1], point[0]) for point in points]
                # encoded_polyline = polyline.encode(points_tuples, 5)
                # url = f"https://api.mapbox.com/styles/v1/mapbox/{styles['streets']}/static/" \
                #     f"path-5+c00-0.5({encoded_polyline})/" \
                #     f"auto/200x200"
                
                # Вариант 2 - пока рабочий. Засылаем подходящий для MapBox GeoJSON с первым полигоном из мультиполигона.
                url = f"https://api.mapbox.com/styles/v1/mapbox/{styles['streets']}/static/" \
                    f"geojson({encoded})/" \
                    f"auto/{str(size)}x{str(size)}"
                
                params = {"access_token": token,
                        "padding": f'{str(padding)},{str(padding)},{str(padding)}'}
                j = 1
                status = 0
                response = None
                while (not response) and i <= 10 and status != 200:
                    try:
                        i += 1
                        response = s.get(url, params=params)
                        status = response.status_code
                    except:
                        pass
                # add to a file
                if response and status == 200:
                    with open(ofile, 'wb') as output:
                        _ = output.write(response.content)
                        return True
    return False


if __name__ == '__main__':
    with open('.pgdsn', encoding='utf-8') as dsnf:
        dsn = dsnf.read().replace('\n', '')
    with open('.mapbox_token', encoding='utf-8') as mbtkf:
        mb_token = mbtkf.read().replace('\n', '')
    with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
        jdata = json.load(f)
        log_bot_info = (jdata['token'], jdata['chatid'])
        report_bot_info = (jdata['token'], jdata['chatid'])
    with open('2024_blocks_nr_ne.webhook', 'r', encoding='utf-8') as f:
        nr_ne_webhook_2023 = f.read().replace('\n', '')
    with open('vgdb_bot_tests.webhook', 'r', encoding='utf-8') as f:
        vgdb_bot_tests_webhook = f.read().replace('\n', '')
    with open('.egssh', 'r', encoding='utf-8') as f:
        egssh = json.load(f)
    
    refresh_lotcards(dsn=dsn, log_bot_info=log_bot_info, report_bot_info=report_bot_info, webhook=vgdb_bot_tests_webhook, mapbox_token=mb_token)
    # synchro_table([('torgi_gov_ru', ['lotcards'])], '.pgdsn', '.ext_pgdsn', ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=log_bot_info)

    # with open('tmp.txt', 'w') as logf, requests.Session() as s:
    #     message = '[lotcard](http://192.168.117.3:5000/collections/license_hcs_lotcards/items/22000033960000000013)'
    #     send_to_telegram(s, logf, bot_info=log_bot_info, message=message)

    
    with requests.Session() as s:
        # lot = '22000039810000000086'
        # lot = '22000039810000000046'
        # lot = '22000033960000000024'
        # lot = '22000039810000000086'
        # lots = ['22000043270000000069','22000039810000000035','22000039810000000082','22000039810000000082',
        #         '22000043270000000036','22000039810000000072','22000039810000000072','22000039810000000083',
        #         '22000059140000000015','22000039810000000058']
        # lots = ['22000039810000000090']
        lots = ['22000033960000000028']

        # response = s.get(f'http://192.168.117.3:5000/collections/license_hcs_lotcards/items/{lot}?f=json')
        # jd = response.json()
        # # jd = {"type": jd["type"], "properties": {k: v for k, v in jd['properties'].items() if k == 'Название лота'}, "geometry": jd["geometry"]}
        # # xmin = min([point[0] for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]) - 0.01
        # # xmax = max([point[0] for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]) + 0.01
        # # ymin = min([point[1] for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]) - 0.01
        # # ymax = max([point[1] for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]) + 0.01
        # points = [point for pol in jd['geometry']['coordinates'] for ring in pol for point in ring]
        # # points_string = ', '.join(['[' + str(point[1]) +', ' + str(point[0]) + ']' for point in points])
        # points_tuples = [(point[1], point[0]) for point in points]
        # encoded_polyline = polyline.encode(points_tuples, 5)
        # # https://docs.mapbox.com/api/maps/styles/
        # styles = {'streets': 'streets-v12', 'dark': 'dark-v11', 'light': 'light-v11', 'sat': 'satellite-v9', 
        #           'sat-str': 'satellite-streets-v12', 'out': 'outdoors-v12'}
        # # os.environ["MAPBOX_ACCESS_TOKEN"] = mb_token
        # url = f"https://api.mapbox.com/styles/v1/mapbox/{styles['out']}/static/" \
        #       f"path-5+c00-0.5({encoded_polyline})/" \
        #       f"auto/200x200"
        # params = {"access_token": mb_token,
        #           "padding": '50,50,50'}
        # response = s.get(url, params=params)
        # # add to a file
        # with open('./output_file.png', 'wb') as output:
        #     _ = output.write(response.content)
        

        # with open('tmp.txt', 'w') as logf:
        #     if get_lot_on_mapbox_png(lot, 'torgi_gov_ru/lot.png', mb_token):
        #         pass
        #         message = f'[lotcard GeoJSON](http://192.168.117.3:5000/collections/license_hcs_lotcards/items/{lot})'
        #         if send_to_telegram(s, logf, bot_info=log_bot_info, message=message, photo='torgi_gov_ru/lot.png'):
        #             pass

        # generate_lot_mapbox_html(lot, f"torgi_gov_ru/{lot}.htm", mb_token)

        with open('tmp.txt', 'w') as logf:
            pass
            # for lot in lots:
            #     if get_lot_on_mapbox_png(lot, f'torgi_gov_ru/{lot}.png', mb_token, size=400, padding=100):
            #         pass
            #     if generate_lot_mapbox_html(lot, f"torgi_gov_ru/{lot}.html", mb_token):
            #         message = f'{lot}'
                    
                    
                    
                    # success = False
                    # try:                    
                    #     def createSSHClient(server, port, user):
                    #         client = paramiko.SSHClient()
                    #         client.load_system_host_keys()
                    #         client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    #         client.connect(server, port, user)
                    #         return client

                    #     ssh = createSSHClient('195.2.79.9', '22', 'stepan')
                    #     scp = SCPClient(ssh.get_transport())
                    #     scp.put(f"torgi_gov_ru/{lot}.html", recursive=True, remote_path='/home/stepan/apache/htdocs')
                    #     scp.close()
                    #     success = True
                    # except:
                    #     pass
                    # pass
                    # # if send_to_telegram(s, logf, bot_info=log_bot_info, message='Откройте файл в браузере для отображения на карте', document=f"torgi_gov_ru/{lot}.html"):
                    # if success:
                    
                    # if send_to_telegram(s, logf, bot_info=log_bot_info, message=f'<a href="http://195.2.79.9:8080/{lot}.html">Отобразить на карте</a>', parse_mode='HTML', photo=f'torgi_gov_ru/{lot}.png'):
                    #     pass

                    # if send_to_telegram(s, logf, bot_info=log_bot_info, message=f'<a href="http://195.2.79.9:8080/{lot}.html">Отобразить на карте</a>', parse_mode='HTML'):
                    #     pass

                    # if send_to_teams(vgdb_bot_tests_webhook, message, logf, sections=['SECTION 1']):
                    #     pass
        
    # # This is telegram credentials to send message to the 'VG Database Techinfo' group
    # with open('bot_info_vgdb_bot_toAucGroup.json', 'r', encoding='utf-8') as f:
    #     jdata = json.load(f)
    #     report_bot_info = (jdata['token'], jdata['chatid'])
    
    # if send_to_telegram(s, logf, bot_info=report_bot_info, message=f'Простите меня, опять у меня отладка, опять спамлю! надеюсь, больше такое не повторится!', parse_mode='HTML'):
    #     pass
