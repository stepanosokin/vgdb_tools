import requests, os
import json
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime
from vgdb_general import *
from synchro_evergis import *

# samples from: https://github.com/RapidScada/scada-community/blob/master/Samples/WebApiClientSample/WebApiClientSample/Program.cs

def load_from_scada(objects_fields, scada_login, folder='scada', bot_info=('token', 'id')):
    result = []
    current_directory = os.getcwd()
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:
        log_message(s, logf, bot_info, f'vgdb_scada: Начинаю обновление телеметрии по объектам: {", ".join([x[0] for x in objects_fields])}')
        if login_to_scada(s, scada_login['host'], scada_login['user'], scada_login['password'], logf, bot_info=bot_info):
            for object_fields in objects_fields:
                url = f"https://{scada_login['host']}/Api/Main/GetCurData?cnlNums={','.join(object_fields[2])}"
                i = 1
                code = 0
                while code != 200 and i <= 10:
                    try:
                        i += 1
                        response = s.get(url, verify=False)
                        code = response.status_code
                    except Exception as err:
                        print(err)
                if code == 200:
                    log_message(s, logf, bot_info, f'vgdb_scada: Получены данные по объекту {object_fields[0]}')
                    data = response.json()
                    result.append((object_fields[0], object_fields[1], data['data']))
                else:
                    pass
    if result:
        return result
    else:
        log_message(s, logf, bot_info, f'vgdb_scada: Не получено данных из SCADA')
        return None


def login_to_scada(s, host, user, password, logf, port=80, bot_info=('token', 'id')):
    result = None
    i = 1
    code = 0
    url = f"https://{host}/Login"
    while code != 200 and i <= 10:
        try:
            i += 1
            result = s.get(url, verify=False)
            code = result.status_code
        except Exception as err:
            print(err)
    if result:
        soup = BeautifulSoup(result.text, 'html.parser')
        token = soup.find(attrs={"name": "__RequestVerificationToken"}).attrs["value"]
        data = {"Username": user, "Password": password, "__RequestVerificationToken": token, "RememberMe": "true"}
        code = 0
        i = 1
        while code not in [200, 302] and i <= 10:
            try:
                i += 1
                result = s.post(url, data=data, verify=False)
                code = result.status_code
            except Exception as err:
                print(err, f'login request {str(i - 1)} failed')
        if code in [200, 302]:
            log_message(s, logf, bot_info, 'vgdb_scada: Подключение к SCADA установлено')
            return True
        else:
            log_message(s, logf, bot_info, 'vgdb_scada: Ошибка подключения к SCADA')
            return False
    log_message(s, logf, bot_info, 'vgdb_scada: Ошибка подключения к SCADA')
    return False


def send_to_postgres(dsn, table, data, channels_dict, folder='scada', bot_info=('token', 'id')):
    current_directory = os.getcwd()
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:
        log_message(s, logf, bot_info, 'vgdb_scada: Начинаю загрузку данных SCADA в Postgres')
        i = 1
        pgconn = None
        while not pgconn and i <= 10:
            i += 1
            try:
                pgconn = psycopg2.connect(dsn)
            except:
                pass
        if pgconn:
            with pgconn:
                timestamp = datetime.utcnow()
                with pgconn.cursor() as cur:
                    vals = ["'" + x[0] + "', '" + x[1] + "', '{" + ", ".join([chr(34) + channels_dict.get(str(y["cnlNum"]), str(y["cnlNum"])) + chr(34) + ": " + str(y["val"]) for y in x[2]]) + "}', '" + datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S") + "+00" + "'" for x in data]
                    sql = f"insert into {table}(object_name, object_type, attrs, datetime) values({'), ('.join(vals)});"
                    message = ''
                    i = 1
                    while message != 'INSERT 0 1' and i <= 10:
                        i += 1
                        cur.execute(sql)
                        message = cur.statusmessage
            pgconn.close()
            if i > 10:
                log_message(s, logf, bot_info, 'vgdb_scada: Ошибка отправки данных в Postgres')
                return False
            else:
                log_message(s, logf, bot_info, 'vgdb_scada: Данные успешно загружены в Postgres')
                return True
        else:
            log_message(s, logf, bot_info, 'vgdb_scada: Ошибка подключения к Postgres')
            return False



if __name__ == '__main__':

    with open('.scadadsn', 'r', encoding='utf-8') as f:
        scada_login = json.load(f)
    with open('.pgdsn', encoding='utf-8') as f:
        pgdsn = f.read()
    with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
        jdata = json.load(f)
        bot_info = (jdata['token'], jdata['chatid'])

    # data = load_from_scada([('Интинская-18', 'Скважина', ['101-108'])], scada_login, bot_info=bot_info)
    for _ in range(10):
        data = load_from_scada([('Интинская-18', 'ДЭЛ-150', ['110-123'])], scada_login, bot_info=bot_info)
        if data:
            # channels_dict = {
            #     "101": "Давление трубное",
            #     "102": "Температура газа",
            #     "103": "Давление затрубное",
            #     "104": "Давление газа на входе сепаратора (после штуцера)",
            #     "105": "Температура газа на входе сепаратора (после штуцера)",
            #     "106": "Давление в дренажной линии сепаратора",
            #     "107": "Уровень в сепараторе",
            #     "108": "Test1"
            # }
            channels_dict = {
                "110": "Нагрузка на крюк [тс]",
                "111": "Давление нагнетания ЦА [кгс/см2]",
                "112": "ПЖ ВХ ДАВЛ 2 [кгс/см2]",
                "113": "ПЖ ВХ ДАВЛ 3 [кгс/см2]",
                "114": "ГК МОМЕНТ [кгс*м]",
                "115": "ГК ДАВЛЕНИЕ [кгс/см2]",
                "116": "ПЛОТН ПЖ 1 [г/см3]",
                "117": "ПЛОТН ПЖ 2 [г/см3]",
                "118": "ПЖ УРОВ 1 [м3]",
                "119": "ПЖ УРОВ 2 [м3]",
                "120": "CH4_1 [% НКПР]",
                "121": "H2S_2 [мг/м3]",
                "122": "CH4_3 [% НКПР]",
                "123": "H2S_4 [мг/м3]"
            }
            send_to_postgres(pgdsn, 'culture.from_scada', data, channels_dict, bot_info=bot_info)
    synchro_table([('culture', ['from_scada'])], '.pgdsn', '.ext_pgdsn', bot_info=bot_info)