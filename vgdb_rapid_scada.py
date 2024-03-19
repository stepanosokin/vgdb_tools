import requests
import json
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime

# samples from: https://github.com/RapidScada/scada-community/blob/master/Samples/WebApiClientSample/WebApiClientSample/Program.cs

def load_from_scada(objects_fields, scada_login):
    result = []
    with requests.Session() as s:
        if login_to_scada(s, scada_login['host'], scada_login['user'], scada_login['password']):
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
                    data = response.json()  # пример текущих данных, полученных по первой в списке скважине
                    result.append((object_fields[0], object_fields[1], data['data']))
                    # data_fields = ['Давление трубное', 'Температура газа', 'Давление затрубное',
                    #                'Давление газа на входе сепаратора (после штуцера)',
                    #                'Температура газа на входе сепаратора (после штуцера)',
                    #                'Давление в дренажной линии сепаратора', 'Уровень в сепараторе', 'Test1']
                else:
                    pass
    if result:
        return result
    else:
        return None


def login_to_scada(s, host, user, password, port=80):
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
            return True
        else:
            return False
    return False


def send_to_postgres(dsn, table, data, channels_dict):
    pass
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
            return False
        else:
            return True
    else:
        return False



if __name__ == '__main__':

    with open('.scadadsn', 'r', encoding='utf-8') as f:
        scada_login = json.load(f)
    with open('.pgdsn', encoding='utf-8') as f:
        pgdsn = f.read()

    data = load_from_scada([('Интинская-18', 'Скважина', ['101-108'])], scada_login)
    if data:
        channels_dict = {
            "101": "Давление трубное",
            "102": "Температура газа",
            "103": "Давление затрубное",
            "104": "Давление газа на входе сепаратора (после штуцера)",
            "105": "Температура газа на входе сепаратора (после штуцера)",
            "106": "Давление в дренажной линии сепаратора",
            "107": "Уровень в сепараторе",
            "108": "Test1"
        }
        send_to_postgres(pgdsn, 'culture.from_scada', data, channels_dict)

