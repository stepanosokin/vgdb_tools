import requests
import json
from bs4 import BeautifulSoup

# samples from: https://github.com/RapidScada/scada-community/blob/master/Samples/WebApiClientSample/WebApiClientSample/Program.cs

def load_from_scada(objects_fields, scada_login):

    with requests.Session() as s:
        if login_to_scada(s, scada_login['host'], scada_login['user'], scada_login['password']):
            url = f"https://{scada_login['host']}/Api/Main/GetCurData?cnlNums={','.join(objects_fields[0][1])}"
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
                data_fields = ['Давление трубное', 'Температура газа', 'Давление затрубное',
                               'Давление газа на входе сепаратора (после штуцера)',
                               'Температура газа на входе сепаратора (после штуцера)',
                               'Давление в дренажной линии сепаратора', 'Уровень в сепараторе', 'Test1']


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


if __name__ == '__main__':

    with open('.scadadsn', 'r', encoding='utf-8') as f:
        scada_login = json.load(f)

    load_from_scada([('14', ['101-108'])], scada_login)

