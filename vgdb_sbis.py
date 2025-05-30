import requests
import json
from vgdb_general import smart_http_request
import os, shutil


def sbis_authenticate(s: requests.Session, user=None, password=None, url='https://online.sbis.ru/auth/service/'):
    if s and user and password:
        json = {
            "jsonrpc": "2.0",
            "method": "СБИС.Аутентифицировать",
            "params": {
                "Параметр": {
                    "Логин": user,
                    "Пароль": password
                }
            },
            "id": 0
        }
        headers = {
            "content-type": "application/json; charset=UTF-8"
        }
        status, result = smart_http_request(s, url=url, method='post', json=json)
        jresult = result.json()
        if status == 200:
            return jresult['result']
        else:
            return None


def sbis_payment_requests(s: requests.Session, folder='sbis', url='https://online.sbis.ru/service/', x_version='25.2148-168'):
    params = {
        "x_version": x_version
    }
    with open(f"{folder}/ЗаявкаНаОплату.CommonList.json", 'r', encoding='utf-8') as f:
        jdata = json.load(f)
    headers = {
            "content-type": "application/json; charset=UTF-8"
    }
    status, result = smart_http_request(s, url=url, method='post', json=jdata)
    if status == 200:
        sbis_requests = result.json()
        sbis_request_dict = sbis_requests['result']['s']
        current_directory = os.getcwd()
        download_path = os.path.join(current_directory, folder, 'downloaded')
        if os.path.exists(download_path):
            clear_folder(download_path)
        else:
            os.makedirs(download_path)
        for sbis_request in sbis_requests['result']['d']:
            parsed_request = parse_sbis_request(sbis_request=sbis_request, sbis_request_dict=sbis_request_dict)
            if parsed_request:
                save_pay_request(s, parsed_request=parsed_request, folder=download_path, url=url, x_version=x_version)
                pass
    else:
        return False
    pass


def save_pay_request(s: requests.Session, url=None, x_version=None, parsed_request=None, folder=None):    
    request_folder = os.path.join(folder, parsed_request.get('Дата').replace('-', '') + '_' + str(parsed_request.get('@Документ')))
    if os.path.exists(request_folder):
        clear_folder(request_folder)
    else:
        os.makedirs(request_folder)    
    with open(os.path.join(request_folder, parsed_request.get('Дата').replace('-', '') + '_' + str(parsed_request.get('@Документ')) + '.json'), 'w', encoding='utf-8') as of:
        json.dump(parsed_request, of, ensure_ascii=False)
        download_pay_request_attachs(s, parsed_request=parsed_request, folder=request_folder, url=url, x_version=x_version)
    pass
    

def download_pay_request_attachs(s: requests.Session, parsed_request=None, folder=None, url=None, x_version=None):
    json_path = os.path.join(os.path.split(os.path.split(folder)[0])[0], 'ЗаявкаНаОплату.AttachList.json')
    with open(json_path, 'r', encoding='utf-8') as f:
        jdata = json.load(f)
    jdata['params']['Фильтр']['d'][0] = parsed_request['@Документ']
    headers = {
        "content-type": "application/json; charset=UTF-8"
    }
    params = {
        "x_version": x_version
    }
    status, result = smart_http_request(s, url=url, method='post', params=params, headers=headers, json=jdata)
    attachments=result.json()
    sbis_attach_dict = attachments['result']['s']
    for sbis_attach in attachments['result']['d']:
        parsed_attach = parse_pay_request_attachment(sbis_attachment=sbis_attach, sbis_attach_dict=sbis_attach_dict)
        pass
    

def clear_folder(folder):
    for root, dirs, files in os.walk(folder):
        for f in files:
            if f != 'logfile.txt':
                os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


def parse_pay_request_attachment(sbis_attachment=None, sbis_attach_dict=None):
    thesaurus = [
        "redactionId",
        "fileName",
        "relativeUrl"
    ]
    attach_n_dict = {}
    attach_dict = {}
    for i, val in enumerate(sbis_attach_dict):
        if val['n'] in thesaurus:
            attach_n_dict[val['n']] = i
    if attach_n_dict:
        for name, n in attach_n_dict.items():
            attach_dict[name] = sbis_attachment[n]
    if attach_dict:
        return attach_dict
    else:
        return None


def parse_sbis_request(sbis_request=None, sbis_request_dict=None):
    thesaurus = [
        "@Документ", 
        "ПользовательскоеНазначение",
        "Дата",
        "ДокументРасширение.Название2",
        "ДокументРасширение.Сумма",
        "ДокументРасширение.СостояниеКраткое",
        "ПДРасширение.РасчетныйСчет.Номер",
        "ПДРасширение.РасчетныйСчет.Банк.Н",
        "Лицо1.Название",
        "Сотрудник.Название",
        "ДокументНашаОрганизация.Контрагент.Название",
        "НазваниеТипаДокумента",
        "Операция.Название",
        "Лицо1.Название",
        "Контрагент.ИНН"
    ]
    request_n_dict = {}
    request_dict = {}
    for i, val in enumerate(sbis_request_dict):
        if val['n'] in thesaurus:
            request_n_dict[val['n']] = i
    if request_n_dict:
        for name, n in request_n_dict.items():
            request_dict[name] = sbis_request[n]
    if request_dict:
        return request_dict
    else:
        return None
    

if __name__ == '__main__':
    with open('.sbisdsn', 'r', encoding='utf-8') as f:
        jdata = json.load(f)
        user = jdata.get('user')
        password = jdata.get('password')
    
    with requests.Session() as s:
        token = sbis_authenticate(s, user=user, password=password)
        if token:
            sbis_payment_requests(s)
        pass