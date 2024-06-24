import requests, os
import json
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime
from vgdb_general import *
from synchro_evergis import *
import time
from collections import namedtuple
from fabric import Connection

# samples from: https://github.com/RapidScada/scada-community/blob/master/Samples/WebApiClientSample/WebApiClientSample/Program.cs

def load_from_scada(objects_fields, scada_login, folder='scada', bot_info=('token', 'id'), log=False):
    result = []
    current_directory = os.getcwd()
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:
        if log:
            log_message(s, logf, bot_info, f'vgdb_scada: Начинаю обновление телеметрии по объектам: {", ".join([x[0] for x in objects_fields])}')
        if login_to_scada(s, scada_login['host'], scada_login['user'], scada_login['password'], logf, bot_info=bot_info):
            scada_response = namedtuple('scada_response', 'obj_id obj_name obj_type data')
            for object_fields in objects_fields:
                url = f"https://{scada_login['host']}/Api/Main/GetCurData?cnlNums={','.join(object_fields['channels'])}"
                i = 1
                code = 0
                while code != 200 and i <= 10:
                    try:
                        i += 1
                        response = s.get(url, verify=False)
                        code = response.status_code
                        time.sleep(10)
                    except Exception as err:
                        print(err)
                if code == 200:
                    if log:
                        log_message(s, logf, bot_info, f'vgdb_scada: Получены данные по объекту {object_fields[0]}')
                    data = response.json()

                    # result.append((object_fields["obj_id"], object_fields["obj_name"], object_fields["obj_type"], data['data']))
                    result.append(scada_response(obj_id=object_fields["obj_id"], obj_name=object_fields["obj_name"], obj_type=object_fields["obj_type"], data=data['data']))

                else:
                    pass
    if result:
        return result
    else:
        log_message(s, logf, bot_info, f'vgdb_scada: Не получено данных из SCADA')
        return None


def login_to_scada(s, host, user, password, logf, port=80, bot_info=('token', 'id'), log=False):
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
            if log:
                log_message(s, logf, bot_info, 'vgdb_scada: Подключение к SCADA установлено')
            return True
        else:
            if log:
                log_message(s, logf, bot_info, 'vgdb_scada: Ошибка подключения к SCADA')
            return False
    if log:
        log_message(s, logf, bot_info, 'vgdb_scada: Ошибка подключения к SCADA')
    return False


def send_to_ssh_postgres(ext_pgdsn_path, table, data, channels_dict, timestamp, ssh_host='', ssh_user='',
                         bot_info=('token', 'id'), folder='scada', log=False, shrink=True):
    with open(ext_pgdsn_path, encoding='utf-8') as f:
        ext_pgdsn = f.read()

    ext_pgdsn_dict = dict([x.split('=') for x in ext_pgdsn.split(' ')])
    # new_ext_pgdsn = ext_pgdsn.replace(f"port={ext_pgdsn_dict['port']}", f"port={str(local_port_for_ext_pg)}")


    current_directory = os.getcwd()

    # new_ext_pgdsn_dict = dict([x.split('=') for x in new_ext_pgdsn.split(' ')])
    # new_ext_pgpass = os.path.join(current_directory, folder, '.new_ext_pgpass')
    # with open(new_ext_pgpass, 'w', encoding='utf-8') as f:
    #     f.write(f"{new_ext_pgdsn_dict['host']}:{new_ext_pgdsn_dict['port']}:{new_ext_pgdsn_dict['dbname']}:{new_ext_pgdsn_dict['user']}:{new_ext_pgdsn_dict['password']}")
    # os.chmod(new_ext_pgpass, 0o600)
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
                with pgconn:
                    with pgconn.cursor() as cur:
                        vals = [str(x.obj_id) + ", '" + x.obj_name + "', '" + x.obj_type + "', '{" + ", ".join(
                            [chr(34) + channels_dict.get(
                                str(y["cnlNum"]), str(y["cnlNum"])) + chr(34) + ": " + str(y["val"]) for y in
                             x.data]) + "}', '" + datetime.strftime(
                            timestamp, "%Y-%m-%d %H:%M:%S") + "+00" + "'" for x in data]
                        sql = f"insert into {table}(object_id, object_name, object_type, attrs, datetime) values({'), ('.join(vals)});"
                        message = ''
                        i = 1
                        while message != 'INSERT 0 1' and i <= 2:
                            i += 1
                            cur.execute(sql)
                            message = cur.statusmessage
                        sql = f"delete from {table} where timezone('UTC', timezone('UTC', current_timestamp)) - timezone('UTC', timezone('UTC', datetime)) > make_interval(days => 1);"
                        if shrink:
                            message = ''
                            j = 1
                            while 'DELETE' not in message and j <= 2:
                                j += 1
                                cur.execute(sql)
                                message = cur.statusmessage
                pgconn.close()
                if i > 2 or j > 2:
                    if log:
                        log_message(s, logf, bot_info, '<send_to_ssh_postgres>: Ошибка отправки данных в Postgres')
                    result = False
                else:
                    if log:
                        log_message(s, logf, bot_info, '<send_to_ssh_postgres>: Данные успешно загружены в Postgres')
                    result = True
            else:
                if log:
                    log_message(s, logf, bot_info, 'vgdb_scada: Ошибка подключения к Postgres')
                result = False
        ssh_conn = None
    return result


def send_to_postgres(dsn, table, data, channels_dict, timestamp, folder='scada', bot_info=('token', 'id'), log=False, shrink=True):
    current_directory = os.getcwd()
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:
        if log:
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
                # timestamp = datetime.utcnow()
                with pgconn.cursor() as cur:
                    # vals = [str(x[0]) + ", '" + x[1] + "', '" + x[2] + "', '{" + ", ".join([chr(34) + channels_dict.get(str(y["cnlNum"]), str(y["cnlNum"])) + chr(34) + ": " + str(y["val"]) for y in x[3]]) + "}', '" + datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S") + "+00" + "'" for x in data]
                    vals = [str(x.obj_id) + ", '" + x.obj_name + "', '" + x.obj_type + "', '{" + ", ".join([chr(34) + channels_dict.get(
                        str(y["cnlNum"]), str(y["cnlNum"])) + chr(34) + ": " + str(y["val"]) for y in x.data]) + "}', '" + datetime.strftime(
                        timestamp, "%Y-%m-%d %H:%M:%S") + "+00" + "'" for x in data]
                    sql = f"insert into {table}(object_id, object_name, object_type, attrs, datetime) values({'), ('.join(vals)});"
                    message = ''
                    i = 1
                    while message != 'INSERT 0 1' and i <= 10:
                        i += 1
                        cur.execute(sql)
                        message = cur.statusmessage
                    sql = f"delete from {table} where timezone('UTC', timezone('UTC', current_timestamp)) - timezone('UTC', timezone('UTC', datetime)) > make_interval(days => 1);"
                    if shrink:
                        message = ''
                        j = 1
                        while 'DELETE' not in message and j <= 10:
                            j += 1
                            cur.execute(sql)
                            message = cur.statusmessage
            pgconn.close()
            if i > 10 or j > 10:
                if log:
                    log_message(s, logf, bot_info, 'vgdb_scada: Ошибка отправки данных в Postgres')
                return False
            else:
                if log:
                    log_message(s, logf, bot_info, 'vgdb_scada: Данные успешно загружены в Postgres')
                return True
        else:
            if log:
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

    with open('.egssh', 'r', encoding='utf-8') as f:
        egssh = json.load(f)

    # data = load_from_scada([('Интинская-18', 'Скважина', ['101-108'])], scada_login, bot_info=bot_info)
    data = load_from_scada(
        [{"obj_id": 751, "obj_name": 'Интинская-18',  "obj_type": 'ДЭЛ-150',  "channels": ['110-123']}],
        scada_login,
        bot_info=bot_info
    )
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
        timestamp = datetime.utcnow()
        if send_to_postgres(pgdsn, 'culture.from_scada', data, channels_dict, timestamp, bot_info=bot_info):
            send_to_ssh_postgres('.ext_pgdsn', 'culture.from_scada', data, channels_dict, timestamp, ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)
            # synchro_table([('culture', ['from_scada'])], '.pgdsn', '.ext_pgdsn', bot_info=bot_info)

    