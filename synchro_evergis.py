import os, sys, subprocess
import psycopg2
import requests
import json
from psycopg2.extras import *
from osgeo import ogr, gdal
from fabric import Connection
from vgdb_general import *
import socket, errno


def login_to_evergis(name, passd):
    s = requests.Session()
    payload = {'username': name, 'password': passd, 'remember': 1,}
    res = s.post('https://geomercury.ru/sp/account/login', json=payload)
    # print(res)
    return s


def map_table(user, pwd, tables, schema=None):
    with login_to_evergis(user, pwd) as session:
        url = f'https://geomercury.ru/sp/tables/map-table'
        if schema:
            url += f'?dataProvider={schema}'
        for table in tables:
            payload = {
                "name": f"os.{table}",
                "alias": f"{table}",
                "owner": "os"
            }
            if schema:
                payload["alias"] = f"{schema}__{table}"
            r = session.post(url, json=payload)
            print(r.content)



def unmap_table(user, pwd, tables):
    with login_to_evergis(user, pwd) as session:
        for table in tables:
            url = f'https://geomercury.ru/sp/tables/map-table/os.{table}'
            d = session.delete(url)
            print(d.content)


def map_table_view(user, pwd, views, schema=None):
    with login_to_evergis(user, pwd) as session:
        url = f'https://geomercury.ru/sp/tables/map-table?type=View'
        if schema:
            url += f"&dataProvider={schema}"
        for view in views:
            payload = {
                "name": f"os.{view}",
                "alias": f"{view}",
                "owner": "os"
            }
            if schema:
                payload['alias'] = f"{schema}__{view}"
            r = session.post(url, json=payload)
            print(r.content)


def synchro_layer(schemas_tables, local_pgdsn, ext_pgdsn,
                  ssh_host='', ssh_user='', bot_info=('token', 'id'), folder='evergis'):

    # ext_pgdsn_dict = dict([x.split('=') for x in ext_pgdsn.split(' ')])
    # new_ext_pgdsn = ext_pgdsn.replace(f"port={ext_pgdsn_dict['port']}", f"port={str(local_port_for_ext_pg)}")

    current_directory = os.getcwd()
    # create a pathname for the logfile
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    # now we open the logfile and start logging
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:
        log_message(s, logf, bot_info, 'Начинаю синхронизацию слоев с Evergis...')
        j = 1
        ssh_conn = None
        while not ssh_conn and j <= 10:
            log_message(s, logf, bot_info, f'Установка подключения к удаленному серверу по SSH, попытка {str(j)}...', to_telegram=False)
            try:
                j += 1
                local_port_for_ext_pg = get_free_port((5433, 5440))
                if local_port_for_ext_pg:
                    ext_pgdsn_dict = dict([x.split('=') for x in ext_pgdsn.split(' ')])
                    new_ext_pgdsn = ext_pgdsn.replace(f"port={ext_pgdsn_dict['port']}",
                                                      f"port={str(local_port_for_ext_pg)}")
                    ssh_conn = Connection(ssh_host, user=ssh_user, connect_kwargs={"banner_timeout": 60}).forward_local(local_port_for_ext_pg,
                                                                   remote_port=int(ext_pgdsn_dict['port']))
            except Exception as err:
                log_message(s, logf, bot_info, f'Ошибка подключения к удаленному серверу по SSH (попытка {str(j)})', to_telegram=False)
        if not ssh_conn:
            log_message(s, logf, bot_info, 'Ошибка подключения к удаленному серверу по SSH')
            return False
        # with Connection(ssh_host, user=ssh_user).forward_local(local_port_for_ext_pg,
        #                                                        remote_port=int(ext_pgdsn_dict['port'])):
        with ssh_conn:
            log_message(s, logf, bot_info, 'Подключение установлено')
            local_conn = None
            i = 1
            while not local_conn and i <= 10:
                i += 1
                try:
                    log_message(s, logf, bot_info, f'Установка подключения к локальному postgres, попытка {str(i - 1)}...', to_telegram=True)
                    local_conn = ogr.Open(f"PG:{local_pgdsn}")
                except:
                    log_message(s, logf, bot_info, 'Ошибка подключения к локальному postgres')
            if not local_conn:
                return False
            for (schema, tables) in list(schemas_tables):
                for table in tables:
                    log_message(s, logf, bot_info, f'Начинаю синхронизацию слоя {schema}.{table}...')
                    source_layer = None
                    try:
                        source_layer = local_conn.GetLayer(f"{schema}.{table}")
                    except:
                        log_message(s, logf, bot_info, f'Ошибка получения исходного слоя {schema}.{table}')
                        # print('error connecting to source database')
                    if source_layer:
                        feature = source_layer.GetNextFeature()
                        if feature:
                            status = None
                            try:
                                pgconn = psycopg2.connect(new_ext_pgdsn)
                                with pgconn:
                                    with pgconn.cursor() as cur:
                                        sql = f'DELETE FROM {schema}.{table};'
                                        log_message(s, logf, bot_info,
                                                    f'Удаляю данные из слоя {schema}.{table} на удаленном сервере...')
                                        # print(f'attempting to delete data from layer {schema}.{table} on dest')
                                        cur.execute(sql)
                                        status = cur.statusmessage
                                pgconn.close()
                            except:
                                log_message(s, logf, bot_info, f'Ошибка удаления старых данных из слоя {schema}.{table}')
                                # print('error deleting old data from destination layer')
                            if status:
                                if 'DELETE' in status:
                                    log_message(s, logf, bot_info,
                                                'Удаление старых данных из конечного слоя - успешно', to_telegram=False)
                                    # print(f'successfully deleted data from layer {schema}.{table} on dest')
                                    source_layer_geom_type = feature.GetGeometryRef().GetGeometryName()
                                    source_layer_crs = source_layer.GetSpatialRef()
                                    myoptions = gdal.VectorTranslateOptions(
                                        layerName=f'{schema}.{table}',
                                        format='PostgreSQL',
                                        accessMode='append',
                                        dstSRS=source_layer_crs,
                                        layers=[f'{schema}.{table}'],
                                        geometryType=source_layer_geom_type
                                    )
                                    i = 1
                                    success = False
                                    while not success and i <= 10:
                                        i += 1
                                        try:
                                            log_message(s, logf, bot_info,
                                                        f'Перенос данных в слой {schema}.{table} - попытка {str(i - 1)} из 10')
                                            # print(f"attempt {str(i - 1)} to translate data to layer {schema}.{table}")
                                            success = gdal.VectorTranslate(f"PG:{new_ext_pgdsn}", f"PG:{local_pgdsn}", options=myoptions)
                                        except:
                                            log_message(s, logf, bot_info,
                                                        f'Ошибка переноса данных в слой {schema}.{table} (попытка {str(i - 1)} из 10)')
                                    if success:
                                        log_message(s, logf, bot_info,
                                                    f'Слой {schema}.{table} синхронизирован успешно')
                                        # print(f'table {schema}.{table} synchronized successfully')
                                    else:
                                        log_message(s, logf, bot_info,
                                                    f'Ошибка синхронизации слоя {schema}.{table} после {str(i - 1)} попыток')
                                        # print(f'table {schema}.{table} synchronization failed after {str(i - 1)} tries')
                                else:
                                    log_message(s, logf, bot_info,
                                                f'Операция прервана - ошибка удаления данных из конечного слоя {schema}.{table}')
                                    # print('process aborted - delete from dest operation failed')
                            else:
                                log_message(s, logf, bot_info, f'Ошибка получения статуса операции удаления данных из слоя {schema}.{table}')
                        else:
                            log_message(s, logf, bot_info, f'слой {schema}.{table} пуст')
                    else:
                        log_message(s, logf, bot_info, f'слой {schema}.{table} не существует')
            local_conn = None
        ssh_conn = None
        log_message(s, logf, bot_info, f'Синхронизация слоев с Evergis завершена')
        return True


def synchro_table(schemas_tables, local_pgdsn_path, ext_pgdsn_path,
                  ssh_host='', ssh_user='', bot_info=('token', 'id'), folder='evergis', log=True):

    with open(ext_pgdsn_path, encoding='utf-8') as f:
        ext_pgdsn = f.read()

    with open(local_pgdsn_path, encoding='utf-8') as f:
        local_pgdsn = f.read()

    # local_pgdsn_dict = dict([x.split('=') for x in local_pgdsn.split(' ')])
    # ext_pgdsn_dict = dict([x.split('=') for x in ext_pgdsn.split(' ')])
    # new_ext_pgdsn = ext_pgdsn.replace(f"port={ext_pgdsn_dict['port']}", f"port={str(local_port_for_ext_pg)}")
    # new_ext_pgdsn_dict = dict([x.split('=') for x in new_ext_pgdsn.split(' ')])
    #
    # # chmod 0600 ~/.pgpass
    # with open('.new_ext_pgpass', 'w', encoding='utf-8') as f:
    #     f.write(f"{new_ext_pgdsn_dict['host']}:{new_ext_pgdsn_dict['port']}:{new_ext_pgdsn_dict['dbname']}:{new_ext_pgdsn_dict['user']}:{new_ext_pgdsn_dict['password']}")
    # os.chmod('.new_ext_pgpass', 0o600)
    #
    # with open('.local_pgpass', 'w', encoding='utf-8') as f:
    #     f.write(f"{local_pgdsn_dict['host']}:{local_pgdsn_dict['port']}:{local_pgdsn_dict['dbname']}:{local_pgdsn_dict['user']}:{local_pgdsn_dict['password']}")
    # os.chmod('.local_pgpass', 0o600)

    current_directory = os.getcwd()
    # create a pathname for the logfile
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    # now we open the logfile and start logging
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:
        if log:
            log_message(s, logf, bot_info, 'Начинаю синхронизацию таблиц с Evergis...')

        j = 1
        ssh_conn = None
        while not ssh_conn and j <= 10:
            if log:
                log_message(s, logf, bot_info, f'Установка подключения к удаленному серверу по SSH, попытка {str(j)}...', to_telegram=False)
            try:
                j += 1
                local_port_for_ext_pg = get_free_port((5433, 5440))
                if local_port_for_ext_pg:
                    local_pgdsn_dict = dict([x.split('=') for x in local_pgdsn.split(' ')])
                    ext_pgdsn_dict = dict([x.split('=') for x in ext_pgdsn.split(' ')])
                    new_ext_pgdsn = ext_pgdsn.replace(f"port={ext_pgdsn_dict['port']}",
                                                      f"port={str(local_port_for_ext_pg)}")
                    new_ext_pgdsn_dict = dict([x.split('=') for x in new_ext_pgdsn.split(' ')])

                    # chmod 0600 ~/.pgpass
                    with open('.new_ext_pgpass', 'w', encoding='utf-8') as f:
                        f.write(
                            f"{new_ext_pgdsn_dict['host']}:{new_ext_pgdsn_dict['port']}:{new_ext_pgdsn_dict['dbname']}:{new_ext_pgdsn_dict['user']}:{new_ext_pgdsn_dict['password']}")
                    os.chmod('.new_ext_pgpass', 0o600)

                    with open('.local_pgpass', 'w', encoding='utf-8') as f:
                        f.write(
                            f"{local_pgdsn_dict['host']}:{local_pgdsn_dict['port']}:{local_pgdsn_dict['dbname']}:{local_pgdsn_dict['user']}:{local_pgdsn_dict['password']}")
                    os.chmod('.local_pgpass', 0o600)
                    ssh_conn = Connection(ssh_host, user=ssh_user, connect_kwargs={"banner_timeout": 60}).forward_local(local_port_for_ext_pg,
                                                                   remote_port=int(ext_pgdsn_dict['port']))
            except:
                if log:
                    log_message(s, logf, bot_info, f'Ошибка подключения к удаленному серверу по SSH (попытка {str(j)})', to_telegram=False)
        if not ssh_conn:
            if log:
                log_message(s, logf, bot_info, 'Ошибка подключения к удаленному серверу по SSH')
            return False

        with ssh_conn:
            if log:
                log_message(s, logf, bot_info, f'Подключение установлено')
            my_env = os.environ.copy()
            my_env["PGPASSFILE"] = '.local_pgpass'
            # loop through the specified schemas/tables tuples. list() used to allow multiple loops through schemas_tables.
            if log:
                log_message(s, logf, bot_info, f'Начинаю копирование данных из исходных таблиц...')

            from sys import platform
            if platform == "linux" or platform == "linux2":
                pg_dump = '/usr/lib/postgresql/15/bin/pg_dump'
                psql = '/usr/lib/postgresql/15/bin/psql'
            elif platform == "darwin":
                # OS X
                pg_dump = 'pg_dump'
                psql = 'psql'
            elif platform == "win32":
                # Windows...
                pg_dump = 'pg_dump'
                psql = 'psql'

            for (schema, tables) in list(schemas_tables):
                # each 'tables' is a list. loop through it now.
                for table in tables:
                    status = -1
                    i = 1
                    # launch pg_dump to dump the current table
                    while status != 0 and i <= 10:
                        try:
                            if log:
                                log_message(s, logf, bot_info, f'Копирование данных из таблицы {schema}.{table}, попытка {str(i)}...', to_telegram=False)
                            i += 1
                            result = subprocess.run([pg_dump, '-h', local_pgdsn_dict['host'], '-p', local_pgdsn_dict['port'],
                                            '-d', local_pgdsn_dict['dbname'], '-U',
                                            local_pgdsn_dict['user'], '--inserts', '-t', f'{schema}.{table}', '--no-publications',
                                            '--quote-all-identifiers', '-v', '-w', '-F', 'p', '-f',
                                            f'data/vgdb_5432_{schema}_{table}.dump'],
                                           env=my_env)
                            status = result.returncode
                        except:
                            if log:
                                log_message(s, logf, bot_info, f'Ошибка копирования данных из таблицы {schema}.{table} (попытка {str(i - 1)} из 10)')
                    if status != 0:
                        if log:
                            log_message(s, logf, bot_info, f'Ошибка копирования данных из таблицы {schema}.{table} после 10 попыток')
            # loop through the specified schemas/tables tuples. list() used to allow multiple loops through schemas_tables.
            my_env["PGPASSFILE"] = '.new_ext_pgpass'
            if log:
                log_message(s, logf, bot_info, f'Начинаю загрузку данных в целевые таблицы...')
            for (schema, tables) in list(schemas_tables):
                # each 'tables' is a list. loop through it now.
                for table in tables:
                    # launch psql to delete rows from current table. use my_env as env parameter.
                    filepath = os.path.join(current_directory, 'data', f"vgdb_5432_{schema}_{table}.dump")
                    if os.path.exists(filepath):


                        i = 1
                        # status = -1
                        status = None
                        # while status != 0 and i <= 10:
                        while not status and i <= 10:
                            try:
                                if log:
                                    log_message(s, logf, bot_info,
                                            f'Удаление данных из внешней таблицы {schema}.{table}, попытка {str(i)}...',
                                            to_telegram=False)
                                i += 1
                                pgconn = psycopg2.connect(new_ext_pgdsn)
                                if log:
                                    log_message(s, logf, bot_info,
                                            f'Подключение к внешней базе создано',
                                            to_telegram=False)
                                with pgconn:
                                    with pgconn.cursor() as cur:
                                        sql = f'DELETE FROM {schema}.{table};'
                                        if log:
                                            log_message(s, logf, bot_info,
                                                        f'Удаляю данные из таблицы {schema}.{table} на удаленном сервере...')
                                        cur.execute(sql)
                                        status = cur.statusmessage
                                pgconn.close()
                                # result = subprocess.run(
                                #     [psql, '-U', new_ext_pgdsn_dict['user'], '-h', new_ext_pgdsn_dict['host'],
                                #      '-p', new_ext_pgdsn_dict['port'], '-d', new_ext_pgdsn_dict['dbname'],
                                #      '-w', '-c', f'delete from {schema}.{table};'],
                                #     env=my_env)
                                # status = result.returncode
                            except Exception as err:
                                if log:
                                    log_message(s, logf, bot_info,
                                            f'{err}\nОшибка удаления данных из внешней таблицы {schema}.{table} (попытка {str(i - 1)} из 10)')


                        # if status != 0:
                        if status:
                            if 'DELETE' not in status:
                                if log:
                                    log_message(s, logf, bot_info,
                                            f'Ошибка удаления данных из внешней таблицы {schema}.{table} после 10 попыток. Пропускаю запись данных в таблицу.')

                            else:
                                i = 1
                                status = -1
                                while status != 0 and i <= 10:
                                    try:
                                        if log:
                                            log_message(s, logf, bot_info,
                                                    f'Запись данных во внешнюю таблицу {schema}.{table}, попытка {str(i)}...',
                                                    to_telegram=False)
                                        i += 1
                                        # launch psql to insert data to current table. use my_env as env parameter.
                                        result = subprocess.run(
                                            [psql, '-U', new_ext_pgdsn_dict['user'], '-h', new_ext_pgdsn_dict['host'],
                                             '-p', new_ext_pgdsn_dict['port'], '-d', new_ext_pgdsn_dict['dbname'],
                                             '-w', '-f', f'data/vgdb_5432_{schema}_{table}.dump'],
                                            env=my_env)
                                        status = result.returncode
                                    except Exception as err:
                                        if log:
                                            log_message(s, logf, bot_info,
                                                    f'{err}\nОшибка записи данных во внешнюю таблицу {schema}.{table} (попытка {str(i - 1)} из 10)')
                                if status != 0:
                                    if log:
                                        log_message(s, logf, bot_info,
                                                f'Ошибка записи данных во внешнюю таблицу {schema}.{table} после 10 попыток.')
                                else:
                                    if log:
                                        log_message(s, logf, bot_info, f'Таблица {schema}.{table} синхронизирована')

            for (schema, tables) in list(schemas_tables):
                for table in tables:
                    filepath = os.path.join(current_directory, 'data', f"vgdb_5432_{schema}_{table}.dump")
                    if os.path.exists(filepath):
                        os.remove(filepath)

        ssh_conn = None
        if log:
            log_message(s, logf, bot_info, f'Синхронизация таблиц с Evergis завершена')
        return True


def get_free_port(ports_range: tuple[int, int]):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = None
    for p in range(ports_range[0], ports_range[1] + 1):
        try:
            s.bind(("127.0.0.1", p))
            port = p
        except:
            pass
    return port


def synchro_schema(schemas, local_pgdsn_path, ext_pgdsn_path,
                  ssh_host='', ssh_user='',
                  local_port_for_ext_pg=5433, bot_info=('token', 'id'), folder='evergis', recreate=False):

    with open(ext_pgdsn_path, encoding='utf-8') as f:
        ext_pgdsn = f.read()

    with open(local_pgdsn_path, encoding='utf-8') as f:
        local_pgdsn = f.read()

    local_pgdsn_dict = dict([x.split('=') for x in local_pgdsn.split(' ')])
    ext_pgdsn_dict = dict([x.split('=') for x in ext_pgdsn.split(' ')])
    new_ext_pgdsn = ext_pgdsn.replace(f"port={ext_pgdsn_dict['port']}", f"port={str(local_port_for_ext_pg)}")
    new_ext_pgdsn_dict = dict([x.split('=') for x in new_ext_pgdsn.split(' ')])

    # chmod 0600 ~/.pgpass
    with open('.new_ext_pgpass', 'w', encoding='utf-8') as f:
        f.write(f"{new_ext_pgdsn_dict['host']}:{new_ext_pgdsn_dict['port']}:{new_ext_pgdsn_dict['dbname']}:{new_ext_pgdsn_dict['user']}:{new_ext_pgdsn_dict['password']}")
    os.chmod('.new_ext_pgpass', 0o600)

    with open('.local_pgpass', 'w', encoding='utf-8') as f:
        f.write(f"{local_pgdsn_dict['host']}:{local_pgdsn_dict['port']}:{local_pgdsn_dict['dbname']}:{local_pgdsn_dict['user']}:{local_pgdsn_dict['password']}")
    os.chmod('.local_pgpass', 0o600)

    current_directory = os.getcwd()
    # create a pathname for the logfile
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    # now we open the logfile and start logging
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:
        log_message(s, logf, bot_info, 'Начинаю синхронизацию схем с Evergis...')

        j = 1
        ssh_conn = None
        while not ssh_conn and j <= 10:
            log_message(s, logf, bot_info, f'Установка подключения к удаленному серверу по SSH, попытка {str(j)}...', to_telegram=False)
            try:
                j += 1
                ssh_conn = Connection(ssh_host, user=ssh_user, connect_kwargs={"banner_timeout": 60}).forward_local(local_port_for_ext_pg,
                                                                   remote_port=int(ext_pgdsn_dict['port']))
            except:
                log_message(s, logf, bot_info, f'Ошибка подключения к удаленному серверу по SSH (попытка {str(j)})', to_telegram=False)
        if not ssh_conn:
            log_message(s, logf, bot_info, 'Ошибка подключения к удаленному серверу по SSH')
            return False

        with ssh_conn:
            log_message(s, logf, bot_info, f'Подключение установлено')
            my_env = os.environ.copy()
            my_env["PGPASSFILE"] = '.local_pgpass'
            # loop through the specified schemas/tables tuples. list() used to allow multiple loops through schemas_tables.
            log_message(s, logf, bot_info, f'Начинаю копирование структуры исходных схем...')

            from sys import platform
            if platform == "linux" or platform == "linux2":
                pg_dump = '/usr/lib/postgresql/15/bin/pg_dump'
                psql = '/usr/lib/postgresql/15/bin/psql'
            elif platform == "darwin":
                # OS X
                pg_dump = 'pg_dump'
                psql = 'psql'
            elif platform == "win32":
                # Windows...
                pg_dump = 'pg_dump'
                psql = 'psql'

            for schema in list(schemas):
                status = -1
                i = 1
                # launch pg_dump to dump the current table
                while status != 0 and i <= 10:
                    try:
                        log_message(s, logf, bot_info, f'Копирование структуры схемы {schema}, попытка {str(i)}...', to_telegram=False)
                        i += 1
                        result = subprocess.run([pg_dump, '-h', local_pgdsn_dict['host'], '-p', local_pgdsn_dict['port'],
                                        '-d', local_pgdsn_dict['dbname'], '-U',
                                        local_pgdsn_dict['user'], '--inserts', '--no-publications', '-n', schema,
                                        '--quote-all-identifiers', '-v', '-s', '-w', '-F', 'p', '-f',
                                        f'data/vgdb_5432_schema_{schema}.dump'],
                                       env=my_env)
                        status = result.returncode
                    except:
                        log_message(s, logf, bot_info, f'Ошибка копирования структуры схемы {schema} (попытка {str(i - 1)} из 10)')
                if status != 0:
                    log_message(s, logf, bot_info, f'Ошибка копирования структуры схемы {schema} после 10 попыток')
            # loop through the specified schemas/tables tuples. list() used to allow multiple loops through schemas_tables.
            my_env["PGPASSFILE"] = '.new_ext_pgpass'
            log_message(s, logf, bot_info, f'Начинаю обновление структуры целевых схем...')
            for schema in list(schemas):
                # launch psql to delete rows from current table. use my_env as env parameter.
                filepath = os.path.join(current_directory, 'data', f"vgdb_5432_schema_{schema}.dump")
                if os.path.exists(filepath):
                    status = 0
                    if recreate:
                        i = 1
                        status = -1
                        while status != 0 and i <= 10:
                            try:
                                log_message(s, logf, bot_info,
                                            f'Удаление внешней схемы {schema}, попытка {str(i)}...',
                                            to_telegram=False)
                                i += 1
                                result = subprocess.run(
                                    [psql, '-U', new_ext_pgdsn_dict['user'], '-h', new_ext_pgdsn_dict['host'],
                                     '-p', new_ext_pgdsn_dict['port'], '-d', new_ext_pgdsn_dict['dbname'],
                                     '-w', '-c', f'drop schema {schema} CASCADE;'],
                                    env=my_env)
                                status = result.returncode
                            except:
                                log_message(s, logf, bot_info,
                                            f'Ошибка удаления внешней схемы {schema} (попытка {str(i - 1)} из 10)')
                    if status != 0:
                        log_message(s, logf, bot_info,
                                    f'Ошибка удаления внешней схемы {schema} после 10 попыток. Пропускаю запись данных в таблицу.')
                    else:
                        i = 1
                        status = -1
                        while status != 0 and i <= 10:
                            try:
                                log_message(s, logf, bot_info,
                                            f'Обновление внешней схемы {schema}, попытка {str(i)}...',
                                            to_telegram=False)
                                i += 1

                                result = subprocess.run(
                                    [psql, '-U', new_ext_pgdsn_dict['user'], '-h', new_ext_pgdsn_dict['host'],
                                     '-p', new_ext_pgdsn_dict['port'], '-d', new_ext_pgdsn_dict['dbname'],
                                     '-w', '-f', f'data/vgdb_5432_schema_{schema}.dump'],
                                    env=my_env)
                                status = result.returncode
                            except:
                                log_message(s, logf, bot_info,
                                            f'Ошибка обновления внешней схемы {schema} (попытка {str(i - 1)} из 10)')
                        if status != 0:
                            log_message(s, logf, bot_info,
                                        f'Ошибка обновления внешней схемы {schema} после 10 попыток.')
                        else:
                            log_message(s, logf, bot_info, f'Схема {schema} синхронизирована')

            for schema in list(schemas):
                filepath = os.path.join(current_directory, 'data', f"vgdb_5432_schema_{schema}.dump")
                if os.path.exists(filepath):
                    os.remove(filepath)

        ssh_conn = None
        log_message(s, logf, bot_info, f'Синхронизация схем с Evergis завершена')
        return True


if __name__ == '__main__':
    # read the telegram bot credentials
    with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
        jdata = json.load(f)
        bot_info = (jdata['token'], jdata['chatid'])

    with open('.ext_pgdsn', encoding='utf-8') as f:
        ext_pgdsn = f.read()

    with open('.pgdsn', encoding='utf-8') as f:
        local_pgdsn = f.read()

    with open('.egdsn', 'r', encoding='utf-8') as f:
        egdata = json.load(f)
        pass

    with open('.egssh', 'r', encoding='utf-8') as f:
        egssh = json.load(f)


    # synchro_schema(['wialon'], '.pgdsn', '.ext_pgdsn', ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)
    # synchro_layer([('culture', ['gps_tracks', 'waypoints'])],
    #               local_pgdsn, ext_pgdsn, ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)
    # synchro_layer([('culture', ['parcels_planning'])], local_pgdsn, ext_pgdsn, ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)
    # synchro_layer([('culture', ['parcels_planning'])], local_pgdsn, ext_pgdsn, ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)
    # synchro_table([('dm', ['contracts', 'parcels_to_contracts'])], '.pgdsn', '.ext_pgdsn', ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)
    synchro_table([('wialon', ['wialon_evts'])], '.pgdsn', '.ext_pgdsn', ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)


    # # new seismic 2d
    # synchro_table([('dm', ['datasets_to_geometries', 'companies', 'links', 'processings', 'surveys', 'reports', 'links_to_datasets', 
    #                        'reports_to_surveys', 'seismic_datasets', 'contracts'])], '.pgdsn', '.ext_pgdsn', ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)
    # synchro_layer([('dm', ['seismic_lines_processed_2d'])], local_pgdsn, ext_pgdsn, ssh_host=egssh["host"],
    #               ssh_user=egssh["user"], bot_info=bot_info)
    
    # # # new seismic 3d
    # synchro_table([('dm', ['datasets_to_geometries', 'companies', 'links', 'processings', 'surveys', 'reports', 'links_to_datasets', 
    #                        'reports_to_surveys', 'seismic_datasets'])], '.pgdsn', '.ext_pgdsn', ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)
    # synchro_layer([('dm', ['seismic_pols_processed_3d'])], local_pgdsn, ext_pgdsn, ssh_host=egssh["host"],
    #               ssh_user=egssh["user"], bot_info=bot_info)

    
    # map_table_view(egdata["user"], egdata["password"], ['aerograv_planning_gin_view'], schema='culture')
    
    # unmap_table(egdata["user"], egdata["password"], ['lotcards_spatial_licensed'])
    #
    # map_table(egdata["user"], egdata["password"], ['wells_planning'], 'culture')
    # map_table(egdata["user"], egdata["password"], ['field_points'], 'geology')

    # print(get_free_port((5432, 5434)))
    
    

    # vnigni.struct01_24_new_props_and_geom;
    # vnigni.struct01_24_old_geom_new_props;
    # vnigni.struct01_24_old_props_and_geom;
    # vnigni.struct01_24_old_props_new_geom;
    # vnigni.struct01_24_removed_geom_mv;


#     with requests.Session() as s:
#         url = 'https://geomercury.ru/ds'
#         result = s.get(url)
#         pass

#         data = {
#             "name": "testDs",
#             "alias": null,
#             "description": null,
#             "type": "Postgres",
#             "connectionString": "Server=172.25.64.1;Port=5432;User Id=postgres;Password=postgres;Database=develop;Maximum Pool Size=25;"
# }


    # with login_to_evergis(egdata['user'], egdata['password']) as s:
    #     url = 'https://geomercury.ru/sp/ds'
    #     result = s.get(url)
        
    #     pass