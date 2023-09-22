import requests
import os
# import json
from bs4 import BeautifulSoup
import csv
import psycopg2
from psycopg2.extras import *
from vgdb_general import send_to_telegram
from datetime import datetime


def request_reports(**kwargs):
    """
    This function makes request to rfgf.ru/catalog/index.php website and returns the result
    from all or from requested pages of the result as dictionary and optionally writes the result to csv file.
    :param kwargs:
    ftext='some text' - mandatory. Search string for the request.
    out_csv='path to output csv' - optional. Path to result csv file. The default is null, which means not to write output file.
    start_page=number - optional. Number of the first request result page to process. The default is 1.
    end_page=number - optional. Number of the last request result page to process. The default is the last page.
    :return:
    The function returns the dictionary with search results from rfgf.ru/catalog/index.php website.
    If out_csv='path to output csv' is specified, the result will be written to the file too.
    """
    with requests.Session() as s:
        # this makes the request to rfgf catalog. The structure of request is hacked from Chrome F12 mode.
        # first kwargs['ftext'] parameter is used as a search string.
        status_code = 0
        i = 1
        response = None
        while status_code != 200 and i <= 10:
            try:
                response = requests.post('https://rfgf.ru/catalog/index.php',
                                         headers={'accept': '*/*',
                                                  'accept-encoding': 'gzip, deflate, br',
                                                  'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,en-GB;q=0.6',
                                                  'Connection': 'keep-alive',
                                                  'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                                                  # 'cookie': 'PHPSESSID=c46b455dd42f1126c309c3aadbd03d62; selfpath=/catalog/index.php; detalied=1; _ym_uid=1658903758737911296; _ym_d=1658903758; _ym_isad=2; _ym_visorc=w',
                                                  'dnt': '1'
                                                  },
                                         data={
                                             'ftext': kwargs['ftext'],
                                             'search': '0',
                                             'docname': '',
                                             'authors': '',
                                             'invn': '',
                                             'nom': '',
                                             'pnum': '',
                                             'pdate': '',
                                             'penddate': '',
                                             'year': '',
                                             'place': '',
                                             'full': '1',
                                             'gg': '',
                                             'mode': 'limctl',
                                             'orgisp': '',
                                             'source': '',
                                             'pi': ''
                                         }
                                         )
                status_code = response.status_code
            except:
                pass
            i += 1
        if response:
            # parse the html result of the request with BeautifulSoup parser
            soup = BeautifulSoup(response.text, 'html.parser')
            # this is the default number of pages to process
            pages = 1
            # now let's find how many pages the result has
            # check if the result is not empty
            if 'Поиск не дал результатов' not in soup.text:
                # find the <ul class="hr2" id="list_pages2"> tag which contains the number of pages, then loop through the <li> tags in it
                for li in soup.find(id='list_pages2').find_all('li'):
                    # if 'из' is in the tag text, then whe are inside the desired tag
                    if li.text.find('из') > 0:
                        # take the tag text and remove anything but the numbers from it
                        pages = int(str(li.text[li.text.find('из') + 3:].replace('>', '').replace(' ', '')))
                        # print(pages)


                # create an empty list for the reports. Each report will be appended here as dictionary
                reports = []
                # Flag to write output to csv or not
                write_csv = False
                # If more than one arg is specified, we assume that csv must be written
                if 'out_csv' in kwargs.keys() and len(kwargs['out_csv']) > 1:
                    write_csv = True
                # open csv file for writing if write_csv is True
                if write_csv:
                    csvfile = open(kwargs['out_csv'], 'w', newline='', encoding='utf-8')

                # this loop is to discover the field names only
                # find the html tag with id='report_table' and then loop through all tables in it
                fields = []
                for report_table in soup.find(id='report_table').find_all('table'):
                    # create empty lists for field names and for the values
                    # fields = []
                    values = []
                    # find the html table with class='report'. This is usually the table with all search results on the page
                    if report_table['class'] == ['report']:
                        # start the row counter for the report table
                        row_counter = 0
                        # loop through all rows of the table
                        for row in report_table.find_all('tr'):
                            # iterate row counter
                            row_counter += 1
                            # This <if> section is to create the fieldnames list.
                            # check if we are in the 'head' row.
                            if 'class' in row.attrs.keys() and row['class'] == ['head']:
                                # start the cells counter
                                td_counter = 0
                                # loop through all cells in a row
                                for td in row.find_all('td'):
                                    # iterate cells counter
                                    td_counter += 1
                                    # the first cell is always blank, so we'll skip it
                                    if td_counter > 1:
                                        # There are two head rows in the table. All cells are rowspanned except the
                                        # Предметно-систематический классификатор.
                                        # so we check if we are in this cell and therefore add two fieldnames
                                        # Раздел and Подраздел to the list
                                        if td.text == 'Предметно-систематический классификатор':
                                            fields.append('Раздел')
                                            fields.append('Подраздел')
                                        # if we are in any other cell in two head rows except Раздел and Подраздел,
                                        # add its value to the list of fieldnames.
                                        elif td.text not in ('Раздел', 'Подраздел'):
                                            fields.append(td.text)
                                # if we've passed through both head rows
                                if row_counter == 2:
                                    # add additional fieldname for the url to this report
                                    fields.append('Ссылка')
                                    # now we've passed through the header rows, and it's time to create
                                    # the DictWriter for the csv file if we need it
                                    if write_csv:
                                        writer = csv.DictWriter(csvfile, fieldnames=fields, delimiter='|')
                                        # write csv header to the output file
                                        writer.writeheader()

                reports_written_counter = 0
            else:
                return []
        else:
            return []
        #################################################################################################################
        # this loop is to discover the data itself, looping through the pages of the result
        # the defaut start page
        start = 1
        # if the start page is specified in **kwargs, then use it
        if 'start_page' in kwargs.keys() and kwargs['start_page'] and type(kwargs['start_page'] == int):
            start = kwargs['start_page']
        # the default end page is the total number of pages. if the start page is specified in **kwargs, then use it
        if 'end_page' in kwargs.keys() and kwargs['end_page'] and type(kwargs['end_page'] == int):
            pages = kwargs['end_page']
        # loop through the pages of the result
        for i in range(start, pages + 1):
            # make http post request to the current page of the result
            k = 1
            status_code = 0
            new_response = None
            while status_code != 200 and k <=10:
                try:
                    new_response = s.post('https://rfgf.ru/catalog/index.php',
                                             headers={'accept': '*/*',
                                                      'accept-encoding': 'gzip, deflate, br',
                                                      'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,en-GB;q=0.6',
                                                      'Connection': 'keep-alive',
                                                      'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                                                      # 'cookie': 'PHPSESSID=c46b455dd42f1126c309c3aadbd03d62; selfpath=/catalog/index.php; detalied=1; _ym_uid=1658903758737911296; _ym_d=1658903758; _ym_isad=2; _ym_visorc=w',
                                                      'dnt': '1'
                                                      },
                                             data={
                                                 'ftext': kwargs['ftext'],
                                                 'search': '0',
                                                 'docname': '',
                                                 'authors': '',
                                                 'invn': '',
                                                 'nom': '',
                                                 'pnum': '',
                                                 'pdate': '',
                                                 'penddate': '',
                                                 'year': '',
                                                 'place': '',
                                                 'full': '1',
                                                 'gg': '',
                                                 'mode': 'limctl',
                                                 'orgisp': '',
                                                 'source': '',
                                                 'pi': '',
                                                 # this is the page number
                                                 'page': str(i)
                                             }
                                             )
                    status_code = new_response.status_code
                except:
                    pass
                k += 1
            if new_response:
                # create new BeautifulSoup from the current page
                new_soup = BeautifulSoup(new_response.text, 'html.parser')
                # loop through all the tables in <div id="report_table"> tag
                for new_report_table in new_soup.find(id='report_table').find_all('table'):
                    # create empty lists for the values
                    values = []
                    # find the html table with class='report'. This is usually the table with all search results on the page
                    if new_report_table['class'] == ['report']:
                        # start the row counter for the report table
                        row_counter = 0
                        # loop through all rows of the table
                        for row in new_report_table.find_all('tr'):
                            # iterate row counter
                            row_counter += 1
                            # if we are not in the table head
                            if not ('class' in row.attrs.keys() and row['class'] == ['head']):
                                # start new cell-in-a-row counter
                                td_counter = 0
                                # string for the link to this report in the catalog
                                link = ''
                                # loop through all cells in a row
                                for td in row.find_all('td'):
                                    # There are rows with colspan=21, which we don't need. Check if we are not in it.
                                    if not ('colspan' in td.attrs.keys() and td['colspan'] == '21'):
                                        # iterate cell counter
                                        td_counter += 1
                                        # we don't need the data from the first cell
                                        if td_counter > 1:
                                            # if we are in the 10-th cell, which is for the
                                            # 'Доступен для загрузки через реестр ЕФГИ',
                                            # and the link exists in the cell (<a href=....>Да</a> hyperlink)
                                            # then write the url from the <a> instead of the text
                                            if td_counter == 10:
                                                if 'Да' in td.text:
                                                    values.append('https://rfgf.ru/catalog/' + td.a['href'])
                                                # If there is no link in the cell, write just the text
                                                else:
                                                    values.append(td.text.replace('\n', '').replace('\r', '').rstrip())
                                            # if we are in the 8-1th cell, which is the 'Есть сканобраз' column,
                                            # then we again take the url from <a> and append an ending to it
                                            elif td_counter == 8:
                                                if 'Да' in td.text:
                                                    values.append(
                                                        'https://rfgf.ru/catalog/' + td.a['href'] + '&dtype=1#refancor')
                                                # if cell 8 is empty, add just the text
                                                else:
                                                    values.append(td.text.replace('\n', '').replace('\r', '').rstrip())
                                            # if we are in any cell other than 1, 8 or 10, just write the cell value to the list
                                            else:
                                                values.append(td.text.replace('\n', '').replace('\r', '').rstrip())
                                        # take the report url from the 2nd cell's href attribute
                                        if td_counter == 2:
                                            link = td.a['href']
                                # add the link of the report as the last value to the list
                                values.append(link.replace('./', 'https://rfgf.ru/catalog/'))
                                # if we have any values from the row
                                if len(values) > 1:
                                    # create the current report dictionary and append it to the result list
                                    reports.append({fields[i]: values[i] for i in range(len(fields))})
                                    # if we must create the output csv
                                    if write_csv:
                                        # add new data line to output csv
                                        writer.writerow({fields[i]: values[i] for i in range(len(fields))})
                                        # iterate the written reports counter
                                        reports_written_counter += 1
                                        # print message for every 100 reports written
                                        if reports_written_counter % 100 == 0:
                                            print(reports_written_counter, 'reports processed')
                                # empty the values list for the next row loop
                                values = []
            else:
                return []
        #################################################################################################################
        # close the output csv file if we opened it
        if write_csv:
            csvfile.close()
            # print the total number of reports processed
            # print(reports_written_counter, 'reports processed')
        # return the dictionary of reports
        return reports


def get_pages_number():
    headers = {'accept': '*/*',
               'accept-encoding': 'gzip, deflate, br',
               'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,en-GB;q=0.6',
               'Connection': 'keep-alive',
               'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
               'dnt': '1'
               }
    data = {
        'ftext': '',
        'search': '0',
        'docname': '',
        'authors': '',
        'invn': '',
        'nom': '',
        'pnum': '',
        'pdate': '',
        'penddate': '',
        'year': '',
        'place': '',
        'full': '1',
        'gg': '',
        'mode': 'limctl',
        'orgisp': '',
        'source': '',
        'pi': ''
    }
    with requests.Session() as s:
        i = 1
        status_code = 0
        while status_code != 200 and i <= 10:
            try:
                response = s.post('https://rfgf.ru/catalog/index.php', headers=headers, data=data)
                status_code = response.status_code
            except:
                pass
            i += 1
        if status_code != 200:
            return (False, 0)
        # parse the html result of the request with BeautifulSoup parser
        soup = BeautifulSoup(response.text, 'html.parser')
        # this is the default number of pages to process
        pages = 1
        # now let's find how many pages the result has
        # check if the result is not empty
        if 'Поиск не дал результатов' not in soup.text:
            # find the <ul class="hr2" id="list_pages2"> tag which contains the number of pages, then loop through the <li> tags in it
            for li in soup.find(id='list_pages2').find_all('li'):
                # if 'из' is in the tag text, then whe are inside the desired tag
                if li.text.find('из') > 0:
                    # take the tag text and remove anything but the numbers from it
                    pages = int(str(li.text[li.text.find('из') + 3:].replace('>', '').replace(' ', '')))
                    # print(pages)
            return (True, pages)
        else:
            return (True, 0)


def check_report(pgconn, table, report):
    with pgconn:
        with pgconn.cursor() as cur:
            cur.execute(f"Select * FROM {table} LIMIT 0")
            fields = [desc[0] for desc in cur.description]
            doc_type = report['Вид документа'].replace("'", "''")
            doc_name = report['Название документа'].replace("'", "''")
            res_type = report['Полезные ископаемые'].lower()
            sql = f"select * from {table} where \"Инвентарный номер\" = '{report['Инвентарный номер']}' and \"Вид документа\" = '{doc_type}' and \"Название документа\" = '{doc_name}';"
            # sql = f"select * from {table} where \"№ п/п\" = '{report['№ п/п']}';"
            cur.execute(sql)
            result = cur.fetchall()
            if result:
                changes = []
                for i, value in enumerate(list(report.values())):
                    if str(result[0][1:][i]) != value:
                        if i != 0 and any(['нефт' in res_type, 'газ' in res_type, 'конденсат' in res_type, 'углеводород' in res_type]):  # If the field is not "№ п/п" (it changes all the time)
                            change = {"field": list(report.keys())[i], "old_value": str(result[0][1:][i]), "new_value": value}
                            changes.append(change)
                        value = str(value).replace("'", "''")
                        sql = f"update {table} set \"{fields[1:][i]}\" = '{str(value)}' where \"Инвентарный номер\" = '{report['Инвентарный номер']}' and \"Вид документа\" = '{doc_type}'and \"Название документа\" = '{doc_name}';"
                        # sql = f"update {table} set \"{fields[1:][i]}\" = '{str(value)}' where \"№ п/п\" = '{report['№ п/п']}';"
                        cur.execute(sql)
                        # pgconn.commit()
                if changes:
                    return {
                        "update_type": "report_changed",
                        "update_info": {
                            "report_sn": report['Инвентарный номер'],
                            "report_name": report['Название документа'],
                            "report_type": report['Вид документа'],
                            "report_url": report['Ссылка'],
                            "changes": changes
                        }
                    }
                else:
                    return False
            else:
                fields_to_update = ['"' + x + '"' for x in fields]
                values_to_insert = ["'" + x.replace("'", "''") + "'" for x in report.values()]
                sql = f"insert into {table}({', '.join(fields_to_update[1:])}) values({', '.join(values_to_insert)});"
                cur.execute(sql)
                # pgconn.commit()
                if any(['нефт' in res_type, 'газ' in res_type, 'конденсат' in res_type, 'углеводород' in res_type]):
                    return {"update_type": "new_report",
                            "update_info": {"report_sn": report['Инвентарный номер'],
                                            "report_name": report['Название документа'],
                                            "report_type": report['Вид документа'],
                                            "report_url": report['Ссылка']}}
                else:
                    return False


def send_reports_csv_to_telegram(s, logf, fname, reports_list=[], list_type='all_new', bot_info=('token', 'chatid')):
    if reports_list:
        fields = []
        message = ''
        if list_type == 'all_new':
            fields = ['Сериный номер', 'Название', 'Вид документа', 'Адрес']
            message = f"В Каталог Росгеолфонда добавлено {str(len(reports_list))} новых документов"
        elif list_type == 'all_changed':
            fields = ['Сериный номер', 'Название', 'Тип документа', 'Атрибут', 'Старое значение',
                      'Новое значение', 'Адрес']
            message = f'Изменено {str(len(reports_list))} документов в Каталоге Росгеолфонда'
        elif list_type == 'link_added':
            fields = ['Серийный номер', 'Название', 'Тип документа', 'Атрибут',
                      'Старое значение',
                      'Новое значение', 'Адрес']
            message = f"Добавлена ссылка в {str(len(reports_list))} документов по УВС в Каталоге Росгеолфонда"
        elif list_type == 'link_removed':
            fields = ['Серийный номер', 'Название', 'Тип документа', 'Атрибут',
                      'Старое значение',
                      'Новое значение', 'Адрес']
            message = f"Удалена ссылка в {str(len(reports_list))} документах по УВС в Каталоге Росгеолфонда"
        # with open(fname, 'w', newline='', encoding='utf-8') as csvfile:
        with open(fname, 'w', newline='', encoding='Windows-1251') as csvfile:
            # writer = csv.DictWriter(csvfile, fieldnames=fields, delimiter='|')
            writer = csv.DictWriter(csvfile, fieldnames=fields, dialect='excel', delimiter=';')
            writer.writeheader()
            for report in reports_list:
                if list_type == 'all_new':
                    writer.writerow({
                                        "Серийный номер": report['report_sn'],
                                        "Название": report['report_name'],
                                        "Вид документа": report['report_type'],
                                        "Адрес": report['report_url']
                                    })
                elif list_type in ['all_changed', 'link_added', 'link_removed']:
                    for change in report['update_info']['changes']:
                        writer.writerow({
                            "Сериный номер": report['update_info']['report_sn'],
                            "Название": report['update_info']['report_name'],
                            "Тип документа": report['update_info']['report_type'],
                            "Атрибут": change['field'],
                            "Старое значение": change['old_value'],
                            "Новое значение": change['new_value'],
                            "Адрес": report['update_info']['report_url']
                        })
        success = send_to_telegram(s, fname, bot_info=bot_info, message=message, document=fname)
        if not success:
            logf.write(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Send file {fname} to telegram failed")
            print(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Send file {fname} to telegram failed")
        if os.path.exists(os.path.join(os.getcwd(), fname)):
            os.remove(os.path.join(os.getcwd(), fname))



def refresh_rfgf_reports(pgdsn,
                         table='rfgf.rfgf_catalog',
                         pages_pack_size=5000,
                         send_updates=True,
                         log_bot_info=('fake_token', 'fake_chatid'),
                         report_bot_info=('fake_token', 'fake_chatid'),
                         max_packs=10000000):
    pages_result = get_pages_number()
    if pages_result[0]:
        with requests.Session() as s:
            with open('rfgf_reports/rfgf_reports_log.txt', 'w', encoding='utf-8') as f:
                message = f"Запущено обновление отчетов Росгеолфонда. Размер пака {str(pages_pack_size)}, максимум паков {str(max_packs)}."
                send_to_telegram(s, f, bot_info=log_bot_info, message=message)
        # updates_report = []
        pages = pages_result[1]
        # pages_pack_size = 5000
        n_packs = pages // pages_pack_size
        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        for i in range(0, min([n_packs, max_packs]) + 1):
            updates_report = []
            start_page = i * pages_pack_size + 1
            if i < n_packs:
                end_page = (i + 1) * pages_pack_size
            else:
                end_page = pages
            with requests.Session() as s:
                with open('rfgf_reports/rfgf_reports_log.txt', 'w', encoding='utf-8') as f:
                    message = f"Запущена загрузка отчетов Росгеолфонда, страницы с {str(start_page)} по {str(end_page)}."
                    send_to_telegram(s, f, bot_info=log_bot_info, message=message)
            if i < n_packs:
                reports = request_reports(ftext='', start_page=start_page, end_page=end_page)
            else:
                reports = request_reports(ftext='', start_page=start_page)

            with requests.Session() as s:
                with open('rfgf_reports/rfgf_reports_log.txt', 'w', encoding='utf-8') as f:
                    message = f"Загрузка отчетов Росгеолфонда выполнена. Страницы с {str(start_page)} по {str(end_page)}. " \
                              f"Загружено {len(reports)} документов."
                    send_to_telegram(s, f, bot_info=log_bot_info, message=message)

            pgconnection = psycopg2.connect(pgdsn, cursor_factory=DictCursor)
            # with pgconnection:
            for j, report in enumerate(reports):

                update = check_report(pgconnection, table=table, report=report)

                if (j + 1) % 10000 == 0:
                    with requests.Session() as s:
                        with open('rfgf_reports/rfgf_reports_log.txt', 'w', encoding='utf-8') as f:
                            message = f"Проверено {str(j + 1)} отчетов."
                            send_to_telegram(s, f, bot_info=log_bot_info, message=message)
                if update:
                    updates_report.append(update)
                if updates_report and len(updates_report) % 100 == 0:
                    pgconnection.commit()
            pgconnection.commit()
            pgconnection.close()
            if send_updates and updates_report:
                with open('rfgf_reports/rfgf_reports_log.txt', 'w', encoding='utf-8') as f:
                    with requests.Session() as s:
                        new_reports = [x['update_info'] for x in updates_report if x['update_type'] == 'new_report']
                        if new_reports:
                            fname = f"Новые_документы_УВС_РФГФ_{timestamp}_{str(i + 1)}.csv"
                            send_reports_csv_to_telegram(s, f, fname, new_reports, list_type='all_new',
                                                         bot_info=report_bot_info)

                        changed_reports = [x for x in updates_report if x['update_type'] == 'report_changed']
                        if changed_reports:
                            fname = f"Изменения_документов_УВС_РФГФ_{timestamp}_{str(i + 1)}.csv"
                            send_reports_csv_to_telegram(s, f, fname, changed_reports, list_type='all_changed',
                                                         bot_info=report_bot_info)

                            # reports_with_changed_link = [x for x in changed_reports if 'Доступен для загрузки через реестр ЕФГИ' in [change['field'] for change in x['update_info']['changes']]]
                            reports_with_link_added = []
                            reports_with_link_removed = []
                            for report in changed_reports:
                                for change in report['update_info']['changes']:
                                    if change['field'] == 'Доступен для загрузки через реестр ЕФГИ':
                                        if change['new_value']:
                                            reports_with_link_added.append(report)
                                        else:
                                            reports_with_link_removed.append(report)
                            if reports_with_link_added:
                                fname = f"Документы_УВС_РФГФ_с_новой_ссылкой_{timestamp}_{str(i + 1)}.csv"
                                send_reports_csv_to_telegram(s, f, fname, reports_with_link_added,
                                                             list_type='link_added',
                                                             bot_info=report_bot_info)
                            if reports_with_link_removed:
                                fname = f"Документы_УВС_РФГФ_с_удаленной_ссылкой_{timestamp}_{str(i + 1)}.csv"
                                send_reports_csv_to_telegram(s, f, fname, reports_with_link_removed,
                                                             list_type='link_removed',
                                                             bot_info=report_bot_info)


# This is an example of using the class.
# first you create an instance of RfgfCatalogInvestigator.
# second, you call it's request_reports function to download the data from rfgf.ru/catalog/index.php
# You must specify the search string in ftext parameter, and you may specify the output csv in out_csv parameter,
# the start page to download in start_page parameter and the last page to download in end_page parameter.
# To understand what these pages mean, you can go to rfgf.ru/catalog/index.php website, make search with an empty search string (for example).
# Then at the bottom of the page you'll see the result pages switcher. These are the page numbers for start_page and end_page parameters.
# When there are thousands of pages, it may be convenient to split the request in several parts, downloading limited number of pages at a time.
# If you want to download all the results at one time, just skip the startpage and end_page parameters.


# reports = request_reports(ftext='', start_page=1, end_page=1)


# reports = my_investigator.request_reports(ftext='', out_csv='all_reports_from_rfgf_20230911_part1.csv', start_page=1, end_page=4690)
# reports = my_investigator.request_reports(ftext='', out_csv='all_reports_from_rfgf_20230911_part2.csv', start_page=4691, end_page=9380)
# reports = my_investigator.request_reports(ftext='', out_csv='all_reports_from_rfgf_20230911_part3.csv', start_page=9381, end_page=14070)
# reports = my_investigator.request_reports(ftext='', out_csv='all_reports_from_rfgf_20230911_part4.csv', start_page=14071, end_page=18760)
# reports = my_investigator.request_reports(ftext='', out_csv='all_reports_from_rfgf_20230911_part5.csv', start_page=18761, end_page=23450)
# reports = my_investigator.request_reports(ftext='', out_csv='all_reports_from_rfgf_20230911_part6.csv', start_page=23451)

# with open('.pgdsn', encoding='utf-8') as dsnf:
#     dsn = dsnf.read().replace('\n', '')
# #
# # # # This is telegram credentials to send message to stepanosokin
# with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
#     jdata = json.load(f)
#     bot_info = (jdata['token'], jdata['chatid'])
# #
# # refresh_rfgf_reports(dsn, pages_pack_size=1, report_bot_info=bot_info, send_updates=True, max_packs=1)
# refresh_rfgf_reports(dsn, pages_pack_size=10, report_bot_info=bot_info, log_bot_info=bot_info, send_updates=True, max_packs=1)
# pass

