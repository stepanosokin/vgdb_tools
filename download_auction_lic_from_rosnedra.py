from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import locale
import os, fnmatch
import shutil
import pandas as pd


def rus_month_genitive_to_nominative(i_string):
    months_genitive = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
    months_nominative = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь', 'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь']
    for pair in zip(months_genitive, months_nominative):
        i_string = i_string.replace(pair[0], pair[1])
    return i_string


def download_orders(start=datetime(year=2023, month=1, day=1), end=datetime.now(), folder='rosnedra_auc\\'):
    current_directory = os.getcwd()
    logdateformat = '%Y-%m-%d %H:%M:%S'
    log_file = os.path.join(current_directory, f"{folder}logfile.txt")
    open(log_file, 'w', encoding='utf-8').close()
    with open(log_file, 'a', encoding='utf-8') as logf:

        with requests.Session() as s:
            url = 'https://www.rosnedra.gov.ru/index.fcgi'
            params = {
                'page': 'search',
                'step': '1',
                'q': 'Об утверждении Перечня участков недр'
            }
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru - RU, ru;q = 0.9, en - US;q = 0.8, en;q = 0.7, en - GB;q = 0.6',
                'Connection': 'keep-alive',
                'DNT': '1'
            }
            try:
                search_result = s.get(url, params=params, headers=headers, verify=False)
                i = 1
                while search_result.status_code != 200:
                    search_result = s.get(url, params=params, headers=headers, verify=False)
                    i += 1
                    if i > 100:
                        break
            except:
                #print('Initial request to www.rosnedra.gov.ru failed, please check your params')
                logf.write(f"{datetime.now().strftime(logdateformat)} Initial request to www.rosnedra.gov.ru failed, please check your params\n")
            #print(search_result.text)

            first_soup = BeautifulSoup(search_result.text, 'html.parser')
            pages = first_soup.find(attrs={'class': 'Pager'}).find_all('a')
            pages = [p.text for p in pages if p.text != '']
            if len(pages) > 0:
                search_result_number = 1
                for page in pages:
                    url = 'https://www.rosnedra.gov.ru/index.fcgi'
                    params={
                        'page': 'search',
                        'from_day': '28',
                        'from_month': '04',
                        'from_year': '2012',
                        'till_day': datetime.now().strftime('%d'),
                        'till_month': datetime.now().strftime('%m'),
                        'till_year': datetime.now().strftime('%Y'),
                        'q': 'Об утверждении Перечня участков недр',
                        'step': '1',
                        'order': '2',
                        'part': page
                    }
                    try:
                        page_result = s.get(url, params=params, headers=headers, verify=False)
                        i = 1
                        while page_result.status_code != 200:
                            page_result = s.get(url, params=params, headers=headers, verify=False)
                            i += 1
                            if i > 100:
                                break
                    except:
                        #print(f'Request to www.rosnedra.gov.ru search results page {page} failed, please check your params')
                        logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}. Request to www.rosnedra.gov.ru search results page {url} failed, please check your params\n")

                    cur_search_results_page_soup = BeautifulSoup(page_result.text, 'html.parser')
                    for search_result_item in cur_search_results_page_soup.find(attrs={'class': 'search-result-list'}).find_all(attrs={'class': 'search-result-item'}):
                        locale.setlocale(locale.LC_TIME, locale='ru_RU')
                        for search_result_link_info_item in search_result_item.find_all(attrs={'class': 'search-result-link-info-item'}):
                            if 'Дата' in search_result_link_info_item.text:
                                item_date = rus_month_genitive_to_nominative(search_result_link_info_item.text.lower())
                                item_date = datetime.strptime(item_date.title(), 'Дата Документа:\xa0\xa0%d\xa0%B\xa0%Y')

                        if start <= item_date <= end:
                            #filename = f"{folder}{str(search_result_number)}_{item_date.strftime('%Y%m%d')}/result_url.txt"

                            # final_directory = os.path.join(current_directory, f"{folder}{str(search_result_number)}_{item_date.strftime('%Y%m%d')}")
                            # if os.path.exists(final_directory):
                            #     shutil.rmtree(final_directory, ignore_errors=True)
                            ###os.makedirs(final_directory)

                            url = 'https://' + f"rosnedra.gov.ru/{search_result_item.find(attrs={'class': 'search-result-link'})['href']}".replace('//', '/')
                            # name = search_result_item.find(attrs={'class': 'search-result-link'}).text
                            # with open(f"{final_directory}\\result_url.txt", 'w', encoding='utf-8') as f:
                            #     f.write(f"{name}\n\n{url}")
                            try:
                                item_page_result = s.get(url)
                                i = 1
                                while item_page_result.status_code != 200:
                                    page_result = s.get(url, verify=False)
                                    i += 1
                                    if i > 100:
                                        logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Maximum tries to download {url} failed, please check your params\n")
                                        break
                            except:
                                #print(f'Request to {url} failed, please check your params')
                                logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Request to {url} failed, please check your params\n")
                            cur_item_page_result_soup = BeautifulSoup(item_page_result.text, 'html.parser')
                            cur_content_tags = cur_item_page_result_soup.find(attrs={'class': 'Content'})
                            cur_h1_tags = cur_item_page_result_soup.find_all('h1')
                            is_order = False
                            if cur_h1_tags:
                                for h1_tag in cur_h1_tags:
                                    if 'Приказ Роснедр от' in h1_tag.text:
                                        is_order = True

                            if cur_content_tags and is_order:
                                final_directory = os.path.join(current_directory,
                                                               f"{folder}{str(search_result_number)}_{item_date.strftime('%Y%m%d')}")
                                if os.path.exists(final_directory):
                                    shutil.rmtree(final_directory, ignore_errors=True)
                                os.makedirs(final_directory)
                                name = search_result_item.find(attrs={'class': 'search-result-link'}).text
                                with open(f"{final_directory}\\result_url.txt", 'w', encoding='utf-8') as f:
                                    f.write(f"{name}\n\n{url}")

                                for item_doc_tag in cur_content_tags.find_all('a'):
                                    curl = f"https://www.rosnedra.gov.ru{item_doc_tag['href']}"
                                    cname = item_doc_tag.text
                                    try:
                                        dresult = s.get(curl)
                                        i = 1
                                        while dresult.status_code != 200:
                                            dresult = s.get(curl)
                                            i += 1
                                            if i > 100:
                                                print(f'Maximum tries to download resource {curl} exceeded, please check your params')
                                                logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Maximum tries to download resource {curl} exceeded, please check your params\n")
                                                break
                                    except:
                                        #print(f'Request to download resource {curl} failed, please check your params')
                                        logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Request to download resource {curl} from page {url} failed, please check your params\n")
                                    if dresult.status_code == 200:
                                        with open(f"{final_directory}\\{cname}.{curl.split('.')[-1]}", 'wb') as f:
                                            f.write(dresult.content)
                                            pass
                            else:
                                logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Attempt to parse items page {url} failed, please check the page content\n")

                            search_result_number += 1


def parse_blocks_from_orders(folder='rosnedra_auc'):

    current_directory = os.getcwd()
    directory = os.path.join(current_directory, folder)

    block_id = 0
    for path, dirs, files in os.walk(os.path.abspath(directory)):
        for filename in fnmatch.filter(files, '*.xls'):
            excel_file = os.path.join(path, filename)
            # excel_file = 'rosnedra_auc/1_20230425/Приложение к приказу Роснедр от 21.04.2023 № 224.xls'
            if excel_file[-5:] == '.xlsx':
                df = pd.read_excel(excel_file, engine='openpyxl')
            else:
                df = pd.read_excel(excel_file)
            # print(df.size)
            # print(df['Unnamed: 0'])
            # for cell in df['Unnamed: 0']:
            #     print(cell)
            # pass
            #print(filename, df['Unnamed: 0'][0])
            nrows, ncols = df.shape
            ring_id = 0
            for nrow in range(nrows):
                if df.iloc[nrow, 4] == 1:
                    ring_id += 1
                if str(df.iloc[nrow, 0]) != 'nan' and type(df.iloc[nrow, 0]) == int and type(df.iloc[nrow, 7]) == float and str(df.iloc[nrow, 7]) != 'nan':
                    #block_id = df.iloc[nrow, 0]
                    block_id += 1
                    ring_id = 1

                if type(df.iloc[nrow, 7]) == float and str(df.iloc[nrow, 7]) != 'nan':
                    print(filename, f"[block id: {block_id}] [ring id: {ring_id}]" , *df.iloc[nrow, 4:11])

            # for c1_cell in df.iloc[:, 0]:
            #     # print(c1_cell)
            #     if c1_cell == 1:
            #         pass



# download_orders(start=datetime(2023, 1, 1), end=datetime.now(), folder='rosnedra_auc\\')


parse_blocks_from_orders()

