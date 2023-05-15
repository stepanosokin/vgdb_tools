from datetime import datetime, timedelta
# import time
# from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup
import locale
import os, fnmatch, shutil, platform
import pandas as pd
from osgeo import ogr, osr, gdal
import psycopg2
import json
# from timezonefinder import TimezoneFinder
# from tzdata import *


def rus_month_genitive_to_nominative(i_string):
    months_genitive = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
    months_nominative = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь', 'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь']
    for pair in zip(months_genitive, months_nominative):
        i_string = i_string.replace(pair[0], pair[1])
    return i_string


def download_orders(start=datetime(year=2023, month=1, day=1), end=datetime.now(), search_string='Об утверждении Перечня участков недр', folder='rosnedra_auc'):
    '''
    This function downloads license blocks auctions announcement data from www.rosnedra.gov.ru website
    and saves it to the hierarchy of folders.
    The idea is that www.rosnedra.gov.ru has the search engine, which accepts a search string and
    gives a list of results, among which you can find pages with official orders about license blocks auctions
    announcements and data attached to it. This data is usually a pdf with scanned order text and excel spreadsheet
    with license blocks coordinates and attributes. Search result is a list of links to document pages, which,
    in case this is an order, may contain links to the desired pdf and excel. If you give the appropriate search
    string to the engine, e.g. 'Об утверждении Перечня участков недр', than the result will mostly contain desired docs,
    but not guaranteed. This function is an attempt to filter, structurize and download the data about license auction
    announcements for the given period of time.
    :param start: this is the start <datetime> threshold from which to start the search/download.
    :param end: this is the end <datetime> threshold until which to start the search/download.
    The default values for start and end parameters are 1st January 2023 and current date respectively,
    but you can chenge it according to your need.
    :param search_string: this is the search string given to the rosnedra search engine. The default value
    'Об утверждении Перечня участков недр' is OK for most cases of searching the license auction announcements orders.
    :param folder: this a relative path to the folder where to save the result. The result will contain a number
    of folders and a logfile with error reports.
    :return:
    This function returns nothing, but it saves the results to the <folder> location. the results are:
    1. a number of folders with names like NN_YYYYMMDD, where NN is a number of the search result, and YYYYMMDD is a
    date of the Rosnedra order. Each folder contains: pdf with scanned order text, excel with license info,
    result_name.txt file with the full name of the document and result_url.txt file with an url of the source page;
    2. logfile.txt with reports about failed to parse pages from the search result. This logfile is filled cumulatively,
    so if you make several attempts, no lines are deleted from it.
    '''
    # get the current working directory of the script
    current_directory = os.getcwd()
    # define the datetime format for the logfile
    logdateformat = '%Y-%m-%d %H:%M:%S'
    # create a pthname for the logfile
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    # open the logfile
    with open(log_file, 'a', encoding='utf-8') as logf:
        # start a requests session
        with requests.Session() as s:
            # create url string for the main search request
            url = 'https://www.rosnedra.gov.ru/index.fcgi'
            params = {
                'page': 'search',
                'step': '1',
                'q': search_string
            }
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru - RU, ru;q = 0.9, en - US;q = 0.8, en;q = 0.7, en - GB;q = 0.6',
                'Connection': 'keep-alive',
                'DNT': '1'
            }
            # try to retreive the result from the search engine
            try:
                # make get request to the service
                search_result = s.get(url, params=params, headers=headers, verify=False)
                # i is a number of tries to retrieve a result
                i = 1
                # while the service returns anything other than 200 (OK):
                while search_result.status_code != 200:
                    # make one more try
                    search_result = s.get(url, params=params, headers=headers, verify=False)
                    i += 1
                    # until 100 attempts
                    if i > 100:
                        break
            except:
                # if something went wrong, write a line to ligfile
                logf.write(f"{datetime.now().strftime(logdateformat)} Initial request to www.rosnedra.gov.ru failed, please check your params\n")

            # create a beutifulsoup from the first search results page
            first_soup = BeautifulSoup(search_result.text, 'html.parser')
            # find the Pager element, which contains the number of pages with search results, and convert it to a list
            pages = first_soup.find(attrs={'class': 'Pager'}).find_all('a')
            # convert page numbers to text
            pages = [p.text for p in pages if p.text != '']
            # start the downloaded results counter
            results_downloaded = 0

            # if there are any pages with the results
            if len(pages) > 0:
                # create a variable for counting the search results
                search_result_number = 1
                # loop through all search result page numbers
                for page in pages:
                    # for each page number, create an url, params (see the 'part' parameter) and try to make get request,
                    # including error handling
                    url = 'https://www.rosnedra.gov.ru/index.fcgi'
                    params={
                        'page': 'search',
                        'from_day': '28',
                        'from_month': '04',
                        'from_year': '2012',
                        'till_day': datetime.now().strftime('%d'),
                        'till_month': datetime.now().strftime('%m'),
                        'till_year': datetime.now().strftime('%Y'),
                        'q': search_string,
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
                        logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}. Request to www.rosnedra.gov.ru search results page {url} failed, please check your params\n")

                    # create a beautifulsoup from the current search results page
                    cur_search_results_page_soup = BeautifulSoup(page_result.text, 'html.parser')
                    # find all search-result-item tags and put them to the list
                    for search_result_item in cur_search_results_page_soup.find(attrs={'class': 'search-result-list'}).find_all(attrs={'class': 'search-result-item'}):
                        # set the locale to russian to be able to work with the item's date
                        locale.setlocale(locale.LC_ALL, locale='ru_RU.UTF-8')
                        # loop through search-result-link-info-item tags
                        for search_result_link_info_item in search_result_item.find_all(attrs={'class': 'search-result-link-info-item'}):
                            # if the search-result-link-info-item tag contains 'Дата' word, it's a datestamp
                            if 'Дата' in search_result_link_info_item.text:
                                # use a custom function to put the month name to nominative form
                                if platform.system() == 'Windows':
                                    item_date = rus_month_genitive_to_nominative(search_result_link_info_item.text.lower())
                                else:
                                    item_date = search_result_link_info_item.text.lower()
                                # extract the datestamp of the document
                                item_date = item_date.replace(u'\xa0', u' ')
                                item_date = item_date.replace('  ', ' ')
                                item_date = datetime.strptime(item_date.title(), 'Дата Документа: %d %B %Y')
                                pass
                                # item_date = datetime.strptime(item_date.title(), 'Дата Документа:\xa0\xa0%d\xa0%B\xa0%Y')

                        # check if the datestamp matches the given time period
                        if start <= item_date <= end:
                            # find the search-result-link tag inside the current search-result-item and extract url from it
                            url = 'https://' + f"rosnedra.gov.ru/{search_result_item.find(attrs={'class': 'search-result-link'})['href']}".replace('//', '/')
                            # make a standard process of requesting a page for the current search-result-item
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
                                logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Request to {url} failed, please check your params\n")
                            # create a beautifulsoup from search-result-item's webpage
                            cur_item_page_result_soup = BeautifulSoup(item_page_result.text, 'html.parser')
                            # find a content tag inside the page. It contents the links to the downloadable files.
                            cur_content_tags = cur_item_page_result_soup.find(attrs={'class': 'Content'})
                            # find all h1 tags to obtain the full document name and check if it contains the
                            # word 'Приказ Роснедр от'. This is a test to understand that we've found a Rosnedra
                            # order, not some other trash
                            cur_h1_tags = cur_item_page_result_soup.find_all('h1')
                            is_order = False
                            if cur_h1_tags:
                                for h1_tag in cur_h1_tags:
                                    if 'Приказ Роснедр от' in h1_tag.text:
                                        is_order = True
                                        announcement = " ".join(h1_tag.text.replace('\xa0', ' ').split())
                                        ord_i = announcement.find('Приказ Роснедр от')
                                        order_date = datetime.strptime(announcement[ord_i:ord_i + 28], 'Приказ Роснедр от %d.%m.%Y')


                            # if we know that we've found an order and if it contains Content elements
                            if cur_content_tags and is_order:
                                # then we create a new folder to store the current results
                                final_directory = os.path.join(current_directory, folder)
                                final_directory = os.path.join(final_directory, f"{str(search_result_number)}_{item_date.strftime('%Y%m%d')}")
                                # if it already exists, delete it
                                if os.path.exists(final_directory):
                                    shutil.rmtree(final_directory, ignore_errors=True)
                                # create a new folder
                                os.makedirs(final_directory)
                                results_downloaded += 1

                                metadata_dict = {}
                                metadata_dict['url'] = url
                                metadata_dict['announce_date'] = item_date.strftime('%Y-%m-%d')

                                # extract the full name of a hyperlink and store it to the result_name.txt file.
                                # store url to the result_url.txt file.
                                name = search_result_item.find(attrs={'class': 'search-result-link'}).text
                                metadata_dict['name'] = name
                                metadata_dict['order_date'] = order_date.strftime('%Y-%m-%d')

                                # find all </a> tags inside the Content element and loop through them
                                for item_doc_tag in cur_content_tags.find_all('a'):
                                    # create a full url string from the current </a>
                                    curl = f"https://www.rosnedra.gov.ru{item_doc_tag['href']}"
                                    # extract a name of the document from the hyperlink text
                                    cname = item_doc_tag.text
                                    # standard process of downloading a file by the link and logging the errors.
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
                                        logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Request to download resource {curl} from page {url} failed, please check your params\n")
                                    # if something has been downloaded
                                    if dresult.status_code == 200:
                                        # create a new file using cname and the file extension from the curl
                                        with open(os.path.join(final_directory, f"{cname}.{curl.split('.')[-1]}"), 'wb') as f:
                                            f.write(dresult.content)
                                            pass
                                for item_doc_p_tag in cur_content_tags.find_all('p'):
                                    if 'последнийсрокприема' in item_doc_p_tag.text.replace(' ', '').replace('\xa0', '').lower():
                                        deadlinestr = item_doc_p_tag.text
                                        deadlinestr = " ".join(deadlinestr.replace('\xa0', ' ').split())
                                        if platform.system() == 'Windows':
                                            deadlinestr = rus_month_genitive_to_nominative(deadlinestr.lower())
                                        else:
                                            deadlinestr = deadlinestr.lower()
                                        try:
                                            deadline = datetime.strptime(deadlinestr.title(), 'Последний Срок Приема Заявок: До %d %B %Y Г.')
                                        except:
                                            try:
                                                deadline = datetime.strptime(deadlinestr.title(), 'Последний Срок Приема Заявок: До %d %B %Y Г.)')
                                            except:
                                                try:
                                                    deadline = datetime.strptime(deadlinestr.title(), 'Последний Срок Приема Заявок: До %H:%M (Местное Время) %d %B %Y Г.')
                                                except:
                                                    try:
                                                        deadline = datetime.strptime(deadlinestr.title(), 'Последний Срок Приема Заявок: До %H:%M (Местное Время) %d %B %Y Г.)')
                                                    except:
                                                        try:
                                                            deadline = datetime.strptime(deadlinestr.title(), 'Последний Срок Приема Заявок: До %H.%M (Местное Время) %d %B %Y Г.')
                                                            pass
                                                        except:
                                                            try:
                                                                deadline = datetime.strptime(deadlinestr.title(),'Последний Срок Приема Заявок: До %H.%M (Местное Время) %d %B %Y Г.)')
                                                            except:
                                                                deadline = datetime(1970, 1, 1)
                                                                logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Could not parse application deadline from {url}, used the 1970-01-01. Please check the page content\n")
                                        metadata_dict['deadline'] = deadline.strftime('%Y-%m-%d')
                                        # with open(os.path.join(final_directory, 'application_deadline.txt'), 'w', encoding='UTF-8') as df:
                                        #     df.write(deadline.strftime('%Y-%m-%d %H:$M'))
                                with open(os.path.join(final_directory, 'result_metadata.json'), "w", encoding='utf-8') as outfile:
                                    json.dump(metadata_dict, outfile, ensure_ascii=False)
                            else:
                                logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Attempt to parse items page {url} failed, please check the page content\n")
                            # iterate the search result number
                            search_result_number += 1
            # return the locale settings to the initial state
            locale.setlocale(locale.LC_ALL, locale='')
            # write log message about results downloaded count
            logf.write(f"{datetime.now().strftime(logdateformat)} Rosnedra orders download from "
                       f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')} run successfully. "
                       f"{results_downloaded} results downloaded.\n")


def parse_blocks_from_orders(folder='rosnedra_auc', gpkg='rosnedra_result.gpkg'):

    current_directory = os.getcwd()
    directory = os.path.join(current_directory, folder)
    # define the datetime format for the logfile
    logdateformat = '%Y-%m-%d %H:%M:%S'
    # create a pthname for the logfile
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    with open(log_file, 'a', encoding='utf-8') as logf:

        gpkg_path = os.path.join(directory, gpkg)

        gsk2011_crs = osr.SpatialReference()
        gsk2011_crs.ImportFromProj4('+proj=longlat +ellps=GSK2011 +towgs84=0.013,-0.092,-0.03,-0.001738,0.003559,-0.004263,0.00739999994614493 +no_defs +type=crs')
        wgs84_crs = osr.SpatialReference()
        wgs84_crs.ImportFromProj4('+proj=longlat +datum=WGS84 +no_defs')
        transform_gsk_to_wgs = osr.CoordinateTransformation(gsk2011_crs, wgs84_crs)
        gdriver = ogr.GetDriverByName('GPKG')
        if os.path.exists(gpkg_path):
            gdriver.DeleteDataSource(gpkg_path)
        gdatasource = gdriver.CreateDataSource(gpkg_path)
        out_layer = gdatasource.CreateLayer('license_blocks_rosnedra_orders', srs=wgs84_crs, geom_type=ogr.wkbPolygon)
        # out_layer = gdatasource.CreateLayer('license_blocks_rosnedra_orders', srs=wgs84_crs, geom_type=ogr.wkbMultiPolygon)
        field_names = ['resource_type', 'name', 'area_km', 'reserves_predicted_resources', 'exp_protocol', 'usage_type', 'lend_type', 'planned_terms_conditions', 'source_name', 'source_url', 'order_date', 'announce_date', 'appl_deadline']
        field_types = [ogr.OFTString, ogr.OFTString, ogr.OFTReal, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTDate, ogr.OFTDate, ogr.OFTDate]
        for f_name, f_type in zip(field_names, field_types):
            out_layer.CreateField(ogr.FieldDefn(f_name, f_type))
        featureDefn = out_layer.GetLayerDefn()

        blocks_parsed = 0

        for path, dirs, files in os.walk(os.path.abspath(directory)):
            for filename in fnmatch.filter(files, '*.xls*'):
                with open(os.path.join(path, 'result_metadata.json'), 'r', encoding='utf-8') as jf:
                    meta_dict = json.load(jf)
                excel_file = os.path.join(path, filename)
                df = pd.read_excel(excel_file)
                nrows, ncols = df.shape
                block_id = 0
                ring_id = 0
                cur_ring = ogr.Geometry(ogr.wkbLinearRing)
                cur_block_geom = ogr.Geometry(ogr.wkbPolygon)
                field_cols = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                excel_col_nums = {'block_num': 0, 'point_num': 0, 'y_d': 0, 'y_m': 0, 'y_s': 0, 'x_d': 0, 'x_m': 0, 'x_s': 0}
                excel_col_nums.update(dict(zip(field_names, field_cols)))

                # tf = TimezoneFinder()

                for nrow in range(nrows):

                    for ncol in range(ncols):
                        if 'град' in str(df.iloc[nrow, ncol]).replace(' ', '').replace('\n', '') \
                                and 'мин' in str(df.iloc[nrow, ncol + 1]).replace(' ', '').replace('\n', '') \
                                and excel_col_nums['y_d'] == 0:
                            excel_col_nums['y_d'] = ncol
                            excel_col_nums['y_m'] = ncol + 1
                            excel_col_nums['y_s'] = ncol + 2
                            excel_col_nums['x_d'] = ncol + 3
                            excel_col_nums['x_m'] = ncol + 4
                            excel_col_nums['x_s'] = ncol + 5
                            excel_col_nums['point_num'] = ncol - 1
                            excel_col_nums['area_km'] = ncol - 2
                        if str(df.iloc[nrow, ncol]).replace(' ', '').replace('\n', '').lower() == 'видполезногоископаемого' and excel_col_nums['resource_type'] == 0:
                            excel_col_nums['resource_type'] = ncol
                        if 'наименованиеучастканедр' in str(df.iloc[nrow, ncol]).replace('\n', '').replace(' ', '').lower() and excel_col_nums['name'] == 0:
                            excel_col_nums['name'] = ncol
                        if 'ресурсы' in str(df.iloc[nrow, ncol]).replace('\n', '').lower() and excel_col_nums['reserves_predicted_resources'] == 0:
                            excel_col_nums['reserves_predicted_resources'] = ncol
                        if 'протокол' in str(df.iloc[nrow, ncol]).replace('\n','').lower() and excel_col_nums['exp_protocol'] == 0:
                            excel_col_nums['exp_protocol'] = ncol
                        if 'видпользованиянедрами' in str(df.iloc[nrow, ncol]).replace('\n','').replace(' ','').lower() and excel_col_nums['usage_type'] == 0:
                            excel_col_nums['usage_type'] = ncol
                        if 'формапредоставленияучастканедрвпользоание' in str(df.iloc[nrow, ncol]).replace('\n','').replace(' ','').lower()  and excel_col_nums['lend_type'] == 0:
                            excel_col_nums['lend_type'] = ncol
                        if 'планируемыесрокипроведения' in str(df.iloc[nrow, ncol]).replace('\n', '').replace(' ', '').lower() and excel_col_nums['planned_terms_conditions'] == 0:
                            excel_col_nums['planned_terms_conditions'] = ncol

                    point_n = df.iloc[nrow, excel_col_nums['point_num']]
                    y_d = df.iloc[nrow, excel_col_nums['y_d']]
                    if str(point_n) == '1' or (cur_ring.GetPointCount() > 2 and not str(y_d).isdigit()):
                        if ring_id > 0:
                            if cur_ring.GetPointCount() > 2:
                                cur_ring.CloseRings()
                                cur_block_geom.AddGeometry(cur_ring)
                        ring_id += 1
                        cur_ring = ogr.Geometry(ogr.wkbLinearRing)
                    if str(df.iloc[nrow, excel_col_nums['block_num']]) != 'nan' and len(str(df.iloc[nrow, excel_col_nums['block_num']])) > 0 and str(df.iloc[nrow, excel_col_nums['y_s']]).replace(',', '').replace('.', '').isdigit() and str(df.iloc[nrow, excel_col_nums['y_s']]) != 'nan':
                        if block_id > 0:
                            if cur_ring.GetPointCount() > 2:
                                cur_ring.CloseRings()
                                cur_block_geom.AddGeometry(cur_ring)
                            cur_block_geom.CloseRings()
                            cur_block_geom.Transform(transform_gsk_to_wgs)
                            feature = ogr.Feature(featureDefn)
                            feature.SetGeometry(cur_block_geom)



                            for f_name, f_val in zip(field_names, field_vals):
                                if f_name == 'appl_deadline' and f_val:
                                    # tz = tf.timezone_at(lng=cur_block_geom.Centroid().GetX(),
                                    #                     lat=cur_block_geom.Centroid().GetY())
                                    # f_val_dt = datetime.strptime(f_val, '%Y-%m-%d %H:%M')
                                    # f_val_dt = f_val_dt.replace(tzinfo=ZoneInfo(tz))
                                    # tzinfo = ZoneInfo(tz)
                                    # os.environ['TZ'] = tz
                                    # time.tzset()
                                    # windows_timezone = f"{tz[tz.find('/') + 1:]} Standard Time"
                                    # os.environ['TZ'] = windows_timezone
                                    # os.system(f"tzutil /s \"{windows_timezone}\"")
                                    feature.SetField(f_name, f_val)
                                else:
                                    feature.SetField(f_name, f_val)
                            blocks_parsed += 1
                            out_layer.CreateFeature(feature)

                        block_id += 1

                        # if meta_dict.get('deadline') != None:
                        #     deadlinedt = datetime.strptime(meta_dict.get('deadline'), '%Y-%m-%d %H:%M')
                        #     pass
                        # else:
                        #     deadlinedt = meta_dict.get('deadline')
                        #     pass
                        field_vals = [
                            df.iloc[nrow, excel_col_nums['resource_type']],
                            df.iloc[nrow, excel_col_nums['name']],
                            float(str(df.iloc[nrow, excel_col_nums['area_km']]).replace(',', '.')),
                            df.iloc[nrow, excel_col_nums['reserves_predicted_resources']],
                            df.iloc[nrow, excel_col_nums['exp_protocol']],
                            df.iloc[nrow, excel_col_nums['usage_type']],
                            df.iloc[nrow, excel_col_nums['lend_type']],
                            df.iloc[nrow, excel_col_nums['planned_terms_conditions']],
                            meta_dict['name'],
                            meta_dict['url'],
                            # datetime.strptime(path[-8:], '%Y%m%d').strftime('%Y-%m-%d')
                            meta_dict['order_date'],
                            meta_dict['announce_date'],
                            meta_dict.get('deadline')
                        ]
                        cur_block_geom = ogr.Geometry(ogr.wkbPolygon)
                        ring_id = 1


                    y_s = df.iloc[nrow, excel_col_nums['y_s']]
                    if str(y_s).replace(',', '').replace('.', '').isdigit() and str(df.iloc[nrow, excel_col_nums['y_s']]) != 'nan':
                        y = float(str(df.iloc[nrow, excel_col_nums['y_d']]).replace(',', '.')) + \
                            float(str(df.iloc[nrow, excel_col_nums['y_m']]).replace(',', '.')) / 60 + \
                            float(str(df.iloc[nrow, excel_col_nums['y_s']]).replace(',', '.')) / 3600
                        x = float(str(df.iloc[nrow, excel_col_nums['x_d']]).replace(',', '.')) + \
                            float(str(df.iloc[nrow, excel_col_nums['x_m']]).replace(',', '.')) / 60 + \
                            float(str(df.iloc[nrow, excel_col_nums['x_s']]).replace(',', '.')) / 3600
                        cur_ring.AddPoint(x, y)
                if block_id > 0:
                    if cur_ring.GetPointCount() > 2:
                        cur_ring.CloseRings()
                        cur_block_geom.AddGeometry(cur_ring)
                    cur_block_geom.CloseRings()
                    cur_block_geom.Transform(transform_gsk_to_wgs)
                    feature = ogr.Feature(featureDefn)
                    feature.SetGeometry(cur_block_geom)

                    for f_name, f_val in zip(field_names, field_vals):
                        if f_name == 'appl_deadline' and f_val:
                        #     tz = tf.timezone_at(lng=cur_block_geom.Centroid().GetX(), lat=cur_block_geom.Centroid().GetY())
                        #     f_val_dt = datetime.strptime(f_val, '%Y-%m-%d %H:%M')
                        #     f_val_dt = f_val_dt.replace(tzinfo=ZoneInfo(tz))
                        #     tzinfo = ZoneInfo(tz)
                        #     # os.environ['TZ'] = tz
                        #     # time.tzset()
                        #     windows_timezone = f"{tz[tz.find('/') + 1:]} Standard Time"
                        #     os.environ['TZ'] = windows_timezone
                        #     os.system(f"tzutil /s \"{windows_timezone}\"")
                        #     print('hi')
                            feature.SetField(f_name, f_val)
                            pass
                        feature.SetField(f_name, f_val)
                    blocks_parsed += 1
                    out_layer.CreateFeature(feature)
        logf.write(f"{datetime.now().strftime(logdateformat)} downloaded Rosnedra orders data parsed successfully. {blocks_parsed} blocks parsed.\n")


def get_latest_order_date_from_synology(pgconn):
    with pgconn.cursor() as cur:
        cur.execute("SELECT max(announce_date) as latest_order_date FROM rosnedra.license_blocks_rosnedra_orders")
        ldate = cur.fetchall()[0][0]
        ldatetime = datetime(ldate.year, ldate.month, ldate.day)
        return ldatetime


def update_synology_table(gdalpgcs, folder='rosnedra_auc',  gpkg='rosnedra_result.gpkg'):
    current_directory = os.getcwd()
    directory = os.path.join(current_directory, folder)
    # define the datetime format for the logfile
    logdateformat = '%Y-%m-%d %H:%M:%S'
    # create a pthname for the logfile
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    with open(log_file, 'a', encoding='utf-8') as logf:
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        sourcepath = os.path.join(folder, gpkg)
        sourceds = gdal.OpenEx(sourcepath, gdal.OF_VECTOR)
        myoptions = gdal.VectorTranslateOptions(
            layerName='rosnedra.license_blocks_rosnedra_orders',
            format='PostgreSQL',
            accessMode='append',
            dstSRS=srs,
            layers=['license_blocks_rosnedra_orders'],
            geometryType='MULTIPOLYGON'
        )
        try:
            gdal.VectorTranslate(gdalpgcs, sourceds, options=myoptions)
            logf.write(f"{datetime.now().strftime(logdateformat)} Synology table rosnedra.license_blocks_rosnedra_orders "
                       f"updated successfully.\n")
        except:
            logf.write(f"{datetime.now().strftime(logdateformat)} Synology table rosnedra.license_blocks_rosnedra_orders "
                       f"update FAILED.\n")


def clear_folder(folder):
    for root, dirs, files in os.walk(folder):
        for f in files:
            if f != 'logfile.txt':
                os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


# with open('.pgdsn', encoding='utf-8') as dsnf:
#     dsn = dsnf.read().replace('\n', '')
#
# with psycopg2.connect(dsn) as pgconn:
#     startdt = get_latest_order_date_from_synology(pgconn) + timedelta(days=1)
#
# with open('.pggdal', encoding='utf-8') as gdalf:
#     gdalpgcs = gdalf.read().replace('\n', '')
#
#
# clear_folder('rosnedra_auc')
#
# # # startdt = datetime(2023, 1, 1)
# download_orders(start=startdt, end=datetime.now(), search_string='Об утверждении Перечня участков недр', folder='rosnedra_auc')
# #
# parse_blocks_from_orders(folder='rosnedra_auc', gpkg='rosnedra_result.gpkg')
#
# update_synology_table(gdalpgcs, folder='rosnedra_auc')









