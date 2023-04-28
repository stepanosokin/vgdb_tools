from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import locale
import os, fnmatch, shutil
import pandas as pd
from osgeo import ogr, osr, gdal
import psycopg2


def rus_month_genitive_to_nominative(i_string):
    months_genitive = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
    months_nominative = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь', 'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь']
    for pair in zip(months_genitive, months_nominative):
        i_string = i_string.replace(pair[0], pair[1])
    return i_string


def download_orders(start=datetime(year=2023, month=1, day=1), end=datetime.now(), search_string='Об утверждении Перечня участков недр', folder='rosnedra_auc'):
    current_directory = os.getcwd()
    logdateformat = '%Y-%m-%d %H:%M:%S'
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    # log_file = os.path.join(log_file, "logfile.txt")
    open(log_file, 'w', encoding='utf-8').close()
    with open(log_file, 'a', encoding='utf-8') as logf:

        with requests.Session() as s:
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
                                final_directory = os.path.join(current_directory, folder)
                                final_directory = os.path.join(final_directory, f"{str(search_result_number)}_{item_date.strftime('%Y%m%d')}")
                                if os.path.exists(final_directory):
                                    shutil.rmtree(final_directory, ignore_errors=True)
                                os.makedirs(final_directory)
                                name = search_result_item.find(attrs={'class': 'search-result-link'}).text
                                with open(f"{final_directory}\\result_url.txt", 'w', encoding='utf-8') as f:
                                    f.write(f"{url}")
                                with open(f"{final_directory}\\result_name.txt", 'w', encoding='utf-8') as f:
                                    f.write(f"{name}")


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


def parse_blocks_from_orders(folder='rosnedra_auc', gpkg='rosnedra_result.gpkg'):

    current_directory = os.getcwd()
    directory = os.path.join(current_directory, folder)
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
    # out_layer = gdatasource.CreateLayer('license_blocks_rosnedra_orders', srs=wgs84_crs, geom_type=ogr.wkbPolygon)
    out_layer = gdatasource.CreateLayer('license_blocks_rosnedra_orders', srs=wgs84_crs, geom_type=ogr.wkbMultiPolygon)
    field_names = ['resource_type', 'name', 'area_km', 'reserves_predicted_resources', 'exp_protocol', 'usage_type', 'lend_type', 'planned_terms_conditions', 'source_name', 'source_url', 'order_date']
    field_types = [ogr.OFTString, ogr.OFTString, ogr.OFTReal, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTDate]
    for f_name, f_type in zip(field_names, field_types):
        out_layer.CreateField(ogr.FieldDefn(f_name, f_type))
    featureDefn = out_layer.GetLayerDefn()


    # block_id = 0
    # ring_id = 0
    for path, dirs, files in os.walk(os.path.abspath(directory)):
        for filename in fnmatch.filter(files, '*.xls*'):
            with open(os.path.join(path, 'result_url.txt')) as uf:
                curl = uf.read().replace('\n', '')
            with open(os.path.join(path, 'result_name.txt')) as sf:
                csource = sf.read().replace('\n', '')
            excel_file = os.path.join(path, filename)

            df = pd.read_excel(excel_file)

            nrows, ncols = df.shape
            block_id = 0
            ring_id = 0
            cur_ring = ogr.Geometry(ogr.wkbLinearRing)
            # cur_block_geom = ogr.Geometry(ogr.wkbMultiPolygon)
            cur_block_geom = ogr.Geometry(ogr.wkbPolygon)

            field_cols = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            excel_col_nums = {'block_num': 0, 'point_num': 0, 'y_d': 0, 'y_m': 0, 'y_s': 0, 'x_d': 0, 'x_m': 0, 'x_s': 0}
            excel_col_nums.update(dict(zip(field_names, field_cols)))

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
                    #block_id = df.iloc[nrow, 0]
                    if block_id > 0:
                        if cur_ring.GetPointCount() > 2:
                            cur_ring.CloseRings()
                            cur_block_geom.AddGeometry(cur_ring)
                        cur_block_geom.CloseRings()
                        cur_block_geom.Transform(transform_gsk_to_wgs)
                        feature = ogr.Feature(featureDefn)
                        feature.SetGeometry(cur_block_geom)

                        for f_name, f_val in zip(field_names, field_vals):
                            feature.SetField(f_name, f_val)
                        out_layer.CreateFeature(feature)

                    block_id += 1

                    field_vals = [
                        df.iloc[nrow, excel_col_nums['resource_type']],
                        df.iloc[nrow, excel_col_nums['name']],
                        float(str(df.iloc[nrow, excel_col_nums['area_km']]).replace(',', '.')),
                        df.iloc[nrow, excel_col_nums['reserves_predicted_resources']],
                        df.iloc[nrow, excel_col_nums['exp_protocol']],
                        df.iloc[nrow, excel_col_nums['usage_type']],
                        df.iloc[nrow, excel_col_nums['lend_type']],
                        df.iloc[nrow, excel_col_nums['planned_terms_conditions']],
                        csource,
                        curl,
                        datetime.strptime(path[-8:], '%Y%m%d').strftime('%Y-%m-%d')
                    ]
                    # cur_block_geom = ogr.Geometry(ogr.wkbMultiPolygon)
                    cur_block_geom = ogr.Geometry(ogr.wkbPolygon)
                    ring_id = 1

                y_d = df.iloc[nrow, excel_col_nums['y_d']]
                y_m = df.iloc[nrow, excel_col_nums['y_m']]
                y_s = df.iloc[nrow, excel_col_nums['y_s']]
                x_d = df.iloc[nrow, excel_col_nums['x_d']]
                x_m = df.iloc[nrow, excel_col_nums['x_m']]
                x_s = df.iloc[nrow, excel_col_nums['x_s']]
                if str(y_s).replace(',', '').replace('.', '').isdigit() and str(df.iloc[nrow, excel_col_nums['y_s']]) != 'nan':
                    # print(filename, f"[block id: {block_id}] [ring id: {ring_id}]" , *df.iloc[nrow, 4:11])
                    y = float(str(df.iloc[nrow, excel_col_nums['y_d']]).replace(',', '.')) + \
                        float(str(df.iloc[nrow, excel_col_nums['y_m']]).replace(',', '.')) / 60 + \
                        float(str(df.iloc[nrow, excel_col_nums['y_s']]).replace(',', '.')) / 3600
                    x = float(str(df.iloc[nrow, excel_col_nums['x_d']]).replace(',', '.')) + \
                        float(str(df.iloc[nrow, excel_col_nums['x_m']]).replace(',', '.')) / 60 + \
                        float(str(df.iloc[nrow, excel_col_nums['x_s']]).replace(',', '.')) / 3600
                    # print(x, y)
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
                    feature.SetField(f_name, f_val)
                out_layer.CreateFeature(feature)



def get_latest_order_date_from_synology(pgconn):
    with pgconn.cursor() as cur:
        cur.execute("SELECT max(order_date) as latest_order_date FROM rosnedra.license_blocks_rosnedra_orders")
        ldate = cur.fetchall()[0][0]
        return datetime(ldate.year, ldate.month, ldate.day)


def update_synology_table(gdalpgcs, folder='rosnedra_auc',  gpkg='rosnedra_result.gpkg'):
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
    gdal.VectorTranslate(gdalpgcs, sourceds, options=myoptions)


def clear_folder(folder):
    for root, dirs, files in os.walk(folder):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


with open('.pgdsn', encoding='utf-8') as dsnf:
    dsn = dsnf.read().replace('\n', '')

with psycopg2.connect(dsn) as pgconn:
    startdt = get_latest_order_date_from_synology(pgconn) + timedelta(days=1)

with open('.pggdal', encoding='utf-8') as gdalf:
    gdalpgcs = gdalf.read().replace('\n', '')


# download_orders(start=startdt, end=datetime.now(), search_string='Об утверждении Перечня участков недр', folder='rosnedra_auc')

# parse_blocks_from_orders(folder='rosnedra_auc', gpkg='rosnedra_result.gpkg')

# update_synology_table(gdalpgcs, folder='rosnedra_auc')

# clear_folder('rosnedra_auc')







