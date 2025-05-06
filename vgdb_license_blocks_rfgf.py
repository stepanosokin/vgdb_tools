
import requests, json, os, psycopg2
from datetime import datetime
from vgdb_general import log_message, send_to_teams
from osgeo import ogr, osr, gdal
from synchro_evergis import *


def download_rfgf_blocks(json_request, json_result, folder='rfgf_blocks', bot_info=(None, None)):
    '''
    This is a function to download data about license blocks in json format from https://rfgf.ru/ReestrLic/ site
    :param json_request: This is the json request file. Instructions to get a sample of this file: 1. Use Chrome
    to open https://rfgf.ru/ReestrLic/ site; 2. Activate DevTools by pressing F12. Go to Network tab; 3. Make some
    request on the Rfgf catalog page with no filters; 4. in DevTools, select the last query object in Name pane.
    Then go to the Payload tab on the right. Click 'view source'. Then click 'Show more' at the bottom. You now
    see the complete search request to the webservice in json format. Just copy/paste this json text to any text
    editor; 5. Find the "limit":100 parameter and change it to some big value, e.g. 250000. Save the file.
    This is your json request file that you can use for this function.
    :param json_result: path to the result data json file
    :return: bool success
    '''

    current_directory = os.getcwd()
    success = False
    try:
        with open(os.path.join(current_directory, folder, 'logfile.txt'), 'a', encoding='utf-8') as logf:

            with open(os.path.join(current_directory, folder, json_request), "r", encoding='utf-8') as a_file:
                json_object = json.load(a_file)

            with requests.Session() as s:
                message = 'LicenseBlockUpdater: Download License blocks data from Rosgeolfond started'
                log_message(s, logf, bot_info, message)
                url = 'https://bi.rfgf.ru/corelogic/api/query'
                headers = {'accept': 'application/json, text/javascript, */*; q=0.01',
                           'accept-encoding': 'gzip, deflate, br',
                           'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,en-GB;q=0.6',
                           'authorization': 'Bearer NoAuth',
                           'content-type': 'application/json',
                           # 'cookie': '_ym_uid=1656406763932208622; _ym_d=1656406763; _ym_isad=2',
                           'dnt': '1'
                           }
                response = None
                data = None
                err_code = 0
                i = 1
                while err_code != 200 and i <= 10:
                # try:
                    try:
                        i += 1
                        response = s.post(url, headers=headers, json=json_object, verify=False)
                        err_code = response.status_code                        
                        # if i >= 10:
                        #     message = f'Download License blocks data from Rosgeolfond FAILED after 10 tries, status code {response.status_code}'
                        #     log_message(s, logf, bot_info, message)
                        #     break
                        # response = s.post(url, headers=headers, json=json_object, verify=False)
                    except:
                        message = f'LicenseBlockUpdater: Download License blocks data from Rosgeolfond attempt {i} FAILED , error making HTTP request. Retrying...'
                        log_message(s, logf, bot_info, message)
                if err_code == 200 and response:
                    try:
                        data = response.json()                        
                    except:
                        pass
                    if data:
                        if data.get('result').get('data').get('rows'):
                            with open(os.path.join(current_directory, folder, json_result), "w", encoding='utf-8') as a_file:
                                json.dump(data, a_file, ensure_ascii=False)
                            message = f"LicenseBlockUpdater: Downloaded License blocks data from Rosgeolfond successfully, {len(data['result']['data']['rows'])} records downloaded"
                            log_message(s, logf, bot_info, message)
                            success = True
                            pass
                        else:
                            message = f"LicenseBlockUpdater: Downloaded empty or NULL result, skipping update..."
                            log_message(s, logf, bot_info, message)
                    else:
                        message = f"LicenseBlockUpdater: Downloaded empty or NULL result, skipping update..."
                        log_message(s, logf, bot_info, message)
                else:
                    message = f'LicenseBlockUpdater: Download License blocks data from Rosgeolfond FAILED after {i} tries, error making HTTP request. Skipping update.'
                    log_message(s, logf, bot_info, message)
                # if response.status_code == 200:
                #     message = f"LicenseBlockUpdater: Downloaded License blocks data from Rosgeolfond successfully, {len(data['result']['data']['rows'])} records downloaded"
                #     log_message(s, logf, bot_info, message)
                # with open(os.path.join(current_directory, folder, json_result), "w", encoding='utf-8') as a_file:
                #     json.dump(data, a_file, ensure_ascii=False)
                # success = True
    except:
        success = False
        message = f"LicenseBlockUpdater: Download license blocks from Rosgeolfond FAILED"
        log_message(s, logf, bot_info, message)

    return success


def parse_rfgf_blocks(json_file, gpkg_file='d_r.gpkg', layer_name='l_b', folder='rfgf_blocks', bot_info=(None, None)):
    # read json file downloaded from RFGF
    current_directory = os.getcwd()
    logpath = os.path.join(current_directory, folder, 'logfile.txt')
    with open(logpath, 'a', encoding='utf-8') as logf, requests.Session() as s:
        with open(os.path.join(folder, json_file), 'r', encoding='utf-8') as j_file:
            json_data = json.load(j_file)
        gpkg_path = os.path.join(current_directory, folder, gpkg_file)
        gdriver = ogr.GetDriverByName('GPKG')
        if os.path.exists(gpkg_path):
            gdriver.DeleteDataSource(gpkg_path)
        gdatasource = gdriver.CreateDataSource(gpkg_path)
        wgs84_crs = osr.SpatialReference()
        wgs84_crs.ImportFromProj4('+proj=longlat +datum=WGS84 +no_defs')
        out_layer = gdatasource.CreateLayer(layer_name, srs=wgs84_crs, geom_type=ogr.wkbMultiPolygon)
        # counter for blocks written to layer
        b_counter = 0
        # create a list of fieldnames for license blocks
        field_names = ['rfgf_link', 'gos_reg_num', 'date_register', 'license_purpose', 'resource_type',
                                 'license_block_name', 'region', 'status', 'user_info', 'licensor',
                                 'license_doc_requisites', 'license_update_info', 'license_re_registration_info',
                                 'license_cancel_order_info', 'date_stop_subsoil_usage',
                                 'limit_conditions_stop_subsoil_usage', 'date_license_stop',
                                 'previous_license_info', 'coords_text']
        # create a list of field types for license blocks. The order must match the field_names list.
        field_types = [ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString,
                       ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString,
                       ogr.OFTString, ogr.OFTString, ogr.OFTString,
                       ogr.OFTString, ogr.OFTString,
                       ogr.OFTString, ogr.OFTString,
                       ogr.OFTString, ogr.OFTString]
        # list of json data record indexes
        json_attr_index_list = [0, 1, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 8]
        # add fields to the result layer
        for f_name, f_type in zip(field_names, field_types):
            out_layer.CreateField(ogr.FieldDefn(f_name, f_type))
        # get the layer definition from the layer to use it when creating new features
        featureDefn = out_layer.GetLayerDefn()

        message = f"LicenseBlockUpdater: Parsing License blocks data from Rosgeolfond started"
        log_message(s, logf, bot_info, message)

        # loop through json data rows
        try:
            for i in range(len(json_data['result']['data']['rows'])):
                # check if the record has geometry
                if json_data['result']['data']['values'][8][i]:
                    if '°' in json_data['result']['data']['values'][8][i]:

                        # first, let's parse record's geometry
                        try:
                            record_has_geom, geom = parse_geometry(json_data['result']['data']['values'][8][i], 0.1)
                        except:
                            pass
                            # message = f"LicenseBlockUpdater: Attempt to parse license block geometry failed, record # {i + 1}"
                            # log_message(s, logf, bot_info, message)

                        # If the record has valid multipolygon geometry:
                        if record_has_geom:
                            # create new feature
                            feature = ogr.Feature(featureDefn)
                            k = 0  # index of an item in gpkg_field_names_list
                            # loop through  json data record indexes
                            for j in json_attr_index_list:
                                if j not in (3, 16, 18):  # if not date format
                                    # add field value to feature
                                    feature.SetField(field_names[k], json_data['result']['data']['values'][j][i])
                                else:  # if date format
                                    feature.SetField(field_names[k], json_data['result']['data']['values'][j][i])
                                k += 1
                            feature.SetGeometry(geom)
                            # and add a new feature to the layer.
                            out_layer.CreateFeature(feature)
                            # next blocks counter
                            b_counter += 1
                if (i + 1) % 50000 == 0:
                    message = f"LicenseBlockUpdater: Parsing in progress..., {str(i + 1)} records processed, {str(b_counter)} blocks parsed"
                    log_message(s, logf, bot_info, message)
            message = f"LicenseBlockUpdater: License blocks parsed successfully, {str(i + 1)} records processed, {str(b_counter)} blocks parsed"
            log_message(s, logf, bot_info, message)
            success = True
        except:
            success = False
            message = f"LicenseBlockUpdater: Error parsing license blocks, failed on record {str(i + 1)}"
            log_message(s, logf, bot_info, message)

        return success


def parse_geometry(source_geom, coords_threshold):
    '''
    This function converts the single license block geometry data from https://rfgf.ru/ReestrLic/ from text to
    QgsGeometry. New multipolygon parts are triggered by 'Объект №' or 'Система координат' keywords.
    New rings are triggered by points with number 1. 'Мультиточка' objects are ignored.
    The supported coordinate systems for points are ГСК-2011 (GSK-2011), Пулково-42 (Pulkovo-1942) and WGS-84.
    Transformations are made with GOST 32453-2017 parameters.
    :param source_geom: this is the coordinates of the block copied from https://rfgf.ru/ReestrLic/
    :param coords_threshold: this is the minimum value for coordinates under which the value is ignored
    :return: tuple with two objects: 1 - bool, shows whether the record has valid polygon geometry at all;
    2 - ogr.Geometry(ogr.wkbMultiPolygon) object
    '''

    if coords_threshold == 0:
        coords_threshold = 0.0001
    splitted_geom = split_strip(source_geom)

    record_has_geometry = False

    ring_of_points = ogr.Geometry(ogr.wkbLinearRing)
    pol_of_rings = ogr.Geometry(ogr.wkbPolygon)
    multipol_of_pols = ogr.Geometry(ogr.wkbMultiPolygon)

    Multipoint = False

    first_points_after_multipoint_counter = 0
    # create GSK-2011 CRS from proj string, using GOST 32453-2017 parameters
    gsk2011_crs = osr.SpatialReference()
    gsk2011_crs.ImportFromProj4(
      '+proj=longlat +ellps=GSK2011 +towgs84=0.013,-0.092,-0.03,-0.001738,0.003559,-0.004263,0.00739999994614493 +no_defs +type=crs')
    # create Pulkovo-1942 CRS from proj string, using GOST 32453-2017 parameters
    pulkovo42_crs = osr.SpatialReference()
    pulkovo42_crs.ImportFromProj4('+proj=longlat +ellps=krass +towgs84=23.57,-140.95,-79.8,0.0,0.35,0.79,-0.22 +no_defs')
    # create WGS-84 CRS from proj string to avoid automatic datum transformation
    wgs84_crs = osr.SpatialReference()
    wgs84_crs.ImportFromProj4('+proj=longlat +datum=WGS84 +no_defs')
    # create a transformation from GSK-2011 to WGS-84.
    transform_gsk_to_wgs = osr.CoordinateTransformation(gsk2011_crs, wgs84_crs)
    # create a transformation from Pulkovo-1942 to WGS-84.
    transform_pulk_to_wgs = osr.CoordinateTransformation(pulkovo42_crs, wgs84_crs)

    transform_wgs_to_wgs = osr.CoordinateTransformation(wgs84_crs, wgs84_crs)

    cur_transform = transform_gsk_to_wgs

    for row in splitted_geom:
        if len(row) > 0:
            row_has_coords = False
            for word in list(row):
                if 'ГСК-2011' in word:
                    cur_transform = transform_gsk_to_wgs
                    cur_crs_name = 'ГСК-2011'
                elif 'Пулково' in word and '42' in word:
                    cur_transform = transform_pulk_to_wgs
                    cur_crs_name = 'Пулково-1942'
                elif 'WGS' in word:
                    cur_transform = transform_wgs_to_wgs
                    cur_crs_name = 'WGS-1984'
                if '°' in word:
                    row_has_coords = True
                if 'Мультиточка' in word:
                    Multipoint = True
                    first_points_after_multipoint_counter = 0

            if (any('Объект' in word1 for word1 in list(row)) and any('№' in word2 for word2 in list(row))) or \
                    (any('Система' in word3 for word3 in list(row)) and any(
                        'координат' in word4 for word4 in list(row))):
                if ring_of_points.GetPointCount() > 0:
                    if ring_of_points.GetPointCount() > 2:
                        ring_of_points.CloseRings()
                        pol_of_rings.AddGeometry(ring_of_points)
                        record_has_geometry = True
                        multipol_of_pols.AddGeometry(pol_of_rings)
                    ring_of_points = ogr.Geometry(ogr.wkbLinearRing)
                    pol_of_rings = ogr.Geometry(ogr.wkbPolygon)

            if row[0] == '1' and row_has_coords:
                if Multipoint:
                    first_points_after_multipoint_counter += 1
                    if first_points_after_multipoint_counter > 1:
                        Multipoint = False
                        first_points_after_multipoint_counter = 0

                if ring_of_points.GetPointCount() > 2:
                    ring_of_points.CloseRings()
                    pol_of_rings.AddGeometry(ring_of_points)
                    record_has_geometry = True
                ring_of_points = ogr.Geometry(ogr.wkbLinearRing)
                ring_first_point = ogr.Geometry(ogr.wkbPoint)
                ring_first_point.AddPoint(0, 0)

            if row_has_coords and not Multipoint:

                if dms_to_dec(row[2]) > coords_threshold and dms_to_dec(row[1]) > coords_threshold:
                    point = ogr.Geometry(ogr.wkbPoint)
                    point.AddPoint(dms_to_dec(row[2]), dms_to_dec(row[1]))
                    if abs(point.GetY()) <= 90 and abs(point.GetX()) <= 180:
                        point.Transform(cur_transform)
                        ring_of_points.AddPoint(point.GetX(), point.GetY())

    if ring_of_points.GetPointCount() > 2:
        ring_of_points.CloseRings()
        pol_of_rings.AddGeometry(ring_of_points)
        record_has_geometry = True
        multipol_of_pols.AddGeometry(pol_of_rings)

    # c_type_1 = ogr.wkbMultiPolygon
    # g_type_1 = multipol_of_pols.GetGeometryType()
    
    try:
        if not multipol_of_pols.IsValid():
            try:
                valid_geom = multipol_of_pols.MakeValid()
                # gdal geometry types list: https://github.com/OSGeo/gdal/blob/8943200d5fac69f0f995fc11af7e7e3696823b37/gdal/ogr/ogr_core.h#L314-L402
                if valid_geom and valid_geom.GetGeometryType() in [ogr.MultiPolygon25D, ogr.MultiPolygon]:
                    multipol_of_pols = valid_geom
                else:
                    record_has_geometry = False
            except:
                pass
    except:
        pass
    
        # if record_has_geometry:
        #     # c_type_2 = ogr.wkbMultiPolygon
        #     # g_type_2 = multipol_of_pols.GetGeometryType()
        #     if multipol_of_pols.GetGeometryType() != ogr.MultiPolygon25D:
        #         pass
                # record_has_geometry = False
    
    return (record_has_geometry, multipol_of_pols)


def update_postgres_table(gdalpgcs, folder='rfgf_blocks',  gpkg='d_r.gpkg', layer='l_b', bot_info=('token', 'chatid'), webhook='', where="license_block_name NOT LIKE '%Северо-Врангелевский%'"):
    current_directory = os.getcwd()
    logpath = os.path.join(current_directory, folder, 'logfile.txt')
    logdateformat = '%Y-%m-%d %H:%M:%S'
    # create a standard EPSG WGS-1984 CRS
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)

    with open(logpath, 'a', encoding='utf-8') as logf, requests.Session() as s:
        message = f"LicenseBlockUpdater: Updating PostgreSQL table started"
        log_message(s, logf, bot_info, message)

        # create a backup of the layer
        bk_gpkg = os.path.join(current_directory, folder, 'rfgf_backup.gpkg')

        gpkg_drv = gdal.GetDriverByName('GPKG')
        if os.path.exists(bk_gpkg):
            gpkg_drv.Delete(bk_gpkg)
        targetds = gpkg_drv.Create(bk_gpkg, 0, 0, 0, gdal.OF_VECTOR)

        # targetds = gdal.OpenEx(bk_gpkg, gdal.OF_VECTOR)
        backup_success = False
        myoptions = gdal.VectorTranslateOptions(
            layerName=f"license_blocks_rfgf_{datetime.now().strftime('%Y%m%d')}",
            format='GPKG',
            accessMode='overwrite',
            dstSRS=srs,
            layers=['rfgf.license_blocks_rfgf'],
            geometryType='MULTIPOLYGON'
        )
        try:
            message = f"LicenseBlockUpdater: Backup of rfgf.license_blocks_rfgf table started"
            log_message(s, logf, bot_info, message)
            if gdal.VectorTranslate(targetds, gdalpgcs, options=myoptions):
                message = f"LicenseBlockUpdater: Backup of rfgf.license_blocks_rfgf table successfully done to {bk_gpkg}"
                log_message(s, logf, bot_info, message)
                backup_success = True
            else:
                message = f"LicenseBlockUpdater: Backup of rfgf.license_blocks_rfgf table FAILED - skipping database update.."
                log_message(s, logf, bot_info, message)
                backup_success = False
                return False
        except:
            message = f"LicenseBlockUpdater: Backup of rfgf.license_blocks_rfgf table FAILED - skipping database update.."
            log_message(s, logf, bot_info, message)
            backup_success = False
            return False

        # create a path to the geopackage
        sourcepath = os.path.join(current_directory, folder, gpkg)
        # create an ogr datasource
        sourceds = gdal.OpenEx(sourcepath, gdal.OF_VECTOR)

        if backup_success:
            if not os.path.exists(sourcepath):
                message = f"LicenseBlockUpdater: Source package does not exist. Skipping update."
                log_message(s, logf, bot_info, message)
                return False
            elif not sourceds.GetLayer(layer):
                message = f"LicenseBlockUpdater: Source layer does not exist. Skipping update."
                log_message(s, logf, bot_info, message)
                return False
            else:
                try:
                    with psycopg2.connect(gdalpgcs[3:]) as pgconn:
                        with pgconn.cursor() as cur:
                            message = f"LicenseBlockUpdater: Started deleting old license blocks from the server..."
                            log_message(s, logf, bot_info, message)
                            sql = 'DELETE FROM rfgf.license_blocks_rfgf'
                            cur.execute(sql)
                        pass
                    delete_success = True
                except:
                    delete_success = False

                if not delete_success:
                    message = f"LicenseBlockUpdater: Delete all old Rosgeolfond blocks from server FAILED, skipping update..."
                    log_message(s, logf, bot_info, message)
                else:
                    message = f"LicenseBlockUpdater: Successfully deleted all old Rosgeolfond blocks from server. Starting update..."
                    log_message(s, logf, bot_info, message)

                    # create VectorTranslateOptions to specify the data conversion parameters
                    # layerName: full destination table name
                    # format: destination format
                    # accessMode: append or overwrite data to the destination
                    # dstSRS: destination CRS
                    # layers: source layer names list
                    # geometryType: destination geometry type
                    myoptions = gdal.VectorTranslateOptions(
                        layerName='rfgf.license_blocks_rfgf',
                        format='PostgreSQL',
                        accessMode='append',
                        dstSRS=srs,
                        layers=[layer],
                        geometryType='MULTIPOLYGON',
                        where=where
                    )
                    try:
                        message = f"LicenseBlockUpdater: Started updating Rosgeolfond license blocks table on server..."
                        log_message(s, logf, bot_info, message)
                        # try to do the conversion
                        if gdal.VectorTranslate(gdalpgcs, sourceds, options=myoptions):
                            message = f"Выполнено оновление лицензионных участков РФГФ в базе данных vgdb"
                            log_message(s, logf, bot_info, message)
                            # if webhook:
                            #     send_to_teams(webhook, message, logf)
                        else:
                            message = f"LicenseBlockUpdater: Update Rosgeolfond license blocks table on server FAILED"
                            log_message(s, logf, bot_info, message)
                            return False
                    except:
                        message = f"LicenseBlockUpdater: Update Rosgeolfond license blocks table on server FAILED"
                        log_message(s, logf, bot_info, message)
                        return False
    return True



def split_strip(my_str):
    while '  ' in my_str:
        my_str = my_str.replace('  ', ' ')
    my_list = [i.strip().split() for i in my_str.splitlines()]
    return my_list


# converts DDD°MM'SS.SSSSSS"E to decimal
def dms_to_dec(dms_coords):
    dec_coords = 0.0
    if dms_coords[:dms_coords.find('°')].replace('-', '').isdigit() \
            and dms_coords[dms_coords.find('°') + 1:dms_coords.find('\'')].replace('-', '').isdigit() \
            and dms_coords[dms_coords.find('\'') + 1:dms_coords.find('"')].replace('-', '').replace('.', '').isdigit():
        dec_coords = abs(float(dms_coords[:dms_coords.find('°')])) + \
                     abs(float(dms_coords[dms_coords.find('°') + 1:dms_coords.find('\'')])) / 60 + \
                     abs(float(dms_coords[dms_coords.find('\'') + 1:dms_coords.find('"')])) / 3600

    if dms_coords[dms_coords.find('"') + 1:] in ['W', 'S']:
        dec_coords *= -1

    if '-' in dms_coords:
        dec_coords *= -1

    return dec_coords


def check_license_border_along_region(gos_reg_num, pgdsn):
    license_blocks_rfgf = 'rfgf.license_blocks_rfgf_test'
    
    i = 1
    pgconn = None
    while not pgconn and i <= 10:
        try:
            i += 1
            pgconn = psycopg2.connect(pgdsn)
        except:
            pass
    if pgconn:
        with pgconn.cursor(cursor_factory=psycopg2.extras.DictCursor) as dictcur:
            sql = f"select * from {license_blocks_rfgf} where gos_reg_num = '{gos_reg_num}';"
            dictcur.execute(sql)
            license_row = dictcur.fetchall()[0]

            sql = f"select st_distance"
            pass
        pgconn.close()


if __name__ == '__main__':
    #
    #
    # read the telegram bot credentials
    # with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
    #     jdata = json.load(f)
    #     bot_info = (jdata['token'], jdata['chatid'])
    #
    # # read the postgres login credentials for gdal from file
    # with open('test.pggdal', encoding='utf-8') as gdalf:
    #     gdalpgcs = gdalf.read().replace('\n', '')
    #
    # # download the license blocks data from Rosgeolfond
    # if download_rfgf_blocks('rfgf_request_noFilter_300000.json', 'rfgf_result_300000.json', bot_info=bot_info):
    #     # parse the blocks from downloaded json
    #     if parse_rfgf_blocks('rfgf_result_300000.json', bot_info=bot_info):
    #         # update license blocks on server
    #         update_postgres_table(gdalpgcs, bot_info=bot_info)

    # read the telegram bot credentials
    with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
        jdata = json.load(f)
        bot_info = (jdata['token'], jdata['chatid'])

    # read the postgres login credentials for gdal from file
    with open('.pggdal', encoding='utf-8') as gdalf:
        gdalpgcs = gdalf.read().replace('\n', '')

    with open('license_blocks_general.webhook', 'r', encoding='utf-8') as f:
        lb_general_webhook = f.read().replace('\n', '')

    with open('.ext_pgdsn', encoding='utf-8') as f:
        ext_pgdsn = f.read()

    with open('.pgdsn', encoding='utf-8') as f:
        local_pgdsn = f.read()

    with open('.egssh', 'r', encoding='utf-8') as f:
        egssh = json.load(f)

    # # download the license blocks data from Rosgeolfond
    # if download_rfgf_blocks('rfgf_request_noFilter_300000.json', 'rfgf_result_300000.json', bot_info=bot_info):
    #     pass
    #     # parse the blocks from downloaded json
    #     if parse_rfgf_blocks('rfgf_result_300000.json', bot_info=bot_info):
    #         pass
    #     #     # update license blocks on server
    #     #     if update_postgres_table(gdalpgcs, bot_info=bot_info, webhook=lb_general_webhook):
    #     #         synchro_layer([('rfgf', ['license_blocks_rfgf'])], local_pgdsn, ext_pgdsn, ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)

    # check_license_border_along_region('СВЕ03821НР', local_pgdsn)
    
    #synchro_layer([('rfgf', ['license_blocks_rfgf'])], local_pgdsn, ext_pgdsn, ssh_host=egssh["host"], ssh_user=egssh["user"], bot_info=bot_info)

    # if download_rfgf_blocks('rfgf_request_noFilter_300000.json', 'rfgf_result_300000.json', bot_info=bot_info):
    #     pass
    if parse_rfgf_blocks('rfgf_result_300000.json', bot_info=bot_info):
        pass
    # if update_postgres_table(gdalpgcs, bot_info=bot_info, webhook=lb_general_webhook):
    #     pass