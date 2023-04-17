import json
import os.path
import os

from osgeo import ogr
from osgeo import osr

## subsoil.json file downloaded from https://lands.qoldau.kz/ru/lands-map/subsoils website using devtools.
## got as a response from POST request, cUrl bash command:
# curl 'https://lands.qoldau.kz/ru/lands-map/subsoils?MenuAction=GetData&MapLayerName=HYDROCARBON&MapSubLayerName=HYDROCARBON' \
#   -H 'Accept: */*' \
#   -H 'Accept-Language: ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,en-GB;q=0.6' \
#   -H 'Connection: keep-alive' \
#   -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
#   -H 'Cookie: visitor-id=57e0bd54dc8f4269aa9b0048ab627311; .AspNetCore.Antiforgery.XdDePD1kDzU=CfDJ8H15cnMABphMlsBJggUN7ZNCuEldDMatIwWSgkVitgHYdleMWDRoQYHFyqeLRsXUTp2oGupz2p6cTj0NB3KMGPelSzVSTjR9dJsTfk_ggNsRwgSXylqxRJX28CDojCuczaL27eqJ7nmHSQB0RqY4A_k' \
#   -H 'DNT: 1' \
#   -H 'Origin: https://lands.qoldau.kz' \
#   -H 'Referer: https://lands.qoldau.kz/ru/lands-map/subsoils' \
#   -H 'Sec-Fetch-Dest: empty' \
#   -H 'Sec-Fetch-Mode: cors' \
#   -H 'Sec-Fetch-Site: same-origin' \
#   -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36' \
#   -H 'X-Requested-With: XMLHttpRequest' \
#   -H 'sec-ch-ua: "Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"' \
#   -H 'sec-ch-ua-mobile: ?0' \
#   -H 'sec-ch-ua-platform: "Windows"' \
#   --data-raw '__RequestVerificationToken=CfDJ8H15cnMABphMlsBJggUN7ZOss_JBaJZdNqWeneDlFcSeTKfXtH6wJE2qS2Sy1Q_UjrFTcPqfQd1KCEBpT0pDyrLWt75-lismerlGIwD9bwobDDDVpYewcPRDnXEDlkcVICEoRyat7mR7VQI4bogHw6I&HYDROCARBON.Filter=%5B%5D' \
#   --compressed

o_file = 'data/SHP/subsoil.gpkg'
o_layer = 'subsoil'

os.environ['SHAPE_ENCODING'] = "Windows-1251"

with open('data/subsoil.json', 'r', encoding='utf-8') as i_f:
    json_data = json.load(i_f)
    # print(json_data)
    driver = ogr.GetDriverByName('GPKG')
    if os.path.exists(o_file):
        driver.DeleteDataSource(o_file)
    out_ds = driver.CreateDataSource(o_file)
    o_srs = osr.SpatialReference()
    o_srs.ImportFromEPSG(4326)
    out_layer = out_ds.CreateLayer(o_layer, srs=o_srs, geom_type=ogr.wkbMultiPolygon)
    textField = ogr.FieldDefn('Text', ogr.OFTString)
    out_layer.CreateField(textField)
    fDefn = out_layer.GetLayerDefn()
    for record in json_data:
        cur_name = record['HoverTemplate']['Template']['Text']
        cur_wkt = record['WKT']
        feat = ogr.Feature(fDefn)
        geom = ogr.CreateGeometryFromWkt(cur_wkt)
        feat.SetGeometry(geom)
        feat.SetField('Text', cur_name)
        out_layer.CreateFeature(feat)
        #print(cur_name, cur_wkt)





