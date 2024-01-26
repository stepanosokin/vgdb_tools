# 1) Открыть QGIS
# 2) Открыть Python Console
# 3) Отобразить редактор кода
# 4) Открыть скрипт из этого файла
# 5) Запустить скрипт кнопкой Run Script
# 6) Кликнуть в любом месте на карте
# 7) Кадастровый номер отобразится в уведомлении

import requests, pyproj


def show_cadnum(pointTool):
    point_geom = QgsGeometry.fromPointXY(pointTool)
    proj_crs = iface.mapCanvas().mapSettings().destinationCrs()
    wgs_crs = QgsCoordinateReferenceSystem(4326)
    tr = QgsCoordinateTransform(proj_crs, wgs_crs, QgsProject.instance())
    point_geom.transform(tr)
    x_dec = point_geom.centroid().asPoint().x()
    y_dec = point_geom.centroid().asPoint().y()

    with requests.Session() as s:
        # url = f'https://pkk.rosreestr.ru/api/features/1?sq=\{"type":"Point","coordinates":[{str(x_dec)},{str(y_dec)}]\}&tolerance=1&limit=11'
        payload = {"sq": {"type": "Point", "coordinates": f"[{str(x_dec)}, {str(y_dec)}]"}, "tolerance": "1",
                   "limit": "11"}
        headers = {'Origin': 'https://egrp365.org'}
        url = '''https://pkk.rosreestr.ru/api/features/1?sq={"type":"Point","coordinates":['''
        url += str(x_dec)
        url += ', '
        url += str(y_dec)
        url += ']}&tolerance=1&limit=11'
        shorturl = 'https://pkk.rosreestr.ru/api/features/1'
        # result = s.get(shorturl, params=payload, headers=headers, verify=False)
        code = 0
        tries = 0
        data = None
        while code != 200 and tries < 10:
            try:
                result = s.get(url, headers=headers, verify=False)
                code = result.status_code
                tries += 1
                data = result.json()
            except:
                message = 'Ошибка запроса кадастрового номера'
        if data:
            message = ''
            for f in data.get('features'):
                message += f.get('attrs').get('cn') + '\n'
        iface.messageBar().pushMessage('Кадастровые номера:', message, level=Qgis.Warning)


mc = iface.mapCanvas()

# this QGIS tool emits as QgsPoint after each click on the map canvas
pointTool = QgsMapToolEmitPoint(mc)
pointTool.canvasClicked.connect(show_cadnum)
mc.setMapTool(pointTool)