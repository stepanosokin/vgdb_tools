# 1) Открыть QGIS
# 2) Открыть Python Console
# 3) Отобразить редактор кода
# 4) Открыть скрипт из этого файла
# 5) Запустить скрипт кнопкой Run Script
# 6) Кликнуть в любом месте на карте
# 7) Кадастровый номер отобразится в уведомлении

import requests
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from bs4 import BeautifulSoup

def show_cadnum(pointTool):
    point_geom = QgsGeometry.fromPointXY(pointTool)
    proj_crs = iface.mapCanvas().mapSettings().destinationCrs()
    wgs_crs = QgsCoordinateReferenceSystem(4326)
    tr = QgsCoordinateTransform(proj_crs, wgs_crs, QgsProject.instance())
    point_geom.transform(tr)
    x_dec = point_geom.centroid().asPoint().x()
    y_dec = point_geom.centroid().asPoint().y()

    driver_exe = 'edgedriver'
    options = Options()
    options.add_argument("--headless")
    # options.add_argument("--headless")

    # Eldar Mamedov's Selenium options:
    options.add_argument("--headless=new")  # Запуск браузера в безголовом режиме
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-infobars")
    options.add_argument("--start-maximized")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    # driver.get('https://egrp365.ru')
    driver.get(f'https://pkk.rosreestr.ru/api/features/1/?sq=%7B%22type%22%3A%22Point%22%2C%22coordinates%22%3A%5B{str(x_dec)}%2C{str(y_dec)}%5D%7D&tolerance=1&limit=11')
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    data = soup.find('div', attrs={'hidden': 'true'})
    data = json.loads(data.text)
    message = ''
    for f in data.get('features'):
        message += f.get('attrs').get('cn') + '\n'
    iface.messageBar().pushMessage('Кадастровые номера:', message, level=Qgis.Warning)
    
    # deprecated 2024-09-13
    # with requests.Session() as s:        
    #     payload = {"sq": {"type": "Point","coordinates": f"[{str(x_dec)}, {str(y_dec)}]"}, "tolerance": "1", "limit": "11"}
    #     headers = {'Origin': 'https://egrp365.org'}
    #     url = '''https://pkk.rosreestr.ru/api/features/1?sq={"type":"Point","coordinates":['''
    #     url += str(x_dec)
    #     url += ', '
    #     url += str(y_dec)
    #     url += ']}&tolerance=1&limit=11'
    #     shorturl = 'https://pkk.rosreestr.ru/api/features/1'
    #     code = 0
    #     tries = 0
    #     data = None
    #     while code != 200 and tries < 10:
    #         try:
    #             result = s.get(url, headers=headers, verify=False)
    #             code = result.status_code
    #             tries += 1
    #             data = result.json()
    #         except:
    #             message = 'Ошибка запроса кадастрового номера'
    #     if data:
    #         message = ''
    #         for f in data.get('features'):
    #             message += f.get('attrs').get('cn') + '\n'
    #     iface.messageBar().pushMessage('Кадастровые номера:', message, level=Qgis.Warning)

            
mc = iface.mapCanvas()

# this QGIS tool emits as QgsPoint after each click on the map canvas
pointTool = QgsMapToolEmitPoint(mc)
pointTool.canvasClicked.connect( show_cadnum )
mc.setMapTool( pointTool )