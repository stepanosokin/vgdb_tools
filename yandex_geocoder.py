import requests, csv

with open('yandex_api_key', encoding='utf-8') as f:
    apikey = f.read().replace('\n', '')

# https://yandex.ru/dev/geocode/doc/ru/

with open('yandex_geocoder/Т+Справочник_предприятий_Оренбург_Пермь_с_V.csv', encoding='utf-8', newline='') as in_f:
    with open('yandex_geocoder/Адреса.csv', 'w', encoding='utf-8', newline='') as o_f:
        reader = csv.DictReader(in_f, delimiter=';')
        fieldnames = list(reader.fieldnames)
        fieldnames.extend(['lon', 'lat'])
        writer = csv.DictWriter(o_f, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        with requests.Session() as s:
            for i_row in reader:
                address = i_row['Адрес объекта']
                if address:
                    url = f"https://geocode-maps.yandex.ru/1.x/?apikey={apikey}&geocode={address.replace(' ', '+')}&format=json"
                    err_code = 0
                    i = 1
                    response = None
                    while err_code != 200 and i <= 10:
                        try:
                            response = s.get(url)
                            err_code = response.status_code
                        except:
                            print(f"Ошибка запроса {url}")
                        i += 1
                    if response:
                        j = response.json()
                        coords = j['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos'].split(' ')
                        writer.writerow(dict(zip(fieldnames, list(i_row.values()) + coords)))