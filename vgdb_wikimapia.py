import requests, json

with open('wikimapia_token', encoding='utf-8') as f:
    token = f.read()

keywords = ['гпз', 'укпг', 'упн', 'псн', 'упсв', 'гтэс', 'бкнс',
            'газоперерабатывающ', 'подготовки газа', 'подготовки нефти',
            'сдачи нефти', 'сброса воды', 'газотурбин', 'насосная']

gj = {"type": "FeatureCollection", "features": []}

with open('sibur_gpz.geojson', encoding='utf-8') as f:
    points = json.load(f)

with requests.Session() as s:
    url = 'http://api.wikimapia.org'
    for point in points['features']:
        x = point['geometry']['coordinates'][0]
        y = point['geometry']['coordinates'][1]
        params = {'function': 'place.getnearest',
                  'key': token,
                  'lat': str(y),
                  'lon': str(x),
                  'format': 'json',
                  'language': 'ru',
                  }
        response = s.get(url, params=params)
        data = response.json()
        for place in data['places']:
            # if ('газоперерабатывающий' in place['title'].lower() or 'гпз' in place['title'].lower()) and \
            #         'polygon' in place.keys():
            if any(x in place['title'].lower() for x in keywords) and 'polygon' in place.keys():
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": 'Polygon',
                        "coordinates": []
                    },
                    "properties": {}
                }
                for k, v in place.items():
                    if k not in ['location', 'polygon']:
                        feature["properties"][k] = v
                feature["geometry"]["coordinates"] = [[list(i.values()) for i in place['polygon']]]
                gj["features"].append(feature)

    with open('result.json', 'w', encoding='utf-8') as out:
        json.dump(gj, out)


#60.688445,72.861712