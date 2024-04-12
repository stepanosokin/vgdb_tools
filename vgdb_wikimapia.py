import requests, json

def get_nearest(points, keywords, token):
    gj = {"type": "FeatureCollection", "features": []}
    with requests.Session() as s:
        url = 'http://api.wikimapia.org'

        # #########################
        # params = {'function': 'category.getall',
        #           'key': token,
        #           'format': 'json'
        #           }
        # response = s.get(url, params=params)
        # data = response.json()
        # print(data)
        #########################

        for point in points['features']:
            x = point['geometry']['coordinates'][0]
            y = point['geometry']['coordinates'][1]
            params = {'function': 'place.getnearest',
                      'key': token,
                      'lat': str(y),
                      'lon': str(x),
                      'format': 'json',
                      'language': 'ru',
                      # 'category': '48368'
                      }
            response = s.get(url, params=params)
            data = response.json()
            for place in data['places']:
                # if ('газоперерабатывающий' in place['title'].lower() or 'гпз' in place['title'].lower()) and \
                #         'polygon' in place.keys():
                # if any(x in place['title'].lower() for x in keywords) and 'polygon' in place.keys():
                if 'polygon' in place.keys():
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


def get_by_id(id, token):
    gj = {"type": "FeatureCollection", "features": []}
    with requests.Session() as s:
        url = 'http://api.wikimapia.org'
        params = {'function': 'place.getbyid',
                  'key': token,
                  'id': id,
                  'format': 'json',
                  'language': 'ru',
                  # 'category': '48368'
                  }
        response = s.get(url, params=params)
        data = response.json()

        if 'polygon' in data.keys():
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": 'Polygon',
                    "coordinates": []
                },
                "properties": {}
            }
            for k, v in data.items():
                if k not in ['location', 'polygon']:
                    feature["properties"][k] = v
            feature["geometry"]["coordinates"] = [[list(i.values()) for i in data['polygon']]]
            gj["features"].append(feature)

    with open('result.json', 'w', encoding='utf-8') as out:
        json.dump(gj, out)


if __name__ == '__main__':

    with open('wikimapia_token', encoding='utf-8') as f:
        token = f.read()

    # keywords = ['гпз', 'укпг', 'упн', 'псн', 'упсв', 'гтэс', 'бкнс',
    #             'газоперерабатывающ', 'подготовки газа', 'подготовки нефти',
    #             'сдачи нефти', 'сброса воды', 'газотурбин', 'насосная']

    keywords = ['Оренбургский']

    # with open('sibur_gpz.geojson', encoding='utf-8') as f:
    #     points = json.load(f)

    with open('burtin_step.geojson', encoding='utf-8') as f:
        points = json.load(f)


    # get_by_id('16683066', token)

    get_nearest(points, keywords, token)



#60.688445,72.861712