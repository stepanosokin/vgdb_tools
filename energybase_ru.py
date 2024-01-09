import json

with open('D:/OneDrive - Verde Generation/WORKS/VerdeG/2023/202312/20231208_energybase/pipeline.json', encoding='utf-8') as f:
    data = json.load(f)

    for i, feature in enumerate(data['features']):
        if feature['geometry']['type'] == 'LineString':
            for j, coordpair in enumerate(feature['geometry']['coordinates']):
                # cury, curx = data['features'][i]['geometry']['coordinates'][j][0], data['features'][i]['geometry']['coordinates'][j][1]
                data['features'][i]['geometry']['coordinates'][j] = coordpair[::-1]
        elif feature['geometry']['type'] == 'Point':
            data['features'][i]['geometry']['coordinates'] = feature['geometry']['coordinates'][::-1]
    with open('D:/OneDrive - Verde Generation/WORKS/VerdeG/2023/202312/20231208_energybase/pipeline_mod.json', 'w', encoding='utf-8') as of:
        json.dump(data, of, ensure_ascii=False)

    pass