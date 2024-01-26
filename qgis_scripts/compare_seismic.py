import csv

# Этот скрипт сравнивает между собой геометрию каждого оъекта в каждом видимом
# линейном слое в проекте с геометрией остальных объектов в этих слоях.
# Если другой объект полностью попадает внутрь буфера вокруг текущего объекта,
# то в результат записывается данное сочетание двух объектов.
# Результат записывается в CSV out_file. epsg - это код проекции входных данных.
# bsize - размер буфера

bsize = 500
epsg = 28470
out_file = 'D:/OneDrive - Verde Generation/WORKS/VerdeG/2023/202312/20231226_TPGK_All_Seis/tpgk_all_seis_match.csv'

mc = iface.mapCanvas()
vl = QgsVectorLayer('MultiLineStringM', 'matching_lines', 'memory')
crs = QgsCoordinateReferenceSystem(epsg)
vl.setCrs(crs)
pr = vl.dataProvider()
vl.startEditing()
pr.addAttributes([QgsField('layer', QVariant.String), QgsField('SRV_NAME', QVariant.String),
QgsField('matching_layer', QVariant.String), QgsField('matching_line', QVariant.String)])

with open(out_file, 'w', encoding='utf-8', newline='') as out:
    fields = ['layer', 'line', 'matching_layer', 'matching_line']
    writer = csv.DictWriter(out, fieldnames=fields, delimiter=';')
    writer.writeheader()
    match_result = []
    for layer in [y for y in mc.layers() if y.type() == QgsMapLayerType.VectorLayer and y.geometryType() in [1, 2, 5]]:
        for f in layer.getFeatures():
            cur_buf = f.geometry().buffer(bsize, 5)
            for l in [x for x in mc.layers() if x.type() == QgsMapLayerType.VectorLayer]:
                if l.geometryType() in [1, 2, 5] and layer != l:   #https://qgis.org/pyqgis/3.0/core/Wkb/QgsWkbTypes.html?highlight=qgswkbtypes#qgis.core.QgsWkbTypes.geometryType
                    for cf in l.getFeatures():
                        if cur_buf.contains(cf.geometry()):
                            match = {'layer': layer.name(), 'line': f['SRV_NAME'],
                                'matching_layer': l.name(), 'matching_line': cf['SRV_NAME']}
                            reversed_match = {'layer': l.name(), 'line': cf['SRV_NAME'],
                                'matching_layer': layer.name(), 'matching_line': f['SRV_NAME']}
                            if match not in match_result and reversed_match not in match_result:
                                match_result.append(match)
                            # Если засунуть следующие 5 строк внутрь предыдущего if,
                            # то в результат попадут только уникальные сочетания
                            writer.writerow(match)
                            nf = QgsFeature()
                            nf.setGeometry(f.geometry())
                            nf.setAttributes([layer.name(), f['SRV_NAME'], l.name(), cf['SRV_NAME']])
                            pr.addFeatures([nf])
    vl.commitChanges()
    QgsProject.instance().addMapLayer(vl)