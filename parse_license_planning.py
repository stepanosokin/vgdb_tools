from osgeo import ogr, osr
import csv, os

out_geopackage = 'data/parse_license_planning.gpkg'
out_layer_name = 'Auctions2023'

with open('data/Auctions2023.csv', newline='') as f:
    dictReader = csv.DictReader(f, delimiter=';')
    driver = ogr.GetDriverByName('GPKG')
    if os.path.exists(out_geopackage):
        driver.DeleteDataSource(out_geopackage)
    out_ds = driver.CreateDataSource(out_geopackage)
    o_srs = osr.SpatialReference()
    o_srs.ImportFromEPSG(4284)

    out_layer = out_ds.CreateLayer(out_layer_name, srs=o_srs, geom_type=ogr.wkbMultiPolygon)

    out_fields = ['AUCTION_INFO', 'RESOURCE_TYPE', 'name_ru', 'block_n', 'AREA_KM', 'comments', 'RESOURCES', 'EXPERT_PROTOCOL', 'USAGE_TYPE', 'LEND_TYPE', 'AUCTION_DATE']
    for out_field_name in list(out_fields):
        field_def = ogr.FieldDefn(out_field_name, ogr.OFTString)
        out_layer.CreateField(field_def)

    feature_def = out_layer.GetLayerDefn()

    cur_block = '1'
    cur_ring = '1'
    cur_block_geom = ogr.Geometry(ogr.wkbMultiPolygon)
    cur_pol_geom = ogr.Geometry(ogr.wkbPolygon)
    cur_ring_geom = ogr.Geometry(ogr.wkbLinearRing)
    cur_feature = ogr.Feature(feature_def)

    for row in dictReader:

        if row['block_n'] == cur_block:
            cur_fields = row
            if row['ring_n'] == cur_ring:
                lon = float(row['pulk42_l_deg']) + float(row['pulk42_l_min']) / 60 + float(row['pulk42_l_sec']) / 3600
                lat = float(row['pulk42_b_deg']) + float(row['pulk42_b_min']) / 60 + float(row['pulk42_b_sec']) / 3600
                cur_ring_geom.AddPoint(lon, lat)
            else:
                cur_ring_geom.CloseRings()
                cur_pol_geom.AddGeometry(cur_ring_geom)
                cur_pol_geom.CloseRings()
                cur_ring = row['ring_n']
                cur_ring_geom = ogr.Geometry(ogr.wkbLinearRing)
                lon = float(row['pulk42_l_deg']) + float(row['pulk42_l_min']) / 60 + float(row['pulk42_l_sec']) / 3600
                lat = float(row['pulk42_b_deg']) + float(row['pulk42_b_min']) / 60 + float(row['pulk42_b_sec']) / 3600
                cur_ring_geom.AddPoint(lon, lat)
        else:
            cur_ring_geom.CloseRings()
            cur_pol_geom.AddGeometry(cur_ring_geom)
            cur_pol_geom.CloseRings()
            cur_block_geom.AddGeometry(cur_pol_geom)
            cur_feature.SetGeometry(cur_block_geom)
            for field in list(out_fields):
                cur_feature.SetField(field, cur_fields[field])
            out_layer.CreateFeature(cur_feature)
            print(cur_fields['AUCTION_INFO'], cur_fields['name_ru'])
            cur_fields = row
            cur_block = row['block_n']
            cur_ring = row['ring_n']
            cur_block_geom = ogr.Geometry(ogr.wkbMultiPolygon)
            cur_pol_geom = ogr.Geometry(ogr.wkbPolygon)
            cur_ring_geom = ogr.Geometry(ogr.wkbLinearRing)
            cur_feature = ogr.Feature(feature_def)
            lon = float(row['pulk42_l_deg']) + float(row['pulk42_l_min']) / 60 + float(row['pulk42_l_sec']) / 3600
            lat = float(row['pulk42_b_deg']) + float(row['pulk42_b_min']) / 60 + float(row['pulk42_b_sec']) / 3600
            cur_ring_geom.AddPoint(lon, lat)


    cur_ring_geom.CloseRings()
    cur_pol_geom.AddGeometry(cur_ring_geom)
    cur_pol_geom.CloseRings()
    cur_block_geom.AddGeometry(cur_pol_geom)
    cur_block_geom.CloseRings()
    cur_feature.SetGeometry(cur_block_geom)
    for field in list(out_fields):
        cur_feature.SetField(field, cur_fields[field])
    out_layer.CreateFeature(cur_feature)






