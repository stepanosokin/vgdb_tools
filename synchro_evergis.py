import psycopg2
from psycopg2.extras import *
from osgeo import ogr, gdal
from fabric import Connection


def synchro_layer(schemas_tables, local_pgdsn, ext_pgdsn,
                  ssh_host='45.139.25.199', ssh_user='dockeruser',
                  local_port_for_ext_pg=5433):

    ext_pgdsn_dict = dict([x.split('=') for x in ext_pgdsn.split(' ')])
    new_ext_pgdsn = ext_pgdsn.replace(f"port={ext_pgdsn_dict['port']}", f"port={str(local_port_for_ext_pg)}")

    with Connection(ssh_host, user=ssh_user).forward_local(local_port_for_ext_pg,
                                                           remote_port=int(ext_pgdsn_dict['port'])):

        local_conn = ogr.Open(f"PG:{local_pgdsn}")

        for (schema, tables) in list(schemas_tables):
            for table in tables:
                source_layer = None
                try:
                    source_layer = local_conn.GetLayer(f"{schema}.{table}")
                except:
                    print('error connecting to source database')
                if source_layer:
                    feature = source_layer.GetNextFeature()
                    if feature:
                        status = None
                        try:
                            pgconn = psycopg2.connect(new_ext_pgdsn)
                            with pgconn:
                                with pgconn.cursor() as cur:
                                    sql = f'DELETE FROM {schema}.{table};'
                                    print(f'attempting to delete data from layer {schema}.{table} on dest')
                                    cur.execute(sql)
                                    status = cur.statusmessage
                            pgconn.close()
                        except:
                            print('error deleting old data from destination layer')
                        if status:
                            if 'DELETE' in status:
                                print(f'successfully deleted data from layer {schema}.{table} on dest')
                                source_layer_geom_type = feature.GetGeometryRef().GetGeometryName()
                                source_layer_crs = source_layer.GetSpatialRef()
                                myoptions = gdal.VectorTranslateOptions(
                                    layerName=f'{schema}.{table}',
                                    format='PostgreSQL',
                                    accessMode='append',
                                    dstSRS=source_layer_crs,
                                    layers=[f'{schema}.{table}'],
                                    geometryType=source_layer_geom_type
                                )
                                i = 1
                                success = False
                                while not success and i <= 10:
                                    i += 1
                                    try:
                                        print(f"attempt {str(i - 1)} to translate data to layer {schema}.{table}")
                                        success = gdal.VectorTranslate(f"PG:{new_ext_pgdsn}", f"PG:{local_pgdsn}", options=myoptions)
                                    except:
                                        pass
                                if success:
                                    print(f'table {schema}.{table} synchronized successfully')
                                else:
                                    print(f'table {schema}.{table} synchronization failed after f{str(i - 1)} tries')
                            else:
                                print('process aborted - delete from dest operation failed')
        pass

if __name__ == '__main__':
    with open('.ext_pgdsn', encoding='utf-8') as f:
        ext_pgdsn = f.read()

    with open('.pgdsn', encoding='utf-8') as f:
        local_pgdsn = f.read()

    synchro_layer([('rosnedra', ['license_blocks_rosnedra_orders'])], local_pgdsn, ext_pgdsn)
