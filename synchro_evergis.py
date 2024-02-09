import os, sys, subprocess
import psycopg2
import requests
from psycopg2.extras import *
from osgeo import ogr, gdal
from fabric import Connection


def synchro_layer(schemas_tables, local_pgdsn, ext_pgdsn,
                  ssh_host='45.139.25.199', ssh_user='dockeruser',
                  local_port_for_ext_pg=5433, bot_info=('token', 'id')):

    ext_pgdsn_dict = dict([x.split('=') for x in ext_pgdsn.split(' ')])
    new_ext_pgdsn = ext_pgdsn.replace(f"port={ext_pgdsn_dict['port']}", f"port={str(local_port_for_ext_pg)}")

    current_directory = os.getcwd()
    # create a pathname for the logfile
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    # now we open the logfile and start logging
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:

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
                                            print(f"failed attempt {str(i - 1)} of 10 to translate data to layer {schema}.{table}")
                                    if success:
                                        print(f'table {schema}.{table} synchronized successfully')
                                    else:
                                        print(f'table {schema}.{table} synchronization failed after f{str(i - 1)} tries')
                                else:
                                    print('process aborted - delete from dest operation failed')


def synchro_table(schemas_tables, local_pgdsn_path, ext_pgdsn_path,
                  ssh_host='45.139.25.199', ssh_user='dockeruser',
                  local_port_for_ext_pg=5433):

    with open(ext_pgdsn_path, encoding='utf-8') as f:
        ext_pgdsn = f.read()

    with open(local_pgdsn_path, encoding='utf-8') as f:
        local_pgdsn = f.read()

    local_pgdsn_dict = dict([x.split('=') for x in local_pgdsn.split(' ')])
    ext_pgdsn_dict = dict([x.split('=') for x in ext_pgdsn.split(' ')])
    new_ext_pgdsn = ext_pgdsn.replace(f"port={ext_pgdsn_dict['port']}", f"port={str(local_port_for_ext_pg)}")
    new_ext_pgdsn_dict = dict([x.split('=') for x in new_ext_pgdsn.split(' ')])
    with open('.new_ext_pgpass', 'w', encoding='utf-8') as f:
        f.write(f"{new_ext_pgdsn_dict['host']}:{new_ext_pgdsn_dict['port']}:{new_ext_pgdsn_dict['dbname']}:{new_ext_pgdsn_dict['user']}:{new_ext_pgdsn_dict['password']}")
    with open('.local_pgpass', 'w', encoding='utf-8') as f:
        f.write(f"{local_pgdsn_dict['host']}:{local_pgdsn_dict['port']}:{local_pgdsn_dict['dbname']}:{local_pgdsn_dict['user']}:{local_pgdsn_dict['password']}")

    with Connection(ssh_host, user=ssh_user).forward_local(local_port_for_ext_pg,
                                                           remote_port=int(ext_pgdsn_dict['port'])):
        my_env = os.environ.copy()
        my_env["PGPASSFILE"] = '.local_pgpass'
        # loop through the specified schemas/tables tuples. list() used to allow multiple loops through schemas_tables.
        for (schema, tables) in list(schemas_tables):
            # each 'tables' is a list. loop through it now.
            for table in tables:
                # launch pg_dump to dump the current table
                subprocess.run(['pg_dump', '-h', local_pgdsn_dict['host'], '-p', local_pgdsn_dict['port'],
                                '-d', local_pgdsn_dict['dbname'], '-U',
                                local_pgdsn_dict['user'], '--inserts', '-t', f'{schema}.{table}', '--no-publications',
                                '--quote-all-identifiers', '-v', '-w', '-F', 'p', '-f',
                                f'data/vgdb_5432_{schema}_{table}.dump'],
                               env=my_env)
        # loop through the specified schemas/tables tuples. list() used to allow multiple loops through schemas_tables.
        my_env["PGPASSFILE"] = '.new_ext_pgpass'
        for (schema, tables) in list(schemas_tables):
            # each 'tables' is a list. loop through it now.
            for table in tables:
                # launch psql to delete rows from current table. use my_env as env parameter.
                subprocess.run(
                    ['psql', '-U', new_ext_pgdsn_dict['user'], '-h', new_ext_pgdsn_dict['host'],
                     '-p', new_ext_pgdsn_dict['port'], '-d', new_ext_pgdsn_dict['dbname'],
                     '-w', '-c', f'delete from {schema}.{table};'],
                    env=my_env)
                # launch psql to insert data to current table. use my_env as env parameter.
                subprocess.run(
                    ['psql', '-U', new_ext_pgdsn_dict['user'], '-h', new_ext_pgdsn_dict['host'],
                     '-p', new_ext_pgdsn_dict['port'], '-d', new_ext_pgdsn_dict['dbname'],
                     '-w', '-f', f'data/vgdb_5432_{schema}_{table}.dump'],
                    env=my_env)




if __name__ == '__main__':
    with open('.ext_pgdsn', encoding='utf-8') as f:
        ext_pgdsn = f.read()

    with open('.pgdsn', encoding='utf-8') as f:
        local_pgdsn = f.read()

    synchro_layer([('rosnedra', ['license_blocks_rosnedra_orders'])], local_pgdsn, ext_pgdsn)
    # synchro_table([('torgi_gov_ru', ['lotcards'])], '.pgdsn', '.ext_pgdsn')
