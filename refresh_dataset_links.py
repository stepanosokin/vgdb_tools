import psycopg2
import json
import os
import fnmatch


def process_vgjson(filepath, conn):
    with open(filepath, 'r') as f:
        vgjson = json.load(f)
        for item in vgjson['data']:
            if 'type' in item.keys():
                if item['type'] == 'seismic_dataset' and 'id' in item.keys():
                    update_seismic_link(item, filepath.replace('vgdb.vgjson', ''), conn)
                    print('seismic_dataset', item['id'], filepath.replace('vgdb.vgjson', ''))
                elif item['type'] == 'well_attribute' and 'id' in item.keys():
                    update_well_attr_link(item, filepath.replace('vgdb.vgjson', ''), conn)
                    print('well_attr', item['id'], filepath.replace('vgdb.vgjson', ''))
                elif all([item['type'] == 'license_block_processed', 'name' in item.keys(), 'file' in item.keys()]):
                    update_license_processed_link(item, filepath.replace('vgdb.vgjson', ''), conn)
                    print('license_block_processed', item['name'], filepath.replace('vgdb.vgjson', '') + '\\' + item['file'])


def update_license_processed_link(item, link, conn):
    cur = conn.cursor()
    link = link + item["file"]
    cur.execute(f"""update culture.license_blocks_procesed_info set link = '{link}' where name = '{item["name"]}';""")


def update_seismic_link(item, link, conn):
    cur = conn.cursor()
    if 'link_id' in item.keys():
        cur.execute(f"""
                   update dm.seismic_datasets set link = '{link}' where dataset_id = {item['id']};
                   update dm.links set link = '{link}' where link_id == {item['link_id']};
                   """)
    else:
        cur.execute(f"""
        update dm.seismic_datasets set link = '{link}' where dataset_id = {item['id']};
        update dm.links set link = '{link}' where link_id in (select link_id from dm.links_to_datasets where dataset_id = {item['id']});
        """)


def update_well_attr_link(item, link, conn):
    cur = conn.cursor()
    cur.execute(f"""
    update dm.links set link = '{link}' where link_id = (select link_id from dm.well_attributes where well_attribute_id = {item['id']});
    """)


def update_links(links_vgjson_path, vgdb_pass):
    with open(links_vgjson_path, 'r') as f:
        links = json.load(f)
    with psycopg2.connect(f"dbname=vgdb host=192.168.117.3 user=s.osokin password={vgdb_pass}") as conn:
        for link in links['data']:
            if 'type' in link.keys() and link['type'] == 'link':
                #print(link['value'])
                for path, dirs, files in os.walk(os.path.abspath(link['value'])):
                    for filename in fnmatch.filter(files, 'vgdb.vgjson'):
                        filepath = os.path.join(path, filename)
                        process_vgjson(filepath, conn)


# p = getpass.getpass()
# print('Password entered:', p)

# update_links('links_selected.vgjson', input('vgdb password: '))
update_links('links.vgjson', input('vgdb password: '))