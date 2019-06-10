from library.dbConnector import DbConn
import pandas as pd
import datetime


def escape_string(string):
    return string.strip().replace('"', '""')


def today_date():
    now = datetime.datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


def units_query(row):
    global ins_prod_unit_query
    query = """insert into product_unit(version,date_created,
                last_updated,unit_code,unit_name) values (0,"{}","{}","{}", "{}");\n""".format(
        today_date(),
        today_date(),
        escape_string(row['unit_name']),
        escape_string(row['unit_name']))
    ins_prod_unit_query += query


def cat_query(row):
    global ins_cat_query
    query = """insert into product_category(version,date_created,
                last_updated,category_code,name) values (0,"{}","{}","{}", "{}");\n""".format(
        today_date(),
        today_date(),
        escape_string(row['category_name']).replace(" ", "-").lower(),
        escape_string(row['category_name']))
    ins_cat_query += query


def prod_query(row, none_db_id):
    global ins_sample_prod_query
    try:
        desc = row['description'].replace("'", "''").strip().replace('"', '""')
    except:
        desc = ""

    if str(row['id_old_cat_old_id']) != 'nan':
        new_unit_id = row['id_old_cat_old_id']
    else:
        new_unit_id = none_db_id

    query = """insert into sample_product(item_no, version, category_id,
                date_created, description, image_url, last_updated, name, unit_id) 
                VALUES ("{}", 0, {},"{}","{}","{}","{}","{}",{} );\n""".format(
        escape_string(row['item_no']),
        row['id_new_cat_id'],
        today_date(),
        desc,
        escape_string(row['image_url']),
        today_date(),
        row['name_old_cat_old_id'],
        new_unit_id

    )
    ins_sample_prod_query += query


def producer_query(row):
    global ins_producer_query
    query = """insert into producer(version, address, code, contact_info, date_created, description, last_updated, name)
                VALUES (0,"{}","{}","{}","{}","{}","{}","{}");\n""".format(
        escape_string(row['address']),
        escape_string(row['producer_name']).replace(" ", "-").lower(),
        escape_string(row['contact_no']),
        today_date(),
        escape_string(row['producer_desc']),
        today_date(),
        escape_string(row['producer_name'])
    )
    ins_producer_query += query


def client_query(row):
    global ins_client_query
    query = """insert into client(version, billing_address, billing_address_same_as_shipping_address, 
                country, date_created, description, last_updated, name, shipping_address)
                VALUES (0,"{}",{},"{}","{}","{}","{}","{}", {});\n""".format(
        escape_string(row['address']),
        1,
        "",
        today_date(),
        escape_string(row['client_desc']).strip(),
        today_date(),
        escape_string(row['client_name']),
        "null"
    )
    ins_client_query += query


def migrate_units(old_db, new_db):
    old_units = old_db.exec_query("select * from product_unit")
    new_units = new_db.exec_query("select * from product_unit")
    old_unit_name = old_units['unit_name'].tolist()
    new_unit_name = new_units['unit_name'].tolist()

    unit_to_insert = list(set(old_unit_name) - set(new_unit_name))
    insert_df = old_units[old_units['unit_name'].isin(unit_to_insert)].copy()
    insert_df.apply(lambda row: units_query(row), axis=1)

    with open('/home/sshakya/PycharmProjects/manushi_parser/query/product_units.sql', 'w') as w:
        print(dir(w))
        w.writelines(ins_prod_unit_query)


def migrate_category(old_db, new_db):
    old_categories = old_db.exec_query("select * from product_category;")
    new_categories = new_db.exec_query("select * from product_category;")
    old_category = old_categories['category_name']
    new_category = new_categories['name']
    old_category = map(lambda x: x.strip(), old_category)
    new_category = map(lambda x: x.strip(), new_category)
    cat_insert = list(set(old_category) - set(new_category))
    old_categories = old_categories[old_categories['category_name'].isin(cat_insert)]
    old_categories.apply(lambda row: cat_query(row), axis=1)
    with open('/home/sshakya/PycharmProjects/manushi_parser/query/prod_cat.sql', 'w') as w:
        w.writelines(ins_cat_query)


def migrate_product(old_db, new_db):
    old_prod_df = old_db.exec_query("select * from product;")
    new_prod_df = new_db.exec_query("select * from sample_product;")
    old_unit_df = old_db.exec_query("select * from product_unit")
    old_category = old_db.exec_query("select * from product_category")
    new_unit_df = new_db.exec_query("select * from product_unit;")
    new_cat_df = new_db.exec_query("select * from product_category")
    none_id = new_unit_df[new_unit_df['unit_code'] == 'none']['id'].tolist()
    if len(none_id) != 1:
        print("Insert None unit in product_unit")
        exit(0)
    else:
        none_db_id = none_id[0]
    old_prod_df = pd.merge(old_prod_df, old_unit_df, how="left", left_on="unit", right_on="id",
                           suffixes=('_old_unit_left', '_old_unit_right'))
    old_prod_df = pd.merge(old_prod_df, old_category, how="left", left_on="category", right_on="id",
                           suffixes=('_old_cat_left', '_old_cat_right'))
    old_prod_df = pd.merge(old_prod_df, new_unit_df, how="left", left_on="unit_name", right_on="unit_name",
                           suffixes=('_old_unit_old_id', '_new_unit_id'))
    old_prod_df = pd.merge(old_prod_df, new_cat_df, how="left", left_on="category_name", right_on="name",
                           suffixes=('_old_cat_old_id', '_new_cat_id'))
    old_prod = old_prod_df['item_no']
    new_prod = new_prod_df['item_no']
    old_prod = map(lambda x: x.strip(), old_prod)
    new_prod = map(lambda x: x.strip(), new_prod)
    prod_ins = list(set(old_prod) - set(new_prod))
    old_prod_df = old_prod_df[old_prod_df['item_no'].isin(prod_ins)]
    old_prod_df.apply(prod_query, axis=1, args=([none_db_id]))

    with open('/home/sshakya/PycharmProjects/manushi_parser/query/sample_products.sql', 'w') as w:
        print(dir(w))
        w.writelines(ins_sample_prod_query)


def migrate_producers(old_db, new_db):
    old_producer = old_db.exec_query("select * from producer")
    new_producer = new_db.exec_query("select * from producer")
    old_producer_name = old_producer['producer_name'].tolist()
    new_producer_name = new_producer['name'].tolist()

    producer_to_insert = list(set(old_producer_name) - set(new_producer_name))
    old_producer = old_producer[old_producer['producer_name'].isin(producer_to_insert)].copy()
    old_producer.apply(lambda row: producer_query(row), axis=1)

    with open('/home/sshakya/PycharmProjects/manushi_parser/query/producer.sql', 'w') as w:
        print(dir(w))
        w.writelines(ins_producer_query)


def migrate_client(old_db, new_db):
    old_client = old_db.exec_query("select * from client")
    new_client = new_db.exec_query("select * from client")
    old_client_name = old_client['client_name'].tolist()
    new_client_name = new_client['name'].tolist()

    client_to_insert = list(set(old_client_name) - set(new_client_name))
    old_client = old_client[old_client['client_name'].isin(client_to_insert)].copy()
    old_client.apply(lambda row: client_query(row), axis=1)

    with open('/home/sshakya/PycharmProjects/manushi_parser/query/client.sql', 'w') as w:
        print(dir(w))
        w.writelines(ins_client_query)


def migrate_batch_entry(old_db, new_db):
    # prod_entry to warehouse_inventory
    # bill_code gen with id
    #     producer_id
    #     bill total null
    old_prod_entry = old_db.exec_query("select * from product_entry")
    old_producer = old_db.exec_query("select * from producer")
    old_prod_entry['batch_code'] = old_prod_entry['id'].astype('str') + '__' + old_prod_entry['product'].astype('str')
    old_prod_entry = pd.merge(old_prod_entry,old_producer, how="left", left_on="producer", right_on="id")

    batch_entry = new_db.exec_query("select * from batch_entry")
    new_producer = new_db.exec_query("select * from producer")
    old_prod_entry = pd.merge(old_prod_entry, new_producer, how="left", left_on="producer_name", right_on="name")
    new_sample_prod = new_db.exec_query("select * from sample_product")
    old_prod_entry = pd.merge(old_prod_entry, new_sample_prod, how="left", left_on="product", right_on="item_no")

    print(old_prod_entry.shape)
    print(old_prod_entry)
    asd = 10


def main():
    global ins_prod_unit_query
    global ins_cat_query
    global ins_sample_prod_query
    global ins_producer_query
    global ins_client_query
    ins_prod_unit_query = ""
    ins_cat_query = ""
    ins_sample_prod_query = ""
    ins_producer_query = ""
    ins_client_query = ""

    mysql = DbConn(db='db_manushi_old',
                   user='root',
                   password='password')

    to_db = DbConn(db='db_manushi_new_migrate',
                   user='root',
                   password='password')
    # tables = mysql.exec_query("show tables")
    # mysql.export_tables(mysql)
    migrate_units(mysql, to_db)
    migrate_category(mysql, to_db)

    to_db.exec_file_sql("/home/sshakya/PycharmProjects/manushi_parser/query/product_units.sql")
    to_db.exec_file_sql("/home/sshakya/PycharmProjects/manushi_parser/query/prod_cat.sql")

    migrate_product(mysql, to_db)
    to_db.exec_file_sql("/home/sshakya/PycharmProjects/manushi_parser/query/sample_products.sql")

    migrate_producers(mysql, to_db)
    to_db.exec_file_sql("/home/sshakya/PycharmProjects/manushi_parser/query/producer.sql")

    migrate_client(mysql, to_db)
    to_db.exec_file_sql("/home/sshakya/PycharmProjects/manushi_parser/query/client.sql")

    migrate_batch_entry(mysql, to_db)

    asdf = 10


if __name__ == "__main__":
    main()
