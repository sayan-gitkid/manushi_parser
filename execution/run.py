from library.dbConnector import DbConn
import datetime


def today_date():
    now = datetime.datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


def migrate_units(old_db, new_db):
    old_units = old_db.exec_query("select * from product_unit")
    new_units = new_db.exec_query("select * from product_unit")
    print(today_date())


def main():
    mysql = DbConn(db='db_manushi_old',
                   user='root',
                   password='password')

    to_db = DbConn(db='manushi',
                   user='root',
                   password='password')
    # tables = mysql.exec_query("show tables")
    # mysql.export_tables(mysql)
    migrate_units(mysql, to_db)

    asdf = 10


if __name__ == "__main__":
    main()
