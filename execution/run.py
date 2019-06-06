from library.dbConnector import DbConn


def export_table():
    pass


def main():
    mysql = DbConn(db='db_manushi_old',
                   user='root',
                   password='password')
    tables = mysql.exec_query("show tables")
    mysql.export_tables(mysql)

    asdf = 10


if __name__ == "__main__":
    main()
