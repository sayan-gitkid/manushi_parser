import csv
import os

from sqlalchemy import create_engine
import pandas as pd


class DbConn:

    def __init__(self, db, user='root', password='password', host="127.0.0.1", port="3306"):
        self.db = db
        self.engine = create_engine('mysql://{user}:{password}@{host}:{port}/{db}'.format(
            user=user,
            password=password,
            host=host,
            port=port,
            db=db,
        ), echo=False)

    def exec_query(self, query):
        return pd.read_sql_query(query, self.engine)

    def export_csv(self, df, table):
        try:
            os.makedirs('../export/{db}'.format(db=self.db))
        except FileExistsError:
            pass
        df.to_csv('../export/{db}/{table}.csv'.format(table=table,
                                                      db=self.db), index=False, quoting=csv.QUOTE_ALL)

    def export_tables(self, mysql):
        tables = mysql.exec_query("show tables")
        for table in tables['Tables_in_' + self.db].tolist():
            table_data = self.exec_query("select * from {table}".format(table=table))

            self.export_csv(table_data, table)


