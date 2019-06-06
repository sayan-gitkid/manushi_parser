from sqlalchemy import create_engine
import pandas as pd


engine = create_engine('mysql://root:password@127.0.01:3306/db_manushi_old', echo=False)
f = pd.read_sql_query('show  tables', engine)
asd = 10

