from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import pandas as pd

# Please pay attention to username & trusted_connection & driver

username=r'khoek@cardia.nl'
password='20@)qfsiMA'
server='NLVECA-SQL-02.cardia.local'
port='1433'
database='OnsDB'
dialect='mssql'
driver='ODBC Driver 17 for SQL Server'

# create engines to connect to databases
DATABASE_CONNECTION = f"{dialect}://{username}:{password}@{server}:{port}/{database}?driver={driver}&trusted_connection=yes"

print('----------------------------------------------------------')
print('----------------------------------------------------------')
print(DATABASE_CONNECTION)

engine = create_engine(DATABASE_CONNECTION, echo = True)
session = Session(engine)

data = pd.read_sql_query("Select top 5 * from  products", session.bind)
print(data)

