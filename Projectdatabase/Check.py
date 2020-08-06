#Controleren op aanwezige tabellen en inhoud van tabellen in de publieke database

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.inspection import inspect

import pandas as pd

DATABASE_CONNECTION = f'mssql+pyodbc://ddi.sa.shared:K&9*2hZqxE63=uU;@eiklsfzhfc.database.windows.net:1433/clientjourney?driver=ODBC Driver 17 for SQL Server'

engine = create_engine(DATABASE_CONNECTION,echo = True)
session = Session(engine)

print(engine.table_names())

data3 = pd.read_sql_query("Select * from  organisatie", session.bind)
print(data3)
