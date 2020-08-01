#Verwijderen van tabellen in de publieke database. Door de tabellen in een aparte regel te benoemen worden deze verwijderd.

from sqlalchemy import create_engine,MetaData, Table, Column, Integer, String, Boolean, DateTime
from sqlalchemy.orm import Session
import pandas as pd
meta = MetaData()

DATABASE_CONNECTION = f'mssql+pyodbc://ddi.sa.shared:K&9*2hZqxE63=uU;@eiklsfzhfc.database.windows.net:1433/clientjourney?driver=ODBC Driver 17 for SQL Server'

engine = create_engine(DATABASE_CONNECTION,echo = True)
session = Session(engine)

# Delete multiple tables
clienten = Table('clienten', meta)
producten = Table('producten', meta)
burgerlijke_status = Table('burgerlijke_status', meta)
uitstroom = Table('uitstroom', meta)
organisatie = Table('organisatie', meta)
type_touchpoint = Table('type_touchpoint', meta)
touchpoint = Table('touchpoint', meta)
product_id_vertaling = Table('product_id_vertaling', meta)

meta.drop_all(engine)