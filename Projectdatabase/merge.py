from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.inspection import inspect
import seaborn as sns
import pandas as pd
import plotly
import plotly.graph_objects as go
import chart_studio.plotly as py

DATABASE_CONNECTION = f'mssql+pyodbc://ddi.sa.shared:K&9*2hZqxE63=uU;@eiklsfzhfc.database.windows.net:1433/clientjourney?driver=ODBC Driver 17 for SQL Server'

engine = create_engine(DATABASE_CONNECTION,echo = True)
session = Session(engine)

# Select producten and touchpoints
data_producten = pd.read_sql_query("Select product_id, producttype from producten", session.bind)
data_touchpoints = pd.read_sql_query("Select client_id, target_id, touchpoint_tijd from  touchpoint Where touchpoint.type_touchpoint_id = 'tt_1'" , session.bind)

# Rename column product_id to target_id
data_producten.rename(columns={'product_id': 'target_id'}, inplace=True)

# Merge producten and touchpoints
data = pd.merge(data_producten,data_touchpoints, on ='target_id', how = 'inner')

# Drop column target_id
data.drop(columns='target_id', axis = 1, inplace = True)

# Rename column producttype to target_id
data.rename(columns={'producttype': 'target_id'}, inplace=True)

print(data)