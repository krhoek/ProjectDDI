from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import yaml

import pandas as pd

# Please pay attention to username & trusted_connection & driver

# Load config file
with open(r'Projectdatabase/config.yaml') as file:
    config = yaml.full_load(file)
    organization = config.get('organization')
    db_org = config.get('database_org') # configurations about organizations database
    db_pub = config.get('database_pub') # configurations about public database


# create engines to connect to databases
connection_string = f"{db_org.get('dialect')}://{db_org.get('username')}:{db_org.get('password')}@{str(db_org.get('server'))}:{str(db_org.get('port'))}/{db_org.get('database')}"
if db_org.get('dialect') == 'mssql':
    connection_string = connection_string + f"?driver={db_org.get('driver')}&trusted_connection={db_org.get('trusted_connection')}"

engine = create_engine(connection_string)
session = Session(engine)

data = pd.read_sql_query("Select top 5 * from  products", session.bind)
print(data)

