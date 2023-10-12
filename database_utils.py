import yaml
from sqlalchemy import create_engine, inspect


DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'

class DatabaseConnector:
    
    def read_db_creds(self) -> dict:
        with open('db_creds.yaml','r') as file:
            data = yaml.safe_load(file)
        return data
    
    def init_db_engine(self) -> create_engine:
        db_creds = self.read_db_creds()
        USER = db_creds['RDS_USER']
        PASSWORD = db_creds['RDS_PASSWORD']
        ENDPOINT = db_creds['RDS_HOST']
        PORT = db_creds['RDS_PORT']
        DATABASE = db_creds['RDS_DATABASE']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        return engine
    
    def list_db_tables(self) -> list:
        engine = self.init_db_engine()
        inspector = inspect(engine)

        return inspector.get_table_names()
    
    # Upload to my database!
    def upload_to_db(self, data_frame, table_name: str) -> None:
        my_db_creds = self.read_db_creds()
        USER = my_db_creds['MY_USER']
        PASSWORD = my_db_creds['MY_PASSWORD']
        ENDPOINT = my_db_creds['MY_HOST']
        PORT = my_db_creds['MY_PORT']
        DATABASE = my_db_creds['MY_DATABASE']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        
        try:
            with engine.begin() as connection:
              data_frame.to_sql(name=table_name,con=connection)
              print(f'Successfully uploaded {table_name} to "sales_data" database.')
                
        except Exception as e:
            print(f"An error occurred: {e}")

    # Connection to my PostgreSQL database
    def connect_my_db(self) -> create_engine:
        my_db_creds = self.read_db_creds()
        USER = my_db_creds['MY_USER']
        PASSWORD = my_db_creds['MY_PASSWORD']
        ENDPOINT = my_db_creds['MY_HOST']
        PORT = my_db_creds['MY_PORT']
        DATABASE = my_db_creds['MY_DATABASE']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        
        return engine


