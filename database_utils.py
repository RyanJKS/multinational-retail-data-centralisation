import yaml
from sqlalchemy import create_engine, inspect


DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'

# The `DatabaseConnector` class initializes database credentials and provides access to them.
class DatabaseConnector:

    def __init__(self):
        self.db_creds = self.read_db_creds()
        self.RDS_USER = self.db_creds['RDS_USER']
        self.RDS_PASSWORD = self.db_creds['RDS_PASSWORD']
        self.RDS_ENDPOINT = self.db_creds['RDS_HOST']
        self.RDS_PORT = self.db_creds['RDS_PORT']
        self.RDS_DATABASE = self.db_creds['RDS_DATABASE']
        
        self.MY_USER = self.db_creds['MY_USER']
        self.MY_PASSWORD = self.db_creds['MY_PASSWORD']
        self.MY_ENDPOINT = self.db_creds['MY_HOST']
        self.MY_PORT = self.db_creds['MY_PORT']
        self.MY_DATABASE = self.db_creds['MY_DATABASE']
    
    def read_db_creds(self) -> dict:
        """
        The function `read_db_creds` reads the contents of a YAML file containing database credentials and
        returns them as a dictionary.
        :return: a dictionary containing the data read from the 'db_creds.yaml' file.
        """
        with open('db_creds.yaml','r') as file:
            data = yaml.safe_load(file)
        return data
    
    # Create SQLAlchemy engine for either RDS or my database
    def init_db_engine(self,my_db: bool = False) -> create_engine:
        """
        The `init_db_engine` function initializes a database engine based on the provided parameters.
        
        :param my_db: The `my_db` parameter is a boolean flag that indicates whether to use the "my_db"
        configuration or the "RDS" configuration for the database connection. If `my_db` is `True`, the
        "my_db" configuration will be used. Otherwise, the "RDS" configuration, defaults to False
        :type my_db: bool (optional)
        :return: an instance of the `create_engine` class.
        """
        if my_db:
            USER, PASSWORD, ENDPOINT, PORT, DATABASE = self.MY_USER, self.MY_PASSWORD, self.MY_ENDPOINT, self.MY_PORT, self.MY_DATABASE
        else:
            USER, PASSWORD, ENDPOINT, PORT, DATABASE = self.RDS_USER, self.RDS_PASSWORD, self.RDS_ENDPOINT, self.RDS_PORT, self.RDS_DATABASE

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        return engine
    
    def list_db_tables(self) -> list:
        """
        The function `list_db_tables` returns a list of table names in a database.
        :return: a list of table names in the database.
        """
        engine = self.init_db_engine()
        inspector = inspect(engine)

        return inspector.get_table_names()
    
    # Upload to my database!
    def upload_to_db(self, data_frame, table_name: str) -> None:
        """
        The function `upload_to_db` uploads a pandas DataFrame to a specified table in a database using an
        initialized database engine.
        
        :param data_frame: The `data_frame` parameter is a pandas DataFrame object that contains the data
        you want to upload to the database
        :param table_name: The `table_name` parameter is a string that represents the name of the table in
        the database where you want to upload the data frame
        :type table_name: str
        """
        engine = self.init_db_engine(my_db=True)
        
        try:
            with engine.begin() as connection:
              data_frame.to_sql(name=table_name,con=connection)
              print(f'Successfully uploaded {table_name} to "sales_data" database.')
                
        except Exception as e:
            print(f"An error occurred: {e}")


