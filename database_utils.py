from sqlalchemy import create_engine, inspect
import yaml


DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'

class DatabaseConnector:

    def __init__(self, yaml_file_path='db_creds.yaml'):
        self.db_creds = self.read_db_creds(yaml_file_path)
        self.engine = self.init_db_engine()
    
    def read_db_creds(self, yaml_file_path) -> dict:
        """
        The function `read_db_creds` reads the contents of a YAML file containing database credentials and
        returns them as a dictionary.
        :return: a dictionary containing the data read from the 'db_creds.yaml' file.
        """
        try:
            with open(yaml_file_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise Exception(f"YAML file {yaml_file_path} not found")
        except yaml.YAMLError as e:
            raise Exception(f"Error reading YAML file: {e}")
    
    # Create SQLAlchemy engine for either RDS or my database
    def init_db_engine(self) -> create_engine:
        """
        The `init_db_engine` function initializes a database engine based on the provided parameters.
        
        :param my_db: The `my_db` parameter is a boolean flag that indicates whether to use the "my_db"
        configuration or the "RDS" configuration for the database connection. If `my_db` is `True`, the
        "my_db" configuration will be used. Otherwise, the "RDS" configuration, defaults to False
        :type my_db: bool (optional)
        :return: an instance of the `create_engine` class.
        """
        USER = self.db_creds['USER']
        PASSWORD = self.db_creds['PASSWORD']
        ENDPOINT = self.db_creds['HOST']
        PORT = self.db_creds['PORT']
        DATABASE = self.db_creds['DATABASE']

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
        try:
            with self.engine.begin() as connection:
                data_frame.to_sql(name=table_name, con=connection, if_exists='replace')
                print(f'Successfully uploaded {table_name} to the database.')
        except Exception as e:
            print(f"An error occurred: {e}")


