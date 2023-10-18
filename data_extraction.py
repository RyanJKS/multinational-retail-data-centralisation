import pandas as pd
import tabula
import requests
import json
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import io


# The `DataExtractor` class provides static methods for reading tables from a database, extracting data from PDF files, retrieving data from API endpoints, and extracting data from S3 buckets.
class DataExtractor:

    @staticmethod
    def read_rds_table(database_connector, table_name: str) -> pd.DataFrame:
        """
        The function `read_rds_table` reads a table from a database using a given database connector and
        returns the data in a pandas DataFrame format.
        
        :param database_connector: The `database_connector` parameter is an object that provides methods for
        connecting to and interacting with a database. It contains information such as the database
        connection string, username, password, and other necessary details
        :param table_name: a string that specifies the name of the table in the database that you want to read
        :type table_name: str
        :return: a pandas DataFrame.
        """

        # Initialize database engine
        engine = database_connector.init_db_engine()

        try:
            with engine.begin():
                # Read the data and return it in datafram format
                df = pd.read_sql_table(table_name, engine)
                return df

        except Exception as e:
            print(f"An error occurred: {e}")

    @staticmethod
    def retrieve_pdf_data(link: str) -> pd.DataFrame:
        """
        The function retrieves data from a PDF file by extracting tables from all pages and combining
        them into a single DataFrame.
        
        :param link: The link parameter is a string that represents the URL or file path of the PDF file
        from which you want to extract data
        :type link: str
        :return: a pandas DataFrame that contains the data extracted from the PDF.
        """
        # Extract tables from all pages of the PDF
        df = tabula.read_pdf(link, pages="all", multiple_tables=True)
        # Combine all the tables into a single DataFrame
        concatenated_df = pd.concat(df, ignore_index=True)
     
        return concatenated_df
    
    @staticmethod
    def list_number_of_stores(stores_endpoint: str, header_dict: dict) -> int:
        """
        The function `list_number_of_stores` makes a GET request to a specified endpoint with headers,
        retrieves the response data as JSON, and returns the number of stores from the response data.
        
        :param stores_endpoint: The `stores_endpoint` parameter is a string that represents the URL
        endpoint for retrieving the number of stores. It is the endpoint where the API is located that
        provides the number of stores
        :type stores_endpoint: str
        :param header_dict: The `header_dict` parameter is a dictionary that contains the headers to be
        included in the request. Headers are used to provide additional information to the server, such as
        authentication credentials or content type
        :type header_dict: dict
        :return: the number of stores from the response data.
        """
        response = requests.get(stores_endpoint, headers=header_dict)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Access the response data as JSON
            data = response.json()
            return data['number_stores']
                
        # If the request was not successful, print the status code and response text
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")

    @staticmethod
    def retrieve_stores_data(url_endpoint: str, header_dict: dict, total_stores: int) -> pd.DataFrame:
        """
        The function `retrieve_stores_data` retrieves data from a specified URL endpoint for a given number
        of stores and returns a concatenated dataframe of all the retrieved data.
        
        :param url_endpoint: The URL endpoint where the store data is retrieved from
        :type url_endpoint: str
        :param header_dict: The `header_dict` parameter is a dictionary that contains the headers to be
        included in the HTTP request. 
        :type header_dict: dict
        :param total_stores: The total number of stores you want to retrieve data for
        :type total_stores: int
        :return: a pandas DataFrame that contains the data retrieved from the specified URL endpoint for
        each store.
        """

        all_dataframe = []
        # Iterate through each store number
        for store_number in range(0,total_stores):
            complete_endpoint = url_endpoint + str(store_number)

            response = requests.get(complete_endpoint, headers=header_dict)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Access the response data as JSON
                data = response.json()
                all_dataframe.append(pd.DataFrame([data]))
              
            # If the request was not successful, print the status code and response text
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")

        # Concatenate all dataframes into one
        complete_dataframe = pd.concat(all_dataframe,ignore_index=True)

        return complete_dataframe
    
    @staticmethod
    def extract_from_s3(s3_address: str, data_type: str) -> pd.DataFrame:
        """
        The function `extract_from_s3` extracts data from an S3 bucket and returns it as a Pandas DataFrame,
        supporting both CSV and JSON file types.
        
        :param s3_address: The `s3_address` parameter is a string that represents the address of the file in
        the S3 bucket. It should follow the format `s3://bucket_name/file_path`
        :type s3_address: str
        :param data_type: The `data_type` parameter specifies the type of data to be extracted from the S3
        bucket. It can have two possible values: 'csv' or 'json'
        :type data_type: str
        :return: The function `extract_from_s3` returns a Pandas DataFrame containing the data extracted
        from the specified S3 address.
        """
        try:
            s3 = boto3.client('s3')

            # Split address to get bucket and key information
            s3_address_parts = s3_address.split('/')

            # Extract and read CSV data from S3
            if data_type == 'csv':
                bucket_name = s3_address_parts[2]
                object_key = '/'.join(s3_address_parts[3:])

                response = s3.get_object(Bucket=bucket_name, Key=object_key)
                content = response['Body'].read()
        
                # Create a Pandas DataFrame from the CSV data
                df = pd.read_csv(io.BytesIO(content))
                return df
            
            # Extract and read JSON data from S3
            elif data_type == 'json':
                bucket_name = s3_address_parts[2].split(".")[0]
                object_key = s3_address_parts[3]
                
                response = s3.get_object(Bucket=bucket_name, Key=object_key)
                content = response['Body'].read()
                
                df = pd.read_json(io.BytesIO(content))
                return df
            else:
                print("Specify the type of file being read")

        # Handle exceptions related to AWS credentials and S3 access    
        except NoCredentialsError:
            print("AWS credentials not found. Please configure your credentials.")

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist.")
            else:
                print("An error occurred:", e)
