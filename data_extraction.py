import pandas as pd
import tabula
import requests
import json
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import io


class DataExtractor:
    """
    The `DataExtractor` class is designed to extract data from RDS tables, PDF files, API endpoints, and S3 buckets.
    """
    @staticmethod
    def read_rds_table(database_connector, table_name: str) -> pd.DataFrame:

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
        # Extract tables from all pages of the PDF
        df = tabula.read_pdf(link, pages="all", multiple_tables=True)
        # Combine all the tables into a single DataFrame
        concatenated_df = pd.concat(df, ignore_index=True)
     
        return concatenated_df
    
    @staticmethod
    def list_number_of_stores(stores_endpoint: str, header_dict: dict) -> int:
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
