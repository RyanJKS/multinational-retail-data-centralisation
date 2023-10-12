import data_cleaning
import data_extraction
import database_utils
import database_schema


#Create instance of main class
db_connector = database_utils.DatabaseConnector()
db_extractor = data_extraction.DataExtractor()
data_cleaner = data_cleaning.DataCleaning()

# Extract
table_list = db_connector.list_db_tables()
legacy_users = table_list[1]

user_data = db_extractor.read_rds_table(db_connector, legacy_users)

# Clean
cleaned_data = data_cleaner.clean_user_data(user_data)

# Upload
db_connector.upload_to_db(cleaned_data,'dim_users')

####################### Reading PDF from link ##########################################

path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

# Extract
extract_pdf_data = db_extractor.retrieve_pdf_data(path)

# Clean
clean_pdf_data = data_cleaner.clean_card_data(extract_pdf_data)

# Upload
db_connector.upload_to_db(clean_pdf_data,"dim_card_details")

###################### API #########################################

headers = {
    'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
}

number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

number_of_stores = db_extractor.list_number_of_stores(number_of_stores_endpoint,headers)

retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

# Extract
store_data = db_extractor.retrieve_stores_data(retrieve_store_endpoint,headers,number_of_stores)

# Clean
cleaned_store_data = data_cleaner.clean_store_data(store_data)

# Upload
db_connector.upload_to_db(cleaned_store_data,"dim_store_details")

###########################  S3 Buckets ##################################

s3_uri = 's3://data-handling-public/products.csv'

# Extract
products = db_extractor.extract_from_s3(s3_uri,'csv')

# Clean
converted_weights = data_cleaner.convert_product_weights(products)

# Upload
db_connector.upload_to_db(converted_weights,"dim_products")

############################### RDS ##########################################

orders_table = table_list[2]

# Extract
orders_data = db_extractor.read_rds_table(db_connector,orders_table)

# Clean
cleaned_orders = data_cleaner.clean_orders_data(orders_data)

# Upload
db_connector.upload_to_db(cleaned_orders,"orders_table")

################################# JSON FILE #####################################

s3_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

# Extract
date_times = db_extractor.extract_from_s3(s3_url,'json')

# Clean
cleaned_date_times = data_cleaner.clean_date_data(date_times)

# Upload
db_connector.upload_to_db(cleaned_date_times,"dim_date_times")

# ################# MILESTONE 3 - STAR BASED DATABASE SCHEMA ###################################

engine = db_connector.connect_my_db()

database_schema.execute_db_operations(engine)
