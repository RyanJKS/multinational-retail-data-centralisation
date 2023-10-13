import pandas as pd
import re
import numpy as np


class DataCleaning:
    """  
    Methods:

    clean_user_data(data: pd.DataFrame) -> pd.DataFrame:
    clean_card_data(data: pd.DataFrame) -> pd.DataFrame:
    clean_store_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    convert_product_weights(dataframe: pd.DataFrame) -> pd.DataFrame:
    clean_orders_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    clean_date_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """

    # Change date structure by passing through different format
    def restructure_date(self, date_input):
        # Date formats available in dataset - Year-Month-Date, Month(Name)-Date-Year(Abrev), Year-Month(Name)-Date, Month(Name)-Year-Date
        date_formats = {'%Y-%m-%d', '%B:%d:%y', '%Y:%B:%d', '%B:%Y:%d'}

        for date_format in date_formats:
            try:
                return pd.to_datetime(date_input, format=date_format)
            except ValueError:
                pass

        return date_input 


    def clean_user_data(self, data:pd.DataFrame) -> pd.DataFrame:
        """
        Perform cleaning on user data. Includes handling of NULLs, name and date validation,
        email validation, address formatting, country code correction, and phone number formatting.
        """
        df = data.copy()

        # Drop duplicates
        df = df.drop_duplicates()

        # Drop rows containing NULL or NaN values across any column
        df = df.dropna()
        
        # Ensure first names are not NULL
        df = df[df['first_name'] != 'NULL']
        
        # Find all the dates rows that will not convert properly for cleaning
        invalid_date_mask = pd.to_datetime(df['date_of_birth'], errors='coerce').isna()
        # print(df[invalid_date_mask])

        #Check for error in dates (DOB and join_Date) by first changing data type and drop null rows
        df['date_of_birth'] = df['date_of_birth'].astype(str).str.strip()
        df['date_of_birth'] = df['date_of_birth'].apply(self.restructure_date)
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')

        df['join_date'] = df['join_date'].astype(str).str.strip()
        df['join_date'] = df['join_date'].apply(self.restructure_date)
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

        df = df.dropna(subset=["date_of_birth", "join_date"])

        # Ensure the 'company' column is of string type
        df['company'] = df['company'].astype(str)

        #print(df[df['user_uuid'].str.contains('7d33239a-5d5b-40d1-98ae-5fe50')])

        #Check address format and remove unwanted characters
        df['address'] = df['address'].astype(str).str.strip()
        df['address'] = df['address'].replace('\n',', ', regex=True)

        # Fix erroneous 'country_code' and convert column to string type
        df['country'] = df['country'].astype(str).str.strip()
        df['country_code'] = df['country_code'].astype(str).str.strip()
        df['country_code'] = df['country_code'].replace('GGB','GB', regex=True)

        # Drop null values, fix phone number format and drop unwanted values
        def clean_phone_number(df):
            country_code_dict = {'DE': '+49', 'GB': '+44', 'US': '+1'}
            
            original_number = df['phone_number']
            country_code = country_code_dict.get(df['country_code'])
            
            # Keeping only "+" and digits
            cleaned_number = re.sub(r'[^\d\+]', '', original_number)
            
            # Check and append country code if necessary
            if country_code and not cleaned_number.startswith(country_code):
                cleaned_number = country_code + cleaned_number
            
            return cleaned_number
        
        df['phone_number'] = df['phone_number'].astype(str).str.strip()
        df['phone_number'] = df.apply(clean_phone_number, axis=1)
        df = df.dropna(subset=['phone_number'])

        return df
    

    def clean_card_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Perform cleaning on card data. Includes handling NULL values, deduplication, 
        whitespace trimming in card numbers, and validation of card expiration and payment dates.
        """
        df = data.copy()

        # Drop NULL values
        df = df.dropna()

        # Remove duplicates
        df = df.drop_duplicates()

        # Remove any whitespaces in card_number and non-numerical values
        df['card_number'] = df['card_number'].astype(str).str.strip()
        df['card_number'] = df['card_number'].str.replace(r'\D+', '')
        df['card_number'] = df['card_number'].str.replace('?', '')

        # Check date_payment_confirmed for null values and drop
        df['date_payment_confirmed'] = df['date_payment_confirmed'].astype(str).str.strip()
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(self.restructure_date)

        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        
        df = df.dropna(subset=['date_payment_confirmed'])
        
        # Get only card provider name
        df['card_provider'] = df['card_provider'].apply(lambda x: x.split()[0])
        df['card_provider'] = df['card_provider'].astype('category')

        return df
    

    def clean_store_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Perform cleaning on store data. Operations include dropping certain columns,
        handling and correcting datatypes, formatting address strings, and categorical 
        data management.
        """
        df = data.copy()

        # Drop duplicates
        df = df.drop_duplicates()

        # Capitalise for data consistency
        df['store_code'] = df['store_code'].astype(str).str.strip()
        df['store_code'] = df['store_code'].apply(lambda x: x.upper())

        # Dropping rows where 'country_code' is not 'GB', 'US', or 'DE'
        df = df.drop(df[~df['country_code'].isin(['GB', 'US', 'DE'])].index)

        # Check address format and errors
        df['address'] = df['address'].astype(str).str.strip()
        df['address'] = df['address'].replace('\n',', ',regex=True)

        df['locality'] = df['locality'].astype(str).str.strip()

        df['store_type'] = df['store_type'].astype(str).str.strip()

        df['staff_numbers'] = df['staff_numbers'].astype(str).str.strip()
        df['staff_numbers'] = df['staff_numbers'].replace('[^0-9]', '', regex=True)

        # Convert longitude and latitude to numeric, replacing non-numeric values with NaN
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')

        # Find rows where 'continent' starts with "ee" and remove "ee"
        df['continent'] = df['continent'].astype(str).str.strip()
        df['continent'] = df['continent'].apply(lambda x: x.replace("ee", "") if x.startswith("ee") else x)

        # Check opening_date for null values and drop
        df['opening_date'] = df['opening_date'].astype(str).str.strip()
        df['opening_date'] = df['opening_date'].apply(self.restructure_date)

        # Convert 'opening_date' column to datetime
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')

         # Drop rows where there is NaT or NaN
        df = df.dropna(subset=['opening_date'])

        return df

    @staticmethod
    def convert_product_weights(data: pd.DataFrame) ->pd.DataFrame:
        """
        Converts the product weights to a standard unit (kg), handling various formats and units
        present in the input data.
        """
        df = data.copy()

        # Remove rows with NULL, unused column 'Unnamed: 0' and drop duplicates
        df = df.dropna()
        df = df.drop(['Unnamed: 0'], axis=1)

        df = df.drop_duplicates()

        # Capitalise for data consistency
        df['product_code'] = df['product_code'].astype(str).str.strip()
        df['product_code'] = df['product_code'].apply(lambda x: x.upper())

        def convert_to_kg(weight):
            # Base conversion ratio from dictionary key to kg - approximate oz
            conversion_dict = {'kg':1,'g':10**(-3),'ml':10**(-3), 'oz':0.03}

            # Remove any unwanted characters
            weight = weight.lower().replace(' .','')

            for key, value in conversion_dict.items():
                
                if key in weight:
                    try:
                        # Check if format is '1 x 100ml'
                        if 'x' in weight:
                            weight = weight.strip(key)
                            new = weight.split('x')
                            return float(new[0]) * float(new[1]) * value

                        else:
                            weight = float(weight.strip(key))
                            return weight*value
                    except ValueError:
                        # print(f'Could not change this value: {weight} to a float')
                        return None
            return None
            
        # Applying the function to convert weights
        df['weight'] = df['weight'].apply(convert_to_kg)

        # Drop NaN values (if you wish to remove rows where weight couldn't be determined)
        df = df.dropna(subset=['weight'])

        # Convert the 'weight' column to float
        df['weight'] = df['weight'].astype(float)

        return df

    @staticmethod
    def clean_orders_data(data: pd.DataFrame) -> pd.DataFrame:
        """
        Perform cleaning on orders data. Mainly involves dropping specified columns.
        """
        df = data.copy()

        df = df.drop_duplicates()

        # Drop columns that are not needed for further analysis or are causing issues
        df = df.drop(['level_0','first_name','last_name','1'], axis=1)
        df['card_number'] = df['card_number'].astype(str).str.strip()
        df['store_code'] = df['store_code'].astype(str).str.strip()
        
        # Capitalise for data consistency
        df['product_code'] = df['product_code'].astype(str).str.strip()
        df['product_code'] = df['product_code'].apply(lambda x: x.upper())

        return df
    
    @staticmethod
    def clean_date_data(data: pd.DataFrame) -> pd.DataFrame:
        """
        Perform cleaning on date data. Involves combining separated date and time columns
        into a single datetime column and removing the original columns.
        """
        df = data.copy()

        # Drop duplicates
        df = df.drop_duplicates()

        # Convert 'timestamp' column to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S',errors='coerce')

        # Drop erroneous rows
        df = df.dropna(subset=['timestamp'])

        # Extract the time component only
        df['timestamp'] = df['timestamp'].dt.time

        return df
 