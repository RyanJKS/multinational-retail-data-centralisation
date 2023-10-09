import pandas as pd
from validate_email import validate_email
import re


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
    @staticmethod
    def clean_user_data(data:pd.DataFrame) -> pd.DataFrame:
        """
        Perform cleaning on user data. Includes handling of NULLs, name and date validation,
        email validation, address formatting, country code correction, and phone number formatting.
        """
        df = data.copy()

        # Drop rows containing NULL or NaN values across any column
        df = df.dropna()
        
        # Ensure first names are not NULL
        df = df[df['first_name'] != 'NULL']

        #Check for error in dates (DOB and join_Date) by first changing data type and drop null rows
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce', format='%Y-%m-%d')
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce', format='%Y-%m-%d')
        df = df.dropna(subset=["date_of_birth", "join_date"])

        # Ensure the 'company' column is of string type
        df['company'] = df['company'].astype(str)

        #Check if email address is valid
        valid_email = df['email_address'].apply(lambda x:validate_email(x))
        df = df[valid_email]

        #Check address format and remove unwanted characters
        df['address'] = df['address'].astype(str)
        df['address'] = df['address'].str.strip()
        df['address'] = df['address'].replace('\n',', ', regex=True)

        # Fix erroneous 'country_code' and convert column to categorical type
        df['country_code'] = df['country_code'].replace('GGB','GB', regex=True)
        df['country'] = df['country'].astype('category')
        df['country_code'] = df['country_code'].astype('category')

        # Drop null values, fix phone number format and drop unwanted values
        df = df.dropna(subset=["phone_number"])   

        def clean_phone_number(df):
            country_code_dict = {'DE': '+49', 'GB': '+44', 'US': '+1'}
            valid_length_dict = {'DE': 12, 'GB': 13, 'US': 12}
            
            original_number = str(df['phone_number'])
            country_code = country_code_dict.get(df['country_code'])
            
            # Keeping "+" and digits
            cleaned_number = re.sub(r'[^\d\+]', '', original_number)
            
            # Check and append country code if necessary
            if country_code and not cleaned_number.startswith(country_code):
                cleaned_number = country_code + cleaned_number
            
            valid_length = valid_length_dict.get(df['country_code'], 13)  # default to 13 if country code is not in dict

            # Check length and return 'NULL' if invalid
            if len(cleaned_number) != valid_length:
                return None
            
            return cleaned_number
        
        df['phone_number'] = df.apply(clean_phone_number, axis=1)
        df = df[df['phone_number'] != 'NULL']
        
        return df
    
    @staticmethod
    def clean_card_data(data: pd.DataFrame) -> pd.DataFrame:
        """
        Perform cleaning on card data. Includes handling NULL values, deduplication, 
        whitespace trimming in card numbers, and validation of card expiration and payment dates.
        """
        # Remove rows containing any NULL values
        df = data.dropna()
        
        # Remove duplicates
        df = df.drop_duplicates()

        # Remove any whitespaces in card_number
        df['card_number'] = df['card_number'].astype(str)
        df['card_number'] = df['card_number'].str.replace(r'\s+', '', regex=True)

        # Check if card was valid before date_payment_confirmed
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        invalid_payments = df.apply(lambda x: x['expiry_date'] >= x['date_payment_confirmed'], axis=1)
        df = df.loc[invalid_payments]
        
        # Drop rows where 'date_payment_confirmed' is NaN or NaT
        df = df.dropna(subset=['date_payment_confirmed'])
        
        # Get only card provider name
        df['card_provider'] = df['card_provider'].apply(lambda x: x.split()[0])
        df['card_provider'] = df['card_provider'].astype('category')

        return df
    
    @staticmethod
    def clean_store_data(data:pd.DataFrame) -> pd.DataFrame:
        """
        Perform cleaning on store data. Operations include dropping certain columns,
        handling and correcting datatypes, formatting address strings, and categorical 
        data management.
        """
        df = data.copy()

        # Drop column "lat" as not useful
        df = df.drop(["lat"], axis=1)

        # Check address format and errors
        df['address'] = df['address'].astype(str)
        df['address'] = df['address'].str.strip()
        df['address'] = df['address'].replace('\n',', ', regex=True)

        # Remove rows with missing or incorrect 'longitude' and 'latitude' values
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df = df.round({'longitude': 2, 'latitude': 2})

        # Ensure 'staff_numbers' are numerical and drop any null values for 'longitude', 'latitude', and 'opening_date'
        df['staff_numbers'] = df['staff_numbers'].str.replace('[^0-9]', '', regex=True)
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'])

        # Drop rows where there is NULL
        df = df.dropna(subset=['longitude', 'latitude','opening_date'])

        # Convert 'opening_date' column to datetime
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')

         # Drop rows where there is NaT or NaN
        df = df.dropna(subset=['opening_date'])

        # Change 'locality' format and convert 'store_type', 'country_code', and 'continent' to categorical type
        df['locality'] = df['locality'].astype(str).str.strip()
        df['store_type'] = df['store_type'].astype('category')
        df['country_code'] = df['country_code'].astype('category')

        # Find rows where 'continent' starts with "ee" and remove "ee"
        df['continent'] = df['continent'].apply(lambda x: x.replace("ee", "") if str(x).startswith("ee") else x)
        df['continent'] = df['continent'].astype('category')
        
        # Reset index
        df.reset_index(drop=True, inplace=True)

        return df

    @staticmethod
    def convert_product_weights(data: pd.DataFrame) ->pd.DataFrame:
        """
        Converts the product weights to a standard unit (kg), handling various formats and units
        present in the input data.
        """
        df = data.copy()

        df['weight'] = df['weight'].astype(str).str.strip()

        df = df.dropna()

        def convert_to_kg(weight):
            # Base conversion ratio from dictionary key to kg
            conversion_dict = {'kg':1,'g':10**(-3),'ml':10**(-3) }

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
                        print(f'Could not change this value: {weight} to a float')
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

        # Drop columns that are not needed for further analysis or are causing issues
        df = df.drop(['level_0','first_name','last_name','1'], axis=1)

        return df
    
    @staticmethod
    def clean_date_data(data: pd.DataFrame) -> pd.DataFrame:
        """
        Perform cleaning on date data. Involves combining separated date and time columns
        into a single datetime column and removing the original columns.
        """
        df = data.copy()

        # Combine columns to create a full datetime string and convert to correct data type
        df['datetime'] = df['year'] + '-' + df['month'] + '-' + df['day'] + ' ' + df['timestamp']
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S',errors='coerce')

        # Drop erroneous rows and extra columns
        df = df.dropna()
        df = df.drop(['timestamp','month','year','day'], axis=1)

        return df
 