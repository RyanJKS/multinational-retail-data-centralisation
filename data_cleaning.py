import pandas as pd
import re


# The `DataCleaning` class provides methods to clean and restructure data in various columns of a dataFrame.
class DataCleaning:

    def _restructure_date(self, df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        """
        The function `_restructure_date` takes a DataFrame and a column name as input, and attempts to
        convert the values in that column to a standardized date format.
        
        :param df: The parameter `df` is a pandas DataFrame that contains the dataset you want to
        restructure
        :type df: pd.DataFrame
        :param column_name: The `column_name` parameter is a string that represents the name of the column
        in the DataFrame `df` that contains the dates
        :type column_name: str
        :return: the modified DataFrame with the restructured date column.
        """
        # Date formats available in dataset - Year-Month-Date, Month(Name)-Date-Year(Abrev), Year-Month(Name)-Date, Month(Name)-Year-Date
        date_formats = {'%Y-%m-%d', '%B:%d:%y', '%Y:%B:%d', '%B:%Y:%d'}

        def convert_date(date_input):
            for date_format in date_formats:
                try:
                    return pd.to_datetime(date_input, format=date_format)
                except ValueError:
                    pass
            return date_input
        
        df[column_name] = df[column_name].apply(convert_date)
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
 
        return df
    

    def _clean_columns(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        The function `_clean_columns` takes a DataFrame and a list of columns, and returns the DataFrame
        with the specified columns cleaned by removing leading and trailing whitespace.
        
        :param df: A pandas DataFrame that needs to be cleaned
        :type df: pd.DataFrame
        :param columns: The `columns` parameter is a list of column names in the `df` DataFrame that you
        want to clean
        :type columns: list
        :return: a cleaned version of the input DataFrame, where the specified columns have been stripped of
        leading and trailing whitespace.
        """
        for column in columns:
            df[column] = df[column].astype(str).str.strip()
        return df


    def clean_user_data(self, df:pd.DataFrame) -> pd.DataFrame:
        """
        The `clean_user_data` function takes a DataFrame as input, drops duplicates and missing values,
        cleans specific columns, checks for errors in date columns, formats the address and phone number
        columns, and drops rows with missing values in specific columns before returning the cleaned
        DataFrame.
        
        :param df: The parameter `df` is a pandas DataFrame that contains user data
        :type df: pd.DataFrame
        :return: a cleaned version of the input DataFrame, df.
        """

        df = df.drop_duplicates().dropna()

        df = df[df['first_name'] != 'NULL']

        columns_to_clean = ['date_of_birth', 'join_date', 'company', 'address', 'country', 'country_code', 'phone_number']
        df = self._clean_columns(df, columns_to_clean)

        # Check for error in dates (DOB and join_Date) by first changing data type
        df = self._restructure_date(df, 'date_of_birth')
        df = self._restructure_date(df, 'join_date')
        df = df.dropna(subset=['date_of_birth', 'join_date'])

        df['address'] = df['address'].replace('\n',', ', regex=True)

        df['country_code'] = df['country_code'].replace('GGB','GB', regex=True)
        df['country_code'] = df['country_code'].astype('category')

        # Fix phone number format by removing non-numeric characters and adding country code
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
        
        df['phone_number'] = df.apply(clean_phone_number, axis=1)

        df = df.dropna(subset=['phone_number'])

        return df
    

    def clean_card_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        The `clean_card_data` function takes a DataFrame as input, drops duplicates and null values, cleans
        specific columns, removes non-numerical values from the 'card_number' column, checks for errors in
        the 'date_payment_confirmed' column, extracts only the card provider name, and returns the cleaned
        DataFrame.
        
        :param df: The parameter `df` is a pandas DataFrame that contains card data
        :type df: pd.DataFrame
        :return: a cleaned version of the input DataFrame, `df`.
        """

        df = df.drop_duplicates().dropna()

        columns_to_clean = ['card_number', 'card_provider', 'date_payment_confirmed']
        df = self._clean_columns(df, columns_to_clean)

        # Remove any non-numerical values
        df['card_number'] = df['card_number'].str.replace(r'\D+', '')
        df['card_number'] = df['card_number'].str.replace('?', '')

        # Check for error in dates by first changing data type and drop null rows
        df = self._restructure_date(df, 'date_payment_confirmed')
        df = df.dropna(subset=['date_payment_confirmed'])
        
        # Get only card provider name
        df['card_provider'] = df['card_provider'].apply(lambda x: x.split()[0])

        return df
    

    def clean_store_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        The `clean_store_data` function takes a DataFrame as input, performs various cleaning operations on
        the columns, and returns the cleaned DataFrame.
        
        :param df: The parameter `df` is a pandas DataFrame that contains store data
        :type df: pd.DataFrame
        :return: a cleaned version of the input DataFrame, df.
        """

        df = df.drop_duplicates()

        columns_to_clean = ['store_code', 'country_code', 'address', 'locality', 'store_type', 'staff_numbers', 'longitude', 'latitude', 'continent', 'opening_date']
        df = self._clean_columns(df, columns_to_clean)

        df['store_code'] = df['store_code'].apply(lambda x: x.upper())
        df['country_code'] = df['country_code'].apply(lambda x: x.upper())

        # Dropping rows where 'country_code' is not 'GB', 'US', or 'DE'
        df = df.drop(df[~df['country_code'].isin(['GB', 'US', 'DE'])].index)
        df['country_code'] = df['country_code'].astype('category')

        df['address'] = df['address'].replace('\n',', ',regex=True)

        df['staff_numbers'] = df['staff_numbers'].replace('[^0-9]', '', regex=True)

        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')

        df['continent'] = df['continent'].apply(lambda x: x.replace("ee", "") if x.startswith("ee") else x)
        df['continent'] = df['continent'].astype('category')

        # Check for error in dates by first changing data type and drop null rows
        df = self._restructure_date(df, 'opening_date')
        df = df.dropna(subset=['opening_date'])

        return df


    def convert_product_weights(self, df: pd.DataFrame) ->pd.DataFrame:
        """
        The `convert_product_weights` function takes a DataFrame as input, cleans and converts the weight
        column to kilograms, and returns the modified DataFrame.
        
        :param df: The parameter `df` is a pandas DataFrame that contains product information, including
        columns such as 'product_code' and 'weight'
        :type df: pd.DataFrame
        :return: a modified DataFrame with converted product weights.
        """

        df = df.drop_duplicates().dropna()
        df = df.drop(['Unnamed: 0'], axis=1)

        # Capitalise for data consistency
        df['product_code'] = df['product_code'].astype(str).str.strip()
        df['product_code'] = df['product_code'].apply(lambda x: x.upper())

        def convert_to_kg(weight):
            # Base conversion ratio from dictionary key to kg - approximate oz
            conversion_dict = {'kg':1,'g':10**(-3),'ml':10**(-3), 'oz':0.03}

            weight = weight.lower().replace(' .','')

            for key, value in conversion_dict.items():
                
                if key in weight:
                    try:
                        # Check if format is '1 x 100ml'
                        if 'x' in weight:
                            weight = weight.replace(key, '').strip()
                            return eval(weight.replace('x', '*')) * value

                        else:
                            weight = float(weight.strip(key))
                            return weight*value
                    except ValueError:
                        return None
            return None
            
        # Applying the function to convert weights
        df['weight'] = df['weight'].apply(convert_to_kg)
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
        df = df.dropna(subset=['weight'])

        df['weight'] = df['weight'].astype(float)

        return df


    def clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        The function `clean_orders_data` takes a DataFrame as input, drops unnecessary columns, removes
        duplicate rows, cleans specific columns, converts the 'product_code' column to uppercase, converts
        the 'card_number' column to numeric, drops rows with missing 'card_number' values, and returns the
        cleaned DataFrame.
        
        :param df: The parameter `df` is a pandas DataFrame that contains the orders data
        :type df: pd.DataFrame
        :return: a cleaned version of the input DataFrame, df.
        """

        # Drop columns that are not needed for further analysis or are causing issues
        df = df.drop(['level_0','first_name','last_name','1'], axis=1)

        df = df.drop_duplicates()

        columns_to_clean = ['card_number', 'store_code', 'product_code']
        df = self._clean_columns(df, columns_to_clean)

        df['product_code'] = df['product_code'].apply(lambda x: x.upper())

        return df
    

    @staticmethod
    def clean_date_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        The function `clean_date_data` takes a DataFrame as input, drops duplicate rows, converts the
        'timestamp' column to a datetime format, drops rows with missing values in the 'timestamp' column,
        and converts the 'timestamp' column to time only.
        
        :param df: The parameter `df` is a pandas DataFrame that contains the data to be cleaned
        :type df: pd.DataFrame
        :return: a cleaned version of the input DataFrame, where the 'timestamp' column has been converted
        to a time-only format.
        """

        df = df.copy()

        df = df.drop_duplicates()

        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S',errors='coerce')
        df = df.dropna(subset=['timestamp'])

        # Extract the time component only
        df['timestamp'] = df['timestamp'].dt.time

        return df
 