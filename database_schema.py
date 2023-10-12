import pandas as pd
from sqlalchemy import text


########################################## MILESTONE 3 ##############################################
def execute_sql_query(connection: object, query: str) -> None:
    '''
    Execute a SQL query using a database connection and handle exceptions.

    Args:
    connection (object): Established database connection such as create_engine output.
    query (str): SQL query string to execute.
    '''
    try:
        connection.execute(text(query))
        # print("Successfully executed sql query")
    except Exception as e:
        print(f"Failed to execute query: {e}")


def find_max_lengths(connection: object, query: str) -> tuple:
    '''
    Retrieve maximum lengths for each column based on the provided SQL query.

    Args:
    connection (object): Established database connection.
    query (str): SQL query string to fetch data for determining maximum lengths.

    Returns:
    tuple: Contains maximum lengths for each specified column, or None if an exception occurs.
    '''
    try:
        result = connection.execute(text(query))
        return result.first()
    
    except Exception as e:
        print(f'Failed to fetch data: {e}')
        return None


# Task 1 - orders_table
def update_orders_table(connection: object) -> None:
    """Update data types and structures in the orders_table."""
    
    max_length_query =  '''
                        SELECT MAX(LENGTH(card_number)), MAX(LENGTH(store_code)), MAX(LENGTH(product_code)) FROM orders_table;
                        '''
    max_lengths = find_max_lengths(connection, max_length_query)

    update_type_query = f'''
                        ALTER TABLE orders_table
                        ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,  
                        ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
                        ALTER COLUMN product_quantity TYPE SMALLINT, 
                        ALTER COLUMN card_number TYPE VARCHAR({max_lengths[0]}),
                        ALTER COLUMN store_code TYPE VARCHAR({max_lengths[0]}), 
                        ALTER COLUMN product_code TYPE VARCHAR({max_lengths[0]}); 
                        '''
    execute_sql_query(connection, update_type_query)


# Task 2 - dim_users table
def update_dim_users(connection: object) -> None:
    """Update data types and structures in the dim_users table."""
    
    max_length_query =  '''
                        SELECT MAX(LENGTH(country_code)) FROM dim_users;
                        '''
    max_lengths = find_max_lengths(connection, max_length_query)

    update_type_query = f'''
                        ALTER TABLE dim_users
                        ALTER COLUMN first_name TYPE VARCHAR(255),
                        ALTER COLUMN last_name TYPE VARCHAR(255),
                        ALTER COLUMN date_of_birth TYPE DATE,
                        ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
                        ALTER COLUMN join_date TYPE DATE,
                        ALTER COLUMN country_code TYPE VARCHAR({max_lengths[0]});
                        '''
    execute_sql_query(connection, update_type_query)


# Task 3 - dim_store_details table
def update_dim_store_details(connection: object) -> None:
    """Update data types and structures to the dim_store_details table."""

    update_type_query = f'''
                        ALTER TABLE dim_store_details
                        DROP COLUMN lat;

                        ALTER TABLE dim_store_details
                        ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
                        ALTER COLUMN locality TYPE VARCHAR(255),
                        ALTER COLUMN store_code TYPE VARCHAR(12),
                        ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
                        ALTER COLUMN opening_date TYPE DATE,
                        ALTER COLUMN store_type TYPE VARCHAR(255),
                        ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
                        ALTER COLUMN country_code TYPE VARCHAR(2),
                        ALTER COLUMN continent TYPE VARCHAR(255);
                        '''
    execute_sql_query(connection, update_type_query)


# Task 4 & Task 5 - dim_products table
def update_dim_products(connection: object) -> None:
    """Update data types and structures of the dim_products table."""
    
    # Remove £ symbol
    remove_symbol_query='''
                        UPDATE dim_products
                        SET product_price = REPLACE(product_price,'£','');
                        '''
    execute_sql_query(connection, remove_symbol_query)

    # Add weight_class column with human readable values and set length
    max_length = len('Truck_Required')

    add_column_query =  f'''
                        ALTER TABLE dim_products
                        ADD COLUMN weight_class VARCHAR({max_length});
                        '''
    execute_sql_query(connection, add_column_query)

    update_type_query = '''
                        UPDATE dim_products
                        SET weight_class =
                        CASE
                                WHEN weight < 2 THEN 'Light'
                                WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
                                WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
                                WHEN weight >= 140 THEN 'Truck_Required'
                        ELSE NULL
                        END;
                        '''
    execute_sql_query(connection, update_type_query)

    # Rename 'removed' column and change data
    rename_column_query='''
                        ALTER TABLE dim_products
                        RENAME COLUMN removed TO still_available;
                        '''
    
    execute_sql_query(connection, rename_column_query)
    
    restructure_query = '''
                        UPDATE dim_products
                        SET still_available = REPLACE(still_available,'Still_avaliable','True');

                        UPDATE dim_products
                        SET still_available = REPLACE(still_available,'Removed','False');
                        '''
    execute_sql_query(connection, restructure_query)

    max_length_query =  '''
                        SELECT MAX(LENGTH("EAN")), MAX(LENGTH(product_code)), MAX(LENGTH(weight_class)) FROM dim_products;
                        '''
    max_lengths = find_max_lengths(connection, max_length_query)

    update_type_query = f'''
                        ALTER TABLE dim_products
                        ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
                        ALTER COLUMN weight TYPE FLOAT,
                        ALTER COLUMN "EAN" TYPE VARCHAR({max_lengths[0]}),
                        ALTER COLUMN product_code TYPE VARCHAR({max_lengths[1]}),
                        ALTER COLUMN date_added TYPE DATE USING date_added::date,
                        ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
                        ALTER COLUMN still_available TYPE BOOL USING still_available::boolean,
                        ALTER COLUMN weight_class TYPE VARCHAR({max_lengths[2]});
                        '''
    execute_sql_query(connection, update_type_query)


# Task 6 - dim_date_times table
def update_dim_date_times(connection: object) -> None:
    """Update data types and structures of the dim_date_times table."""
    
    max_length_query =  '''
                        SELECT MAX(LENGTH(month)), MAX(LENGTH(year)), MAX(LENGTH(day)), MAX(LENGTH(time_period)) FROM dim_date_times;
                        '''
    max_lengths = find_max_lengths(connection,max_length_query)

    update_type_query = f'''
                        ALTER TABLE dim_date_times
                        ALTER COLUMN month TYPE VARCHAR({max_lengths[0]}),
                        ALTER COLUMN year TYPE VARCHAR({max_lengths[1]}),
                        ALTER COLUMN day TYPE VARCHAR({max_lengths[2]}),
                        ALTER COLUMN time_period TYPE VARCHAR({max_lengths[3]}),
                        ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
                        '''
    execute_sql_query(connection, update_type_query)

# Task 7 - dim_card_details
def update_dim_card_details(connection: object) -> None:
    """Update data types and structures of dim_card_details table."""

    max_length_query =  '''
                        SELECT MAX(LENGTH(card_number)), MAX(LENGTH(expiry_date)) FROM dim_card_details;
                        '''
    max_lengths = find_max_lengths(connection,max_length_query)

    update_type_query = f'''
                        ALTER TABLE dim_card_details
                        ALTER COLUMN card_number TYPE VARCHAR({max_lengths[0]}),
                        ALTER COLUMN expiry_date TYPE VARCHAR({max_lengths[1]}),
                        ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;
                        '''
    execute_sql_query(connection,update_type_query)

# Task 8 - Adding PRIMARY KEYS to dim tables
def add_primary_keys(connection: object) -> None:
    """ Add primary keys to all tables starting with dim in the connected database. """
    
    # ADD CONTRAINT for better naming convention and ease of accessing
    primary_key_query = '''
                        ALTER TABLE dim_card_details
                        ADD CONSTRAINT pk_card_number PRIMARY KEY (card_number);

                        ALTER TABLE dim_date_times
                        ADD CONSTRAINT pk_date_uuid PRIMARY KEY (date_uuid);

                        ALTER TABLE dim_products
                        ADD CONSTRAINT pk_product_code PRIMARY KEY (product_code);

                        ALTER TABLE dim_store_details
                        ADD CONSTRAINT pk_store_code PRIMARY KEY (store_code);

                        ALTER TABLE dim_users
                        ADD CONSTRAINT pk_user_uuid PRIMARY KEY (user_uuid);
                        '''
    execute_sql_query(connection, primary_key_query)

# Task 9 - Adding FOREIGN KEYS to orders_table which is the single source of truth and complete star-based database schema
def add_foreign_keys(connection: object) -> None:
    """ Add foreign keys to the 'orders_table' in the connected database. """
    
    foreign_key_query = '''
                        ALTER TABLE orders_table
                        ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number),
                        ADD CONSTRAINT fk_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
                        ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code),
                        ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
                        ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
                        '''
    execute_sql_query(connection, foreign_key_query)

# Main function to run all queries to structure the database
def execute_db_operations(engine: object) -> None:
    """
    Perform a series of database operations by using the provided engine as well as distributing it to various functions
    """
    try:
        with engine.begin() as connection:
            update_orders_table(connection)
            update_dim_users(connection)
            update_dim_store_details(connection)
            update_dim_products(connection)
            update_dim_date_times(connection)
            update_dim_card_details(connection)
            add_primary_keys(connection)
            add_foreign_keys(connection)
            print("Successfully integrated star-based database schema")

    except Exception as e:
        print(f"An error occurred during database operations: {e}")