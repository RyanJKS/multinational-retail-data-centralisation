import pandas as pd
from sqlalchemy import text
import database_utils


########################################## MILESTONE 4 - QUERY DATABASE ##############################################
def execute_sql_query(connection: object, query: str) -> None:
    '''
    Execute a SQL query using a database connection and handle exceptions.

    Args:
    connection (object): Established database connection such as create_engine output.
    query (str): SQL query string to execute.
    '''
    try:
        return connection.execute(text(query))
    except Exception as e:
        print(f"Failed to execute query: {e}")


# Task 1 - How many stores does the business have and in which countries?
def total_no_stores(connection: object) -> None:

    # Count store by store_code and exclude online store
    query = '''
            SELECT 	country_code AS country,
                    COUNT(store_code) AS total_no_stores
            FROM dim_store_details
            WHERE store_code != 'WEB-1388012W'
            GROUP BY country_code;
            '''
    result = execute_sql_query(connection, query)

    print("Query 1: ")
    print(pd.DataFrame(result, columns=result.keys()))


# Task 2 - Which locations currently have the most stores?
def most_stores_location(connection: object) -> None:
    
    query = '''
            SELECT 	locality,
                    COUNT(locality) as total_no_stores
            FROM dim_store_details
            GROUP BY locality
            ORDER BY total_no_stores DESC
            LIMIT 7;
            '''
    result = execute_sql_query(connection, query)

    print("Query 2:")
    print(pd.DataFrame(result, columns=result.keys()))

# Task 3 - Which months produce the average highest cost of sales typically?
def highest_sale_months(connection: object) -> None:

    # Add explicit cast numeric before rounding to 2 decimal place
    query = '''
            SELECT 	ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS NUMERIC), 2) AS total_sales,
                    dim_date_times.month
            FROM orders_table
            JOIN dim_products
                ON dim_products.product_code = orders_table.product_code
            JOIN dim_date_times
                ON dim_date_times.date_uuid = orders_table.date_uuid
            GROUP BY dim_date_times.month
            ORDER BY total_sales DESC
            LIMIT 6;
            '''
    result = execute_sql_query(connection, query)

    print("Query 3: ")
    print(pd.DataFrame(result, columns=result.keys()))


# Task 4 - How many sales are coming from online?
def online_sales(connection: object) -> None:
    
    # Count all row for numbers_sales, sort web orders by store code starting with 'WEB'
    query = '''
            SELECT 	COUNT(*) AS numbers_of_sales,
                    SUM(product_quantity) AS product_quantity_count,
                    CASE
                        WHEN store_code LIKE 'WEB%' THEN 'Web'
                        ELSE 'Offline'
                    END AS location
            FROM orders_table
            GROUP BY location
            ORDER BY location DESC;
            '''
    
    result = execute_sql_query(connection, query)

    print("Query 4:")
    print(pd.DataFrame(result, columns=result.keys()))


# Task 5 - What percentage of sales come though each type of store?
def sales_percentage_by_store(connection: object) -> None:

    query = '''
            WITH cte AS (
                SELECT 
                    dsd.store_type,
                    ROUND(CAST(SUM(ot.product_quantity * dp.product_price) AS NUMERIC), 2) AS total_sales,
                    SUM(ot.product_quantity * dp.product_price) AS total_sales_unrounded
                FROM 
                    orders_table AS ot
                JOIN dim_store_details AS dsd
                    ON dsd.store_code = ot.store_code
                JOIN dim_products AS dp
                    ON dp.product_code = ot.product_code
                GROUP BY 
                    dsd.store_type
            )
            SELECT 
                store_type,
                total_sales,
                ROUND(
                    CAST(
                        (total_sales_unrounded / (SELECT SUM(total_sales_unrounded) FROM cte)) * 100
                    AS NUMERIC), 2) AS total_percentage
            FROM 
                cte
            ORDER BY 
                total_sales DESC;
            '''

    result = execute_sql_query(connection, query)

    print("Query 5:")
    print(pd.DataFrame(result, columns=result.keys()))

# Task 6 - Which month in each year produced the highest cost of sales?
def yearly_highest_sale_month(connection: object) -> None:

    query = '''
            SELECT	
                ROUND(CAST(SUM(ot.product_quantity * dp.product_price) AS NUMERIC), 2) AS total_sales,
                ddt.year,
                ddt.month
            FROM 
                orders_table AS ot
            JOIN dim_date_times AS ddt 
                ON ddt.date_uuid = ot.date_uuid
            JOIN dim_products AS dp 
                ON dp.product_code = ot.product_code
            GROUP BY 
                    ddt.year, ddt.month
            ORDER BY 
                    total_sales DESC
            LIMIT 10;
            '''
    result = execute_sql_query(connection, query)

    print("Query 6:")
    print(pd.DataFrame(result, columns=result.keys()))

# Task 7 - What is our staff headcount?
def staff_headcount(connection: object) -> None:
    
    query = '''
            SELECT	SUM(staff_numbers) AS total_staff_numbers,
                    country_code
            FROM dim_store_details
            GROUP BY country_code
            ORDER BY total_staff_numbers DESC;
            '''
    result = execute_sql_query(connection, query)

    print("Query 7:")
    print(pd.DataFrame(result, columns=result.keys()))

# Task 8 - Which German store type is selling the most?
def german_store_type_sales(connection: object) -> None:
    
    query = '''
            SELECT	
                ROUND(CAST(SUM(ot.product_quantity * dp.product_price) AS NUMERIC), 2) AS total_sales,
                dsd.store_type,
                dsd.country_code
            FROM 
                orders_table AS ot
            JOIN dim_products AS dp
                ON dp.product_code = ot.product_code
            JOIN dim_store_details AS dsd
                ON dsd.store_code = ot.store_code
            WHERE 
                dsd.country_code LIKE 'DE'
            GROUP BY 
                dsd.country_code, dsd.store_type
            ORDER BY
                total_sales ASC;
            '''
    result = execute_sql_query(connection, query)

    print("Query 8:")
    print(pd.DataFrame(result, columns=result.keys()))

# Task 9 - How quickly is the company making sales?
def average_sale_time_yearly(connection: object) -> None:
    
    query = '''
            WITH next_time_cte AS(
                SELECT 
                        year,
                        month,
                        day,
                        timestamp,
                        LEAD(timestamp) OVER( PARTITION BY year ORDER BY year, month, day, timestamp) AS next_timestamp
                FROM
                        dim_date_times	
                ORDER BY
                        year, month, day, timestamp
            ),

            new_times_cte AS (

                SELECT	
                        year,
                        month,
                        day,
                        timestamp,
                        next_timestamp,
                        CAST(CONCAT(year, '-', month, '-', day, ' ', timestamp) AS TIMESTAMP) AS purchase_time,
                        CAST(CONCAT(year, '-', month, '-', day, ' ', next_timestamp) AS TIMESTAMP) AS next_purchase_time
                        
                FROM
                        next_time_cte
                ORDER BY
                        year, month, day, timestamp
            ),
            time_difference_cte AS (
                SELECT	
                        year,
                        month,
                        day,
                        timestamp,
                        next_timestamp,
                        ABS(EXTRACT(EPOCH FROM(next_purchase_time - purchase_time))) AS purchase_time_difference
                FROM
                        new_times_cte
                ORDER BY
                        year, month, day, timestamp
            )

            SELECT
                    year,
                    CONCAT(
                    '"hours": ', FLOOR(AVG(purchase_time_difference) / 3600), ', ',
                    '"minutes": ', FLOOR((AVG(purchase_time_difference) % 3600) / 60), ', ',
                    '"seconds": ', FLOOR(AVG(purchase_time_difference) % 60), ', ',
                    '"milliseconds": ', ROUND((AVG(purchase_time_difference) - FLOOR(AVG(purchase_time_difference))) * 1000)
                    ) AS average_yearly_time_difference
            FROM
                    time_difference_cte
            GROUP BY
                    year
            ORDER BY
                    AVG(purchase_time_difference) DESC
            LIMIT
                    5;
            '''
    result = execute_sql_query(connection, query)

    print("Query 9:")
    print(pd.DataFrame(result, columns=result.keys()))

# Main function to run all queries to the database
def execute_db_queries(engine: object) -> None:
    """
    Perform a series of database queries by using the provided engine as well as distributing it to various functions
    """
    try:
        with engine.begin() as connection:
            total_no_stores(connection)
            most_stores_location(connection)
            highest_sale_months(connection)
            online_sales(connection)
            sales_percentage_by_store(connection)
            yearly_highest_sale_month(connection)
            staff_headcount(connection)
            german_store_type_sales(connection)
            average_sale_time_yearly(connection)

            print("End of database queries.")

    except Exception as e:
        print(f"An error occurred during database operations: {e}")

if __name__ == "__main__":
    db_connector = database_utils.DatabaseConnector()
    engine = db_connector.connect_my_db()
    execute_db_queries(engine)