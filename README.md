# Multinational Retail Data Centralisation

## Table of Contents
1. [Description](#description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [File Structure](#file-structure)
5. [License](#license)

## Description
The **Multinational Retail Data Centralisation** is designed to consolidate various datasets related to sales, customers, products, and stores from different data sources into a centralized database on Postgres. As a multinational company dealing with various goods globally, handling data effectively and efficiently from multiple sources like CSV files, APIs, AWS RDS, S3 Buckets, and PDFs, was becoming a time-consuming task. This project aims to eliminate data storage from different areas by creating a single source of truth for all sales-related data which is accessible and analyzable, promoting data-driven decision-making within the organisation.

### Aim of the Project
- Extract data from varied data sources and interacting with different data sources (APIs, Databases, AWS S3 Buckets, PDF)
- Perform data cleaning and processing.
- Handling databases with Python using SQLAlchemy.
- Ensuring secure management of database credentials.
- Consolidate cleaned data into a single, centralized PostgreSQL database.
- Design specific queries to acquire results in order to promote data-driven decision by the organisation.

#### Data Extraction
> Python code is in data_extraction.py

- **Various Data Sources:** Capable of extracting data from different sources, such as:
  - **AWS RDS Database:** Extracts historical user data from an AWS RDS instance.
  - **AWS S3 Bucket (CSV & PDF):** Pulls product information from a CSV file and card details from a PDF document stored in S3.
  - **API Endpoints:** Retrieves store data using API requests, navigating through endpoints which provide store counts and detailed information respectively.
  - **JSON Data:** Can extract data from a JSON file, containing details of sales dates and related attributes.

#### Data Cleaning
> Python code is in data_cleaning.py

- **Adaptive Data Cleaning:** Equipped with utility functions in `data_cleaning.py` to perform adaptive data cleaning specific to each extracted dataset, addressing issues like NULL values, erroneous entries, incorrect data types, and formatting discrepancies.

#### Data Uploading
> Python code is in data_utils.py

- **Database Interaction:** Using the `DatabaseConnector` class to interact with the PostgreSQL database, facilitating data uploads once it has been extracted and cleaned.
  - **Secure Credential Handling:** Utilizes a `db_creds.yaml` (ignored by git for security) to securely handle database credentials.
  - **Table Interaction:** Capable of listing, reading, and writing to the database tables.

#### Database Schema Adjustments
> Python code for all SQL adjustments are found in database_schema.py

- **Updating Table Schemas:** Functions are used to execute SQL queries to modify data types, lengths, and structures in various tables to ensure data consistency and to optimise storage.
- **Primary and Foreign Key Management:** Ensuring coherent relation among various data tables by adding PRIMARY KEYS to dim tables and establishing FOREIGN KEYS in the orders_table, creating a comprehensive star schema, and thereby maintaining referential integrity.
- **Data Cleaning at Database Level:** Executes cleaning operations such as removing symbols from price data, converting data types, and adding new calculated columns (like weight_class in dim_products) which enhance usability of the data.

**Tasks & Examples**

Here are the full list of tasks and a few examples that were done in order to develop the star-based schema of the database using SQL queries with SQLalchemy.

- Task 1: Cast the columns of the orders_table to the correct data types.

```sql
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,  
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN product_quantity TYPE SMALLINT, 
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12), 
ALTER COLUMN product_code TYPE VARCHAR(11); 
```
- Task 2: Cast the columns of the dim_users table to the correct data types.

- Task 3: Cast the columns of the dim_store_details table to the correct data types.

- Task 4: In SQL create a weight_class column in the dim_products table and populate based on the weight column.

- Taks 5: Cast the columns of the dim_products table to the correct data types.

- Task 6: Cast the columns of the dim_date_times table to the correct data types.

- Task 7: Cast the columns of the dim_card_details table to the correct data types.

- Task 8: Create the primary keys in the dimensions tables.

```sql
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
```

- Task 9: Finalising the star-based schema and adding the foreign keys to the orders table.

```sql
ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number),
ADD CONSTRAINT fk_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code),
ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
```

A basic Entity-Relationship (ER) diagram for this database, sourced from pgAdmin4, is shown below.

<div align="center">
  <img src="/images/Database_Schema_Relationship.png" alt="ER_Diagram">
</div>

#### Database Query
> Python code for all queries are found in database_query.py

 - **Querying the data:** Writing business led queries that benefit the business in making informed data-led decisions.

**Tasks & Examples**

Here are the full list of tasks and a few examples that were done using SQL and SQLAlchemy.

- Task 1: How many stores does the business have and in which countries?

Query:
```sql
SELECT 	country_code AS country,
        COUNT(store_code) AS total_no_stores
FROM dim_store_details
WHERE store_code != 'WEB-1388012W'
GROUP BY country_code;
```

Output:
| country       | total_no_stores| 
|:-------------:|:-------------:| 
| US            | 34 |
| GB            | 265 |
| DE            | 141 |

- Task 2: Which locations currently have the most stores?
- Task 3: Which months produce the average highest cost of sales typically?
- Task 4: How many sales are coming from online?

Query:
```sql
SELECT 	COUNT(*) AS numbers_of_sales,
        SUM(product_quantity) AS product_quantity_count,
        CASE
            WHEN store_code LIKE 'WEB%' THEN 'Web'
            ELSE 'Offline'
        END AS location
FROM orders_table
GROUP BY location
ORDER BY location DESC;
```

Output:
| number_of_sales | product_quantity_count| location  | 
|:-------------:  |:-------------:        | :--------:|
|26957            | 107739                | Web       |
| 93166           | 374047                | Offline   |

- Task 5: What percentage of sales come through each type of store?
- Task 6: Which month in each year produced the highest cost of sales?
- Task 7: What is our staff headcount?
- Task 8: Which German store type is selling the most?
- Task 9: How quickly is the company making sales?

  
## Installation
### Prerequisites
- Python 3.x
- PostgreSQL
- PgAdmin4
- AWS CLI configured
- Required Python packages: SQLAlchemy, PyYAML, pandas, boto3, tabula-py

### Instructions
1. **Clone the repository**
```sh
git clone https://github.com/RyanJKS/multinational-retail-data-centralisation.git
```
2. **Install dependencies**
Ensure you have the prerequisites mentioned above installed on your local machine.

3. **Configure PostgreSQL**
- Ensure PostgreSQL is set up and running.
- Set up a database named sales_data.
  
## Usage
### Key Scripts & Usage
- `data_extraction.py`: Contains the DataExtractor class, used for data extraction from various sources.
- `data_cleaning.py`: Includes the DataCleaning class with methods for cleaning extracted data.
- `database_utils.py`: Contains the DatabaseConnector class, which facilitates connection and data upload to the database.
- `main_script.py`: A test script demonstrating the usage of functions and methods from other modules.
- `database_schema.py`: This script is for modifying and refining the database schema. It adjusts data types, introduces keys, and enhances the relational structure of the database. It makes the orders_table the single source of truth which is at the centre of our star-based database schema.
- `database_query.py`: Contains scripts to query the database for specific source of information

In order to have all the information extracted from all sources mentioned above and uploaded in your postgres database, as well as set up the star-based database schema;

Run the following command in all the python script's directory in the terminal: 
```sh
python3 main_script.py
```
After running this command, a message shown below should appear in your terminal, otherwise, please check your database connection or follow the error message as it will guide you towards the source.

<div align="center">
  <img src="/images/main_script_output.png" alt="Main_Script_Output">
</div>

To query the database for selective information, run the following command after having all the database set up from the previous command:
```sh
python3 database_query.py
```

## File Structure
project/
│
├── data_extraction.py    
├── data_cleaning.py
├── database_query.py      
├── database_schema.py      
├── database_utils.py     
├── main_script.py      
│
├── db_creds.yaml         
│
├── .gitignore               
└── README.md  
    

## Licence Information
This project is owned by AiCore and was part of an immersive program with specialisation, Data Engineering. AiCore is a specialist ai & data career accelerator from which I completed several industry-level projects to gain vital experience and be qualified in the use of several technologies.