# Multinational Retail Data Centralisation

## Table of Contents
1. [Description](#description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [File Structure](#file-structure)
5. [License](#license)

## Description
The **Multinational Retail Data Centralisation** is designed to consolidate various datasets related to sales, customers, products, and stores from different data sources into a centralized database on Postgres. As a multinational company dealing with various goods globally, handling data effectively and efficiently from multiple sources like CSV files, APIs, AWS RDS, S3 Buckets, and PDFs, was becoming a daunting task. This project aims to eliminate data silos by creating a single source of truth for all sales-related data which is accessible and analyzable, promoting data-driven decision-making within the organization.

### Aim of the Project
- Extract data from varied data sources and interacting with different data sources (APIs, Databases, S3 Buckets, PDF)
- Perform data cleaning and processing.
- Handling databases with Python using SQLAlchemy.
- Ensuring secure management of database credentials.
- Consolidate cleaned data into a single, centralized PostgreSQL database.

#### Data Extraction
- **Various Data Sources:** Capable of extracting data from different sources, such as:
  - **AWS RDS Database:** Extracts historical user data from an AWS RDS instance.
  - **AWS S3 Bucket (CSV & PDF):** Pulls product information from a CSV file and card details from a PDF document stored in S3.
  - **API Endpoints:** Retrieves store data using API requests, navigating through endpoints which provide store counts and detailed information respectively.
  - **JSON Data:** Can extract data from a JSON file, containing details of sales dates and related attributes.

#### Data Cleaning
- **Adaptive Data Cleaning:** Equipped with utility functions in `data_cleaning.py` to perform adaptive data cleaning specific to each extracted dataset, addressing issues like NULL values, erroneous entries, incorrect data types, and formatting discrepancies.

#### Data Uploading
- **Database Interaction:** Using the `DatabaseConnector` class to interact with the PostgreSQL database, facilitating data uploads once it has been extracted and cleaned.
  - **Secure Credential Handling:** Utilizes a `db_creds.yaml` (ignored by git for security) to securely handle database credentials.
  - **Table Interaction:** Capable of listing, reading, and writing to the database tables.

#### Database Schema Adjustments
- **Updating Table Schemas:** Functions are used to execute sql queries to modify data types, lengths, and structures in various tables to ensure data consistency and to optimise storage.
- **Primary and Foreign Key Management:** Ensuring coherent relation among various data tables by adding PRIMARY KEYS to dim tables and establishing FOREIGN KEYS in the orders_table, creating a comprehensive star schema, and thereby maintaining referential integrity.
- **Data Cleaning at Database Level:** Executes cleaning operations such as removing symbols from price data, converting data types, and adding new calculated columns (like weight_class in dim_products) which enhance the richness and usability of the data.
  
## Installation
### Prerequisites
- Python 3.x
- PostgreSQL
- AWS CLI configured
- Required Python packages: SQLAlchemy, PyYAML, pandas, boto3, tabula-py, validate_email.

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
- data_extraction.py: Contains the DataExtractor class, used for data extraction from various sources.
- data_cleaning.py: Includes the DataCleaning class with methods for cleaning extracted data.
- database_utils.py: Contains the DatabaseConnector class, which facilitates connection and data upload to the database.
- main_script.py: A test script demonstrating the usage of functions and methods from other modules.
- database_schema.py: This script is for modifying and refining the database schema. It adjusts data types, introduces keys, and enhances the relational structure of the database. It makes the orders_table the single source of truth which is at the centre of our star-based database schema.

In order to have all the information extracted from all sources mentioned above and uploaded in your postgres database,
run the following command in all the python script's directory in the terminal: 
```sh
python3 main_script.py
```

## File Structure
project/
│
├── data_extraction.py    
├── data_cleaning.py
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