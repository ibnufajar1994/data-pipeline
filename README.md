


<img width="1351" alt="ilustrasi" src="https://github.com/user-attachments/assets/8f998cc4-90a2-4d0d-a196-2401eb757738" />


# Data-Pipeline
Build Data pipeline using python &amp; MinIO

## Content
[1. Overview](#1-overview)  
[2. Source of Dataset](#2-source-of-dataset)  
[3. Identify the Data](#3-identify-the-data)  
[4. Requirements Gathering](#4-requirements-gathering)  



# 1. Overview
This project streamlines the ETL (Extract, Transform, Load) process by automating data extraction from various sources while maintaining data integrity. It features error logging and stores objects in MinIO object storage. Additionally, it offers a comprehensive overview of the data pipeline, covering data sources, storage destinations, requirement analysis, proposed solutions, and the final pipeline design. 

# 2. Source of Dataset
The data set used in this project comes from sales data for a cafe called paccafe on this [repository](https://github.com/Kurikulum-Sekolah-Pacmann/data_pipeline_paccafe/tree/week-4). The data set contain postgresql database about POS system of a caffe called paccafe. Another source of dataset that will used in this project is in spreadsheet on this [link](https://docs.google.com/spreadsheets/d/1SRCNk27p78EQAifVZvAWWfUqKgrCyA2AOtFxFw-6fCg/edit?gid=0#gid=0)

# 3. Identify the Data
The first step to do is to identify the data from the given dataset, we will identify the business process by the ERD of paccafe shown on the spoiler bellow:
<details>
  
![sources - public](https://github.com/user-attachments/assets/16171662-4f61-49ef-a1b5-328914f1eade)

</details>

The ERD illustrates that Paccafe's business process consists of:
1. Customer Management
     Manage customers data such as : Name, email, phone number and loyalty points.
2. Order & Selling Item
     Manage customer orders and product sales. 
3. Inventory Management
  Track and manage product stock levels, including products, quantity change and change date.    
4. Employee
   Handle employee records such as: name, hire date, role and email.

# 4. Requirements Gathering
Before we start to build the pipeline for paccafe, the first thing to do is to gather all required information to build the data pipeline. Imagine you are the data engineers and paccafe is your client, you and paccafe will conduct Q&A question to gather all the information needed. The Q&A bellow is the possible scenario of paccafe requirement gatherings:

**Question 1**: What is the main purpose of creating data pipeline for paccafe?

**Answer**: Our main goals are threefold: First, we want better inventory management to reduce wastage and stockouts. Second, we need deeper customer insights to improve our loyalty program. Third, we want to optimize our staffing based on sales patterns. Overall, we're looking to become more data-driven in our decision-making.

**Question 2**: Could you describe your current data environment? What systems are currently storing the data from your customers, orders, products, and inventory?

**Answer: We're currently using a cloud-based POS system that stores most of our transaction data, but our inventory tracking related to products is partially manual with spreadsheets

**Question 3**: What is the main problem you are facing with the current POS system?

**Answer**: we have obstacle to analyze our data with current system because our data is scattered on some platform. We need to collect our data in single datawarehouse.

**Question 4**: Have you experienced any data quality issues you'd like addressed in the new pipeline? For example, duplicate orders, missing customer information, or inventory discrepancies?

**Answer**: We've had issues with duplicate customer records when people use different emails or phone numbers. Our inventory counts don't always match reality due to manual counting errors or staff not recording product waste properly. We sometimes have orphaned order details if a transaction is voided incorrectly. We also facing that some negative value from our data, especially on cost price and unit price from our products. we need all the data in our data pipeline is a good quality data.

**Question 5**: Do you need to store any failed data on your data pipeline process?
**Answer: Absolutely, we want to store all the failed data in some object storage platforms. This process will help us on debugging and ensure the data quality
-----------------------------------------------------------------------------------------------------------------------------------------------------------------

Based on the requirements gathering process, we will provide solution for paccafe by designing data pipeline for paccafe.  

![ilust](https://github.com/user-attachments/assets/9fffc918-da87-47d1-8c5f-5dc42434a4b6)

This pipeline will involve the following steps:
- Data Extraction: Extract data from spreadsheets and databases. we will use both full and incremental extraction methods to retrieve data efficiently.
- Data Load:
  - Staging: Load raw data into a staging database (PostgreSQL) without transformation.
  - Final Load: Transfer clean and transformed data to the final destination.
  - Failure Handling: Log failed data loads to MinIO object storage for reprocessing
    
- Data Transformation:
  - Cleaning: Handle missing values, incorrect data formats, and other data quality issues.
  - Trasnforming: Add derived fields and calculated metrics as needed.
    
- Data Validation: process of checking and ensuring that data meets predefined rules

Tools and Technologies Using in this project including:
- Python: build and manage data Pipeline
- PostgreSQL: act as database for log, staging and final data storage.
- MinIO: handling failed and invalid data on data loading process
- Docker: build and Run MinIO

## Source To Target Mapping
  Source to Target Mapping (S2T) is a crucial document or process in data pipeline design that defines how data from a source system (e.g., a database, API, file, etc.) is transformed and loaded into a target system. When implementing complex data pipelines,Source To Target Mapping  documents often serve as the blueprint for developers and the reference for stakeholders to understand data flows throughout the organization. The target system schema for paccafe is shown below:

<Details>
  
![dwh - public](https://github.com/user-attachments/assets/63cc5e67-74ab-4130-aa04-d1fda5dc341b)

</Details>


After identify the target schema, we will evaluate source to target mapping for paccafe as follow:

**Source: Staging**

**Target: Warehouse**

## A. dim_customers table
 - Source Table: customers
 - Target Table: dim_customers

<Details>

 ![image](https://github.com/user-attachments/assets/676993a2-6bf9-4821-8ce2-767b988df1ec)


</Details>

## B. dim_employees table
- Source Table: employees
- Target Table: dim_employees

<Details>

![image](https://github.com/user-attachments/assets/28312bbc-94e2-4cd4-91d6-795e19681432)


</Details>

## C. dim_store_branch table
 - Source Table: store_branch
 - Target Table: dim_store_branch

<Details>
  
![image](https://github.com/user-attachments/assets/b66e9469-0f03-4c4e-833f-363bdb3c4756)


</Details>

## D. dim_products table
 - Source Table: products
 - Target Table: dim_products

<Details>

  ![image](https://github.com/user-attachments/assets/f79f0c34-2a13-47de-aad5-a41b753c2708)

</Details>

## E. fct_inventory_table
 - Source Table: inventory_tracking
 - Target Table: fct_inventory_tracking

<Details>

![image](https://github.com/user-attachments/assets/55ff8765-afe7-423a-a5a2-8b56466c7ded)


</Details>

## F. fct_order
 - Source Table: orders, order_details
 - Target Table: fct_order

<Details>

  ![image](https://github.com/user-attachments/assets/a6728c9d-acd9-4fa2-961e-fab7e2d10c78)

</Details>

