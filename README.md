


<img width="1351" alt="ilustrasi" src="https://github.com/user-attachments/assets/8f998cc4-90a2-4d0d-a196-2401eb757738" />


# data-pipeline
Build Data pipeline using python &amp; MinIO


The aim for this project is to demonstrate the data pipeline process using ETL Method. The data set used in this project comes from sales data for a cafe called paccafe on this [repository](https://github.com/Kurikulum-Sekolah-Pacmann/data_pipeline_paccafe/tree/week-4). The first step to do is to identify the data from the given dataset, we will identify the business process by the ERD of paccafe shown on the spoiler bellow:
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

Before we start to build the pipeline for paccafe, the first thing to do is to gather all required information 
