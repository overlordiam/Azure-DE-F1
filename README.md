# Data Engineering and Data Analysis End-to-End Pipeline: Formula-1

## Overview
This project demonstrates the creation of an end-to-end data pipeline on Azure, utilizing Delta Lake—a robust, open-source storage layer ensuring ACID transactions and effective metadata handling. The pipeline showcases data movement from the bronze to gold layers, implementing incremental load strategies, creating external tables for data analytics, and orchestrating the pipeline with tools such as PySpark, Azure Data Lake Storage (ADLS), Azure Databricks, and Azure Data Factory.

![Pipeline Overview](https://github.com/user-attachments/assets/8f1aca63-8e25-4bbf-9a22-b2b8b1df289a)

## Architecture
![Architecture Diagram](https://github.com/user-attachments/assets/58a060ef-6c53-4c0e-b216-9c7daf3121f6)

The Delta Lake architecture is segmented into three key layers:
- **Bronze Layer**: The repository for raw data.
- **Silver Layer**: The stage for data transformation.
- **Gold Layer**: The final layer, hosting enriched and aggregated data ready for analysis.

## Technologies
- Python
- PySpark
- SQL
- Power BI

## Azure Services Utilized

### **PySpark and Spark SQL**
- Managed tables with over 10,000 records efficiently using partitioning.
- Implemented incremental load and full-load handling.
- Performed data analysis using SQL and Spark SQL to extract meaningful insights from processed data.

### **Databricks**
- Utilized Jupyter-style notebooks for data processing and analysis.
- Leveraged Databricks compute to efficiently handle big data processing.
- Created and executed jobs to streamline data flow and test end-to-end functionality.

  
  ![image](https://github.com/user-attachments/assets/566cebd6-f255-4252-8ff2-e05d90e4efad)


### **Data Lake Storage Gen2**
- Utilized containers to store different data use-cases (raw, processed, and presentation).
- Employed the data lake to store external tables.
- Implemented IAM policies for enhanced security and access control.

### **Service Principal**
- Enabled secure, automated access to Azure services by applications, eliminating the need for storing user credentials in code.
- Facilitated centralized management of permissions and access policies, enhancing security and governance.

### **Data Factory**
- Orchestrated workflows from the bronze phase to the gold phase using Data Factory.
- Authored and monitored multiple pipelines, adding triggers for automated execution and debugging.
- Utilized Linked Services to connect Databricks and Data Lake to Data Factory.

  
  ![image](https://github.com/user-attachments/assets/d8ffc954-b7e6-402b-94c3-4cf67b021205)


### **Delta Lake**
- Efficiently upserted (update/insert) new data.
- Supported ACID transactions, ensuring data reliability and consistency, which traditional data lakes do not support.
- Provided data version control and time-travel capabilities for debugging and rollbacks.

### **Unity Catalog**
- Added data governance and centralized user management.
- Offered fine-grained control over user access to different Unity Catalog objects.
- Provided data lineage to understand the data lifecycle.

  
  ![Unity Catalog](https://github.com/user-attachments/assets/264b6796-91a1-4d1a-9c49-27d09e1bc36d)

## Data Analysis

### Dominant Drivers
![Dominant Drivers](https://github.com/user-attachments/assets/b0fe034e-c459-4c20-ac78-4431b8560d44)

### Dominant Teams
![Dominant Teams](https://github.com/user-attachments/assets/ef64bc57-fd43-4fcd-bd54-b336ee2f05e4)

## Key Enhancements and Additions
- **Incremental Loading**: Demonstrates the capability to handle incremental data loads efficiently, ensuring only new or updated data is processed.
- **External Table Creation**: Showcases how external tables can be created for effective data analysis, making data readily available for business intelligence tools like Power BI.
- **Pipeline Orchestration**: Highlights the orchestration of data pipelines using Azure Data Factory, ensuring smooth data flow across different stages and layers.
- **Advanced Analytics**: Leverages the power of Spark SQL and Databricks for advanced data analysis, providing deeper insights into the data.
