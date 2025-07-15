# Retail Store Analytics
Hi! Thanks for visiting.
This is a simple retail store analytics project focused on gathering insights from transactions, products, and store data by building an end-to-end data pipeline using Azure services such as Azure Data Factory, Azure Data Lake Storage Gen2, Azure SQL, and Azure Databricks. The pipeline follows the medallion architecture pattern. Finally, the refined, business-ready data is integrated with Power BI Service to generate meaningful business insights.

## 📂 Project Repository Contents
- [Data source](https://github.com/rohitpaul1998/Retail-Azure-Data-Engineering/tree/main/data-source)
- [Databricks issues & fixes](https://github.com/rohitpaul1998/Retail-Azure-Data-Engineering/blob/main/Databricks_issues_and_fixes.rtf)
- [PySpark source code & Databricks notebooks ](https://github.com/rohitpaul1998/Retail-Azure-Data-Engineering/tree/main/Retail-DataEngineering)
- [Data ready for BI reporting](https://github.com/rohitpaul1998/Retail-Azure-Data-Engineering/tree/main/gold-layer-refined-data)
- [Power BI report](https://github.com/rohitpaul1998/Retail-Azure-Data-Engineering/tree/main/power-bi-report)
- [Project report](https://github.com/rohitpaul1998/Retail-Azure-Data-Engineering/tree/main/project-status-report)

## 📖 Learnings & Takeaways
#### Azure Data Engineering Skills Gained

- Hands-on experience building a complete **end-to-end data pipeline** using Azure services (ADLS Gen2, Azure Data Factory, Azure SQL Database, Azure Databricks, Power BI).
- Exposure to **medallion architecture** – Bronze, Silver, and Gold layers for structured data transformation.

#### Azure Data Lake Storage Gen2 (ADLS Gen2)

- Learned ADLS Gen2 concepts including containers, directories, folder hierarchy, and role-based access control.
- Understood how ADLS Gen2 acts as the **central data lake** for storing raw, cleaned, and refined data in a distributed file system.
- Learned to work with **Mounting** in Databricks to directly access and read/write data from ADLS Gen2 using `dbutils.fs.mount()`.
- Gained practical knowledge on:
  - Managing container directories: Bronze (raw), Silver (cleansed), and Gold (business-level).
  - Handling file formats like **Parquet** and **Delta Lake**.
  - Optimizing data storage and refresh using **overwrite mode** to prevent duplicates.

#### Reporting Layer

- Built final reports in **Power BI Service** using the Gold Layer output.
- Learned alternative reporting techniques such as exporting Delta file data as CSV to bypass Unity Catalog restrictions.
