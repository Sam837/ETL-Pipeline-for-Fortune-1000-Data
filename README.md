# Project Title
`ETL Pipeline for Fortune 1000 Data`
`Owner: Sam`

`This program will load the dataset in pgAdmin, perform ETL pipelines in airflow, load and visualize the clean data using Elastic Kibana, and lastly perform data validation using Great Expectations.`

## Repository Outline
```
1. README.md - Contains a description of the project repository documentation
2. P2M3_Sam_GX.ipynb - Notebook is containing data validation on a cleaned dataset (P2M3_Sam_data_clean.csv) using the Great Expectations (GX) library.
3. P2M3_Sam_ddl.txt - Contains URL dataset, DDL syntax for creating tables, and DML syntax for inserting data into a database
4. P2M3_Sam_data_raw.csv - Contains raw data
5. P2M3_Sam_data_clean.csv - Contains clean data
6. P2M3_Sam_DAG.py - Contains a data pipeline with three main tasks: extract, transform, and load (ETL)
7. P2M3_Sam_DAG_graph.jpg - Picture of DAG graph
8. LICENSE - MIT license
9. P2M3_Sam_data_introduction.ipynb - Serves as an initial data exploration and quality check for the raw Fortune 1000 dataset (P2M3_Sam_data_raw.csv)
10. images - Contains pictures of data visualization from Elastic Kibana
        ├── introduction & objective.png - contains introduction and objective of the data visualization from Elastic Kibana
        ├── plot & insight 01.png - contains Top 5 Number of Companies by State
        ├── plot & insight 02.png - contains Top 5 State by Revenues
        ├── plot & insight 03.png - contains Top 5 by Sectors
        ├── plot & insight 04.png - contains Top 5 Sectors by Revenues
        ├── plot & insight 05.png - contains Top 5 Companies by Revenue
        ├── plot & insight 06.png - contains Line Graph for Revenue per Rank
        └── conclusion.png - contains conclusion of the data visualization from Elastic Kibana
```

## Problem Background
`The dataset under analysis contains information about the Fortune 1000 companies, including key metrics such as revenue, profit, sector, employee count, and market capitalization. While the dataset provides valuable insights into corporate performance, several data quality issues were identified during the initial exploration, which could impact downstream analytics and decision-making.`

## Project Output
`The goal is to analyze the dataset to gain insights into corporate performance and distribution.`

`- Analyze company performance by comparing companies based on their revenues metric.`

`- Study industry trends by understanding how different sectors are performing, and identifying the most highest-revenue by sectors.`

`- Geographical analysis by examining the distribution of companies across different states.`

## Data
`Fortune 1000 is about a large group of companies that is based on the Forbes Fortune 500, ranking companies by revenue.`

## Method
`We used a combination of SQL Data Definition Language (DDL) and Data Manipulation Language (DML) to define and populate a PostgreSQL table for the Fortune 1000 dataset and a data pipeline with three main tasks: extract, transform, and load (ETL) in airflow.`

## Stacks
`I am using python on VS Code, airflow, Elastic Kibana and pgAdmin4 to analyze, process, and visualize the data. I am also using libraries; pandas, numpy, great_expectations, airflow, and elasticsearch to help me analyze, process, and visualize the data.`

## Reference
URL Dataset: https://www.kaggle.com/datasets/winston56/fortune-500-data-2021?resource=download
