# **UK ENERGY GRIDWATCH DATA ENGINEERING PROJECT**

You may view the live web app here: https://ukenergygridwatch.streamlit.app/

## Data Stacks
- Database: DuckDB
- Visualization: Plotly, Streamlit
- Hosting: Streamlit Community Cloud
- Programming Language - Python

## Data Modelling
- Star Schema
- 1 fact table 
- 3 dimension table (time, interconnectors, and energy sources)


## Pipeline
- 'Data Lake': Converting raw csv file into parquet for more efficient storage and processing
- 'Initial Cleaning': Removing duplicates in parquet file before creating data model
- 'Database Aggregation': Creating aggregation table per daily from parquet to ease dashboard processing and query analysing
- 'Data Modelling': Based on one big aggregation table, create multiple small table following star schema model. This part involves creating unique primary key, data insertion, and table management, final product is 'datawarehouse.duckdb'.
- 'Query Analysis': Separating queries in one for analysis to improve code organization and readability. This queries will be called for visualization purposes using Streamlit.
- 'Web Development': Analysis is visualized using Plotly library while implementing 'concurrent.futures.ThreadPoolExecutor()' for concurrent and faster chart loading

