# **UK ENERGY GRIDWATCH DATA ENGINEERING PROJECT**

You may view the live web app here: https://ukenergygridwatch.streamlit.app/

## Data Stacks
- **Database**: DuckDB
- **Visualization**: Plotly, Streamlit
- **Hosting**: Streamlit Community Cloud
- **Language**: Python, SQL

## Data Modelling
- Star Schema
- 1 fact table 
- 3 dimension table (time, interconnectors, and energy sources)


## Pipeline
- **Data Lake**: Converting raw csv file into parquet for more efficient storage and processing.
- **Initial Cleaning**: Removing duplicates in parquet file before creating data model.
- **Database Aggregation**: Creating aggregation table per hour from parquet to ease dashboard processing and query analysing.
- **Data Modelling**: Based on one big aggregation table, create multiple small table following star schema model. This part involves creating unique primary key, data insertion, and table management, final product is 'datawarehouse.duckdb'.
- **Query Analysis**: Separating queries in one for analysis to improve code organization and readability. This queries will be called for visualization purposes using Streamlit.
- **Web Development**: Analysis is visualized using Plotly library while implementing 'concurrent.futures.ThreadPoolExecutor()' for concurrent and faster chart loading in the web app.

## Highlights and Findings
- Streamlit may not be the best tools to do visualization for a large dataset, and you should always thinking of strategies to visualize your data to the best potential.
- This dataset has around 1.4+ million rows which is quite huge for a simple project. Nevertheless, taking this project using DuckDB is a guide choice considering its performance on OLAP/analysis dashboard.
- I intend to use Streamlit for the visualization part, due to the fact that I never tried this tool as extensive as this project and also trying to improve my Python skill.
- Discussing my strategies, first improvement that I have done is aggregating the data to hour from unique timestamp, this would crammed all the 1.4+ million rows into around 100k rows.
- The justification is quite easy, it does not makes much sense to view the data per timestamp in the dashboard because the data point is just too small and also the chart will be very noisy.
- Other addition is rendering unique primary key for each table since the structure has changed and this is important part so that we can join the data altogether during analysis part.
- I also carefully craft the queries to align with Moving Average(MA) windowing, this easily allows the query to adjust for the user input in the web app to get users required Moving Average(MA).
- Furthermore, I try to implement concurrent library to render all the charts concurrently. My idea is that I do not like the charts being rendered consequentially, which takes quite sometime to load - thus implementing this thing will make the web app loads faster than normal.

## Improvements
- Dashboard aesthetics (obviously).
- 

