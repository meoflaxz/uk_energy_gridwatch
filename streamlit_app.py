import concurrent.futures
import streamlit as st
import duckdb as duck
import pandas as pd
import plotly.express as px
from queries import *

# Refactor to fetch data in a reusable function
def fetch_data(query):
    with duck.connect('data_warehouse.duckdb') as con:
        return con.execute(query).fetchdf()

# Define each function to create its chart
def demand_over_time(window_size):
    df = fetch_data(demand_over_time_query())  
    df['smoothed_demand'] = df['total_demand'].rolling(window=window_size).mean()
    fig = px.line(df, x='time', y='smoothed_demand', title='Highly Smoothed Total Demand Over Time')
    fig.update_traces(line=dict(width=1.2))
    return "Demand Over Time", fig  # Return name and figure

def energy_contribution():
    df = fetch_data(energy_contribution_query())
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    energy_sources = [col for col in df.columns if col not in ['year', 'month', 'day']]
    df.set_index('date', inplace=True)
    yearly_data = df.resample('YE').sum().reset_index()
    fig = px.line(yearly_data, x='date', y=energy_sources, title='Yearly Energy Sources Contribution')
    return "Energy Contribution", fig  # Return name and figure

def demand_during_sleep():
    df = fetch_data(demand_during_sleep_query())
    fig = px.line(df, x='hour', y='avg_demand', title='Average Hourly Electricity Demand')
    return "Demand During Sleep", fig  # Return name and figure

def ict_visualization():
    df = fetch_data(ict_visualization_query())
    df_melted = df.melt(id_vars='year', value_vars=['avg_french_ict', 'avg_dutch_ict', 'avg_irish_ict',
                                                    'avg_nemo_belgium_ict', 'avg_other_generator',
                                                    'avg_north_south', 'avg_scotland_england', 'avg_ifa2',
                                                    'avg_intelec_ict', 'avg_norway_ict', 'avg_viking_ict'],
                        var_name='ICT', value_name='Average Flow (MWh)')
    fig = px.line(df_melted, x='year', y='Average Flow (MWh)', color='ICT', title='Yearly Average Flows for Each ICT')
    return "ICT Visualization", fig  # Return name and figure

def demand_v_production():
    df = fetch_data(demand_v_production_query())
    fig = px.line(df, x='year', y=['total_demand', 'total_production'],
                  title='Yearly Electricity Demand vs Production',
                  labels={'value': 'Electricity (MWh)', 'year': 'Year'},
                  color_discrete_sequence=['blue', 'orange'])
    fig.update_traces(mode='lines+markers')
    return "Demand vs Production", fig  # Return name and figure

# Function to run all the plots concurrently and display them in a specified order
def plot_concurrently(window_size):
    chart_sequence = ["Demand Over Time", "Energy Contribution", "Demand During Sleep", "ICT Visualization", "Demand vs Production"]
    results = {}

    # Use ThreadPoolExecutor for concurrent execution
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(demand_over_time, window_size): "Demand Over Time",
            executor.submit(energy_contribution): "Energy Contribution",
            executor.submit(demand_during_sleep): "Demand During Sleep",
            executor.submit(ict_visualization): "ICT Visualization",
            executor.submit(demand_v_production): "Demand vs Production"
        }

        # Store results in a dictionary by name
        for future in concurrent.futures.as_completed(futures):
            chart_name, fig = future.result()  # Unpack the tuple correctly
            results[chart_name] = fig

    # Display charts in the specified order
    for chart_name in chart_sequence:
        if chart_name in results:
            st.plotly_chart(results[chart_name])

# Get input for rolling window from user
window_size = st.sidebar.slider("Select Rolling Window Size", min_value=100, value=400, step=10)

# Call the function to plot all graphs concurrently and in the correct order
plot_concurrently(window_size)
