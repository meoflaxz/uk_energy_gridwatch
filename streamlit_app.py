import concurrent.futures
import streamlit as st
import duckdb as duck
import pandas as pd
import plotly.express as px
from queries import *

#######################
# Page configuration
st.set_page_config(
    page_title="Energy Demand Dashboard",
    page_icon="⚡",
    initial_sidebar_state="expanded"
)

st.title("UK Electricity Demand Dashboard")
st.markdown("""
### Introduction

Welcome to the **UK Electricity Demand Dashboard**, this dashboard provide insights into electricity consumption patterns and energy source contributions in the UK. The data is sourced from Gridwatch, an open-source dataset that tracks real-time electricity demand data for the UK.

""")
st.markdown("<br>", unsafe_allow_html=True)

def fetch_data(query):
    with duck.connect('data_warehouse.duckdb') as con:
        return con.execute(query).fetchdf()

def demand_over_time(window_size):
    df = fetch_data(demand_over_time_query())
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    df.set_index('date', inplace=True)
    df_daily = df.resample('D').mean()
    df_daily['smoothed_demand'] = df_daily['total_demand'].rolling(window=window_size).mean()
    df_daily.reset_index(inplace=True)

    fig = px.line(df_daily, x='date', y='smoothed_demand', 
                  title=f'Electricity demand vs Time ({window_size}-Day Moving Average)',
                  labels={'smoothed_demand': 'Electricity  Demand (GW)', 'date': 'Time'})
    fig.update_traces(line=dict(width=1.2))

    return "Total Electricity Demand Over Time", fig, f"This chart shows the total electricity demand over time, smoothed using a {window_size}-day moving average. \
            Throughout the time, we can see overall the electricity demand is growing smaller each year. \
            We can also observed significant dip during year 2020, probably due to the pandemic COVID-19 that impacted the electricity supply."

def energy_contribution(window_size):
    df = fetch_data(energy_contribution_query())
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    df.set_index('date', inplace=True)
    rolling_data = df.rolling(window_size).mean()
    rolling_data.reset_index(inplace=True)

    fig = px.line(  rolling_data, x='date', y=rolling_data.columns[4:], 
                    title=f'Energy sources comparison ({window_size}-Day Moving Average)',
                    labels={'value': 'Energy', 'date': 'Time'})
    
    return "Average Energy Sources Trend", fig, f"This chart shows the contribution of various energy sources over time using a {window_size}-day moving average. \
                                                    There is a clear downtrend of non renewable energy sources such as coal and oil. \
                                                    While at the same time there is an uptrend of renewable energy sources such as solar, biomass and wind. \
                                                    Overall majority of renewable energy sources have a consistent increasing growing trend."

def demand_during_sleep():
    df = fetch_data(demand_during_sleep_query())
    fig = px.line(df, x='hour', y='avg_demand')
    return "Electricity Demand Per Day", fig, "This chart depicts the average electricity demand during sleep hours . \
        This simple chart illustrate the polar relationship between electricity every day. \
            It can be clearly seen that the electricity demand starts to spike around 9am which is commonly the start working hours and peaked at around 6pm of the day. \
                The electricity demand then will drop aligning with them time of people sleeping. \
                     The demand also have little dip around 12pm-2pm which marks the common time of people having a break from their work. "

def ict_visualization(window_size):
    df = fetch_data(ict_visualization_query())
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    df.set_index('date', inplace=True)
    rolling_data = df.rolling(window_size).mean()
    rolling_data.reset_index(inplace=True)
    fig = px.line(  rolling_data, x='date', y=rolling_data.columns[4:], 
                    title=f'Average Interconnector Flows Over Time ({window_size}-Day Moving Average)',
                    labels={'value': 'Average Flow', 'date': 'Date'})
    
    return "Interconnectors Comparison", fig, "This chart shows the average flows of various interconnectors over time. \
        This chart illustrate the flows of in and out of interconnectors in the UK. \
            We can also see north south interconnector contribute the highest average amount of electricity."

def demand_v_production(window_size):
    df = fetch_data(demand_v_production_query())
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    numeric_columns = ['total_demand', 'total_production', 'avg_frequency']
    rolling_data = df[numeric_columns].rolling(window_size).mean()
    rolling_data['date'] = df['date']
    rolling_data.reset_index(drop=False, inplace=True)

    fig = px.line(  rolling_data, x='date', y=['total_demand', 'total_production'],
                    title=f'Yearly Electricity Demand vs Production ({window_size}-Day Moving Average)',
                    labels={'value': 'Electricity (GW)', 'date': 'Date'},
                    color_discrete_sequence=['blue', 'orange'])
    return "Demand vs Production", fig, "This chart compares the yearly electricity demand against the total production. \
        Overall bigger picture is for quite some time, total electricity produce is larger than total demand needed which is always good in case of supplying backup electricity due to blackout. \
            But it started around 2016 which coincidently is when north south interconnector coming into play. \
                This probably helped UK grid stabilizes the electricty to overcome the demand spikes of it."

def plot_concurrently(window_size):
    chart_sequence = ["Total Electricity Demand Over Time", "Average Energy Sources Trend", "Demand vs Production", "Electricity Demand Per Day", "Interconnectors Comparison"]
    results = {}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(demand_over_time, window_size): "Total Electricity Demand Over Time",
            executor.submit(energy_contribution, window_size): "Average Energy Sources Trend",
            executor.submit(demand_v_production, window_size): "Demand vs Production",
            executor.submit(demand_during_sleep): "Electricity Demand Per Day",
            executor.submit(ict_visualization, window_size): "Interconnectors Comparison",
        }

        for future in concurrent.futures.as_completed(futures):
            chart_name, fig, description = future.result() 
            results[chart_name] = (fig, description)

    for chart_name in chart_sequence:
        if chart_name in results:

            st.header(chart_name)
            fig, description = results[chart_name]  
            
            st.plotly_chart(fig) 
            st.markdown(f"""
            <div style='padding: 10px; border: 1px solid #ccc; border-radius: 5px; background-color: #f9f9f9; margin-bottom: 20px;'>
                <h4>{chart_name}</h4>
                <p>{description}</p>
            </div>
            """, unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True) 
            
st.sidebar.header("Settings")
window_size = st.sidebar.slider("Choose desired Moving Average (MA)", min_value=10, max_value=100, value=50, step=10)

plot_concurrently(window_size)