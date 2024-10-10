# queries.py

# calculate demand of electricity over time
def demand_over_time_query():
    return """
        SELECT 
            dim_time_table.time,
            AVG(fact_table.total_demand) AS total_demand
        FROM data_warehouse.fact_table AS fact_table
        JOIN data_warehouse.dim_time_table AS dim_time_table
            ON fact_table.time_id = dim_time_table.time_id
        GROUP BY dim_time_table.time
        ORDER BY dim_time_table.time
    """

# calculate trend of enery over time
def energy_contribution_query():
    return """
        SELECT 
            dim_time_table.year,
            dim_time_table.month,
            dim_time_table.day,
            SUM(dim_energy_table.coal) AS coal,
            SUM(dim_energy_table.nuclear) AS nuclear,
            SUM(dim_energy_table.ccgt) AS ccgt,
            SUM(dim_energy_table.wind) AS wind,
            SUM(dim_energy_table.solar) AS solar,
            SUM(dim_energy_table.pumped) AS pumped,
            SUM(dim_energy_table.hydro) AS hydro,
            SUM(dim_energy_table.biomass) AS biomass,
            SUM(dim_energy_table.oil) AS oil,
            SUM(dim_energy_table.ocgt) AS ocgt
        FROM data_warehouse.fact_table AS fact_table
        JOIN data_warehouse.dim_time_table AS dim_time_table
            ON fact_table.time_id = dim_time_table.time_id
        JOIN data_warehouse.dim_energy_table AS dim_energy_table
            ON fact_table.energy_id = dim_energy_table.energy_id
        GROUP BY dim_time_table.year, dim_time_table.month, dim_time_table.day
        ORDER BY dim_time_table.year, dim_time_table.month, dim_time_table.day
    """

def ict_contribution_query():
    return """
        SELECT 
            dim_time_table.year,
            dim_time_table.month,
            dim_time_table.day,
            SUM(dim_ict_table.coal) AS coal,
            SUM(dim_ict_table.nuclear) AS nuclear,
            SUM(dim_ict_table.ccgt) AS ccgt,
            SUM(dim_ict_table.wind) AS wind,
            SUM(dim_ict_table.solar) AS solar,
            SUM(dim_ict_table.pumped) AS pumped,
            SUM(dim_ict_table.hydro) AS hydro,
            SUM(dim_ict_table.biomass) AS biomass,
            SUM(dim_ict_table.oil) AS oil,
            SUM(dim_ict_table.ocgt) AS ocgt
        FROM data_warehouse.fact_table AS fact_table
        JOIN data_warehouse.dim_time_table AS dim_time_table
            ON fact_table.time_id = dim_time_table.time_id
        JOIN data_warehouse.dim_ict_table AS dim_ict_table
        """

def demand_during_sleep_query():

    sleep_start_hour = 22  # 10 PM
    sleep_end_hour = 6      # 6 AM
    return """
            SELECT 
                EXTRACT(HOUR FROM time) AS hour,
                AVG(demand) AS avg_demand
            FROM data_warehouse.aggregate_main_table
            GROUP BY hour
            ORDER BY hour
            """

def ict_visualization_query():
    return """
        SELECT
            dt.year,
            AVG(ict.french_ict) AS avg_french_ict,
            AVG(ict.dutch_ict) AS avg_dutch_ict,
            AVG(ict.irish_ict) AS avg_irish_ict,
            AVG(ict.east_west_ict) AS avg_east_west_ict,
            AVG(ict.nemo_belgium_ict) AS avg_nemo_belgium_ict,
            AVG(ict.other_generator) AS avg_other_generator,
            AVG(ict.north_south) AS avg_north_south,
            AVG(ict.scotland_england) AS avg_scotland_england,
            AVG(ict.ifa2) AS avg_ifa2,
            AVG(ict.intelec_ict) AS avg_intelec_ict,
            AVG(ict.norway_ict) AS avg_norway_ict,
            AVG(ict.viking_ict) AS avg_viking_ict
        FROM data_warehouse.fact_table AS f
        JOIN data_warehouse.dim_time_table AS dt
            ON f.time_id = dt.timestamp_id
        JOIN data_warehouse.dim_ict_table AS ict
            ON f.time_id = ict.timestamp_id 
        GROUP BY dt.year
        ORDER BY dt.year;
        """

def demand_v_production_query():
    return """
    SELECT 
        dim_time_table.year, 
        SUM(fact_table.total_demand) AS total_demand,
        SUM(p.coal + p.nuclear + p.ccgt + p.wind + p.pumped + p.hydro + 
            p.biomass + p.oil + p.solar + p.ocgt + ict.french_ict + ict.dutch_ict + ict.irish_ict + ict.nemo_belgium_ict + ict.other_generator +
            ict.north_south + ict.scotland_england + ict.ifa2 + ict.intelec_ict + ict.norway_ict + ict.viking_ict) AS total_production,
        AVG(fact_table.avg_frequency) AS avg_frequency 
    FROM fact_table          
    JOIN dim_energy_table AS p
        ON fact_table.time_id = p.timestamp_id
    JOIN dim_time_table
        ON fact_table.time_id = dim_time_table.timestamp_id
    JOIN dim_ict_table AS ict
        ON fact_table.time_id = ict.timestamp_id
    GROUP BY dim_time_table.year
    ORDER BY dim_time_table.year;
    """