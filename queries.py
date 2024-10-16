# queries.py

# calculate demand of electricity over time
def demand_over_time_query():
    return """
        SELECT 
            dim_time_table.year,
            dim_time_table.month,
            dim_time_table.day,
            AVG(fact_table.total_demand) AS total_demand
        FROM data_warehouse.fact_table AS fact_table
        JOIN data_warehouse.dim_time_table AS dim_time_table
            ON fact_table.time_id = dim_time_table.time_id
        GROUP BY dim_time_table.year, dim_time_table.month, dim_time_table.day
        ORDER BY dim_time_table.year, dim_time_table.month, dim_time_table.day
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

def demand_during_sleep_query():
    return """
            SELECT 
                dim_time_table.hour,
                AVG(fact_table.total_demand) AS avg_demand
            FROM data_warehouse.fact_table
            JOIN data_warehouse.dim_time_table
                ON fact_table.time_id = dim_time_table.timestamp_id
            GROUP BY hour
            ORDER BY hour
            """

def ict_visualization_query():
    return """
        SELECT
            dim_time_table.year,
            dim_time_table.month,
            dim_time_table.day,
            AVG(dim_ict_table.french_ict) AS french_ict,
            AVG(dim_ict_table.dutch_ict) AS dutch_ict,
            AVG(dim_ict_table.irish_ict) AS irish_ict,
            AVG(dim_ict_table.nemo_belgium_ict) AS nemo_belgium_ict,
            AVG(dim_ict_table.other_generator) AS other_generator,
            AVG(dim_ict_table.north_south) AS north_south,
            AVG(dim_ict_table.scotland_england) AS scotland_england,
            AVG(dim_ict_table.ifa2) AS ifa2,
            AVG(dim_ict_table.intelec_ict) AS intelec_ict,
            AVG(dim_ict_table.norway_ict) AS norway_ict,
            AVG(dim_ict_table.viking_ict) AS viking_ict
        FROM data_warehouse.fact_table
        JOIN data_warehouse.dim_time_table
            ON fact_table.time_id = dim_time_table.timestamp_id
        JOIN data_warehouse.dim_ict_table 
            ON fact_table.time_id = dim_ict_table.timestamp_id 
        GROUP BY dim_time_table.year, dim_time_table.month, dim_time_table.day
        ORDER BY dim_time_table.year, dim_time_table.month, dim_time_table.day
        """

def demand_v_production_query():
    return """
    SELECT 
        dim_time_table.year,
        dim_time_table.month,
        dim_time_table.day,
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
    GROUP BY dim_time_table.year, dim_time_table.month, dim_time_table.day
    ORDER BY dim_time_table.year, dim_time_table.month, dim_time_table.day;
    """