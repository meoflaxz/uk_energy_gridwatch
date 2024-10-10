import pandas as pd
import duckdb as duck

raw_data = 'gridwatch.parquet'
con = duck.connect('data_warehouse.duckdb')

def data_cleaning(raw_data):
    df = pd.read_parquet(raw_data)
    df.drop_duplicates(inplace=True)
    df.columns = df.columns.str.strip()
    return df

def create_schema_aggregate_main_table(con, df):

    aggregate_main_table_df = con.execute("""
        WITH data_aggregate AS (
            SELECT
                DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) AS time,
                SUM(demand) AS demand,
                AVG(frequency) AS avg_frequency,
                SUM(coal) AS coal,
                SUM(nuclear) AS nuclear,
                SUM(ccgt) AS ccgt,
                SUM(wind) AS wind,
                SUM(pumped) AS pumped,
                SUM(hydro) AS hydro,
                SUM(biomass) AS biomass,
                SUM(oil) AS oil,
                SUM(solar) AS solar,
                SUM(ocgt) AS ocgt,
                SUM(french_ict) AS french_ict,
                SUM(dutch_ict) AS dutch_ict,
                SUM(irish_ict) AS irish_ict,
                SUM(ew_ict) AS ew_ict,
                SUM(nemo) AS nemo,
                SUM(other) AS other,
                SUM(north_south) AS north_south,
                SUM(scotland_england) AS scotland_england,
                SUM(ifa2) AS ifa2,
                SUM(intelec_ict) AS intelec_ict,
                SUM(nsl) AS nsl,
                SUM(vkl_ict) AS vkl_ict
            FROM df
            GROUP BY time
            ORDER BY time
            )
            SELECT 
                ROW_NUMBER() OVER () AS timestamp_id,
                *
            FROM data_aggregate                   
            """).fetchdf()
    con.execute('''CREATE OR REPLACE TABLE data_warehouse.aggregate_main_table (
                        timestamp_id BIGINT PRIMARY KEY,
                        time TIMESTAMP,
                        demand DOUBLE,
                        avg_frequency DOUBLE,
                        coal DOUBLE,
                        nuclear DOUBLE,
                        ccgt DOUBLE,
                        wind DOUBLE,
                        pumped DOUBLE,
                        hydro DOUBLE,
                        biomass DOUBLE,
                        oil DOUBLE,
                        solar DOUBLE,
                        ocgt DOUBLE,
                        french_ict DOUBLE,
                        dutch_ict DOUBLE,
                        irish_ict DOUBLE,
                        ew_ict DOUBLE,
                        nemo DOUBLE,
                        other DOUBLE,
                        north_south DOUBLE,
                        scotland_england DOUBLE,
                        ifa2 DOUBLE,
                        intelec_ict DOUBLE,
                        nsl DOUBLE,
                        vkl_ict DOUBLE);''')
    con.execute('''INSERT INTO data_warehouse.aggregate_main_table 
                        SELECT * FROM aggregate_main_table_df
                        ON CONFLICT (timestamp_id) DO UPDATE SET
                            time = excluded.time,
                            demand = excluded.demand,
                            avg_frequency = excluded.avg_frequency,
                            coal = excluded.coal,
                            nuclear = excluded.nuclear,
                            ccgt = excluded.ccgt,
                            wind = excluded.wind,
                            pumped = excluded.pumped,
                            hydro = excluded.hydro,
                            biomass = excluded.biomass,
                            oil = excluded.oil,
                            solar = excluded.solar,
                            ocgt = excluded.ocgt,
                            french_ict = excluded.french_ict,
                            dutch_ict = excluded.dutch_ict,
                            irish_ict = excluded.irish_ict,
                            ew_ict = excluded.ew_ict,
                            nemo = excluded.nemo,
                            other = excluded.other,
                            north_south = excluded.north_south,
                            scotland_england = excluded.scotland_england,
                            ifa2 = excluded.ifa2,
                            intelec_ict = excluded.intelec_ict,
                            nsl = excluded.nsl,
                            vkl_ict = excluded.vkl_ict''')
    return aggregate_main_table_df

def create_schema_time_table(con, aggregate_main_table):

    dim_time_table_df = con.execute(""" SELECT
                                        ROW_NUMBER() OVER () AS time_id,
                                        timestamp_id,
                                        time,
                                        EXTRACT(YEAR FROM time) AS year,
                                        EXTRACT(MONTH FROM time) AS month,
                                        EXTRACT(DAY FROM time) AS day,
                                        EXTRACT(HOUR FROM time) AS hour,
                                    FROM data_warehouse.aggregate_main_table""").fetchdf()
    
    con.execute('''CREATE OR REPLACE TABLE data_warehouse.dim_time_table (
                    time_id BIGINT PRIMARY KEY,
                    timestamp_id BIGINT,
                    time TIMESTAMP,
                    year BIGINT,
                    month BIGINT,
                    day BIGINT,
                    hour BIGINT
                    );''')

    con.execute('''INSERT INTO data_warehouse.dim_time_table
                SELECT * FROM dim_time_table_df
                ON CONFLICT (time_id) DO UPDATE SET
                    time = excluded.time,
                    year = excluded.year,
                    month = excluded.month,
                    day = excluded.day,
                    hour = excluded.hour
                ''')
    return dim_time_table_df

def create_schema_energy_table(con, aggregate_main_table):


    dim_energy_table_df = con.execute("""
            SELECT
                ROW_NUMBER() OVER () AS energy_id,
                timestamp_id,
                coal,
                nuclear,
                ccgt,
                wind,
                pumped,
                hydro,
                biomass,
                oil,
                solar,
                ocgt
            FROM aggregate_main_table
    """).fetchdf()
    con.execute('''CREATE OR REPLACE TABLE data_warehouse.dim_energy_table (
                        energy_id BIGINT PRIMARY KEY,
                        timestamp_id BIGINT,
                        coal DOUBLE,
                        nuclear DOUBLE,
                        ccgt DOUBLE,
                        wind DOUBLE,
                        pumped DOUBLE,
                        hydro DOUBLE,
                        biomass DOUBLE,
                        oil DOUBLE,
                        solar DOUBLE,
                        ocgt DOUBLE);
                    ''')
    
    con.execute('''INSERT INTO data_warehouse.dim_energy_table
                    SELECT * FROM dim_energy_table_df
                    ON CONFLICT (energy_id) DO UPDATE SET
                        coal = excluded.coal,
                        nuclear = excluded.nuclear,
                        ccgt = excluded.ccgt,
                        wind = excluded.wind,
                        pumped = excluded.pumped,
                        hydro = excluded.hydro,
                        biomass = excluded.biomass,
                        oil = excluded.oil,
                        solar = excluded.solar,
                        ocgt = excluded.ocgt''')
    return dim_energy_table_df

def create_schema_interconnectors_table(con, aggregate_main_table):

    dim_ict_table_df = con.execute("""
            SELECT
                ROW_NUMBER() OVER () AS ict_id,
                timestamp_id,
                french_ict,
                dutch_ict,
                irish_ict,
                ew_ict AS east_west_ict,
                nemo AS nemo_belgium_ict,
                other AS other_generator,
                north_south,
                scotland_england,
                ifa2,
                intelec_ict,
                nsl AS norway_ict,
                vkl_ict AS viking_ict
            FROM aggregate_main_table
        """).fetchdf()
    con.execute('''CREATE OR REPLACE TABLE data_warehouse.dim_ict_table (
                        ict_id BIGINT PRIMARY KEY,
                        timestamp_id BIGINT,
                        french_ict DOUBLE,
                        dutch_ict DOUBLE,
                        irish_ict DOUBLE,
                        east_west_ict DOUBLE,
                        nemo_belgium_ict DOUBLE,
                        other_generator DOUBLE,
                        north_south DOUBLE,
                        scotland_england DOUBLE,
                        ifa2 DOUBLE,
                        intelec_ict DOUBLE,
                        norway_ict DOUBLE,
                        viking_ict DOUBLE)''')
    
    con.execute('''INSERT INTO data_warehouse.dim_ict_table
                    SELECT * FROM dim_ict_table_df
                    ON CONFLICT (ict_id) DO UPDATE SET
                        french_ict = excluded.french_ict,
                        dutch_ict = excluded.dutch_ict,
                        irish_ict = excluded.irish_ict,
                        east_west_ict = excluded.east_west_ict,
                        nemo_belgium_ict = excluded.nemo_belgium_ict,
                        other_generator = excluded.other_generator,
                        north_south = excluded.north_south,
                        scotland_england = excluded.scotland_england,
                        ifa2 = excluded.ifa2,   
                        intelec_ict = excluded.intelec_ict,
                        norway_ict = excluded.norway_ict,
                        viking_ict = excluded.viking_ict;''')
    return dim_ict_table_df

def create_schema_fact_table(con, aggregate_main_table, time_table, energy_sources):

    fact_gridwatch_df = con.execute("""
            WITH main AS (
                SELECT
                    dim_time_table.time_id,
                    dim_energy_table.energy_id,
                    dim_ict_table.ict_id,
                    aggregate_main_table.demand AS total_demand,
                    aggregate_main_table.avg_frequency AS avg_frequency,
                FROM dim_time_table
                JOIN dim_energy_table
                    ON dim_time_table.timestamp_id = dim_energy_table.timestamp_id
                JOIN dim_ict_table
                    ON dim_time_table.timestamp_id = dim_ict_table.timestamp_id
                JOIN aggregate_main_table
                    ON dim_time_table.timestamp_id = aggregate_main_table.timestamp_id
            )
            SELECT
                ROW_NUMBER() OVER () AS fact_id,
                *
            FROM main
    """).fetchdf()
    con.execute('''CREATE OR REPLACE TABLE data_warehouse.fact_table AS
                SELECT * FROM fact_gridwatch_df''')
    return fact_gridwatch_df


def get_fact_gridwatch():

    con = duck.connect('data_warehouse.duckdb')
    
    try:
        bigtable = data_cleaning(raw_data)
        aggregate_main_table = create_schema_aggregate_main_table(con, bigtable)
        time_table = create_schema_time_table(con, aggregate_main_table)
        energy_sources = create_schema_energy_table(con, aggregate_main_table)
        interconnectors = create_schema_interconnectors_table(con, aggregate_main_table)
        
        fact_gridwatch = create_schema_fact_table(con, time_table, energy_sources, interconnectors)
        
        return fact_gridwatch 
    finally:
        con.close()


# This block is for testing purposes
if __name__ == "__main__":
    fact_gridwatch = get_fact_gridwatch()
    print(fact_gridwatch)


