import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, sql_expr, to_date, to_timestamp, when, to_char, cast, replace
from typing import List, Dict, Optional
from datetime import datetime
from snowflake.snowpark.types import TimestampType
from snowflake.snowpark.exceptions import SnowparkSQLException


WAREHOUSE_OBJECT = "DFA23RAWDATA"
DATABASE_NAME = "DATAFUSION"

sensor_data = f"{WAREHOUSE_OBJECT}.{DATABASE_NAME}.SensorDataStaging"
weather_data = f"{WAREHOUSE_OBJECT}.{DATABASE_NAME}.WeatherDataStaging"
soil_data = f"{WAREHOUSE_OBJECT}.{DATABASE_NAME}.SoilDataStaging"
crop_data = f"{WAREHOUSE_OBJECT}.{DATABASE_NAME}.CropDataStaging"
pest_data = f"{WAREHOUSE_OBJECT}.{DATABASE_NAME}.PestDataStaging"
irrigation = f"{WAREHOUSE_OBJECT}.{DATABASE_NAME}.IrrigationDataStaging"
location = f"{WAREHOUSE_OBJECT}.{DATABASE_NAME}.LocationDataStaging"

staging_tables = [sensor_data, weather_data, soil_data, crop_data, pest_data, irrigation, location]

def get_source_table(staging_table, convert_to_table: bool = False, session: Optional[snowpark.Session] = None):
    """ Gets the source table for this staging table. Converts the string to table if convert_to_table is True """

    table_path =  staging_table.replace("Staging", "Raw").replace(DATABASE_NAME, "RAWDATA")
    if convert_to_table & isinstance(session, snowpark.Session):
        return session.table(table_path)
    return table_path
        
    
source_tables = [get_source_table(staging_table) for staging_table in staging_tables]


def build_sql(table_name: str, table_dict: Dict):
    """ Builds sql command to generate a table from the given parameters """
    create_table_sql = f"CREATE OR REPLACE TABLE {table_name} (\n"
    columns_sql = []
    for column, data_type in table_dict.items():
        columns_sql.append(f"{column} {data_type}")
    create_table_sql += ",\n".join(columns_sql)
    create_table_sql += "\n);"

    return create_table_sql



def create_staging_tables(session: snowpark.Session):
    """ handles staging and moving data from source to staging tables """
        

    for staging_table in staging_tables:
        get_source_table(staging_table, convert_to_table=True, session = session).write.save_as_table(staging_table, mode="overwrite") 

def format_timestamp_column(df: snowpark.DataFrame, match_format: str = 'MM/DD/YYYY HH24:MI') -> snowpark.DataFrame:
    """ Formats the timestamp column to format ->'2023-05-01 10:30:00' """

    result = df.with_column("TIMESTAMP",sql_expr(f"to_char(to_timestamp(TIMESTAMP, '{match_format}'), 'YYYY-MM-DD HH24:MI:SS')"))
    return result

def write_to_staging_table(df: snowpark.DataFrame, table_name: str):
    """ Writes the resulting dataframe to a staging table """
    df.write.save_as_table(table_name, mode="overwrite")

def update_column_data_type(session: snowpark.Session,table, column, data_type):
    """ Updates the column data type 
    """
    
    update_statements = [
        f"alter table {table} add {column}_2 {data_type}",
        f"update {table} set {column}_2 = {column}",
        f"alter table {table} drop {column}",
        f"alter table {table} rename column {column}_2 to {column}"
    ]

    # Iterate through sql statements and execute them one by one
    for sql_statement in update_statements:
        # Execute statement
        try:
            session.sql(sql_statement).collect()
        except SnowparkSQLException:
            session.sql(f"alter table {table} drop {column}_2")
    
def update_column_name(session: snowpark.Session, table, column, new_column):
    """ Update the name of a column """

    session.sql(f"alter table {table} rename column {column} to  {new_column}").collect()

def strip_quotes(session: snowpark.Session, table, column):
    """ Removes the quote character from a timestamp so 
    that they can be casted as timestamp object without error '"""

    session.sql(f"""update {table} set {column}=REPLACE({column}, '"')""").collect()

def clean_sensor_id(session: snowpark.Session, table):
    """ Strips the unwanted part in the sensor id to get the main sensor_id value needed """
    
    session.sql(f"UPDATE {table} SET sensor_id = SUBSTRING(sensor_id, 5, 4)").collect()

def transform_staging_tables(session: snowpark.Session):
    """ processes and tarnsform the tables"""

    # Transform SensorDataStaging
    table = session.table(sensor_data)
    clean_na(session, sensor_data, table.columns)
    strip_quotes(session, sensor_data, "TIMESTAMP")
    strip_quotes(session, sensor_data, "SENSOR_ID")
    update_column_data_type(session, sensor_data, "TIMESTAMP", "TIMESTAMP")
    update_column_data_type(session, sensor_data, "SENSOR_ID", "VARCHAR(10)")
    update_column_data_type(session, sensor_data, "TEMPERATURE", "DECIMAL(5,2)")
    update_column_name(session, sensor_data, "TEMPERATURE", "TEMPERATURE_F")

    # Transform WeatherDataStaging
    table = session.table(weather_data)
    clean_na(session, weather_data, table.columns)
    table = format_timestamp_column(table)
    table.write.save_as_table(weather_data, mode="overwrite")
    update_column_data_type(session, weather_data, "TIMESTAMP", "TIMESTAMP")
    update_column_data_type(session, weather_data, "WIND_SPEED", "DECIMAL(5,2)")
    update_column_data_type(session, weather_data, "PRECIPITATION", "DECIMAL(5,2)")
    update_column_name(session, weather_data, "WIND_SPEED", "WIND_SPEED_MPH")
    update_column_name(session, weather_data, "PRECIPITATION", "PRECIPITATION_INCHES")

    # Transform SoilDataStaging
    table = session.table(soil_data)
    clean_na(session, soil_data, table.columns)
    table = format_timestamp_column(table)
    table.write.save_as_table(soil_data, mode="overwrite")
    update_column_data_type(session, soil_data, "TIMESTAMP", "TIMESTAMP")
    update_column_data_type(session, soil_data, "SOIL_MOISTURE", "DECIMAL(5,2)")
    update_column_data_type(session, soil_data, "SOIL_PH", "DECIMAL(4,2)")
    update_column_name(session, soil_data, "SOIL_MOISTURE", "SOIL_MOISTURE_PERCENT")
    
    # Transform CropDataStaging
    table = session.table(crop_data)
    clean_na(session, crop_data, table.columns)
    table = format_timestamp_column(table)
    table.write.save_as_table(crop_data, mode="overwrite")
    update_column_data_type(session, crop_data, "TIMESTAMP", "TIMESTAMP")
    update_column_data_type(session, crop_data, "CROP_TYPE", "VARCHAR(20)")
    update_column_data_type(session, crop_data, "CROP_YIELD", "DECINAL(8,2)")

    # Transform PestDataStaging
    table = session.table(pest_data)
    clean_na(session, pest_data, table.columns)
    table = format_timestamp_column(table)
    table.write.save_as_table(pest_data, mode="overwrite")
    update_column_data_type(session, pest_data, "PEST_TYPE", "VARCHAR(20)")
    update_column_data_type(session, pest_data, "PEST_DESCRIPTION", "TEXT")

    # Transform IrrigationDataStaging
    table = session.table(irrigation)
    clean_na(session, irrigation, table.columns)
    table = format_timestamp_column(table)
    table.write.save_as_table(irrigation, mode="overwrite")
    clean_sensor_id(session, irrigation)
    update_column_data_type(session, irrigation, "WATER_SOURCE", "VARCHAR(20)")
    update_column_data_type(session, irrigation, "IRRIGATION_METHOD", "VARCHAR(20)")


    # Transform LocationDataStaging
    table = session.table(location)
    clean_na(session, location, table.columns)
    clean_sensor_id(session, location)
    
    # Drop null rows
    session.sql(f"DELETE FROM {location} WHERE LOCATION_NAME IS NULL").collect()
    update_column_data_type(session, location, "LOCATION_NAME", "VARCHAR(30)")
    update_column_data_type(session, location, "LATITUDE", "DECIMAL(8,6)")
    update_column_data_type(session, location, "LONGITUDE", "DECIMAL(9,6)")
    update_column_data_type(session, location, "ELEVATION", "DECIMAL(8,2)")
    update_column_data_type(session, location, "REGION", "VARCHAR(20)")
    
    
    
def clean_na(session: snowpark.Session, table, columns: List[str]):
    """ Converts the NA in any column to sql null types """
    
    for column in columns:
        session.sql(f"update {table} set {column}=NULL where {column}='NA'").collect()
        
def create_dimention_tables(session:snowpark.Session):
    """ Creates all the dimention tables """

    # Create dimentions table Location
    session.sql(f"""
        CREATE OR REPLACE TABLE DFA23RAWDATA.DATAFUSION.DIMLOCATION AS (
        SELECT SENSOR_ID,
        LOCATION_NAME,
        REGION
        FROM {location});
    """).collect()

    # Create dimentions table Crop Type
    session.sql(f"""
        CREATE OR REPLACE TABLE DFA23RAWDATA.DATAFUSION.DIMCROPTYPE AS (
            SELECT TIMESTAMP,
            CROP_TYPE,
            GROWTH_STAGE,
            PEST_ISSUE
            FROM {crop_data});
    """).collect()

    # Create dimentions table Pest Type
    session.sql(f"""
        CREATE OR REPLACE TABLE DFA23RAWDATA.DATAFUSION.DIMPESTTYPE AS (
            SELECT TIMESTAMP,
            PEST_TYPE,
            PEST_DESCRIPTION,
            PEST_SEVERITY
            FROM {pest_data});
    """).collect()

    # Create dimentions table Irrigation method
    session.sql(f"""
        CREATE OR REPLACE TABLE DFA23RAWDATA.DATAFUSION.DIMIRRIGATIONMETHOD AS (
            SELECT SENSOR_ID,
            TIMESTAMP,
            WATER_SOURCE,
            IRRIGATION_METHOD
            FROM {irrigation});
    """).collect()

    # Create dimentions table Weather method
    session.sql(f"""
        CREATE OR REPLACE TABLE DFA23RAWDATA.DATAFUSION.DIMWEATHER AS (
            SELECT TIMESTAMP,
            WEATHER_CONDITION
            FROM {weather_data});
    """).collect()

    # Create dimentions table Soil method
    session.sql(f"""
        CREATE OR REPLACE TABLE DFA23RAWDATA.DATAFUSION.DIMSOIL AS (
            SELECT TIMESTAMP
            FROM {soil_data});
    """).collect()

def create_fact_table_timestamp(session: snowpark.Session):
    """ Creates the facts table for timestamps """

    session.sql(f"""
        CREATE OR REPLACE TABLE {WAREHOUSE_OBJECT}.{DATABASE_NAME}.FACTSTIMESTAMP AS (
            SELECT C.TIMESTAMP, S.SOIL_COMP, S.NITROGEN_LEVEL, 
                    S.PHOSPHORUS_LEVEL, S.ORGANIC_MATTER, 
                    S.SOIL_MOISTURE_PERCENT, S.SOIL_PH, 
                    W.WIND_SPEED_MPH, W.PRECIPITATION_INCHES
                
            FROM {crop_data} AS C
            FULL OUTER JOIN {pest_data} AS P
            ON C.TIMESTAMP=P.TIMESTAMP
            FULL OUTER JOIN {soil_data} AS S
            ON C.TIMESTAMP=S.TIMESTAMP
            FULL OUTER JOIN {weather_data} as W
            ON C.TIMESTAMP=W.TIMESTAMP
        )
    """).collect()

def create_fact_table_sensor_id(session: snowpark.Session):
    session.sql(f"""
        CREATE OR REPLACE TABLE DFA23RAWDATA.DATAFUSION.FACTSSENSORID AS (
            SELECT S.SENSOR_ID, S.TIMESTAMP,S.HUMIDITY,
                    S.SOIL_MOISTURE, S.LIGHT_INTENSITY, 
                    S.BATTERY_LEVEL, S.TEMPERATURE_F,
                    I.IRRIGATION_DURATION_MIN,
                    L.ELEVATION, L.LATITUDE, L.LONGITUDE
                
            FROM DFA23RAWDATA.DATAFUSION.SENSORDATASTAGING AS S
            
            FULL OUTER JOIN DFA23RAWDATA.DATAFUSION.IRRIGATIONDATASTAGING AS I
            ON S.SENSOR_ID=I.SENSOR_ID
            FULL OUTER JOIN DFA23RAWDATA.DATAFUSION.LOCATIONDATASTAGING AS L
            ON S.SENSOR_ID=L.SENSOR_ID
        )
    """).collect()
    
def main(session: snowpark.Session): 
    
    # Create staging tables
    create_staging_tables(session)

    # Transform staging tables
    transform_staging_tables(session)

    # Create dimenton tables
    create_dimention_tables(session)

    # Create fact tables timestamp
    create_fact_table_timestamp(session)

    # Create fact table sensor_id
    create_fact_table_sensor_id(session)
    
    
