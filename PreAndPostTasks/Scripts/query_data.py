import os
import time
import duckdb
import datetime

def configure_views(con) -> None:
    """Configure all the base views to pull out specific reporting periods, phases and properties - this could be optimized to produce smaller files by limiting date ranges, regions, properties etc."""
    view_command = """CREATE OR REPLACE VIEW regional_generation_capacity AS
	SELECT
		key.*, 
		d.PeriodId, 
		d.Value, 
	FROM fullkeyinfo AS key
    INNER JOIN data d on d.SeriesId = key.SeriesId
	WHERE
        key.PeriodTypeName = 'Interval' AND
		key.PhaseName = 'ST' AND
		key.ParentClassName = 'System' AND
		key.ChildClassName = 'Generator' AND
		key.PropertyName IN ('Generation', 'Available Capacity');"""

#Querying specific properties can be accomplished by appending this line to previous view
#AND key.ChildObjectCategoryName IN ('Hydro Pump', 'Other', 'Solar', 'Wind', 'Biomass', 'Hard Coal', 'Fossil Gas', 'Waste', 'Fossil Oil', 'Lignite', 'Nuclear', 'Hydro')

    con.execute(view_command)
    
    view_command = """CREATE OR REPLACE VIEW region_aggregate_totals AS
    SELECT
        PhaseName, BandId, PeriodTypeName, 
        ParentObjectCategoryName, ParentObjectName, ChildObjectCategoryName, 
        ParentClassName, CollectionName, ChildClassName, PropertyName, UnitValue, TimesliceName, ModelName, SampleId, SampleName, PeriodId,
        SUM(Value) as TotalValue
    FROM regional_generation_capacity ag 
    GROUP BY 
        PhaseName, BandId, PeriodTypeName, 
        ParentObjectCategoryName, ParentObjectName, ChildObjectCategoryName, 
        ParentClassName, CollectionName, ChildClassName, PropertyName, UnitValue, TimesliceName, ModelName, SampleId, SampleName, PeriodId;"""
    
    con.execute(view_command)
    
    view_command = """CREATE OR REPLACE VIEW reporting_data AS
    SELECT 
        t.PhaseName, t.BandId, t.PeriodTypeName, 
        t.ParentObjectCategoryName, t.ParentObjectName, t.ChildObjectCategoryName, 
        t.ParentClassName, t.CollectionName, t.ChildClassName, t.PropertyName, t.UnitValue, t.TimesliceName, t.ModelName, t.SampleId, t.SampleName, 
        p.StartDate, p.EndDate, current_localtimestamp() as SolutionDate, 
        t.TotalValue
    FROM region_aggregate_totals t
    INNER JOIN Period p on p.PeriodId = t.PeriodId;"""
    
    con.execute(view_command)

def write_data(con, file_path: str, convert_csv: bool = False) -> None:
    """Persists properties to parquet, and optionally to csv"""
    start_time = time.time()
    query_command = f"COPY (select * from reporting_data) TO '{file_path}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100_000);"
    con.execute(query_command)
    execution_time = time.time() - start_time
    print(f"{file_path} produced in: {execution_time} seconds")
    
    if convert_csv:
        start_time = time.time()
        csv_path = file_path.replace(".parquet", ".csv")
        convert_command = f"""COPY (select * from '{file_path}') TO '{csv_path}' (DELIMITER ',');"""
        con.execute(convert_command)
        execution_time = time.time() - start_time
        print(f"{csv_path} produced in: {execution_time} seconds")

def main() -> None:
    try:
        output_path = os.environ.get('output_path', "/output") 
        duckFilePath = os.environ.get('duck_db_path', os.path.join(output_path, 'solution_views.ddb'))
        today = datetime.date.today()
        date_string = today.strftime("%Y-%m-%d")

        with duckdb.connect(duckFilePath) as con:

            start_time = time.time()
            
            configure_views(con)
            
            region_file_path = os.path.join(output_path, f"solution_data_{date_string}.parquet")
            write_data(con, region_file_path, convert_csv = False) #If CSV is required, enable it here 
            
            execution_time = time.time() - start_time
            print(f"Complete execution took: {execution_time} seconds")

    except Exception as e: 
        print('Task failed with exception:')
        print(e)
    finally:
        print("done")
    
if __name__ == "__main__":
    main()