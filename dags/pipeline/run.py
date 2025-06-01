# Import necessary modules from Airflow
from airflow.decorators import dag, task_group  # DAG and task group decorators for defining workflows
from airflow.operators.python import PythonOperator  # PythonOperator to execute Python functions
from airflow.operators.postgres_operator import PostgresOperator  # PostgresOperator to run SQL queries
from datetime import datetime  # For defining the start date of the DAG
from pathlib import Path  # For reading SQL files from the filesystem

# Import the Extract and Load classes from the pipeline
from pipeline.assets.extract import Extract  # Extract class to pull data from the database
from pipeline.assets.load import Load  # Load class to insert data into the database

# ========== Task Groups ==========

# Task group for extracting data from specified tables
@task_group(group_id="extract_group")
def extract_group(table_list):
    """
    Task group for extracting data from multiple tables.
    
    Args:
        table_list (list): List of table names to extract data from.
    """
    # Loop over each table in the table_list and create a PythonOperator for each extraction task
    for table in table_list:
        PythonOperator(
            task_id=f"extract_{table}",
            python_callable=Extract._pacflight_db,  # Python function to extract data from the database
            op_kwargs={'table_name': table}  # Pass the table name as a parameter
        )

# Task group for loading data into the warehouse
@task_group(group_id="load_group")
def load_group(table_list, table_pkey):
    """
    Task group for loading data into the database.
    
    Args:
        table_list (list): List of table names to load data into.
        table_pkey (dict): Dictionary containing primary keys for each table.
    """
    load_tasks = []  # List to store all load tasks

    # Loop through each table and create a PythonOperator to load data
    for table in table_list:
        task = PythonOperator(
            task_id=f"load_{table}",
            python_callable=Load._pacflight_db,  # Python function to load data into the database
            op_kwargs={
                'table_name': table,  # Pass table name to the load function
                'table_pkey': table_pkey  # Pass table primary keys
            }
        )
        load_tasks.append(task)  # Add the task to the load_tasks list

    # Set sequential dependencies for the load tasks, ensuring they run one after the other
    for i in range(1, len(load_tasks)):
        load_tasks[i - 1] >> load_tasks[i]

# Task group for transforming data using SQL queries
@task_group(group_id="transform_group")
def transform_group(transform_tables):
    """
    Task group for transforming data using SQL queries stored in files.
    
    Args:
        transform_tables (list): List of tables that require transformation.
    """
    # Loop over each table and create a PostgresOperator to run the transformation SQL
    for table in transform_tables:
        sql_file_path = f"/opt/airflow/dags/pipeline/models/final/{table}.sql"  # Path to the SQL file
        try:
            # Read the SQL query content from the file
            sql_content = Path(sql_file_path).read_text()
        except FileNotFoundError:
            # Raise an error if the SQL file is not found for a table
            raise ValueError(f"SQL file not found for table: {table}")
        
        # Create a PostgresOperator to execute the SQL transformation query
        PostgresOperator(
            task_id=f"transform_{table}",  # Task ID for transformation task
            postgres_conn_id='warehouse_db',  # Connection ID for the PostgreSQL database
            sql=sql_content  # SQL query to execute
        )

# ========== Main DAG ==========

# Define the main DAG for the flights data pipeline
@dag(
    dag_id='flights_data_pipeline',  # DAG ID
    start_date=datetime(2025, 5, 15),  # Start date for the DAG
    schedule_interval='@daily',  # Set the DAG to run daily
    catchup=False,  # Disable catchup for missed runs
    tags=['pacflight', 'ETL']  # Tags for categorizing the DAG
)
def flights_data_pipeline():
    """
    Main flights data pipeline DAG.
    Defines the tables, task groups, and dependencies.
    """
    # List of tables to extract and load data for
    table_list = [
        'aircrafts_data',
        'airports_data',
        'bookings',
        'tickets',
        'seats',
        'flights',
        'ticket_flights',
        'boarding_passes'
    ]

    # Dictionary mapping each table to its primary key(s)
    table_pkey = {
        "aircrafts_data": "aircraft_code",
        "airports_data": "airport_code",
        "bookings": "book_ref",
        "tickets": "ticket_no",
        "seats": ["aircraft_code", "seat_no"],
        "flights": "flight_id",
        "ticket_flights": ["ticket_no", "flight_id"],
        "boarding_passes": ["ticket_no", "flight_id"]
    }

    # List of tables requiring data transformation
    transform_tables = [
        'dim_aircrafts', 'dim_airports', 'dim_seats', 'dim_passengers',
        'fct_boarding_pass', 'fct_booking_ticket',
        'fct_seat_occupied_daily', 'fct_flight_activity'
    ]

    # Define task groups for extracting, loading, and transforming data
    extract = extract_group(table_list)
    load = load_group(table_list, table_pkey)
    transform = transform_group(transform_tables)

    # Set task dependencies to ensure extraction runs first, followed by loading, then transformation
    extract >> load >> transform

# Execute the DAG
flights_data_pipeline()
