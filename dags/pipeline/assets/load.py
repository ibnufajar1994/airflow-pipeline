# Import necessary libraries
from airflow.providers.postgres.hooks.postgres import PostgresHook  # Postgres hook for connecting to PostgreSQL
from airflow.exceptions import AirflowSkipException, AirflowException  # Airflow exceptions for error handling
from helper.minio import CustomMinio  # CustomMinio class for interacting with MinIO storage
import logging  # Logging library for tracking events
import json  # JSON library for working with JSON data
import ast  # AST library for safely evaluating strings as Python expressions
import pandas as pd  # Pandas for data manipulation and handling DataFrame

# Execute class to handle data insertion tasks into PostgreSQL database
class Execute:
    # Static method to insert DataFrame data into PostgreSQL using a query from a file
    @staticmethod
    def _insert_dataframe(connection_id, query_path, dataframe):
        BASE_PATH = "/opt/airflow/dags"  # Path to store DAG-related files

        # Create a PostgresHook instance to connect to the database
        pg_hook = PostgresHook(postgres_conn_id=connection_id)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        full_path = f'{BASE_PATH}/{query_path}'  # Construct the full path to the query file
        # Open and read the query from the file
        with open(full_path, 'r') as file:
            query = file.read()

        try:
            # Iterate over each row in the DataFrame
            for _, row in dataframe.iterrows():
                record = row.to_dict()  # Convert row to a dictionary

                # Convert dictionary-style strings to JSON-compatible format
                for key, value in record.items():
                    if isinstance(value, str):
                        try:
                            # If the value starts with '{', attempt to parse and reformat as valid JSON
                            record[key] = json.dumps(json.loads(value)) if value.startswith('{') else value
                        except json.JSONDecodeError:
                            # If not valid JSON, leave the value unchanged
                            pass

                # Execute the query with the parameters (sanitized data)
                pg_hook.run(query, parameters=record)

        except Exception as e:
            # Log any errors encountered during execution
            logging.error(f"Error executing query: {e}")
            raise AirflowException(f"Error when loading data: {str(e)}")

        finally:
            # Ensure the cursor and connection are properly closed after execution
            cursor.close()
            connection.commit()
            connection.close()


# Load class to handle data loading tasks from MinIO and transformation
class Load:
    # Static method to load data from MinIO, transform it, and insert it into PostgreSQL
    @staticmethod
    def _pacflight_db(table_name, **kwargs):
        # Log the start of the load process
        logging.info(f"[Load] Starting full load for table: {table_name}")

        try:
            object_name = f'/temp/{table_name}.csv'  # Define the object name in MinIO
            bucket_name = 'flights-data-pipeline'  # Define the MinIO bucket name

            # Log the download process
            logging.info(f"[Load] Downloading {object_name} from {bucket_name}")
            # Download the CSV file from MinIO and load it into a DataFrame
            df = CustomMinio._get_dataframe(bucket_name, object_name)

            # Check if the DataFrame is empty and skip if no data is found
            if df.empty:
                raise AirflowSkipException(f"{table_name} has no data to load. Skipped...")

            # Data transformation specific to certain tables
            if table_name == 'aircrafts_data':
                # Apply transformation to 'model' column using JSON-compatible format
                df['model'] = df['model'].apply(lambda x: json.dumps(ast.literal_eval(x)) if isinstance(x, str) else x)

            if table_name == 'airports_data':
                # Apply transformation to 'airport_name' and 'city' columns
                df['airport_name'] = df['airport_name'].apply(lambda x: json.dumps(ast.literal_eval(x)) if isinstance(x, str) else x)
                df['city'] = df['city'].apply(lambda x: json.dumps(ast.literal_eval(x)) if isinstance(x, str) else x)

            if table_name == 'tickets':
                # Apply transformation to 'contact_data' column
                df['contact_data'] = df['contact_data'].apply(lambda x: json.dumps(ast.literal_eval(x)) if isinstance(x, str) else x)

            if table_name == 'flights':
                # Replace NaN values with None in the DataFrame
                df = df.replace({float('nan'): None})

            # Define the path to the SQL query for inserting the data
            query_path = f"pipeline/models/stg/{table_name}.sql"

            # Execute the insert query to load the data into PostgreSQL
            Execute._insert_dataframe(
                connection_id="warehouse_db",
                query_path=query_path,
                dataframe=df
            )

            # Log the successful completion of the load process
            logging.info(f"[Load] Full load completed for table: {table_name}")

        except AirflowSkipException as e:
            # Log and raise AirflowSkipException if no data is found to load
            logging.warning(f"[Load] Skipped loading for {table_name}: {str(e)}")
            raise e

        except Exception as e:
            # Log any errors encountered during the load process
            logging.error(f"[Load] Failed loading {table_name}: {str(e)}")
            raise AirflowException(f"Error when loading {table_name} : {str(e)}")
