# Import necessary libraries and modules
from airflow.exceptions import AirflowException, AirflowSkipException  # Airflow exceptions for error handling
from airflow.providers.postgres.hooks.postgres import PostgresHook  # Postgres hook for connecting to PostgreSQL database
from datetime import timedelta  # Timedelta for working with time-related operations
from helper.minio import MinioClient, CustomMinio  # Minio client for interacting with Minio storage
import logging  # Logging library for logging events
import pandas as pd  # Pandas for data manipulation and handling DataFrame

# Extract class to handle data extraction tasks
class Extract:
    # Static method to extract data from a table in the Pacflight database and save it to MinIO
    @staticmethod
    def _pacflight_db(table_name, **kwargs):
        """
        Retrieve all data from the Pacflight database (non-incremental extraction).

        Parameters:
            table_name (str): The name of the table from which to extract data.
            **kwargs: Additional keyword arguments, if any.

        Exceptions:
            AirflowException: Raised if the data extraction from the Pacflight database fails.
            AirflowSkipException: Raised if no data is found in the specified table.
        """

        # Log the start of the extraction process
        logging.info(f"[Extract] Starting extraction for table: {table_name}")
        try:
            # Create a connection to the Pacflight PostgreSQL database using the PostgresHook
            pg_hook = PostgresHook(postgres_conn_id='aircraft_db')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            # Define the SQL query to select all data from the specified table
            query = f"SELECT * FROM bookings.{table_name};"
            # Define the MinIO object name where the data will be stored
            object_name = f'/temp/{table_name}.csv'

            # Log the query being executed
            logging.info(f"[Extract] Executing query: {query}")
            cursor.execute(query)
            result = cursor.fetchall()  # Fetch all results from the executed query

            # Get column names from the cursor description
            column_list = [desc[0] for desc in cursor.description]
            cursor.close()  # Close the cursor after fetching data
            connection.commit()  # Commit the transaction (even though it's a read-only operation)
            connection.close()  # Close the connection

            # Convert the fetched data into a Pandas DataFrame with the column names
            df = pd.DataFrame(result, columns=column_list)

            # If the DataFrame is empty, log a warning and raise AirflowSkipException to skip the task
            if df.empty:
                logging.warning(f"[Extract] Table {table_name} is empty. Skipping...")                
                raise AirflowSkipException(f"{table_name} has no data. Skipped...")

            # Define the MinIO bucket name where the data will be stored
            bucket_name = 'flights-data-pipeline'
            # Log the information that data will be written to MinIO
            logging.info(f"[Extract] Writing data to MinIO bucket: {bucket_name}, object: {object_name}")

            # Upload the DataFrame as a CSV file to MinIO using the CustomMinio class
            CustomMinio._put_csv(df, bucket_name, object_name)
            logging.info(f"[Extract] Extraction completed for table: {table_name}")  # Log the completion

        except AirflowSkipException as e:
            # Log the warning and raise the exception if the task is skipped due to empty data
            logging.warning(f"[Extract] Skipped extraction for {table_name}: {str(e)}")            
            raise e
        except Exception as e:
            # Log the error and raise AirflowException if any other error occurs during the extraction
            logging.error(f"[Extract] Failed extracting {table_name}: {str(e)}")            
            raise AirflowException(f"Error when extracting {table_name} : {str(e)}")
