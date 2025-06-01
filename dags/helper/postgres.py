# Import necessary libraries
from airflow.providers.postgres.hooks.postgres import PostgresHook  # PostgreSQL hook from Airflow for database operations
import pandas as pd  # Pandas for data manipulation and handling DataFrame
BASE_PATH = "/opt/airflow/dags"  # Base path for file storage in Airflow environment


# Execute class to handle PostgreSQL database operations
class Execute:
    # Static method to execute a SQL query from a file on the PostgreSQL database
    def _query(connection_id, query_path):
        # Create a PostgresHook instance using the provided connection ID from Airflow's connection manager
        hook = PostgresHook(postgres_conn_id = connection_id)
        # Establish the database connection
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        # Read the SQL query from the file specified by query_path
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()

        # Execute the SQL query using the cursor
        cursor.execute(query)
        cursor.close()  # Close the cursor after execution
        connection.commit()  # Commit the transaction
        connection.close()  # Close the connection

    # Static method to execute a SQL query from a file and return the result as a Pandas DataFrame
    def _get_dataframe(connection_id, query_path):
        # Create a PostgresHook instance using the provided connection ID from Airflow's connection manager
        pg_hook = PostgresHook(postgres_conn_id = connection_id)
        # Establish the database connection
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        # Read the SQL query from the file specified by query_path
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()

        # Execute the SQL query using the cursor
        cursor.execute(query)
        result = cursor.fetchall()  # Fetch all results from the query
        # Extract column names from the cursor description
        column_list = [desc[0] for desc in cursor.description]
        # Create a Pandas DataFrame from the results and the column names
        df = pd.DataFrame(result, columns = column_list)

        cursor.close()  # Close the cursor after fetching results
        connection.commit()  # Commit the transaction (although it's a read operation)
        connection.close()  # Close the connection
        
        return df  # Return the DataFrame with the query results

    # Static method to execute an insert query with data from a DataFrame
    def _insert_dataframe(connection_id, query_path, dataframe):
        # Create a PostgresHook instance using the provided connection ID from Airflow's connection manager
        pg_hook = PostgresHook(postgres_conn_id = connection_id)
        # Establish the database connection
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        # Read the SQL insert query from the file specified by query_path
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()

        # Iterate over each row in the DataFrame
        for index, row in dataframe.iterrows():
            record = row.to_dict()  # Convert the row to a dictionary of column-value pairs
            # Execute the insert query with the row's data as parameters
            pg_hook.run(query, parameters = record)

        cursor.close()  # Close the cursor after inserting all rows
        connection.commit()  # Commit the transaction
        connection.close()  # Close the connection
