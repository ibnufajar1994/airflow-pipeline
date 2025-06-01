# Import necessary libraries
from airflow.hooks.base import BaseHook  # BaseHook from Airflow to get connection details
from minio import Minio  # Minio client for interacting with Minio storage
from io import BytesIO  # To handle byte streams for file uploads
import pandas as pd  # Pandas for data manipulation and handling CSV data
import json  # JSON handling

# MinioClient class to handle Minio client operations
class MinioClient:
    # Static method to get Minio client instance using connection details
    @staticmethod
    def _get():
        # Get the Minio connection details stored in Airflow connections
        minio = BaseHook.get_connection('minio')  # Fetch connection details for Minio
        
        # Create a Minio client instance with the necessary parameters
        client = Minio(
            endpoint = minio.extra_dejson['endpoint_url'],  # Minio endpoint URL from connection extra
            access_key = minio.login,  # Minio access key
            secret_key = minio.password,  # Minio secret key
            secure = False  # Disable secure (SSL) connection (set to True for secure connection)
        )

        return client  # Return the Minio client object
    
# CustomMinio class to provide methods for interacting with Minio storage
class CustomMinio:
    # Static method to upload a Pandas DataFrame as a CSV to Minio
    @staticmethod
    def _put_csv(dataframe, bucket_name, object_name):
        # Convert the DataFrame to CSV bytes (without index) and encode as UTF-8
        csv_bytes = dataframe.to_csv(index=False).encode('utf-8')
        # Create a byte buffer from the CSV data
        csv_buffer = BytesIO(csv_bytes)

        # Get Minio client instance
        minio_client = MinioClient._get()
        # Upload the CSV data to the specified Minio bucket and object name
        minio_client.put_object(
            bucket_name = bucket_name,  # Target bucket in Minio
            object_name = object_name,  # Target object name in Minio
            data = csv_buffer,  # Data to upload (CSV buffer)
            length = len(csv_bytes),  # Length of the CSV data
            content_type = 'application/csv'  # Content type for the uploaded data
        )

    # Static method to upload JSON data to Minio
    @staticmethod
    def _put_json(json_data, bucket_name, object_name):
        # Convert the JSON data to string and encode as UTF-8
        json_string = json.dumps(json_data)
        json_bytes = json_string.encode('utf-8')
        # Create a byte buffer from the JSON data
        json_buffer = BytesIO(json_bytes)

        # Get Minio client instance
        minio_client = MinioClient._get()
        # Upload the JSON data to the specified Minio bucket and object name
        minio_client.put_object(
            bucket_name = bucket_name,  # Target bucket in Minio
            object_name = object_name,  # Target object name in Minio
            data = json_buffer,  # Data to upload (JSON buffer)
            length = len(json_bytes),  # Length of the JSON data
            content_type = 'application/json'  # Content type for the uploaded data
        )

    # Static method to fetch a DataFrame from Minio storage
    @staticmethod
    def _get_dataframe(bucket_name, object_name):
        # Get Minio client instance
        minio_client = MinioClient._get()
        # Retrieve the object from Minio and get its data
        data = minio_client.get_object(
            bucket_name = bucket_name,  # Target bucket in Minio
            object_name = object_name  # Target object name in Minio
        )

        # Read the CSV data from the Minio object into a Pandas DataFrame
        df = pd.read_csv(data)

        return df  # Return the DataFrame
