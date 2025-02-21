import snowflake.connector
import boto3
from typing import Dict
import os
from .utils import setup_logging

logger = setup_logging({'logging': {'level': 'INFO', 'file': 'erp_integration.log'}})

class DataLoader:
    """Handles data loading to S3 and Snowflake."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config['aws']['access_key'],
            aws_secret_access_key=config['aws']['secret_key']
        )

    def upload_to_s3(self, df: pd.DataFrame, key: str):
        """Upload DataFrame to S3 as Parquet."""
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"{key}_{timestamp}.parquet"
        df.to_parquet(file_name, compression='snappy')
        self.s3_client.upload_file(file_name, self.config['aws']['s3_bucket'], f"processed/{file_name}")
        os.remove(file_name)
        logger.info(f"Uploaded {file_name} to S3")

    def load_to_snowflake(self, df: pd.DataFrame, table_name: str):
        """Load DataFrame to Snowflake."""
        conn = snowflake.connector.connect(**self.config['snowflake'])
        cursor = conn.cursor()
        try:
            columns = [f"{col} STRING" for col in df.columns]
            cursor.execute(f"CREATE OR REPLACE TABLE {table_name} ({', '.join(columns)})")
            file_name = f"{table_name}.parquet"
            df.to_parquet(file_name, compression='snappy')
            cursor.execute(f"PUT file://{file_name} @%{table_name}")
            cursor.execute(f"COPY INTO {table_name} FROM @%{table_name} FILE_FORMAT = (TYPE = PARQUET)")
            os.remove(file_name)
            logger.info(f"Loaded data into Snowflake table {table_name}")
        finally:
            cursor.close()
            conn.close()

            