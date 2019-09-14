from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # from helper
        tables = []
        tables = self.table.replace(" ", "").split(",")
    
        if "songplays" in tables:
            query_null = SqlQueries.songplays_check_nulls
            records = redshift.get_records(query_null)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {query_null} returned results.")
            if records[0][0] > 0:
                raise ValueError(f"Data quality check failed. {query_null} contained > 0 rows")
            query_count = SqlQueries.songplays_check_nulls 
            records = redshift.get_records(query_count)
            self.log.info(f"RESULTS: {records}")
            self.log.info(f"RESULTS: {query_count} had {records[0][0]} records.")
        if "users" in tables:
            query_null = SqlQueries.users_check_nulls
            records = redshift.get_records(query_null)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {query_null} returned results.")
            if records[0][0] > 0:
                raise ValueError(f"Data quality check failed. {query_null} contained > 0 rows")
            query_count = SqlQueries.users_check_nulls 
            records = redshift.get_records(query_count)
            self.log.info(f"RESULTS: {records}")
            self.log.info(f"RESULTS: {query_count} had {records[0][0]} records.")
        if "songs" in tables:
            query_null = SqlQueries.songs_check_nulls
            records = redshift.get_records(query_null)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {query_null} returned results.")
            if records[0][0] > 0:
                raise ValueError(f"Data quality check failed. {query_null} contained > 0 rows")
            query_count = SqlQueries.songs_check_nulls 
            records = redshift.get_records(query_count)
            self.log.info(f"RESULTS: {records}")
            self.log.info(f"RESULTS: {query_count} had {records[0][0]} records.")
        if "artists" in tables:
            query_null = SqlQueries.artists_check_nulls
            records = redshift.get_records(query_null)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {query_null} returned results.")
            if records[0][0] > 0:
                raise ValueError(f"Data quality check failed. {query_null} contained > 0 rows")
            query_count = SqlQueries.artists_check_nulls 
            records = redshift.get_records(query_count)
            self.log.info(f"RESULTS: {records}")
            self.log.info(f"RESULTS: {query_count} had {records[0][0]} records.")
        if "time" in tables:
            query_null = SqlQueries.time_check_nulls
            records = redshift.get_records(query_null)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {query_null} returned results.")
            if records[0][0] > 0:
                raise ValueError(f"Data quality check failed. {query_null} contained > 0 rows")
            query_count = SqlQueries.time_check_nulls 
            records = redshift.get_records(query_count)
            self.log.info(f"RESULTS: {records}")
            self.log.info(f"RESULTS: {query_count} had {records[0][0]} records.")
         