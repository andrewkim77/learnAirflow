from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {} ({})
        {};
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 columns="",
                 query_table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.columns = columns
        self.redshift_conn_id = redshift_conn_id
        self.query_table = query_table
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # from helper
        if self.query_table == "users_table":
            query_content = SqlQueries.user_table_insert 
        if self.query_table == "songs_table":
            query_content = SqlQueries.song_table_insert 
        if self.query_table == "artists_table":
            query_content = SqlQueries.song_table_insert 
        if self.query_table == "time_table":
            query_content = SqlQueries.time_table_insert 
        
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.columns,
            query_content
        )
        redshift.run(formatted_sql)
