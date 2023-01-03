from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        # Define your operators params (with defaults) here
        self,
        redshift_conn_id = "",
        tables = [],
        *args, **kwargs
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # perform a data quality check

        for table in tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data Quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data Quality check failed. {table} returned 0 rows")
            
            self.log.info(f"Data quality on table {table} check passed with {num_records} records")

# This will ensure there is data