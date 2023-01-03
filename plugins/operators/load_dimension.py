from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    @apply_defaults
    def __init__(
        # Define your operators params (with defaults) here
        self,
        redshift_conn_id = "",
        table = "",
        sql = "",
        update_strategy = "",
        *args, **kwargs
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        if update_strategy == 'overwrite':
            # update with truncate first strategy for dimensional tables
            query = "TRUNCATE {}; INSERT INTO {} ({})".format(self.table, self.table, self.sql)

        elif update_strategy == 'append':
            query = "INSERT INTO {} ({})".format(self.table, self.sql)

        redshift.run(query)
        self.log.info('success')
