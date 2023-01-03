from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class CreateTableOperator(BaseOperator):
    sql_file = "/home/kevin/Desktop/Sparkify-Automated-Data-Pipelines/create_tables.sql"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id = "",
        *args, **kwargs
    ):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Create Redshift Connection')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        # Read tyhe file to get sql statements
        self.log.info('Reading SQL file')
        df = open(CreateTableOperator.sql_file, 'r')
        sql = df.read()
        df.close()

        sql_queries = sql.split(';')
        self.log.info('Creating tables')
        for query in sql_queries:
            if query.rstrip != "":
                redshift.run(query)