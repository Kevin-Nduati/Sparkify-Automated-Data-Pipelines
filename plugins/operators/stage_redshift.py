from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACESS)KEY '{}'
    IGNOREHEADER {}
    {}
    """

    @apply_defaults 
    def __init__(
        self,
        redshift_conn_id = "",
        aws_credentials_id = "",
        table = "", 
        s3_bucket = "",
        s3_key = "",
        delimiter = ",",
        ignore_headers = 1,
        data_format = 'csv',
        jsonpaths="",
        *args, **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.data_format = data_format.lower() 
        self.jsonpaths = jsonpaths




    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Clearing data from destination redshift table')
        redshift.run('DELETE FROM {}'.format(self.table))

        # select format
        if self.data_format == 'csv':
            autoformat = "DELIMITER '{csv}' ".format(self.delimiter)

        if self.data_format == 'json':
            jsonoption = self.jsonpaths or 'auto'
            autoformat = "FORMAT AS JSON '{}'".format(jsonoption)


        self.log.info('Copying data from s3 to redshift')
        #set s3 path based on execution dates
        rendered_key = self.s3_key.format(**context)
        self.log.info(f'Rendered key is '.format(rendered_key))
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            autoformat
        )
        redshift.run(formatted_sql)

     




