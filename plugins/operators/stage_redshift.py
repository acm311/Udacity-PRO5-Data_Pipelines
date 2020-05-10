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
        SECRET_ACCESS_KEY '{}'
        REGION '{}';
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table = '',
                 s3_bucket_path = '',
                 s3_key = '',
                 region = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket_path = s3_bucket_path
        self.s3_key = s3_key
        self.region = region

    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        aws = AwsHook(self.aws_credentials_id)
        credentials = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Deleting info from Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_bucket_path,
            credentials.access_key,
            credentials.secret_key,
            self.region
        )
        self.log.info(f"Executing {formatted_sql} ...")
        redshift.run(formatted_sql)





