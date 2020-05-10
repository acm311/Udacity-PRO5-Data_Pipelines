from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    query_delete = "DELETE FROM {}"

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = '',
                 table = '',
                 sql = '',
                 delete_info = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.delete_info = delete_info

    def execute(self, context):
        """ Execute the operator in order to load a Fact table

        Arguments:
            context
        Returns:
            None
        """

        #self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.delete_info:
            self.log.info(f"Deleting info from fact table {self.table}")
            redshift.run(self.query_delete.format(self.table))
        self.log.info("Inserting info from staging tables into fact table {}".format(self.table))
        formatted_sql = getattr(SqlQueries, self.sql).format(self.table)
        redshift.run(formatted_sql)
