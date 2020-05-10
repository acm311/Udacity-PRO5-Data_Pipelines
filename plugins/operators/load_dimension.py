from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    query_delete = "DELETE FROM {}"

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = '',
                 table = '',
                 sql = '',
                 delete_info = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.delete_info = delete_info

    def execute(self, context):
        """ Execute the operator in order to load a dimension table

        Arguments:
            context
        Returns:
            None
        """

        #self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.delete_info:
            self.log.info(f"Deleting info from dimension table {self.table}")
            redshift.run(self.query_delete.format(self.table))
        self.log.info(f"Inserting data from Fact table into dimension table {self.table}")
        formatted_sql = getattr(SqlQueries, self.sql).format(self.table)
        redshift.run(formatted_sql)