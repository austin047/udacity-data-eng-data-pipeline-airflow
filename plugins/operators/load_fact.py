from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    """
     Description: Operator used to load data into the facts table from staging tables
        
        Params:
        - redshift_connection_id : Redshift Connection id
        - target_table : tagret table on redshift
        - sql_action : Your intended to execute sql statement
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_connection_id="",
                 target_table="",
                 sql_action="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = redshift_connection_id
        self.target_table = target_table
        self.sql_action = sql_action

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        dimension_sql = getattr(SqlQueries,self.sql_action).format(self.target_table)
      
        
        self.log.info("Insert Data into Facts table {}".format(self.target_table))
        redshift.run(dimension_sql)