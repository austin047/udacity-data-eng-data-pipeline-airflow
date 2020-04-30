from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    """
     Description: Operator used to load data into dimension tables
        
        Params:
        - redshift_connection_id : Redshift Connection id
        - insert_mode : A boolean field, determine if dimension table is to be deleted before inserting new records
        - target_table : tagret table on redshift
        - sql_action : Your intended to execute sql statement
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_connection_id="",
                 insert_mode=True,
                 target_table="",
                 sql_action="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = redshift_connection_id
        self.insert_mode = insert_mode
        self.target_table = target_table
        self.sql_action = sql_action

    def execute(self, context):
        self.log.info('LoadDimensionOperator Working...')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        dimension_sql = getattr(SqlQueries, self.sql_action).format(self.target_table)
        
        if self.insert_mode:
            self.log.info("Delete dimention table {} before inserting".format(self.target_table))
            redshift.run("TRUNCATE FROM {}".format(self.target_table))
        
        self.log.info("Insert Data into table {}".format(self.target_table))
        redshift.run(dimension_sql)
