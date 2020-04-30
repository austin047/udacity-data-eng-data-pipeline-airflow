from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
        Description: Operator used to copy from s3 bucket to staging table on reshift
        
        Params:
        - redshift_connection_id : Redshift Connection id
        - s3_bucket : taget bucket to copy from 
        - s3_key : key in bucket
        - json_path : json_path to pass json from bucket
        - target_table : tagret table on redshift
        - aws_credentials_id : aws credential id which will be used to get your aws credentials from air flow
    """
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    copy_sql = ("""
    copy {}  
    from '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    format as json '{}'
    STATUPDATE ON
    region 'us-west-2'
    """)

    @apply_defaults
    def __init__(self,
                 redshift_connection_id="",
                 s3_bucket="",
                 s3_key='',
                 json_path='',
                 target_table="",
                 aws_credentials_id="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.target_table = target_table
        self.redshift_connection_id = redshift_connection_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.aws_credentials_id = aws_credentials_id
        self.execution_date = kwargs.get('execution_date')


    def execute(self, context):
        #Initalize Asw Hook the get aws credentials from it
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        #Initalize a postgreshook to connect to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_connection_id)

        self.log.info('Clear data from destination Redshift table')
        redshift.run("DELETE FROM {}".format(self.target_table))

        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.target_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        redshift.run(formatted_sql)
