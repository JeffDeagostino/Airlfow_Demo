from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 columns"",
                 sql_to_load_tbl ="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns
        self.sql_to_load_tbl = sql_to_load_tbl

    def execute(self, context):
        self.log.info('LoadFactOperator Running . . .')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql_statement = "INSERT INTO {} {} {}".format(self.table, self.columns, self.sql_to_load_tbl)
        
        redshift.run(sql_statement)