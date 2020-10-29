from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_to_load_tbl ="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_to_load_tbl = sql_to_load_tbl

    def execute(self, context):
        self.log.info('LoadDimensionOperator running . . .')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #sql_statement = "INSERT INTO {} {} {}".format(self.table, self.table_columns,self.sql_to_load_tbl)
        sql_statement = '''
            INSERT INTO public.{table}
            {sql_to_load_tbl}
        '''

        redshift.run(sql_statement)