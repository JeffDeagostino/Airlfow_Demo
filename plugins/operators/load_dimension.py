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
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_to_load_tbl = sql_to_load_tbl
        self.append_data = append_data

    def execute(self, context):
        self.log.info('LoadDimensionOperator running . . .')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == True:
            sql_statement = "INSERT INTO {} {}".format(self.table, self.sql_to_load_tbl)
            redshift.run(sql_statement)
        else:
            sql_statement = "DELETE FROM {}".format(self.table)
            redshift.run(sql_statement)
            sql_statement = "INSERT INTO {} {}".format(self.table, self.sql_to_load_tbl)
            redshift.run(sql_statement)