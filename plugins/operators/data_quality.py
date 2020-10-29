from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        self.log.info('Running DataQualityOperator . . . ')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        result = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        record_n = result[0][0]
        if num_records == 0:
            raise ValueError(f"Data is missing: {self.table} contained 0 rows")
            log.info(f"No Data is present: {self.table} contains {record_n} rows")
        else:
            log.info(f"Data is present: {self.table} contains {record_n} rows")