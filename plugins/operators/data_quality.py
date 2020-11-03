from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('Running DataQualityOperator . . . ')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for check in self.dq_checks:
            sql = check['check_sql']
            exp_result = check['expected_result']
            
            result = redshift_hook.get_records(sql)
            record_n = result[0][0]
            
            if record_n != exp_result:
                raise ValueError(f"Quality check failed: {sql} ")
                log.info(f"Quality check failed: {sql} ")
            else:
                log.info(f"Quality check passed: {sql} ")