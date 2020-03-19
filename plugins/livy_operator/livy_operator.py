from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from livy_operator.apache_livy_client import LivyClient


class LivyOperator(BaseOperator):
    """
    Operator to submit a spark application to a Livy Rest Endpoint.
    """

    ui_color = '#e8f7e4'

    # airflow will made the rendered template from these
    template_fields = ('host', 'port', 'deploy_mode', 'master', 'app_name',
                       'entry_point', 'python_files', 'application_args',
                       'spark_conf')

    @apply_defaults
    def __init__(self,
                 host: str,
                 port: int,
                 entry_point: str,
                 deploy_mode: str = 'cluster',
                 master: str = 'yarn',
                 python_files: List[str] = [],
                 application_args: List[str] = [],
                 app_name=None,
                 spark_conf: Dict[str, str] = dict(),
                 *args,
                 **kwargs):

        super(LivyOperator, self).__init__(*args, **kwargs)

        self.host = host
        self.port = port
        self.deploy_mode = deploy_mode
        self.master = master
        self.entry_point = entry_point
        self.python_files = python_files
        self.application_args = application_args
        self.app_name = app_name
        self.spark_conf = spark_conf

    def execute(self, context):
        """
            Executes the Livy Operator.
            An Airflow Exception will be thrown if the operator does not
            finish successfully
        """

        client = LivyClient(host=self.host,
                            port=self.port,
                            deploy_mode=self.deploy_mode,
                            master=self.master)

        livy_batch_id = client.submit_batch(entry_point=self.entry_point,
                                            app_name=self.app_name,
                                            app_args=self.application_args,
                                            python_files=self.python_files,
                                            spark_conf=self.spark_conf
                                            )

        client.consume_log(livy_batch_id)

        client.print_batch_info(livy_batch_id)

        if not client.is_successful_finish(livy_batch_id):
            batch_status = client.batch_status(livy_batch_id)
            message = 'batch id: {batch} failed with status: {status}, type:{type}' \
                .format(batch=livy_batch_id, status=batch_status,
                        type=batch_status)
            raise AirflowException(message)
