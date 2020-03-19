from time import sleep
from typing import Dict, List

import requests

from airflow.exceptions import AirflowException


class LivyClient:

    def __init__(self, host: str, port: int = 8998, deploy_mode: str = 'cluster',
                 master: str = 'yarn'):
        self.host = host
        self.port = port
        self.deploy_mode = deploy_mode
        self.master = master

    def _post_method(self, url, json_data, retries=3, sleep_seconds=20):
        resp = requests.post(url, json=json_data, timeout=30)
        while not resp.ok and retries > 0:
            sleep(sleep_seconds)
            resp = requests.post(url, json=json_data, timeout=30)
            retries = retries - 1
        return resp

    def _get_method(self, url, retries=3, sleep_seconds=20):
        resp = requests.get(url)
        while not resp.ok and retries > 0:
            sleep(sleep_seconds)
            resp = requests.get(url)
            retries -= 1
        return resp

    @classmethod
    def _create_submit_payload(cls, deploy_mode, master, entry_point,
                               app_name=None,
                               python_files: List[str] = [],
                               app_args: List[str] = [],
                               spark_conf: Dict[str, str] = dict()):

        data = {
            'conf': {
                'spark.submit.deployMode': deploy_mode,
                'spark.submit.master': master
            },
            'file': entry_point
        }

        if app_args:
            data['args'] = app_args

        if python_files:
            data['pyFiles'] = python_files

        if app_name:
            data['conf']['spark.app.name'] = app_name

        if spark_conf:
            data.get('conf', dict()).update(spark_conf)

        return data

    def _get_batch_id(self, json_response):
        """return the batch id for a given json response against the
        livy /batch endpoint"""
        return json_response.get('id')

    def _create_submit_url(self):
        return 'http://{host}:{port}/batches'.\
            format(host=self.host, port=self.port)

    def submit_batch(self, entry_point, app_name=None,
                     app_args=[], python_files=[],
                     spark_conf=dict):
        """
        return the batch id for the submitted application
        """
        data = self._create_submit_payload(deploy_mode=self.deploy_mode,
                                           master=self.master,
                                           entry_point=entry_point,
                                           app_name=app_name,
                                           app_args=app_args,
                                           python_files=python_files,
                                           spark_conf=spark_conf)

        url = self._create_submit_url()

        print('making a request to: {url} with payload: data: {data}'.
              format(url=url, data=data))

        response = self._post_method(url, data)

        if not response.ok:
            message = 'could not connect to livy, status code: {code}, ' \
                      'url used: {url}, data: {data}'.\
                format(code=response.status_code, url=url, data=data)
            raise AirflowException(message)

        return self._get_batch_id(response.json())

    def _create_batch_status_url(self, batch_id):
        return 'http://{host}:{port}/batches/{id}/state'.\
            format(host=self.host, port=self.port, id=batch_id)

    def batch_status(self, batch_id):
        """returns the batch status for a given livy batch
        eg. success, running, dead"""

        url = self._create_batch_status_url(batch_id)
        response = self._get_method(url)
        return response.json().get('state')

    def is_batch_finished(self, batch_id):
        """return True if a batch is finished"""
        CODES = ['starting', 'running']
        return self.batch_status(batch_id) not in CODES

    def _create_log_url(self, batch_id, from_index, size) -> str:
        return 'http://{host}:{port}/batches/{id}/log?' \
               'from={from_index}&size={size}'.format(host=self.host,
                                                      port=self.port,
                                                      id=batch_id,
                                                      from_index=from_index,
                                                      size=size)

    def _get_log(self, batch_id, from_index, size) -> dict:
        """returns a dictionary containing the log entries"""

        url = self._create_log_url(batch_id, from_index, size)

        resp = self._get_method(url)

        if not resp.ok:
            message = 'could not connect to livy, status code: {code}, ' \
                      'url used: {url}'.format(code=resp.status_code, url=url)
            raise AirflowException(message)
        response = resp.json()
        return response.get('log')

    def consume_log(self, batch_id):
        """converts the Livy log into Operator log,
        this way the log can be read on the spark"""

        from_index = 0
        size = 100
        log = True
        while not self.is_batch_finished(batch_id) or log:
            log = self._get_log(batch_id, from_index, size)
            for line in log:
                print(line)
            from_index = from_index + len(log)
            sleep(1)

    def is_successful_finish(self, batch_id) -> bool:
        """raises an exception if the application did not finished
        successfully"""
        batch_status = self.batch_status(batch_id)
        return batch_status == 'success'

    def _create_batch_info_url(self, batch_id) -> str:
        return 'http://{host}:{port}/batches/{batch_id}'.format(host=self.host,
                                                                port=self.port,
                                                                batch_id=batch_id)

    def print_batch_info(self, batch_id):

        url = self._create_batch_info_url(batch_id)
        request = self._get_method(url)
        payload = request.json()

        batch_status = self.batch_status(batch_id)
        application_id = payload['appId']
        spark_ui_url = payload['appInfo']['sparkUiUrl']

        print('====================================')
        print('====================================')
        print('====================================')
        print('batch status: {status}'.format(status=batch_status))
        print('application_id: {application_id}'.
              format(application_id=application_id))
        print('spark_ui_url: {spark_ui_url}'.
              format(spark_ui_url=spark_ui_url))
        print('====================================')
        print('====================================')
        print('====================================')
