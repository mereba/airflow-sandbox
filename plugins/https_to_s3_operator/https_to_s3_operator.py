from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.http_hook import HttpHook
from urllib.parse import urlparse
from airflow.utils.decorators import apply_defaults


class HTTPSToS3Operator(BaseOperator):
    """
    This operator enables the transferring of JSON API data to Amazon S3.

    :param http_conn_id: connection that has the base API url. The name or identifier for
        establishing a connection to the HTTP server.
    :type http_conn_id: str
    :param api_endpoint: The HTTP api endpoint. This is the specified route path
        for getting the JSON data from the HTTP server.
    :type api_endpoint: str
    :param s3_conn_id: The s3 connection id. The name or identifier for
        establishing a connection to S3
    :type s3_conn_id: str
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where
        the file is uploaded.
    :type s3_bucket: str
    :param s3_prefix: The targeted s3 prefix(folder). This is the specified path for
        uploading the files to S3.
    :type s3_prefix: str
    :param timeout: set the timeout for the request
    :type timeout: int
    """

    template_fields = ('s3_bucket', 's3_prefix', 'api_endpoint')

    @apply_defaults
    def __init__(self,
                 api_endpoint,
                 s3_bucket,
                 s3_prefix,
                 s3_conn_id='aws_default',
                 http_conn_id='rest_default',
                 timeout=5,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.api_endpoint = api_endpoint
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_conn_id = s3_conn_id
        self.timeout = timeout

    @staticmethod
    def get_s3_key(s3_key):
        """This parses the correct format for S3 keys
            regardless of how the S3 url is passed."""

        parsed_s3_key = urlparse(s3_key)
        return parsed_s3_key.path.lstrip('/')

    def execute(self, context):
        http_hook = HttpHook(http_conn_id=self.http_conn_id, method='GET')
        result = http_hook.run(
            endpoint=self.api_endpoint,
            extra_options={
                'timeout': self.timeout
            }
        )

        s3_hook = S3Hook(self.s3_conn_id)
        s3_key = self.get_s3_key(f'{self.s3_prefix}/data.json')
        s3_hook.load_string(
            result.text,
            key=s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )

        # Add the empty _SUCCESS file to indicate the task is done successfully
        s3_key = self.get_s3_key(f'{self.s3_prefix}/_SUCCESS')
        s3_hook.load_string(
            '',
            key=s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )
