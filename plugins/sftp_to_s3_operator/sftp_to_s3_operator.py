from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.sftp_hook import SFTPHook
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse
from airflow.utils.decorators import apply_defaults


class SFTPToS3Operator(BaseOperator):
    """
    This operator enables the transferring of files from a SFTP server to
    Amazon S3.

    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :type sftp_conn_id: str
    :param sftp_path: The sftp remote path. This is the specified file path
        for downloading the file from the SFTP server.
    :type sftp_path: str
    :param s3_conn_id: The s3 connection id. The name or identifier for
        establishing a connection to S3
    :type s3_conn_id: str
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where
        the file is uploaded.
    :type s3_bucket: str
    :param s3_prefix: The targeted s3 prefix(folder). This is the specified path for
        uploading the files to S3.
    :type s3_prefix: str
    """

    template_fields = ('s3_bucket', 's3_prefix', 'sftp_path')

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_prefix,
                 sftp_path,
                 sftp_conn_id='ssh_default',
                 s3_conn_id='aws_default',
                 file_extensions=('.csv', '.json', '.xlsx'),
                 *args,
                 **kwargs):
        super(SFTPToS3Operator, self).__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_conn_id = s3_conn_id
        self.file_extensions = file_extensions

    @staticmethod
    def get_s3_key(s3_key):
        """This parses the correct format for S3 keys
            regardless of how the S3 url is passed."""

        parsed_s3_key = urlparse(s3_key)
        return parsed_s3_key.path.lstrip('/')

    def execute(self, context):
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        sftp_files = sftp_hook.list_directory(self.sftp_path)
        filtered_files_by_extensions = [
            key for key in sftp_files if key.lower().endswith(self.file_extensions)
        ]

        for sftp_file in filtered_files_by_extensions:
            with NamedTemporaryFile("w") as f:
                sftp_hook.retrieve_file(f'{self.sftp_path}/{sftp_file}', f.name)

                s3_key = self.get_s3_key(f'{self.s3_prefix}/{sftp_file}')
                s3_hook.load_file(
                    filename=f.name,
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
