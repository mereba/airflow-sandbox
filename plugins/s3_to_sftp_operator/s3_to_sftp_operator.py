import os
from tempfile import NamedTemporaryFile

from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToSFTPOperator(BaseOperator):
    """
    This operator enables the transferring of files from S3 to a SFTP server.
    """

    ui_color = '#e8f7e4'

    template_fields = ('s3_bucket', 's3_prefix', 'sftp_filename_prefix', 'sftp_path')

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_prefix,
                 sftp_filename_prefix,
                 sftp_path,
                 sftp_conn_id,
                 s3_conn_id,
                 file_extensions=('.csv', '.json'),
                 * args,
                 **kwargs):
        super(S3ToSFTPOperator, self).__init__(*args, **kwargs)

        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

        self.sftp_filename_prefix = sftp_filename_prefix
        self.sftp_path = sftp_path
        self.file_extensions = file_extensions

        self.sftp_conn_id = sftp_conn_id
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        s3_client = s3_hook.get_conn()
        sftp_client = sftp_hook.get_conn()

        s3_keys = s3_hook.list_keys(self.s3_bucket, prefix=self.s3_prefix)

        s3_keys_filtered_by_extensions = [
            s3_key for s3_key in s3_keys if s3_key.lower().endswith(self.file_extensions)
        ]

        part_count = 0

        for s3_key in s3_keys_filtered_by_extensions:
            with NamedTemporaryFile("w") as f:
                s3_client.download_file(self.s3_bucket, s3_key, f.name)

                _, file_extension = os.path.splitext(s3_key)
                remote_filename = f'{self.sftp_filename_prefix}-part-{part_count}{file_extension}'
                remote_path = os.path.join(self.sftp_path, remote_filename)

                sftp_client.put(f.name, remote_path)

                part_count += 1
