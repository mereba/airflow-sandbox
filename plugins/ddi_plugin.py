from airflow.plugins_manager import AirflowPlugin

from https_to_s3_operator import HTTPSToS3Operator
from livy_operator import LivyOperator
from oauth_http_hook import OAuthHttpHook
from s3_to_sftp_operator import S3ToSFTPOperator
from sftp_to_s3_operator import SFTPToS3Operator


class AirflowDDIPlugin(AirflowPlugin):
    name = 'ddi_plugin'

    operators = [
        LivyOperator,
        S3ToSFTPOperator,
        SFTPToS3Operator,
        HTTPSToS3Operator
    ]

    hooks = [
        OAuthHttpHook,
    ]
