import os
from typing import Dict, List

import pkg_resources


def get_egg_name(package: str, version: str):
    dist = pkg_resources.Distribution(project_name=package, version=version)
    egg_name = dist.egg_name() + '.egg'
    return egg_name


def get_python_files(job_conf: Dict[str, str]):
    binary_path = os.path.join(
        job_conf['binary_repository'],
        job_conf['package'],
        job_conf['version']
    )
    egg_name = get_egg_name(job_conf['package'], job_conf['version'])

    return [
        os.path.join(binary_path, egg_name),
        os.path.join(binary_path, 'libs.zip')
    ]


def get_entrypoint_path(job_conf: Dict[str, str]):
    return os.path.join(job_conf['binary_repository'], 'entrypoint.py')


def get_application_args(job_conf: Dict[str, str], job_args: List[str]):
    job_name = job_conf['package'] + '.' + job_conf['app']

    return [job_name, *job_args]


def get_python_interpreter_spark_configuration(job_conf: Dict[str, str]) -> Dict[str, str]:
    """
    returns the spark conf parameters to use a different python interpreter
    (aka virtual environment)
    """

    if job_conf['spark_python_interpreter']:
        return {'spark.yarn.appMasterEnv.PYSPARK_PYTHON':
                job_conf['spark_python_interpreter'],
                'spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON':
                    job_conf['spark_python_interpreter']}
    else:
        return dict()
