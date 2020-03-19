FROM ubuntu:bionic

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

ARG GITHUB_TOKEN=""
ENV GITHUB_TOKEN=$GITHUB_TOKEN

# Airflow
ARG AIRFLOW_VERSION=1.10.9
ARG REQUESTS_OAUTHLIB_VERSION=1.1.0
ARG WERKZEUG_VERSION=0.16.1
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# Disable noisy "Handling signal" log messages:
# ENV GUNICORN_CMD_ARGS --log-level WARNING

RUN set -ex && \
    apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get install -y build-essential python3.7 python3.7-dev python3-pip && \
    apt-get install -y libssl-dev libffi-dev libxml2-dev libxslt1-dev zlib1g-dev python3-setuptools && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 1 && \
    update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1 && \
    useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow && \
    pip3 install -U \
      cython \
      pytz \
      pyopenssl \
      ndg-httpsclient \
      pyasn1 \
      psycopg2-binary \
      requests-oauthlib==${REQUESTS_OAUTHLIB_VERSION} \
      apache-airflow[postgres,ssh,s3,statsd]==${AIRFLOW_VERSION} \
      prometheus-client &&\
    pip3 install -U dynaconf[yaml,vault] && \
    # pin version since it has breaking changes
    pip3 install -U Werkzeug==${WERKZEUG_VERSION}

COPY entrypoint.sh /entrypoint.sh

RUN chown -R airflow: ${AIRFLOW_USER_HOME}

WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["airflow", "webserver"]
