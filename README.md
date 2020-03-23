# airflow-sandbox

This sandbox helps you to quickly set up an airflow instance using postgres as a backend.

## how to use?

  1. Run `docker-compose up`

  2. Airflow web server is running at port 8080 in the container and exposed at 8082 on your host machine. Access it at **http://localhost:8082**.

  3. Inspect `hello_world.py` workflow/dag. It is turned off by default. [View the source](https://github.com/mereba/airflow-sandbox/blob/master/dags/team_x/workflow_1/hello_world.py) and modify as necessary.

  4. Airflow records metrics using statsd. Included in this sandbox is also a graphite-statsd container. You can view the graphite database dashboard at **http://localhost:8085**

## acknowledgements
Thanks to [Victor](https://github.com/Datkros) for helping with the base docker files!
