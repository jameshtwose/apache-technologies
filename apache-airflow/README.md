## Airflow
Useful functions for Airflow.
- `export AIRFLOW_HOME=~/THIS_DIR`: Set the Airflow home directory
- `airflow db init`: Initialize the database
- `airflow users create`: Create an admin user
```
airflow db upgrade
airflow users create \
    --username admin \
    --firstname James \
    --lastname Twose \
    --password admin \
    --role Admin \
    --email admin@example.org
```
- `airflow users list`: List all users
- `airflow scheduler`: Start the scheduler
- `airflow dags list`: List all DAGs
- `airflow tasks list <dag_id>`: List all tasks of a DAG
- `airflow tasks list <dag_id> --tree`: List all tasks of a DAG in a tree structure 
- `airflow tasks test <dag_id> <task_id> <execution_date>`: Test a task
  - e.g. `airflow tasks test dag_1 sleep 2015-06-01` (can be found in DAG.py) or 
  - `airflow tasks test dag_1 templated 2015-06-01` (can be found in the Airflow UI)
- `airflow dags backfill <dag_id> --start-date YYYY-MM-DD --end-date YYYY-MM-DD`: Backfill a DAG
- e.g. `airflow dags backfill dag_1 --start-date 2015-06-01 --end-date 2015-06-07`
- optional: `airflow webserver --debug`: Start a webserver in debug mode