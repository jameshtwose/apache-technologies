# # DAG (Directed Acyclic Graph)
# A DAG is a collection of all the tasks you want to run,
# organized in a way that reflects their relationships
# and dependencies.
from datetime import timedelta, datetime
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dag_1",  # name of dag
    description="A simple tutorial DAG",
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": ["contact@jamestwose.com"],
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },  # dict of default args
    schedule=timedelta(days=1),  # how often to run
    start_date=datetime(2021, 1, 1),  # when to start
    catchup=False,  # whether to catchup on tasks
    tags=["example"],
) as dag:
    # t1, t2 and t3 are examples of tasks created by
    # instantiating operators
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )
    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3,
    )
    t1.doc_md = dedent(
        """\
        #### Task Documentation
        You can document your task using the attributes
        `doc_md` (markdown),
        `doc` (plain text),
        `doc_rst`,
        `doc_json`,
        `doc_yaml`
        which gets rendered in the UI's Task Instance Details page.
        """
    )
    dag.doc_md = __doc__  # docstring at the top of the file
    dag.doc_md = dedent(
        """\
        This is a documentation placed anywhere
        """
    )

    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, i) }}"
            echo "{{ params.my_param }}"
        {% endfor %}
        """
    )
    t3 = BashOperator(
        task_id="templated",
        depends_on_past=False,
        bash_command=templated_command,
        params={"my_param": "Parameter I passed in"},
    )

    t1 >> [t2, t3]  # t1 must run before t2 and t3

