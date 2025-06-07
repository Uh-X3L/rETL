"""
Sample Airflow DAG for copying data from Synapse to Parquet format.

This DAG demonstrates the migration pattern from Azure Synapse Pipelines
to Airflow using Spark-on-Kubernetes for execution.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    'owner': 'retl-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'synapse_to_parquet_copy',
    default_args=default_args,
    description='Copy data from Synapse SQL Database to Parquet format',
    schedule_interval=None,  # Triggered manually or by external systems
    catchup=False,
    tags=['etl', 'synapse', 'parquet', 'migration'],
)

# Spark application configuration
spark_app_config = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "synapse-to-parquet-{{ ds_nodash }}",
        "namespace": "default",
    },
    "spec": {
        "type": "Python",
        "pythonVersion": "3",
        "mode": "cluster",
        "image": "{{ var.value.spark_image }}",  # e.g., "myregistry/spark:3.4.0-python3"
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/spark/work-dir/scripts/run_copy.py",
        "arguments": [
            "--jdbc-url", "{{ var.value.synapse_jdbc_url }}",
            "--sql", "{{ dag_run.conf.get('sqlStatement', 'SELECT * FROM dbo.account') }}",
            "--output-path", "{{ dag_run.conf.get('outputPath', 's3://data-lake/default/') }}",
            "--username-secret", "SYNAPSE_USERNAME",
            "--password-secret", "SYNAPSE_PASSWORD",
            "--secret-key", "OSS_ACCESS_KEY",
            "--secret-secret", "OSS_SECRET_KEY",
        ],
        "sparkVersion": "3.4.0",
        "restartPolicy": {
            "type": "Never"
        },
        "driver": {
            "cores": 1,
            "memory": "1g",
            "serviceAccount": "spark-driver",
            "envVars": {
                "AWS_REGION": "{{ var.value.aws_region }}",
            }
        },
        "executor": {
            "cores": 1,
            "instances": 2,
            "memory": "2g",
            "serviceAccount": "spark-executor",
        },
        "deps": {
            "jars": [
                "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.2.0.jre8/mssql-jdbc-12.2.0.jre8.jar",
                "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
                "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar",
            ]
        }
    }
}

# Task definition
copy_task = SparkKubernetesOperator(
    task_id='copy_synapse_to_parquet',
    application_file=spark_app_config,
    namespace='default',
    kubernetes_conn_id='kubernetes_default',
    dag=dag,
)

# Set task dependencies (single task in this example)
copy_task
