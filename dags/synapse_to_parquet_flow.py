"""
Sample Prefect flow for copying data from Synapse to Parquet format.

This flow demonstrates the migration pattern from Azure Synapse Pipelines
to Prefect using Spark-on-Kubernetes for execution.
"""

from typing import Optional
from prefect import flow, task
from prefect.blocks.kubernetes import KubernetesJob
from prefect.infrastructure import KubernetesJob as KubernetesJobInfra
from utils.secrets import get_secret, require_secret


@task
def validate_parameters(
    jdbc_url: str,
    sql_statement: str,
    output_path: str
) -> dict:
    """Validate input parameters and prepare job configuration."""
    
    # Basic validation
    if not jdbc_url.startswith('jdbc:'):
        raise ValueError("Invalid JDBC URL format")
    
    if not sql_statement.strip():
        raise ValueError("SQL statement cannot be empty")
    
    if not output_path:
        raise ValueError("Output path cannot be empty")
    
    return {
        "jdbc_url": jdbc_url,
        "sql_statement": sql_statement,
        "output_path": output_path,
        "validated": True
    }


@task
def prepare_spark_job_config(
    jdbc_url: str,
    sql_statement: str,
    output_path: str,
    spark_image: str = "myregistry/spark:3.4.0-python3"
) -> dict:
    """Prepare Kubernetes SparkApplication configuration."""
    
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "generateName": "prefect-synapse-copy-",
            "namespace": "default",
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": spark_image,
            "imagePullPolicy": "Always",
            "mainApplicationFile": "local:///opt/spark/work-dir/scripts/run_copy.py",
            "arguments": [
                "--jdbc-url", jdbc_url,
                "--sql", sql_statement,
                "--output-path", output_path,
                "--username-secret", "SYNAPSE_USERNAME",
                "--password-secret", "SYNAPSE_PASSWORD",
                "--secret-key", "OSS_ACCESS_KEY",
                "--secret-secret", "OSS_SECRET_KEY",
            ],
            "sparkVersion": "3.4.0",
            "restartPolicy": {"type": "Never"},
            "driver": {
                "cores": 1,
                "memory": "1g",
                "serviceAccount": "spark-driver",
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


@task
def submit_spark_job(spark_config: dict) -> str:
    """Submit Spark job to Kubernetes and return job name."""
    
    # Create Kubernetes job infrastructure
    k8s_job = KubernetesJobInfra(
        namespace="default",
        image="bitnami/kubectl:latest",
        command=["kubectl", "apply", "-f", "-"],
        customizations=[
            {"op": "add", "path": "/spec/template/spec/restartPolicy", "value": "Never"}
        ]
    )
    
    # Convert config to YAML and submit
    import yaml
    config_yaml = yaml.dump(spark_config)
    
    # This is a simplified example - in practice you'd use the Kubernetes API
    # or a proper Spark operator client
    job_name = spark_config["metadata"].get("generateName", "spark-job") + "12345"
    
    print(f"Submitted Spark job: {job_name}")
    return job_name


@flow(name="synapse-to-parquet-copy")
def copy_synapse_to_parquet(
    jdbc_url: str,
    sql_statement: str,
    output_path: str,
    spark_image: Optional[str] = None
) -> str:
    """
    Main Prefect flow for copying data from Synapse to Parquet.
    
    Args:
        jdbc_url: JDBC connection string for Synapse database
        sql_statement: SQL query to execute for data extraction
        output_path: Target path for Parquet output (e.g., s3://bucket/path/)
        spark_image: Docker image for Spark jobs (optional)
    
    Returns:
        Job name of the submitted Spark application
    """
    
    # Validate input parameters
    validated_params = validate_parameters(jdbc_url, sql_statement, output_path)
    
    # Prepare Spark job configuration
    spark_config = prepare_spark_job_config(
        jdbc_url=validated_params["jdbc_url"],
        sql_statement=validated_params["sql_statement"],
        output_path=validated_params["output_path"],
        spark_image=spark_image or "myregistry/spark:3.4.0-python3"
    )
    
    # Submit the job
    job_name = submit_spark_job(spark_config)
    
    return job_name


# Example usage and deployment
if __name__ == "__main__":
    # Example invocation
    result = copy_synapse_to_parquet(
        jdbc_url="jdbc:sqlserver://myworkspace.sql.azuresynapse.net:1433;database=salesdb",
        sql_statement="SELECT * FROM dbo.accounts WHERE created_date >= '2025-01-01'",
        output_path="s3://data-lake/raw/accounts/2025/06/07/"
    )
    
    print(f"Flow completed. Spark job: {result}")
