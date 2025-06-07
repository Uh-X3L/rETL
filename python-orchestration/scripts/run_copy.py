#!/usr/bin/env python3
"""
Spark job script for copying data from Synapse SQL Database to Parquet format.

This script serves as a bridge between orchestrators (Airflow/Prefect/Dagster) 
and Spark-on-Kubernetes for ETL operations.
"""

import argparse
import sys
from typing import Optional

from pyspark.sql import SparkSession
from utils.secrets import get_secret


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Copy data from Synapse SQL Database to Parquet"
    )
    parser.add_argument(
        "--jdbc-url", 
        required=True, 
        help="JDBC connection URL for Synapse database"
    )
    parser.add_argument(
        "--sql", 
        required=True, 
        help="SQL query to execute for data extraction"
    )
    parser.add_argument(
        "--output-path", 
        required=True, 
        help="Output path for Parquet files (e.g., s3://bucket/path/)"
    )
    parser.add_argument(
        "--secret-key", 
        help="Secret name for OSS access key"
    )
    parser.add_argument(
        "--secret-secret", 
        help="Secret name for OSS secret key"
    )
    parser.add_argument(
        "--username-secret", 
        help="Secret name for database username"
    )
    parser.add_argument(
        "--password-secret", 
        help="Secret name for database password"
    )
    
    return parser.parse_args()


def create_spark_session(app_name: str = "SynapseToParquet") -> SparkSession:
    """Create and configure Spark session."""
    builder = SparkSession.builder.appName(app_name)
    
    # Add JDBC driver for SQL Server/Synapse
    builder = builder.config(
        "spark.jars.packages", 
        "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8"
    )
    
    # Configure for OSS storage (S3/MinIO)
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    return builder.getOrCreate()


def copy_data(
    spark: SparkSession,
    jdbc_url: str,
    sql_query: str,
    output_path: str,
    username: Optional[str] = None,
    password: Optional[str] = None
):
    """Copy data from Synapse to Parquet format."""
    
    # Build JDBC connection properties
    connection_properties = {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    if username and password:
        connection_properties["user"] = username
        connection_properties["password"] = password
    
    try:
        # Read data from Synapse using JDBC
        print(f"Executing SQL query: {sql_query[:100]}...")
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"({sql_query}) as subquery",
            properties=connection_properties
        )
        
        print(f"Retrieved {df.count()} rows")
        
        # Write to Parquet format
        print(f"Writing to: {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        
        print("Data copy completed successfully")
        
    except Exception as e:
        print(f"Error during data copy: {str(e)}")
        raise


def main():
    """Main execution function."""
    args = parse_args()
    
    # Retrieve secrets if specified
    username = get_secret(args.username_secret) if args.username_secret else None
    password = get_secret(args.password_secret) if args.password_secret else None
    
    # Configure OSS credentials if provided
    if args.secret_key and args.secret_secret:
        import os
        os.environ["AWS_ACCESS_KEY_ID"] = get_secret(args.secret_key)
        os.environ["AWS_SECRET_ACCESS_KEY"] = get_secret(args.secret_secret)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Execute data copy
        copy_data(
            spark=spark,
            jdbc_url=args.jdbc_url,
            sql_query=args.sql,
            output_path=args.output_path,
            username=username,
            password=password
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
