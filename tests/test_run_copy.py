"""
Tests for the run_copy Spark script.
"""

import pytest
from unittest.mock import patch, MagicMock
from scripts.run_copy import parse_args, create_spark_session, copy_data


class TestRunCopy:
    """Test cases for the run_copy Spark script."""

    def test_parse_args_required_params(self):
        """Test argument parsing with required parameters."""
        test_args = [
            "--jdbc-url", "jdbc:sqlserver://test.com:1433;database=testdb",
            "--sql", "SELECT * FROM test_table",
            "--output-path", "s3://test-bucket/output/"
        ]
        
        with patch('sys.argv', ['run_copy.py'] + test_args):
            args = parse_args()
            
        assert args.jdbc_url == "jdbc:sqlserver://test.com:1433;database=testdb"
        assert args.sql == "SELECT * FROM test_table"
        assert args.output_path == "s3://test-bucket/output/"

    def test_parse_args_with_secrets(self):
        """Test argument parsing with secret parameters."""
        test_args = [
            "--jdbc-url", "jdbc:sqlserver://test.com:1433;database=testdb",
            "--sql", "SELECT * FROM test_table",
            "--output-path", "s3://test-bucket/output/",
            "--secret-key", "OSS_ACCESS_KEY",
            "--secret-secret", "OSS_SECRET_KEY",
            "--username-secret", "DB_USERNAME",
            "--password-secret", "DB_PASSWORD"
        ]
        
        with patch('sys.argv', ['run_copy.py'] + test_args):
            args = parse_args()
            
        assert args.secret_key == "OSS_ACCESS_KEY"
        assert args.secret_secret == "OSS_SECRET_KEY"
        assert args.username_secret == "DB_USERNAME"
        assert args.password_secret == "DB_PASSWORD"

    @patch('scripts.run_copy.SparkSession')
    def test_create_spark_session(self, mock_spark_session):
        """Test Spark session creation with proper configuration."""
        mock_builder = MagicMock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()
        
        session = create_spark_session("TestApp")
        
        # Verify builder was configured properly
        mock_builder.appName.assert_called_with("TestApp")
        assert mock_builder.config.call_count >= 2  # At least JDBC and S3 configs
        mock_builder.getOrCreate.assert_called_once()

    @patch('scripts.run_copy.SparkSession')
    def test_copy_data_success(self, mock_spark_session):
        """Test successful data copy operation."""
        # Setup mocks
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_spark.read.jdbc.return_value = mock_df
        
        # Execute function
        copy_data(
            spark=mock_spark,
            jdbc_url="jdbc:sqlserver://test.com:1433;database=testdb",
            sql_query="SELECT * FROM test_table",
            output_path="s3://test-bucket/output/",
            username="test_user",
            password="test_pass"
        )
        
        # Verify interactions
        mock_spark.read.jdbc.assert_called_once()
        mock_df.write.mode.assert_called_with("overwrite")
        mock_df.write.mode().parquet.assert_called_with("s3://test-bucket/output/")

    @patch('scripts.run_copy.SparkSession')
    def test_copy_data_jdbc_error(self, mock_spark_session):
        """Test error handling during JDBC read."""
        # Setup mocks to raise exception
        mock_spark = MagicMock()
        mock_spark.read.jdbc.side_effect = Exception("JDBC connection failed")
        
        # Execute and verify exception is raised
        with pytest.raises(Exception) as exc_info:
            copy_data(
                spark=mock_spark,
                jdbc_url="jdbc:sqlserver://test.com:1433;database=testdb",
                sql_query="SELECT * FROM test_table",
                output_path="s3://test-bucket/output/"
            )
        
        assert "JDBC connection failed" in str(exc_info.value)

    @patch('scripts.run_copy.get_secret')
    @patch('scripts.run_copy.create_spark_session')
    @patch('scripts.run_copy.copy_data')
    def test_main_with_secrets(self, mock_copy_data, mock_create_spark, mock_get_secret):
        """Test main function with secret retrieval."""
        # Setup mocks
        mock_get_secret.side_effect = lambda x: f"secret_value_{x}"
        mock_spark = MagicMock()
        mock_create_spark.return_value = mock_spark
        
        test_args = [
            "run_copy.py",
            "--jdbc-url", "jdbc:sqlserver://test.com:1433;database=testdb",
            "--sql", "SELECT * FROM test_table",
            "--output-path", "s3://test-bucket/output/",
            "--username-secret", "DB_USERNAME",
            "--password-secret", "DB_PASSWORD"
        ]
        
        with patch('sys.argv', test_args):
            from scripts.run_copy import main
            main()
        
        # Verify secrets were retrieved
        mock_get_secret.assert_any_call("DB_USERNAME")
        mock_get_secret.assert_any_call("DB_PASSWORD")
        
        # Verify copy_data was called with correct parameters
        mock_copy_data.assert_called_once()
        call_args = mock_copy_data.call_args
        assert call_args[1]['username'] == "secret_value_DB_USERNAME"
        assert call_args[1]['password'] == "secret_value_DB_PASSWORD"
        
        # Verify Spark session cleanup
        mock_spark.stop.assert_called_once()
