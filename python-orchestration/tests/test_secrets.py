"""
Tests for the secrets utility module.
"""

import os
import pytest
from unittest.mock import patch, mock_open

from utils.secrets import get_secret, require_secret, SecretNotFoundError


class TestSecretsUtility:
    """Test cases for secrets management utilities."""

    def test_get_secret_from_environment(self):
        """Test retrieving secret from environment variables."""
        with patch.dict(os.environ, {'TEST_SECRET': 'test_value'}):
            result = get_secret('TEST_SECRET')
            assert result == 'test_value'

    def test_get_secret_not_found(self):
        """Test behavior when secret is not found."""
        result = get_secret('NONEXISTENT_SECRET')
        assert result is None

    def test_get_secret_from_k8s_file(self):
        """Test retrieving secret from Kubernetes mounted file."""
        mock_file_content = "k8s_secret_value"
        with patch('os.path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data=mock_file_content)):
            result = get_secret('k8s_secret')
            assert result == 'k8s_secret_value'

    def test_require_secret_success(self):
        """Test require_secret when secret exists."""
        with patch.dict(os.environ, {'REQUIRED_SECRET': 'required_value'}):
            result = require_secret('REQUIRED_SECRET')
            assert result == 'required_value'

    def test_require_secret_not_found(self):
        """Test require_secret raises exception when secret not found."""
        with pytest.raises(SecretNotFoundError):
            require_secret('MISSING_REQUIRED_SECRET')

    @patch('utils.secrets._get_k8s_secret')
    @patch('utils.secrets._get_vault_secret')
    @patch('utils.secrets._get_aws_secret')
    def test_secret_source_priority(self, mock_aws, mock_vault, mock_k8s):
        """Test that environment variables take priority over other sources."""
        # Setup mocks
        mock_k8s.return_value = 'k8s_value'
        mock_vault.return_value = 'vault_value'
        mock_aws.return_value = 'aws_value'
        
        # Environment should take priority
        with patch.dict(os.environ, {'PRIORITY_TEST': 'env_value'}):
            result = get_secret('PRIORITY_TEST')
            assert result == 'env_value'
            
            # Other sources should not be called
            mock_k8s.assert_not_called()
            mock_vault.assert_not_called()
            mock_aws.assert_not_called()
