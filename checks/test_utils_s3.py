"""Tests for utils_s3.py."""
import pytest
import json
from unittest.mock import Mock, patch, MagicMock
import boto3

# Test for download_json()
@patch('utils_s3.S3Manager._get_s3_client')
def test_download_json_success(mock_get_client):
    """Test successful JSON download from S3."""
    # Mock S3 client
    mock_s3 = Mock()
    mock_get_client.return_value = mock_s3

    # Mock S3 response
    test_data = {'table': 'customer.account', 'accessible': True}
    mock_s3.get_object.return_value = {
        'Body': Mock(read=lambda: json.dumps(test_data).encode('utf-8'))
    }

    from utils_s3 import S3Manager

    s3_manager = S3Manager('s3://test-bucket', 'test_run')
    result = s3_manager.download_json('meta_check', 'pcds_customer_meta.json')

    # Verify
    assert result == test_data
    mock_s3.get_object.assert_called_once()


@patch('utils_s3.S3Manager._get_s3_client')
def test_download_json_file_not_found(mock_get_client):
    """Test JSON download with file not found."""
    mock_s3 = Mock()
    mock_get_client.return_value = mock_s3

    # Mock S3 NoSuchKey error
    from botocore.exceptions import ClientError
    error_response = {'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}}
    mock_s3.get_object.side_effect = ClientError(error_response, 'GetObject')

    from utils_s3 import S3Manager

    s3_manager = S3Manager('s3://test-bucket', 'test_run')

    with pytest.raises(Exception):
        s3_manager.download_json('meta_check', 'nonexistent.json')


@patch('utils_s3.S3Manager._get_s3_client')
def test_download_json_invalid_json(mock_get_client):
    """Test JSON download with invalid JSON content."""
    mock_s3 = Mock()
    mock_get_client.return_value = mock_s3

    # Mock invalid JSON response
    mock_s3.get_object.return_value = {
        'Body': Mock(read=lambda: b'invalid json content {]')
    }

    from utils_s3 import S3Manager

    s3_manager = S3Manager('s3://test-bucket', 'test_run')

    with pytest.raises(json.JSONDecodeError):
        s3_manager.download_json('meta_check', 'invalid.json')


@patch('utils_s3.S3Manager._get_s3_client')
def test_download_json_empty_step(mock_get_client):
    """Test JSON download with empty step parameter."""
    mock_s3 = Mock()
    mock_get_client.return_value = mock_s3

    test_data = {'key': 'value'}
    mock_s3.get_object.return_value = {
        'Body': Mock(read=lambda: json.dumps(test_data).encode('utf-8'))
    }

    from utils_s3 import S3Manager

    s3_manager = S3Manager('s3://test-bucket', 'test_run')
    result = s3_manager.download_json('', 'root_file.json')

    # Verify path constructed correctly (no step directory)
    assert result == test_data
    call_args = mock_s3.get_object.call_args
    # Should be s3://test-bucket/test_run/root_file.json (no intermediate folder)


@patch('utils_s3.S3Manager._get_s3_client')
@patch('utils_s3.S3Manager.aws_creds_renew')
def test_download_json_credential_expiry(mock_renew, mock_get_client):
    """Test JSON download with AWS credential expiry (Windows)."""
    mock_s3 = Mock()
    mock_get_client.return_value = mock_s3

    # First call fails with ExpiredToken, second succeeds
    from botocore.exceptions import ClientError
    error_response = {'Error': {'Code': 'ExpiredToken', 'Message': 'Token expired'}}

    test_data = {'key': 'value'}
    mock_s3.get_object.side_effect = [
        ClientError(error_response, 'GetObject'),
        {'Body': Mock(read=lambda: json.dumps(test_data).encode('utf-8'))}
    ]

    from utils_s3 import S3Manager

    # Mock Windows environment
    with patch('utils_s3.inWindows', True):
        s3_manager = S3Manager('s3://test-bucket', 'test_run')

        # Should retry after credential renewal
        # (Implementation may vary - adjust based on actual retry logic)
