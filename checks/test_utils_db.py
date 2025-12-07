"""Tests for utils_db.py."""
import pytest
from unittest.mock import Mock, patch, MagicMock
import os

# Test for get_oracle_conn()
@patch('utils_db.cx_Oracle.connect')
def test_get_oracle_conn_success(mock_connect):
    """Test successful Oracle connection."""
    # Setup environment variables
    os.environ['PCDS_USR'] = 'testuser'
    os.environ['PCDS_PWD'] = 'testpass'
    os.environ['PCDS_HOST'] = 'testhost'
    os.environ['PCDS_PORT'] = '1521'

    # Mock connection
    mock_conn = Mock()
    mock_connect.return_value = mock_conn

    from utils_db import get_oracle_conn

    result = get_oracle_conn()

    # Verify connection created
    assert result == mock_conn
    mock_connect.assert_called_once()


@patch('utils_db.cx_Oracle.connect')
def test_get_oracle_conn_missing_env_vars(mock_connect):
    """Test Oracle connection with missing env vars."""
    # Clear environment variables
    for var in ['PCDS_USR', 'PCDS_PWD', 'PCDS_HOST', 'PCDS_PORT']:
        if var in os.environ:
            del os.environ[var]

    from utils_db import get_oracle_conn

    # Should raise KeyError or connection error
    with pytest.raises((KeyError, Exception)):
        get_oracle_conn()


@patch('utils_db.cx_Oracle.connect')
def test_get_oracle_conn_invalid_credentials(mock_connect):
    """Test Oracle connection with invalid credentials."""
    os.environ['PCDS_USR'] = 'baduser'
    os.environ['PCDS_PWD'] = 'badpass'
    os.environ['PCDS_HOST'] = 'testhost'
    os.environ['PCDS_PORT'] = '1521'

    # Mock connection failure
    mock_connect.side_effect = Exception("ORA-01017: invalid username/password")

    from utils_db import get_oracle_conn

    with pytest.raises(Exception) as exc:
        get_oracle_conn()

    assert "invalid username/password" in str(exc.value)


@patch('utils_db.cx_Oracle.connect')
def test_get_oracle_conn_network_unreachable(mock_connect):
    """Test Oracle connection with network error."""
    os.environ['PCDS_USR'] = 'testuser'
    os.environ['PCDS_PWD'] = 'testpass'
    os.environ['PCDS_HOST'] = 'unreachable-host'
    os.environ['PCDS_PORT'] = '1521'

    # Mock network failure
    mock_connect.side_effect = Exception("ORA-12170: TNS:Connect timeout occurred")

    from utils_db import get_oracle_conn

    with pytest.raises(Exception) as exc:
        get_oracle_conn()

    assert "timeout" in str(exc.value).lower()
