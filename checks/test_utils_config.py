"""Tests for utils_config.py."""
import pytest
import os
import tempfile
from unittest.mock import patch, mock_open

# Test for load_env()
def test_load_env_basic():
    """Test loading basic env file."""
    env_content = """RUN_NAME=test_run
CATEGORY=customer
S3_BUCKET=s3://test-bucket
PCDS_USR=testuser
PCDS_PWD=testpass
"""

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.env') as f:
        f.write(env_content)
        temp_file = f.name

    try:
        from utils_config import load_env
        result = load_env(temp_file)

        assert result['RUN_NAME'] == 'test_run'
        assert result['CATEGORY'] == 'customer'
        assert result['S3_BUCKET'] == 's3://test-bucket'
        assert result['PCDS_USR'] == 'testuser'
        assert result['PCDS_PWD'] == 'testpass'

        # Verify env vars set
        assert os.environ['RUN_NAME'] == 'test_run'
        assert os.environ['CATEGORY'] == 'customer'
    finally:
        os.unlink(temp_file)


def test_load_env_with_comments_and_blank_lines():
    """Test loading env file with comments and blank lines."""
    env_content = """# This is a comment
RUN_NAME=test_run

# Another comment
CATEGORY=customer
"""

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.env') as f:
        f.write(env_content)
        temp_file = f.name

    try:
        from utils_config import load_env
        result = load_env(temp_file)

        assert result['RUN_NAME'] == 'test_run'
        assert result['CATEGORY'] == 'customer'
        assert len(result) == 2  # Only two valid entries
    finally:
        os.unlink(temp_file)


def test_load_env_with_whitespace():
    """Test loading env file with whitespace around values."""
    env_content = """RUN_NAME = test_run
CATEGORY=  customer
S3_BUCKET=s3://test-bucket
"""

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.env') as f:
        f.write(env_content)
        temp_file = f.name

    try:
        from utils_config import load_env
        result = load_env(temp_file)

        # Verify whitespace stripped
        assert result['RUN_NAME'] == 'test_run'
        assert result['CATEGORY'] == 'customer'
        assert result['S3_BUCKET'] == 's3://test-bucket'
    finally:
        os.unlink(temp_file)


def test_load_env_file_not_exists():
    """Test loading non-existent file raises error."""
    from utils_config import load_env

    with pytest.raises(FileNotFoundError):
        load_env('nonexistent_file.env')


def test_load_env_malformed_lines():
    """Test loading env file with malformed lines (no '=')."""
    env_content = """RUN_NAME=test_run
INVALID_LINE_NO_EQUALS
CATEGORY=customer
"""

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.env') as f:
        f.write(env_content)
        temp_file = f.name

    try:
        from utils_config import load_env
        result = load_env(temp_file)

        # Should skip malformed line
        assert result['RUN_NAME'] == 'test_run'
        assert result['CATEGORY'] == 'customer'
        assert 'INVALID_LINE_NO_EQUALS' not in result
    finally:
        os.unlink(temp_file)
