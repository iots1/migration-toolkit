"""Shared fixtures for pytest."""
import pytest
import tempfile
import os

@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d
