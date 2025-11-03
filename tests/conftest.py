"""
Pytest configuration and fixtures for orca-hls-utils tests.
"""

import pytest


@pytest.fixture
def default_stream_base():
    """Default stream base URL for tests (rpi_orcasound_lab)."""
    return (
        "https://s3-us-west-2.amazonaws.com/audio-orcasound-net/"
        "rpi_orcasound_lab"
    )


@pytest.fixture
def secondary_stream_base():
    """Secondary stream base URL for tests (rpi_north_sjc)."""
    return (
        "https://s3-us-west-2.amazonaws.com/audio-orcasound-net/rpi_north_sjc"
    )
