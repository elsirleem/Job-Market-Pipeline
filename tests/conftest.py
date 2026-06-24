"""Shared pytest fixtures. The Spark session is session-scoped (expensive to build)."""
from __future__ import annotations

import pytest

from jobpipe.common.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests")
    yield s
    s.stop()
