import pytest
import os
import sys
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Add the parent directory to sys.path to allow importing from 'aggregator'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from aggregator.database import Base, Stats

# We use an in-memory SQLite for some tests or a separate test DB.
# However, to properly test asyncpg and ON CONFLICT DO NOTHING, 
# it's best to test against the real Postgres instance exposed via Compose.
# For these integration tests, we'll assume the Compose stack is running.
# In a real CI/CD, we'd spin up Testcontainers.

BASE_URL = os.getenv("TEST_BASE_URL", "http://localhost:8080")

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

# Note: We will write mostly black-box integration tests using the HTTP API
# since the requirements emphasize testing the behaviors (dedup, stats).
